// Copyright 2026 arvinsg
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! MetaNode partition manager: the route table of the metadata service
//! (01 §5 MetaNode 分区管理).
//!
//! [`MetaPartitionManager`] owns the `meta_partition` column family of the PD
//! state-machine database and mirrors [`BucketManager`](crate::bucket): raft
//! applies replicated partition commands (create) while an in-memory route
//! index serves [`route`](MetaPartitionManager::route) lookups — the
//! (bucket, routing_key) → partition mapping gateways resolve through
//! `GetRoute` (01 §5 路由服务). Partitions are routing-layer descriptors
//! only; keys never embed the partition id (03 §2).
//!
//! Partition **leader** reports arrive via `PartitionHeartbeat` and live only
//! in leader memory (Q18): they never enter raft, are rebuilt within seconds
//! after a PD failover, and are absent from snapshots.
//!
//! Design: docs/design/01-pd.md §3 (MetaPartition) / §5; docs/design/03-metanode.md §2

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::NodeId;
use rocksdb::{DB, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::bucket::NsMode;
use crate::cluster::id_record;
use crate::raft::SmError;

/// The `meta_partition` column family.
pub(crate) const META_PARTITION_CF: &str = "meta_partition";

/// The key of the id-allocation counter record inside the CF.
const COUNTER_KEY: &[u8] = b"__counter";

/// The key of the ino-tag allocation counter record inside the CF (03 §6.5).
const INO_TAG_COUNTER_KEY: &[u8] = b"__ino_tag_counter";

/// The route-ordering tag of a namespace (partitions never span namespaces,
/// 03 §2); keeps the flat and hierarchical coordinate spaces disjoint in the
/// route index.
fn ns_tag(ns: NsMode) -> u8 {
    match ns {
        NsMode::Flat => 0,
        NsMode::Hier => 1,
    }
}

/// One end of a partition interval in `(bucket_id, routing_key)` coordinates
/// (03 §2); `unbounded` denotes ±∞ (start / end of the namespace).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionBound {
    /// The bucket of the bound (meaningless when `unbounded`).
    pub bucket: u64,
    /// The routing key of the bound (flat: object key; hier: `parent_ino BE |
    /// name`), matching the metadata key layout's embedded routing key.
    pub routing_key: Vec<u8>,
    /// Whether the interval is unbounded on this side.
    pub unbounded: bool,
}

impl PartitionBound {
    /// The -∞ bound (namespace start).
    pub fn unbounded_start() -> Self {
        Self {
            bucket: 0,
            routing_key: Vec::new(),
            unbounded: true,
        }
    }

    /// The +∞ bound (namespace end).
    pub fn unbounded_end() -> Self {
        Self {
            bucket: u64::MAX,
            routing_key: Vec::new(),
            unbounded: true,
        }
    }

    /// A concrete bound.
    pub fn at(bucket: u64, routing_key: Vec<u8>) -> Self {
        Self {
            bucket,
            routing_key,
            unbounded: false,
        }
    }
}

/// The route record of one partition (01 §3 MetaPartition). Leader/stats are
/// heartbeat state and deliberately not part of this replicated record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaPartition {
    /// PD-assigned partition identity (== its raft group id; never reused).
    pub partition_id: u64,
    /// The namespace this partition serves (03 §2: 分区不跨命名空间).
    pub ns: NsMode,
    /// Inclusive interval start.
    pub start: PartitionBound,
    /// Exclusive interval end.
    pub end: PartitionBound,
    /// Raft voter node ids (×3 at creation).
    pub peers: Vec<NodeId>,
    /// Route-table epoch: bumped on membership/range changes (splits, M5b);
    /// gateways invalidate cached routes by epoch (01 §5).
    pub epoch: u64,
}

/// Whether the partition's interval contains `(bucket, routing_key)` —
/// start-inclusive, end-exclusive (03 §2).
fn interval_contains(p: &MetaPartition, bucket: u64, routing_key: &[u8]) -> bool {
    let coord = (bucket, routing_key);
    let after_start =
        p.start.unbounded || coord >= (p.start.bucket, p.start.routing_key.as_slice());
    let before_end = p.end.unbounded || coord < (p.end.bucket, p.end.routing_key.as_slice());
    after_start && before_end
}

/// Replicated partition-creation command. Peers are chosen by the proposing
/// leader (AZ-aware node selection lives in the service path), so apply is a
/// pure record write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatePartition {
    /// The namespace the new partition serves.
    pub ns: NsMode,
    /// Inclusive interval start.
    pub start: PartitionBound,
    /// Exclusive interval end.
    pub end: PartitionBound,
    /// Raft voter node ids chosen by the leader.
    pub peers: Vec<NodeId>,
}

/// Replicated partition-split command (03 §2 分裂 = 纯路由变更). Narrows the
/// parent's route interval to `[parent.start, at)` and inserts the child
/// covering `[at, parent.end)` with a fresh id; both records' epochs bump so
/// gateways invalidate cached routes (01 §5). The MetaNode side performs the
/// matching in-log split; PD only mutates the route table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitPartition {
    /// The parent partition to split.
    pub parent_id: u64,
    /// The split boundary (must be strictly inside the parent's interval).
    pub at: PartitionBound,
}

/// Replicated partition-migrate command (03 §2 迁移才动数据; 01 §5): swaps one
/// voter of a partition's replica set (`from` → `to`) and bumps its route
/// epoch. PD records the new membership; the MetaNode side drives the actual
/// openraft AddLearner → Promote → RemovePeer (data movement). Idempotent: a
/// swap already reflected in `peers` is a no-op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigratePartition {
    /// The partition whose replica set changes.
    pub partition_id: u64,
    /// The voter node being removed.
    pub from: NodeId,
    /// The voter node being added.
    pub to: NodeId,
}

/// A partition-leader report held only in PD leader memory (Q18).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderReport {
    /// The partition leader as last reported.
    pub leader: NodeId,
    /// The leader's applied index at report time.
    pub applied_index: u64,
    /// Inline-guard counters for observability (03 §4.3 统计).
    pub inline_bytes: u64,
    /// Inline object count.
    pub inline_count: u64,
    /// Total live object bytes in the partition — the split-size signal the
    /// scheduler thresholds on (01 §5). 0 from legacy MetaNodes.
    pub total_bytes: u64,
    /// Leader-local observation time (millis), for staleness checks.
    pub last_seen_millis: u64,
}

#[derive(Default)]
struct PartitionIndex {
    partitions: BTreeMap<u64, MetaPartition>,
    /// Route index keyed by `(ns_tag, start_bucket, start_key)`; the
    /// unbounded start sorts first within its namespace.
    by_start: BTreeMap<(u8, u64, Vec<u8>), u64>,
    next_id: u64,
    /// Next ino partition tag to hand a split child (03 §6.5). Tags are 16-bit
    /// and must be unique across a namespace's partitions: the bootstrap
    /// partition owns tag 0, so allocation starts at 1. Monotonic, never reused
    /// (a reused tag would collide inode ids across partitions).
    next_ino_tag: u32,
}

/// Owns the `meta_partition` column family, the route index, and the
/// leader-only leader reports (cloneable; clones share all three).
#[derive(Clone)]
pub struct MetaPartitionManager {
    db: Arc<DB>,
    index: Arc<RwLock<PartitionIndex>>,
    leaders: Arc<RwLock<BTreeMap<u64, LeaderReport>>>,
}

impl MetaPartitionManager {
    /// Creates a manager over `db` with empty indexes; call
    /// [`restore`](Self::restore) to load persisted partitions.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(PartitionIndex {
                // Tag 0 is the bootstrap partition (03 §6.5); split children
                // allocate from 1. `restore` overwrites this from the persisted
                // counter — this matches its post-restore value so a manager
                // used before any restore (tests) allocates the same tags.
                next_ino_tag: 1,
                ..PartitionIndex::default()
            })),
            leaders: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, PartitionIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, PartitionIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn read_leaders(&self) -> RwLockReadGuard<'_, BTreeMap<u64, LeaderReport>> {
        self.leaders.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_leaders(&self) -> RwLockWriteGuard<'_, BTreeMap<u64, LeaderReport>> {
        self.leaders.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn cf(&self) -> Result<&rocksdb::ColumnFamily, SmError> {
        id_record::open_cf(&self.db, META_PARTITION_CF)
    }

    /// Rebuilds the in-memory route index from the column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the column family is missing or a
    /// persisted record cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let cf = self.cf()?;
        let mut index = self.write_index();
        index.partitions.clear();
        index.by_start.clear();
        index.next_id = 1;
        index.next_ino_tag = 1;
        for kv in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = kv.map_err(crate::raft::sm_read_err)?;
            if key.as_ref() == COUNTER_KEY {
                let raw: [u8; 8] = value
                    .as_ref()
                    .try_into()
                    .map_err(|_| crate::raft::sm_corrupt("bad partition counter record"))?;
                index.next_id = u64::from_be_bytes(raw);
                continue;
            }
            if key.as_ref() == INO_TAG_COUNTER_KEY {
                let raw: [u8; 4] = value
                    .as_ref()
                    .try_into()
                    .map_err(|_| crate::raft::sm_corrupt("bad ino-tag counter record"))?;
                index.next_ino_tag = u32::from_be_bytes(raw);
                continue;
            }
            let id_bytes: [u8; 8] = key
                .as_ref()
                .try_into()
                .map_err(|_| crate::raft::sm_corrupt("bad partition key length"))?;
            let partition: MetaPartition = serde_json::from_slice(&value)
                .map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
            index
                .by_start
                .insert(start_key(&partition), partition.partition_id);
            index
                .partitions
                .insert(u64::from_be_bytes(id_bytes), partition);
        }
        Ok(())
    }

    /// Applies a partition creation: allocates the next id and records the
    /// route. There is no idempotency key by design — partition creation is
    /// admin-driven, and a retried proposal creating a second identical-range
    /// partition is rejected by the route-coverage check instead (an existing
    /// partition already covering the exact interval is returned).
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized.
    pub(crate) fn apply_create(
        &self,
        batch: &mut WriteBatch,
        cmd: &CreatePartition,
    ) -> Result<u64, SmError> {
        // Exact-range retry: a partition with identical ns/start/end already
        // exists → return it (a duplicated client retry must not fork the
        // route table into two overlapping intervals).
        if let Some(existing) = self
            .read_index()
            .partitions
            .values()
            .find(|p| p.ns == cmd.ns && p.start == cmd.start && p.end == cmd.end)
        {
            return Ok(existing.partition_id);
        }

        let mut index = self.write_index();
        let partition_id = index.next_id;
        let partition = MetaPartition {
            partition_id,
            ns: cmd.ns,
            start: cmd.start.clone(),
            end: cmd.end.clone(),
            peers: cmd.peers.clone(),
            epoch: 1,
        };
        index.next_id = index
            .next_id
            .checked_add(1)
            .ok_or_else(|| crate::raft::sm_corrupt("partition id counter overflow"))?;
        index.by_start.insert(start_key(&partition), partition_id);
        index.partitions.insert(partition_id, partition.clone());
        let cf = self.cf()?;
        let value =
            serde_json::to_vec(&partition).map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
        batch.put_cf(cf, partition_id.to_be_bytes(), value);
        batch.put_cf(cf, COUNTER_KEY, index.next_id.to_be_bytes());
        Ok(partition_id)
    }

    /// Applies a partition split (03 §2): narrows the parent to `[start, at)`
    /// and inserts a child covering `[at, end)` with a fresh id, bumping both
    /// records' epochs. Returns the child id, or `None` if the boundary is not
    /// strictly inside the parent's interval (a deterministic rejection — a
    /// replay of an already-applied split, or a stale boundary).
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized.
    pub(crate) fn apply_split(
        &self,
        batch: &mut WriteBatch,
        cmd: &SplitPartition,
    ) -> Result<Option<(u64, u32)>, SmError> {
        let mut index = self.write_index();
        let Some(parent) = index.partitions.get(&cmd.parent_id).cloned() else {
            return Ok(None); // unknown parent
        };
        // The boundary must be strictly inside the parent: after its start and
        // before its end (start-inclusive, end-exclusive, 03 §2).
        let at = (cmd.at.bucket, cmd.at.routing_key.as_slice());
        let after_start = parent.start.unbounded
            || at > (parent.start.bucket, parent.start.routing_key.as_slice());
        let before_end =
            parent.end.unbounded || at < (parent.end.bucket, parent.end.routing_key.as_slice());
        if cmd.at.unbounded || !after_start || !before_end {
            return Ok(None);
        }

        // Child takes [at, parent.end); parent narrows to [parent.start, at).
        let child_id = index.next_id;
        // Allocate the child a fresh ino tag (03 §6.5): unique across the
        // namespace so child-partition inodes never collide with any sibling's.
        let child_ino_tag = index.next_ino_tag;
        let child = MetaPartition {
            partition_id: child_id,
            ns: parent.ns,
            start: cmd.at.clone(),
            end: parent.end.clone(),
            peers: parent.peers.clone(),
            epoch: 1,
        };
        let mut narrowed = parent;
        narrowed.end = cmd.at.clone();
        narrowed.epoch += 1;

        index.next_id = index
            .next_id
            .checked_add(1)
            .ok_or_else(|| crate::raft::sm_corrupt("partition id counter overflow"))?;
        // Tags are 16-bit (INO_PARTITION_TAG_BITS); refuse to wrap past the
        // space rather than reuse a tag and corrupt inode identity.
        let next_ino_tag = index
            .next_ino_tag
            .checked_add(1)
            .filter(|t| u64::from(*t) < (1u64 << epoch_proto::consts::INO_PARTITION_TAG_BITS))
            .ok_or_else(|| crate::raft::sm_corrupt("ino partition tag space exhausted"))?;
        index.next_ino_tag = next_ino_tag;
        // The parent's start key is unchanged, so its route-index entry stays;
        // only the child adds a new start key.
        index.by_start.insert(start_key(&child), child_id);
        index.partitions.insert(cmd.parent_id, narrowed.clone());
        index.partitions.insert(child_id, child.clone());

        let cf = self.cf()?;
        let parent_value =
            serde_json::to_vec(&narrowed).map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
        let child_value =
            serde_json::to_vec(&child).map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
        batch.put_cf(cf, cmd.parent_id.to_be_bytes(), parent_value);
        batch.put_cf(cf, child_id.to_be_bytes(), child_value);
        batch.put_cf(cf, COUNTER_KEY, index.next_id.to_be_bytes());
        batch.put_cf(cf, INO_TAG_COUNTER_KEY, index.next_ino_tag.to_be_bytes());
        Ok(Some((child_id, child_ino_tag)))
    }

    /// Applies a partition migrate (03 §2): swaps `from` → `to` in the
    /// partition's voter set and bumps its epoch. Returns the new voter set, or
    /// `None` if the partition is unknown or `to` is already a voter and `from`
    /// already absent (a fully-applied replay). Rejects a swap that would empty
    /// the voter set.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the record cannot be serialized.
    pub(crate) fn apply_migrate(
        &self,
        batch: &mut WriteBatch,
        cmd: &MigratePartition,
    ) -> Result<Option<Vec<NodeId>>, SmError> {
        let mut index = self.write_index();
        let Some(mut partition) = index.partitions.get(&cmd.partition_id).cloned() else {
            return Ok(None);
        };
        let has_to = partition.peers.contains(&cmd.to);
        let has_from = partition.peers.contains(&cmd.from);
        if has_to && !has_from {
            return Ok(None); // already swapped (idempotent replay)
        }
        partition.peers.retain(|&p| p != cmd.from);
        if !has_to {
            partition.peers.push(cmd.to);
        }
        if partition.peers.is_empty() {
            return Ok(None); // never empty the voter set
        }
        partition.epoch += 1;

        let peers = partition.peers.clone();
        index.partitions.insert(cmd.partition_id, partition.clone());
        let cf = self.cf()?;
        let value =
            serde_json::to_vec(&partition).map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
        batch.put_cf(cf, cmd.partition_id.to_be_bytes(), value);
        Ok(Some(peers))
    }

    /// The partition covering `(bucket, routing_key)` in `ns` — the `GetRoute`
    /// lookup (01 §5).
    #[must_use]
    pub fn route(&self, ns: NsMode, bucket: u64, routing_key: &[u8]) -> Option<MetaPartition> {
        let index = self.read_index();
        // The covering partition's start is the greatest start ≤ the
        // coordinate within the namespace; verify the end bound.
        let candidate = index
            .by_start
            .range(..=(ns_tag(ns), bucket, routing_key.to_vec()))
            .next_back()?;
        let partition = index.partitions.get(candidate.1)?;
        interval_contains(partition, bucket, routing_key).then(|| partition.clone())
    }

    /// The partition with this id, if present.
    #[must_use]
    pub fn get(&self, partition_id: u64) -> Option<MetaPartition> {
        self.read_index().partitions.get(&partition_id).cloned()
    }

    /// Every partition, in id order.
    #[must_use]
    pub fn list(&self) -> Vec<MetaPartition> {
        self.read_index().partitions.values().cloned().collect()
    }

    /// Every partition whose voter set contains `node_id` (heartbeat
    /// reconciliation input: which groups this node *should* host).
    #[must_use]
    pub fn partitions_on(&self, node_id: NodeId) -> Vec<MetaPartition> {
        self.read_index()
            .partitions
            .values()
            .filter(|p| p.peers.contains(&node_id))
            .cloned()
            .collect()
    }

    /// Records a partition-leader report (leader-only, non-replicated, Q18).
    pub fn record_leader(&self, partition_id: u64, report: LeaderReport) {
        self.write_leaders().insert(partition_id, report);
    }

    /// The last leader report of a partition, if any.
    #[must_use]
    pub fn leader(&self, partition_id: u64) -> Option<LeaderReport> {
        self.read_leaders().get(&partition_id).cloned()
    }

    /// A bulk snapshot of every partition's last leader report (08 §5 console
    /// read): the metadata-node page renders leader distribution + partition
    /// sizes without a call per partition. `(partition_id, report)` in
    /// partition-id order; a partition with no report yet is absent.
    #[must_use]
    pub fn leaders_snapshot(&self) -> Vec<(u64, LeaderReport)> {
        let mut out: Vec<(u64, LeaderReport)> = self
            .read_leaders()
            .iter()
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        out.sort_by_key(|(id, _)| *id);
        out
    }

    /// The route-table contents for snapshots: partitions, the next partition
    /// id, and the next ino tag (both counters must survive a snapshot install
    /// so ids and tags stay monotonic and never reused after a restore).
    pub(crate) fn snapshot_view(&self) -> (Vec<MetaPartition>, u64, u32) {
        let index = self.read_index();
        (
            index.partitions.values().cloned().collect(),
            index.next_id,
            index.next_ino_tag,
        )
    }

    /// Replaces the full route table from an installed snapshot: stages the
    /// record rewrite into `batch` and rebuilds the route index. The caller
    /// flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        partitions: Vec<MetaPartition>,
        next_id: u64,
        next_ino_tag: u32,
    ) -> Result<(), SmError> {
        let cf = self.cf()?;
        // Clear existing records (all keys except nothing — the CF holds only
        // records + the counter).
        for kv in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, _) = kv.map_err(crate::raft::sm_read_err)?;
            batch.delete_cf(cf, key);
        }
        for partition in &partitions {
            let value = serde_json::to_vec(partition)
                .map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
            batch.put_cf(cf, partition.partition_id.to_be_bytes(), value);
        }
        batch.put_cf(cf, COUNTER_KEY, next_id.to_be_bytes());
        batch.put_cf(cf, INO_TAG_COUNTER_KEY, next_ino_tag.to_be_bytes());

        let mut index = self.write_index();
        index.partitions = partitions
            .into_iter()
            .map(|p| (p.partition_id, p))
            .collect();
        index.by_start = index
            .partitions
            .values()
            .map(|p| (start_key(p), p.partition_id))
            .collect();
        index.next_id = next_id;
        index.next_ino_tag = next_ino_tag;
        Ok(())
    }
}

/// The route-index key of a partition: its start bound, with the unbounded
/// start sorting first within the namespace.
fn start_key(p: &MetaPartition) -> (u8, u64, Vec<u8>) {
    (
        ns_tag(p.ns),
        if p.start.unbounded { 0 } else { p.start.bucket },
        if p.start.unbounded {
            Vec::new()
        } else {
            p.start.routing_key.clone()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_manager() -> (tempfile::TempDir, MetaPartitionManager) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[META_PARTITION_CF],
        )
        .expect("open db");
        (dir, MetaPartitionManager::new(Arc::new(db)))
    }

    fn create(
        manager: &MetaPartitionManager,
        ns: NsMode,
        start: PartitionBound,
        end: PartitionBound,
    ) -> u64 {
        let mut batch = WriteBatch::default();
        let id = manager
            .apply_create(
                &mut batch,
                &CreatePartition {
                    ns,
                    start,
                    end,
                    peers: vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
                },
            )
            .expect("create");
        manager.db.write(batch).expect("flush");
        id
    }

    #[test]
    fn route_resolves_covering_partition_per_namespace() {
        let (_dir, manager) = open_manager();
        let flat = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::at(10, b"img/5".to_vec()),
        );
        let _flat2 = create(
            &manager,
            NsMode::Flat,
            PartitionBound::at(10, b"img/5".to_vec()),
            PartitionBound::unbounded_end(),
        );
        create(
            &manager,
            NsMode::Hier,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );

        assert_eq!(
            manager
                .route(NsMode::Flat, 10, b"img/49999")
                .map(|p| p.partition_id),
            Some(flat)
        );
        assert_eq!(
            manager
                .route(NsMode::Flat, 10, b"img/5")
                .map(|p| p.partition_id),
            Some(_flat2),
            "start bound is inclusive"
        );
        assert_eq!(
            manager
                .route(NsMode::Flat, 99, b"zzz")
                .map(|p| p.partition_id),
            Some(_flat2)
        );
        // The hier namespace has its own coordinate space.
        let hier = manager
            .route(NsMode::Hier, 10, b"img/5")
            .expect("hier route");
        assert_eq!(hier.ns, NsMode::Hier);
        // No flat partition covers below its first start… except the
        // unbounded-start one covers everything down to -∞.
        assert_eq!(
            manager.route(NsMode::Flat, 1, b"a").map(|p| p.partition_id),
            Some(flat)
        );
    }

    #[test]
    fn exact_range_retry_returns_existing_partition() {
        let (_dir, manager) = open_manager();
        let first = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );
        let again = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );
        assert_eq!(first, again);
        assert_eq!(manager.list().len(), 1);
    }

    #[test]
    fn restore_and_snapshot_round_trip_the_route_table() {
        let (dir, manager) = open_manager();
        let id = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );
        let (view, next_id, next_ino_tag) = manager.snapshot_view();
        assert_eq!(view.len(), 1);
        assert_eq!(next_id, id + 1);

        let db = manager.db.clone();
        let restored = MetaPartitionManager::new(db);
        restored.restore().expect("restore");
        assert_eq!(
            restored
                .route(NsMode::Flat, 7, b"x")
                .map(|p| p.partition_id),
            Some(id)
        );

        let mut batch = WriteBatch::default();
        restored
            .stage_and_apply_snapshot(&mut batch, view, next_id, next_ino_tag)
            .expect("install");
        assert_eq!(restored.list().len(), 1);
        drop(dir);
    }

    #[test]
    fn leader_reports_are_leader_local() {
        let (_dir, manager) = open_manager();
        let id = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );
        assert!(manager.leader(id).is_none());
        manager.record_leader(
            id,
            LeaderReport {
                leader: NodeId::new(2),
                applied_index: 9,
                inline_bytes: 0,
                inline_count: 0,
                total_bytes: 0,
                last_seen_millis: 1,
            },
        );
        assert_eq!(manager.leader(id).map(|r| r.leader), Some(NodeId::new(2)));
        assert_eq!(
            manager.partitions_on(NodeId::new(2)).len(),
            1,
            "peer lookup drives heartbeat reconciliation"
        );
        assert!(manager.partitions_on(NodeId::new(99)).is_empty());

        // leaders_snapshot returns the same report the single-key `leader` does,
        // in partition-id order (the console's bulk read, 08 §5).
        let snap = manager.leaders_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].0, id);
        assert_eq!(snap[0].1.leader, NodeId::new(2));
        assert_eq!(snap[0].1.applied_index, 9);
    }

    #[test]
    fn split_narrows_parent_inserts_child_and_bumps_epochs() {
        let (_dir, manager) = open_manager();
        let parent = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );

        let mut batch = WriteBatch::default();
        let (child, child_ino_tag) = manager
            .apply_split(
                &mut batch,
                &SplitPartition {
                    parent_id: parent,
                    at: PartitionBound::at(5, b"m".to_vec()),
                },
            )
            .expect("split")
            .expect("boundary inside parent");
        manager.db.write(batch).expect("flush");
        assert_eq!(child_ino_tag, 1, "first split child gets ino tag 1");

        // Parent narrowed to [-inf, (5,"m")), epoch bumped; child covers
        // [(5,"m"), +inf).
        let p = manager.get(parent).expect("parent");
        assert!(p.start.unbounded);
        assert_eq!(p.end, PartitionBound::at(5, b"m".to_vec()));
        assert_eq!(p.epoch, 2, "parent epoch bumped on split");
        let c = manager.get(child).expect("child");
        assert_eq!(c.start, PartitionBound::at(5, b"m".to_vec()));
        assert!(c.end.unbounded);
        assert_eq!(c.peers, p.peers, "child inherits the parent replica set");

        // Routing now splits at the boundary (start-inclusive).
        assert_eq!(
            manager.route(NsMode::Flat, 5, b"a").map(|r| r.partition_id),
            Some(parent)
        );
        assert_eq!(
            manager.route(NsMode::Flat, 5, b"m").map(|r| r.partition_id),
            Some(child)
        );
        assert_eq!(
            manager.route(NsMode::Flat, 9, b"z").map(|r| r.partition_id),
            Some(child)
        );

        // An out-of-range or unknown-parent boundary is a deterministic reject.
        let mut batch = WriteBatch::default();
        assert!(
            manager
                .apply_split(
                    &mut batch,
                    &SplitPartition {
                        parent_id: parent,
                        at: PartitionBound::unbounded_end(),
                    },
                )
                .expect("split call")
                .is_none(),
            "unbounded boundary rejected"
        );
        assert!(
            manager
                .apply_split(
                    &mut batch,
                    &SplitPartition {
                        parent_id: 999,
                        at: PartitionBound::at(1, b"x".to_vec()),
                    },
                )
                .expect("split call")
                .is_none(),
            "unknown parent rejected"
        );
    }

    #[test]
    fn migrate_swaps_a_voter_bumps_epoch_and_is_idempotent() {
        let (_dir, manager) = open_manager();
        let id = create(
            &manager,
            NsMode::Flat,
            PartitionBound::unbounded_start(),
            PartitionBound::unbounded_end(),
        );
        // Created with voters {1,2,3}, epoch 1.
        assert_eq!(manager.get(id).expect("p").epoch, 1);

        // Swap 3 → 4.
        let mut batch = WriteBatch::default();
        let peers = manager
            .apply_migrate(
                &mut batch,
                &MigratePartition {
                    partition_id: id,
                    from: NodeId::new(3),
                    to: NodeId::new(4),
                },
            )
            .expect("migrate")
            .expect("applied");
        manager.db.write(batch).expect("flush");
        assert_eq!(peers, vec![NodeId::new(1), NodeId::new(2), NodeId::new(4)]);
        let p = manager.get(id).expect("p");
        assert_eq!(p.epoch, 2, "epoch bumped on migrate");
        assert!(!p.peers.contains(&NodeId::new(3)));
        assert!(p.peers.contains(&NodeId::new(4)));

        // Re-applying the same swap is an idempotent no-op (to present, from
        // absent).
        let mut batch = WriteBatch::default();
        assert!(
            manager
                .apply_migrate(
                    &mut batch,
                    &MigratePartition {
                        partition_id: id,
                        from: NodeId::new(3),
                        to: NodeId::new(4),
                    },
                )
                .expect("migrate replay")
                .is_none(),
            "already-applied swap is a no-op"
        );

        // Unknown partition rejects.
        let mut batch = WriteBatch::default();
        assert!(
            manager
                .apply_migrate(
                    &mut batch,
                    &MigratePartition {
                        partition_id: 999,
                        from: NodeId::new(1),
                        to: NodeId::new(2),
                    },
                )
                .expect("migrate call")
                .is_none(),
            "unknown partition rejected"
        );
    }
}
