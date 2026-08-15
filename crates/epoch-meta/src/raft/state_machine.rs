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

//! Raft state machine for one partition group, applying through the neutral
//! [`MetaStore`] interface (03 §7).
//!
//! Every committed entry batch lands as one [`MetaStore::apply`] call whose
//! last op is the group's applied record — data and apply position commit or
//! roll back together, and crash recovery replays the raft log from exactly
//! that point (03 §8). Bookkeeping (applied record / membership / current
//! snapshot) lives in the `applied` CF under a group-id prefix, beside the
//! user-data CFs of the shared engine.
//!
//! The op vocabulary ([`MetaEntry`]) starts as resolved [`StoreOp`] batches;
//! the namespace handlers (ns_flat / ns_hier, 06 §9) build those batches *at
//! apply time* inside this machine so overwritten references are captured into
//! the delete queue here, never supplied by the proposer —
//! INVARIANT(design 03 §5): 旧 slices 必须由 apply 捕获.
//!
//! §9 (raft/state_machine.rs)

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::guard::PartitionGuard;
use crate::ns_flat::{self, MetaResponse};
use crate::partition::PartitionRange;
use crate::raft::{NodeId, SmError, sm_corrupt, sm_read_err, sm_write_err};
use crate::ref_extractor::RefExtractor;
use crate::store::{MetaCf, MetaStore, MetaStoreError, StoreOp};

/// Raft application entry: one resolved mutation batch (03 §7 StoreOp).
///
/// Namespace ops (Put / Delete / CompleteMultipart / …) expand to such batches
/// at apply time; until they land (M5a PR sequence), tests and bootstrap use
/// this direct form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetaEntry {
    /// Apply the batch verbatim (data ops only; the machine appends the
    /// applied record itself). Bootstrap/tests only — namespace ops use the
    /// typed variants so their batches are built *at apply time*.
    StoreOps(Vec<StoreOp>),
    /// A flat-namespace op (`ns_flat`, 03 §5): put/delete/list semantics with
    /// apply-time old-slices capture (INVARIANT 03 §5).
    Flat(crate::ns_flat::FlatOp),
    /// A hierarchical-namespace op (`ns_hier`, 03 §6): file/dir semantics with
    /// apply-time old-slices capture and partition-local ino minting.
    Hier(crate::ns_hier::HierOp),
    /// Split this (parent) partition at a routing-key boundary (03 §2): a
    /// deterministic parent-log op. Every replica applies it at the same log
    /// index, narrowing the parent's live range to the left half and recording
    /// a pending child for the right half; the GroupManager then derives the
    /// child group asynchronously (child data already lives in the shared
    /// engine — keys carry no partition id, so a split moves no bytes).
    Split(SplitOp),
}

/// A deterministic partition split (03 §2). All fields are chosen by PD before
/// proposal, so `apply` reads no clock/random and every replica records an
/// identical child (§8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SplitOp {
    /// The right-child group id (== its partition id; PD-assigned, never
    /// reused).
    pub child_group: u64,
    /// The split boundary `(bucket, routing_key)`: the parent keeps
    /// `[start, at)`, the child takes `[at, end)`.
    pub at_bucket: u64,
    /// The boundary routing key bytes.
    pub at_routing_key: Vec<u8>,
    /// The child group's raft voters `(node_id, addr)` (same replica set as the
    /// parent by default; PD may rebalance later via migrate).
    pub child_peers: Vec<(u64, String)>,
    /// The node that bootstraps the child group's membership exactly once
    /// (03 §8; the others create it un-initialized and receive membership by
    /// replication).
    pub bootstrap_node: u64,
    /// The child's ino partition tag (03 §6.5: 分裂只在 ino 分片内部进行; hier
    /// only, ignored for flat).
    pub child_ino_tag: u32,
}

/// The persisted pending-child record (parent's Applied CF, `pending_split`
/// key) the GroupManager reconcile drains to derive the child group. Cleared
/// once the child is running.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingSplit {
    /// The child group id to derive.
    pub child_group: u64,
    /// The child's routing range.
    pub child_range: PartitionRange,
    /// The child group's voters.
    pub child_peers: Vec<(u64, String)>,
    /// The bootstrap node id.
    pub bootstrap_node: u64,
}

openraft::declare_raft_types! {
    /// Application type binding for every partition raft group.
    pub MetaTypeConfig:
        D = MetaEntry,
        R = MetaResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<Self>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
}

/// Applied-CF bookkeeping key for `group`: `group(u64 BE) | suffix`.
fn bookkeeping_key(group: u64, suffix: u8) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[..8].copy_from_slice(&group.to_be_bytes());
    key[8] = suffix;
    key
}

const APPLIED_SUFFIX: u8 = b'a';
const MEMBERSHIP_SUFFIX: u8 = b'm';
const SNAPSHOT_SUFFIX: u8 = b's';
const DELQ_SEQ_SUFFIX: u8 = b'd';
const GUARD_SUFFIX: u8 = b'i';
const INO_SEQ_SUFFIX: u8 = b'n';
const RANGE_SUFFIX: u8 = b'r';
const PENDING_SPLIT_SUFFIX: u8 = b'p';
const INO_TAG_SUFFIX: u8 = b't';

fn applied_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, APPLIED_SUFFIX)
}

fn membership_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, MEMBERSHIP_SUFFIX)
}

fn snapshot_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, SNAPSHOT_SUFFIX)
}

fn delq_seq_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, DELQ_SEQ_SUFFIX)
}

/// Counts the pending delq entries in `range` (state-machine construction /
/// snapshot re-seed of the depth gauge). Paged so a large queue never loads
/// wholesale. Design: draft/design/08-web-console.md §5.1.
fn count_delq_entries(
    store: &dyn MetaStore,
    range: &PartitionRange,
) -> Result<u64, MetaStoreError> {
    let mut depth = 0u64;
    for kr in range
        .key_ranges()
        .into_iter()
        .filter(|r| r.cf == MetaCf::Delq)
    {
        let mut cursor = kr.start.clone();
        loop {
            let page = store.scan(MetaCf::Delq, &cursor, &kr.end, 4096)?;
            if page.is_empty() {
                break;
            }
            depth += page.len() as u64;
            cursor = page
                .last()
                .map(|(k, _)| {
                    let mut next = k.clone();
                    next.push(0);
                    next
                })
                .unwrap_or_else(|| kr.end.clone());
        }
    }
    Ok(depth)
}

/// The net change in delete-queue depth from an apply batch: delq puts (new
/// pending entries) minus delq deletes (dequeued by the deleter). delq keys are
/// unique and monotonic, so each put is one new entry and each delete removes
/// one — counting ops is exact without reading the store.
fn delq_delta(ops: &[StoreOp]) -> i64 {
    ops.iter()
        .filter(|op| op.cf == MetaCf::Delq)
        .map(|op| if op.value.is_some() { 1 } else { -1 })
        .sum()
}

fn guard_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, GUARD_SUFFIX)
}

/// The per-partition ino counter key (03 §6.5: 分区内单调计数器). Persisted in
/// the apply batch that mints from it, so replay re-mints identical inos.
fn ino_seq_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, INO_SEQ_SUFFIX)
}

/// The live routing range of a partition (03 §2). Absent until the first
/// split; a split narrows the parent by persisting its new (left) range here,
/// and the state machine loads it on restart in preference to the bootstrap
/// seed — so a split survives a crash.
fn range_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, RANGE_SUFFIX)
}

/// The pending-child record of a split (03 §2: 派生子组). Written by apply of
/// the parent's `Split` op; drained by the GroupManager reconcile, which
/// derives the child group over the shared engine. Persisted (not just
/// signalled) so a crash between apply and derivation re-drives on restart.
fn pending_split_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, PENDING_SPLIT_SUFFIX)
}

/// The persisted ino partition tag of a group (03 §6.5). Absent → tag 0 (the
/// bootstrap partition, consistent with `ROOT_INO`); a split writes the child's
/// tag so its minted inos never collide with the parent's shard.
fn ino_tag_key(group: u64) -> [u8; 9] {
    bookkeeping_key(group, INO_TAG_SUFFIX)
}

/// Reads the pending-child record of `group`, if a split is awaiting child
/// derivation (03 §2). The GroupManager reconcile drains this.
///
/// # Errors
///
/// Returns a store error if the record cannot be read or decoded.
pub(crate) fn read_pending_split(
    store: &dyn MetaStore,
    group: u64,
) -> Result<Option<PendingSplit>, MetaStoreError> {
    match store.get(MetaCf::Applied, &pending_split_key(group))? {
        Some(bytes) => Ok(Some(
            serde_json::from_slice(&bytes)
                .map_err(|e| MetaStoreError::ValueCodec(e.to_string()))?,
        )),
        None => Ok(None),
    }
}

/// Clears the pending-child record of `group` once its child is running
/// (03 §2). Idempotent — clearing an absent record is a no-op.
///
/// # Errors
///
/// Returns a store error if the delete cannot be applied.
pub(crate) fn clear_pending_split(store: &dyn MetaStore, group: u64) -> Result<(), MetaStoreError> {
    store.apply(&[StoreOp::delete(MetaCf::Applied, pending_split_key(group))])
}

/// The 24-byte guard record: `inline_bytes | inline_count | total_bytes` BE.
fn encode_guard_record(inline: u64, count: u64, total: u64) -> [u8; 24] {
    let mut out = [0u8; 24];
    out[..8].copy_from_slice(&inline.to_be_bytes());
    out[8..16].copy_from_slice(&count.to_be_bytes());
    out[16..].copy_from_slice(&total.to_be_bytes());
    out
}

/// Parses the 24-byte guard record (absent/corrupt → zeros, a fresh guard).
fn decode_guard_record(bytes: Option<Vec<u8>>) -> (u64, u64, u64) {
    let Some(bytes) = bytes else { return (0, 0, 0) };
    let Ok(raw) = <[u8; 24]>::try_from(bytes.as_slice()) else {
        return (0, 0, 0);
    };
    (
        u64::from_be_bytes(raw[..8].try_into().expect("24-byte record")),
        u64::from_be_bytes(raw[8..16].try_into().expect("24-byte record")),
        u64::from_be_bytes(raw[16..].try_into().expect("24-byte record")),
    )
}

/// The serialized state-machine content carried inside a snapshot: the apply
/// position, the membership, the delq sequence (its counter must stay ahead
/// of every exported delq entry, or a reinstalled replica would recycle
/// sequence numbers and overwrite pending deletes), and every user-data entry
/// inside the partition range (per-CF contiguous segments + ttl bucket span,
/// 03 §4.1).
#[derive(Debug, Serialize, Deserialize)]
struct MetaSnapshotData {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    delq_seq: u64,
    #[serde(default)]
    ino_seq: u64,
    inline_bytes: u64,
    inline_count: u64,
    total_bytes: u64,
    entries: Vec<(MetaCf, Vec<u8>, Vec<u8>)>,
}

/// The persisted current-snapshot record (openraft metadata + raw bytes).
#[derive(Debug, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

/// Monotonic local counter keeping snapshot ids unique per rebuild.
static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Reads one bookkeeping record from the `applied` CF.
fn read_bookkeeping<T: serde::de::DeserializeOwned>(
    store: &dyn MetaStore,
    key: [u8; 9],
) -> Result<Option<T>, SmError> {
    match store.get(MetaCf::Applied, &key).map_err(sm_read_err)? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(sm_read_err)?)),
        None => Ok(None),
    }
}

/// Raft state machine of one partition group.
///
/// Clone-cheap to construct (the engine is shared); openraft owns the instance
/// once the group starts. Reads for the service path go through the shared
/// store directly (linearity via ReadIndex, 03 §5), not through this machine.
pub struct MetaStateMachine {
    store: Arc<dyn MetaStore>,
    extractor: Arc<dyn RefExtractor>,
    group: u64,
    range: PartitionRange,
    /// The partition's inline guard: the live counters the service checks
    /// inline PUTs against; persisted per apply batch (03 §4.3).
    guard: Arc<PartitionGuard>,
    /// The partition's delete-event sequence (03 §8: 分区状态机内单调计数器).
    /// Persisted in the same apply batch as the delq entries it numbered, so
    /// a crash loses counter and entries together and replay re-allocates the
    /// identical sequence — replay-safe by construction.
    delq_next: u64,
    /// The partition's ino counter (03 §6.5: 分区内单调计数器, hier namespace
    /// only). Same persist-in-the-minting-batch discipline as `delq_next`, so
    /// replay re-mints byte-identical inos. Combined with the partition tag in
    /// the ino's high bits so a split never collides sibling inos.
    ino_next: u64,
    /// The partition's ino tag (03 §6.5: 分区标识高位). Tag 0 for the bootstrap
    /// partition; a split assigns the child a distinct tag so its inos never
    /// collide with the parent's shard.
    ino_tag: u32,
}

impl MetaStateMachine {
    /// Builds the machine for `group` over the shared `store`, exporting /
    /// installing exactly `range` on snapshot operations. `guard` is the
    /// partition's inline-guard cell shared with the service path.
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError`] if the persisted bookkeeping cannot be read.
    pub fn new(
        store: Arc<dyn MetaStore>,
        extractor: Arc<dyn RefExtractor>,
        group: u64,
        range: PartitionRange,
        guard: Arc<PartitionGuard>,
    ) -> Result<Self, MetaStoreError> {
        let delq_next = store
            .get(MetaCf::Applied, &delq_seq_key(group))?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        let (inline, count, total) =
            decode_guard_record(store.get(MetaCf::Applied, &guard_key(group))?);
        guard.load(inline, count, total);
        let ino_next = store
            .get(MetaCf::Applied, &ino_seq_key(group))?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        // A prior split narrows the live range and persists it; prefer that
        // over the bootstrap seed so a split survives restart (03 §2).
        let range = match store.get(MetaCf::Applied, &range_key(group))? {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| MetaStoreError::ValueCodec(e.to_string()))?,
            None => range,
        };
        let ino_tag = store
            .get(MetaCf::Applied, &ino_tag_key(group))?
            .and_then(|bytes| <[u8; 4]>::try_from(bytes.as_slice()).ok())
            .map(u32::from_be_bytes)
            .unwrap_or(0);
        // Seed the delete-queue depth gauge from the queue once at construction;
        // apply maintains it incrementally thereafter (08 §5.1). Derived, not
        // persisted — a restart re-scans, so the gauge is always the truth.
        guard.load_delq_depth(count_delq_entries(store.as_ref(), &range)?);
        Ok(Self {
            store,
            extractor,
            group,
            range,
            guard,
            delq_next,
            ino_next,
            ino_tag,
        })
    }

    /// The ino partition tag for this group (03 §6.5: 分区标识高位). Tag 0 for
    /// the bootstrap partition (consistent with `ROOT_INO`); a split assigns
    /// the child its own tag, loaded here from the persisted record.
    fn ino_tag(&self) -> u32 {
        self.ino_tag
    }

    /// Builds the mutation batch of a deterministic partition split (03 §2).
    /// Narrows the parent's live range to `[start, at)`, persists it plus a
    /// pending-child record (drained by the GroupManager reconcile to derive
    /// the child over the shared engine), and seeds the child's bookkeeping.
    /// Idempotent on replay: a boundary already at/left of the current range is
    /// a no-op (the parent already narrowed), and every write overwrites.
    fn split_ops(&mut self, op: &SplitOp) -> Result<Vec<StoreOp>, SmError> {
        let at = (
            epoch_proto::BucketId::new(op.at_bucket),
            op.at_routing_key.clone(),
        );
        let Some((left, right)) = self.range.split(at) else {
            // Not inside the current range: an already-applied split (replay)
            // or a stale boundary. No-op — the applied record still commits.
            return Ok(Vec::new());
        };
        let mut ops = Vec::new();
        // 1. Narrow the parent to the left half (persisted range wins on
        //    restart, so the parent stops serving/exporting the right half).
        self.range = left.clone();
        ops.push(StoreOp::put(
            MetaCf::Applied,
            range_key(self.group),
            serde_json::to_vec(&left).map_err(sm_write_err)?,
        ));
        // 2. Record the pending child for the reconcile to derive. Its data
        //    already lives in the shared engine (keys carry no partition id,
        //    03 §2), so this moves no bytes.
        let pending = PendingSplit {
            child_group: op.child_group,
            child_range: right,
            child_peers: op.child_peers.clone(),
            bootstrap_node: op.bootstrap_node,
        };
        ops.push(StoreOp::put(
            MetaCf::Applied,
            pending_split_key(self.group),
            serde_json::to_vec(&pending).map_err(sm_write_err)?,
        ));
        // 3. Seed the child's bookkeeping. Its delq/ino counters start at 0 —
        //    routing keys of the two children are disjoint, so (routing, seq)
        //    and (tag, ino) never collide across the split (03 §8/§6.5).
        ops.push(StoreOp::put(
            MetaCf::Applied,
            delq_seq_key(op.child_group),
            0u64.to_be_bytes().as_slice(),
        ));
        ops.push(StoreOp::put(
            MetaCf::Applied,
            ino_seq_key(op.child_group),
            0u64.to_be_bytes().as_slice(),
        ));
        ops.push(StoreOp::put(
            MetaCf::Applied,
            ino_tag_key(op.child_group),
            op.child_ino_tag.to_be_bytes().as_slice(),
        ));
        Ok(ops)
    }

    fn write_applied(&self, ops: &mut Vec<StoreOp>, log_id: &LogId<NodeId>) -> Result<(), SmError> {
        ops.push(StoreOp::put(
            MetaCf::Applied,
            applied_key(self.group),
            serde_json::to_vec(log_id).map_err(sm_write_err)?,
        ));
        Ok(())
    }

    /// Builds the mutation batch of one entry (03 §1: 单操作单 WriteBatch
    /// 原子) plus its caller-facing response. The applied record rides the
    /// *last* entry's batch of an apply call (appended by
    /// [`apply`](RaftStateMachine::apply)); replay is idempotent either way.
    fn entry_ops(
        &mut self,
        entry: &Entry<MetaTypeConfig>,
    ) -> Result<(Vec<StoreOp>, MetaResponse), SmError> {
        match &entry.payload {
            EntryPayload::Blank => Ok((Vec::new(), MetaResponse::None)),
            EntryPayload::Normal(MetaEntry::StoreOps(batch)) => {
                Ok((batch.clone(), MetaResponse::None))
            }
            EntryPayload::Normal(MetaEntry::Flat(op)) => {
                let apply_start = std::time::Instant::now();
                // One op = one delete-event sequence (03 §8). The counter is
                // persisted in this entry's batch, so log replay after a
                // disableWAL tail loss re-allocates the identical sequence
                // and reproduces byte-identical delq keys (idempotent).
                let seq = self.delq_next;
                self.delq_next += 1;
                let outcome = ns_flat::apply(self.store.as_ref(), self.extractor.as_ref(), op, seq)
                    .map_err(sm_write_err)?;
                let mut ops = outcome.ops;
                ops.push(StoreOp::put(
                    MetaCf::Applied,
                    delq_seq_key(self.group),
                    self.delq_next.to_be_bytes().as_slice(),
                ));
                // The inline guard rides the same atomic batch: counters are
                // accounted in memory and persisted alongside the data, so
                // restart and replay reproduce them exactly (03 §4.3).
                let (inline, count, total) = self.guard.account(outcome.delta);
                ops.push(StoreOp::put(
                    MetaCf::Applied,
                    guard_key(self.group),
                    encode_guard_record(inline, count, total).as_slice(),
                ));
                crate::metrics::record_apply("flat", apply_start.elapsed().as_secs_f64());
                Ok((ops, outcome.response))
            }
            EntryPayload::Normal(MetaEntry::Hier(op)) => {
                // Same replicated-counter discipline as the flat arm: the delq
                // sequence and the ino counter both persist in this batch, so a
                // disableWAL tail-loss replay re-allocates identical sequences /
                // inos and reproduces byte-identical keys (03 §6.5/§8).
                let seq = self.delq_next;
                self.delq_next += 1;
                let mut allocator =
                    crate::ns_hier::InoAllocator::new(self.ino_tag(), self.ino_next);
                let outcome = crate::ns_hier::apply(
                    self.store.as_ref(),
                    self.extractor.as_ref(),
                    op,
                    seq,
                    &mut allocator,
                )
                .map_err(sm_write_err)?;
                self.ino_next = allocator.next();
                let mut ops = outcome.ops;
                ops.push(StoreOp::put(
                    MetaCf::Applied,
                    delq_seq_key(self.group),
                    self.delq_next.to_be_bytes().as_slice(),
                ));
                ops.push(StoreOp::put(
                    MetaCf::Applied,
                    ino_seq_key(self.group),
                    self.ino_next.to_be_bytes().as_slice(),
                ));
                let (inline, count, total) = self.guard.account(outcome.delta);
                ops.push(StoreOp::put(
                    MetaCf::Applied,
                    guard_key(self.group),
                    encode_guard_record(inline, count, total).as_slice(),
                ));
                Ok((ops, outcome.response))
            }
            EntryPayload::Normal(MetaEntry::Split(op)) => {
                let ops = self.split_ops(op)?;
                Ok((ops, MetaResponse::None))
            }
            EntryPayload::Membership(membership) => {
                let stored = StoredMembership::new(Some(entry.log_id), membership.clone());
                Ok((
                    vec![StoreOp::put(
                        MetaCf::Applied,
                        membership_key(self.group),
                        serde_json::to_vec(&stored).map_err(sm_write_err)?,
                    )],
                    MetaResponse::None,
                ))
            }
        }
    }
}

impl RaftStateMachine<MetaTypeConfig> for MetaStateMachine {
    type SnapshotBuilder = MetaSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), SmError> {
        let applied: Option<LogId<NodeId>> =
            read_bookkeeping(self.store.as_ref(), applied_key(self.group))?;
        let membership: Option<StoredMembership<NodeId, BasicNode>> =
            read_bookkeeping(self.store.as_ref(), membership_key(self.group))?;
        Ok((applied, membership.unwrap_or_default()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetaResponse>, SmError>
    where
        I: IntoIterator<Item = Entry<MetaTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(entries.len());
        let last_log_id = entries.last().map(|e| e.log_id);
        let last_index = entries.len() - 1;
        for (index, entry) in entries.iter().enumerate() {
            let (mut ops, response) = self.entry_ops(entry)?;
            if index == last_index
                && let Some(log_id) = &last_log_id
            {
                // INVARIANT(design 03 §8): the applied record rides in the
                // same batch as the data ops — they commit or roll back
                // together, and replay of an unrecorded prefix is
                // idempotent (puts overwrite; delq seq re-allocates
                // identically).
                self.write_applied(&mut ops, log_id)?;
            }
            // One entry = one atomic batch (03 §1): each op's capture reads
            // the state produced by its predecessors, never a stale snapshot.
            self.store.apply(&ops).map_err(sm_write_err)?;
            // Maintain the delq-depth gauge from the same batch (08 §5.1): a
            // Flat/Hier op may enqueue captured slices; a deleter dequeue batch
            // removes entries. Post-apply so a failed apply leaves it unchanged.
            // A Split narrows this partition's range (moving part of the queue to
            // the child's routing space) without deleting entries, so re-seed the
            // gauge from the narrowed range rather than delta-count it.
            if matches!(entry.payload, EntryPayload::Normal(MetaEntry::Split(_))) {
                self.guard.load_delq_depth(
                    count_delq_entries(self.store.as_ref(), &self.range).map_err(sm_read_err)?,
                );
            } else {
                self.guard.account_delq(delq_delta(&ops));
            }
            // Expose the authoritative gauge to /metrics (08 §5.1): the in-memory
            // counter is the source of truth; this just mirrors it outward.
            crate::metrics::set_delq_depth(self.group, self.guard.delq_depth());
            results.push(response);
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaSnapshotBuilder {
            store: Arc::clone(&self.store),
            group: self.group,
            range: self.range.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, SmError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), SmError> {
        let data: MetaSnapshotData =
            serde_json::from_slice(&snapshot.into_inner()).map_err(sm_read_err)?;

        // Serialize the whole payload for the stored-snapshot record up front,
        // then destructure — the entry puts consume `entries` by value.
        let data_bytes = serde_json::to_vec(&data).map_err(sm_write_err)?;
        let MetaSnapshotData {
            last_applied,
            last_membership,
            delq_seq,
            ino_seq,
            inline_bytes,
            inline_count,
            total_bytes,
            entries,
        } = data;

        // Replace this partition's state wholesale: clear everything the range
        // currently holds, then write the snapshot's entries plus bookkeeping
        // in one batch (v1 collects the clear-set — see store/mod.rs note on
        // collected exports; partitions split before ranges outgrow this).
        let mut ops = Vec::new();
        for range in self.range.key_ranges() {
            let mut start = range.start.clone();
            loop {
                let page = self
                    .store
                    .scan(range.cf, &start, &range.end, 4096)
                    .map_err(sm_read_err)?;
                if page.is_empty() {
                    break;
                }
                start = page
                    .last()
                    .map(|(k, _)| {
                        let mut next = k.clone();
                        next.push(0);
                        next
                    })
                    .ok_or_else(|| sm_corrupt("scan page unexpectedly empty"))?;
                for (key, _) in page {
                    ops.push(StoreOp::delete(range.cf, key));
                }
            }
        }
        for (cf, key, value) in entries {
            ops.push(StoreOp::put(cf, key, value));
        }
        match &last_applied {
            Some(log_id) => ops.push(StoreOp::put(
                MetaCf::Applied,
                applied_key(self.group),
                serde_json::to_vec(log_id).map_err(sm_write_err)?,
            )),
            None => ops.push(StoreOp::delete(MetaCf::Applied, applied_key(self.group))),
        }
        ops.push(StoreOp::put(
            MetaCf::Applied,
            membership_key(self.group),
            serde_json::to_vec(&last_membership).map_err(sm_write_err)?,
        ));
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data_bytes,
        };
        ops.push(StoreOp::put(
            MetaCf::Applied,
            snapshot_key(self.group),
            serde_json::to_vec(&stored).map_err(sm_write_err)?,
        ));
        // Restore the delq counter with the data: it must stay ahead of every
        // exported delq entry, or future captures would recycle sequence
        // numbers and overwrite pending deletes.
        ops.push(StoreOp::put(
            MetaCf::Applied,
            delq_seq_key(self.group),
            delq_seq.to_be_bytes().as_slice(),
        ));
        // Restore the ino counter the same way (03 §6.5): it must stay ahead of
        // every minted ino, or a reinstalled replica would re-mint colliding
        // inodes.
        ops.push(StoreOp::put(
            MetaCf::Applied,
            ino_seq_key(self.group),
            ino_seq.to_be_bytes().as_slice(),
        ));
        // Restore the inline guard counters the same way (03 §4.3).
        ops.push(StoreOp::put(
            MetaCf::Applied,
            guard_key(self.group),
            encode_guard_record(inline_bytes, inline_count, total_bytes).as_slice(),
        ));
        self.store.apply(&ops).map_err(sm_write_err)?;
        self.delq_next = delq_seq;
        self.ino_next = ino_seq;
        self.guard.load(inline_bytes, inline_count, total_bytes);
        // Re-seed the delq-depth gauge from the freshly-installed queue (the
        // depth is derived, not carried in the snapshot's guard record).
        self.guard.load_delq_depth(
            count_delq_entries(self.store.as_ref(), &self.range).map_err(sm_read_err)?,
        );
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<MetaTypeConfig>>, SmError> {
        let stored: Option<StoredSnapshot> =
            read_bookkeeping(self.store.as_ref(), snapshot_key(self.group))?;
        Ok(stored.map(|stored| Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        }))
    }
}

/// Builds snapshots by exporting the partition range from the shared engine.
pub struct MetaSnapshotBuilder {
    store: Arc<dyn MetaStore>,
    group: u64,
    range: PartitionRange,
}

impl openraft::RaftSnapshotBuilder<MetaTypeConfig> for MetaSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetaTypeConfig>, SmError> {
        let last_applied: Option<LogId<NodeId>> =
            read_bookkeeping(self.store.as_ref(), applied_key(self.group))?;
        let last_membership: StoredMembership<NodeId, BasicNode> =
            read_bookkeeping(self.store.as_ref(), membership_key(self.group))?.unwrap_or_default();

        // Export the partition interval per CF, then apply the membership rule
        // (03 §2 归属校验) uniformly — the ttl CF's bucket-span bounds require
        // it, and for the contiguous CFs it is a free exactness check.
        let mut exported = self
            .store
            .snapshot(&self.range.key_ranges())
            .map_err(sm_read_err)?;
        exported
            .entries
            .retain(|(_, key, _)| self.range.contains(key));

        let delq_seq = self
            .store
            .get(MetaCf::Applied, &delq_seq_key(self.group))
            .map_err(sm_read_err)?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        let ino_seq = self
            .store
            .get(MetaCf::Applied, &ino_seq_key(self.group))
            .map_err(sm_read_err)?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
            .map(u64::from_be_bytes)
            .unwrap_or(0);
        let (inline_bytes, inline_count, total_bytes) = decode_guard_record(
            self.store
                .get(MetaCf::Applied, &guard_key(self.group))
                .map_err(sm_read_err)?,
        );

        let data = MetaSnapshotData {
            last_applied,
            last_membership: last_membership.clone(),
            delq_seq,
            ino_seq,
            inline_bytes,
            inline_count,
            total_bytes,
            entries: exported.entries,
        };
        let bytes = serde_json::to_vec(&data).map_err(sm_write_err)?;

        let snapshot_id = format!(
            "meta-snapshot-g{}-i{}-n{}",
            self.group,
            last_applied.map(|l| l.index).unwrap_or(0),
            SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        self.store
            .apply(&[StoreOp::put(
                MetaCf::Applied,
                snapshot_key(self.group),
                serde_json::to_vec(&stored).map_err(sm_write_err)?,
            )])
            .map_err(sm_write_err)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Namespace;
    use crate::store::keys::flat_key;
    use crate::store::rocks::RocksEngine;

    use epoch_proto::BucketId;
    use openraft::{LeaderId, RaftSnapshotBuilder};

    fn make_machine(dir: &tempfile::TempDir, group: u64) -> (MetaStateMachine, Arc<RocksEngine>) {
        let engine = Arc::new(RocksEngine::open(dir.path()).expect("open engine"));
        (
            MetaStateMachine::new(
                engine.clone(),
                Arc::new(crate::ref_extractor::EpochRefExtractor),
                group,
                PartitionRange::full(Namespace::Flat),
                Arc::new(PartitionGuard::new()),
            )
            .expect("new machine"),
            engine,
        )
    }

    fn ops_entry(index: u64, ops: Vec<StoreOp>) -> Entry<MetaTypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(MetaEntry::StoreOps(ops)),
        }
    }

    /// Builds a machine sharing an explicit guard so a test can read its gauges.
    fn machine_with_guard(
        dir: &tempfile::TempDir,
        group: u64,
        guard: Arc<PartitionGuard>,
    ) -> (MetaStateMachine, Arc<RocksEngine>) {
        let engine = Arc::new(RocksEngine::open(dir.path()).expect("open engine"));
        (
            MetaStateMachine::new(
                engine.clone(),
                Arc::new(crate::ref_extractor::EpochRefExtractor),
                group,
                PartitionRange::full(Namespace::Flat),
                Arc::clone(&guard),
            )
            .expect("new machine"),
            engine,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delq_depth_gauge_tracks_enqueue_and_dequeue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let guard = Arc::new(PartitionGuard::new());
        let (mut machine, _engine) = machine_with_guard(&dir, 7, Arc::clone(&guard));
        assert_eq!(guard.delq_depth(), 0, "fresh partition has an empty queue");

        // Enqueue three delq entries in one batch → depth 3.
        let delq = |seg: u8| flat_key(MetaCf::Delq, BucketId::new(1), b"k", &[seg]);
        machine
            .apply(vec![ops_entry(
                1,
                vec![
                    StoreOp::put(MetaCf::Delq, delq(0), b"e0".as_slice()),
                    StoreOp::put(MetaCf::Delq, delq(1), b"e1".as_slice()),
                    StoreOp::put(MetaCf::Delq, delq(2), b"e2".as_slice()),
                ],
            )])
            .await
            .expect("enqueue");
        assert_eq!(guard.delq_depth(), 3, "three entries enqueued");

        // Dequeue two (the deleter's batch) → depth 1.
        machine
            .apply(vec![ops_entry(
                2,
                vec![
                    StoreOp::delete(MetaCf::Delq, delq(0)),
                    StoreOp::delete(MetaCf::Delq, delq(1)),
                ],
            )])
            .await
            .expect("dequeue");
        assert_eq!(guard.delq_depth(), 1, "two entries dequeued");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delq_depth_gauge_reseeds_from_queue_on_construction() {
        let dir = tempfile::tempdir().expect("tempdir");
        // First machine enqueues two entries, then drops (simulating a restart).
        let delq = |seg: u8| flat_key(MetaCf::Delq, BucketId::new(1), b"k", &[seg]);
        {
            let guard = Arc::new(PartitionGuard::new());
            let (mut machine, _engine) = machine_with_guard(&dir, 7, guard);
            machine
                .apply(vec![ops_entry(
                    1,
                    vec![
                        StoreOp::put(MetaCf::Delq, delq(0), b"e0".as_slice()),
                        StoreOp::put(MetaCf::Delq, delq(1), b"e1".as_slice()),
                    ],
                )])
                .await
                .expect("enqueue");
        }
        // A fresh machine over the same engine re-seeds the gauge from the queue.
        let guard = Arc::new(PartitionGuard::new());
        let (_machine, _engine) = machine_with_guard(&dir, 7, Arc::clone(&guard));
        assert_eq!(
            guard.delq_depth(),
            2,
            "construction re-scans the persisted queue into the gauge"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn apply_persists_data_and_applied_record_atomically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut machine, engine) = make_machine(&dir, 7);
        let key = flat_key(MetaCf::Meta, BucketId::new(1), b"o", &[]);

        let results = machine
            .apply(vec![ops_entry(
                1,
                vec![StoreOp::put(MetaCf::Meta, key.clone(), b"head".as_slice())],
            )])
            .await
            .expect("apply");
        assert_eq!(results, vec![MetaResponse::None]);
        assert_eq!(
            engine.get(MetaCf::Meta, &key).expect("get").as_deref(),
            Some(&b"head"[..])
        );

        let (applied, membership) = machine.applied_state().await.expect("applied state");
        assert_eq!(applied.map(|l| l.index), Some(1));
        assert_eq!(membership, StoredMembership::default());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn membership_entries_are_stored_verbatim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut machine, _engine) = make_machine(&dir, 7);
        let membership = openraft::Membership::new(
            vec![std::collections::BTreeSet::from([1u64, 2])],
            std::collections::BTreeMap::from([
                (1u64, BasicNode::new("a:1")),
                (2u64, BasicNode::new("a:2")),
            ]),
        );
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 2),
            payload: EntryPayload::Membership(membership.clone()),
        };
        machine.apply(vec![entry]).await.expect("apply");

        let (applied, stored) = machine.applied_state().await.expect("applied state");
        assert_eq!(applied.map(|l| l.index), Some(2));
        assert_eq!(stored.membership(), &membership);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_build_install_round_trip_replaces_state() {
        let src_dir = tempfile::tempdir().expect("src tempdir");
        let (mut machine, _engine) = make_machine(&src_dir, 3);
        let bucket = BucketId::new(1);
        let keep = flat_key(MetaCf::Meta, bucket, b"keep", &[]);
        machine
            .apply(vec![
                ops_entry(
                    1,
                    vec![StoreOp::put(MetaCf::Meta, keep.clone(), b"v1".as_slice())],
                ),
                ops_entry(
                    2,
                    vec![StoreOp::put(
                        MetaCf::Meta,
                        flat_key(MetaCf::Meta, bucket, b"drop", &[]),
                        b"v2".as_slice(),
                    )],
                ),
            ])
            .await
            .expect("apply");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");
        assert_eq!(snapshot.meta.last_log_id.map(|l| l.index), Some(2));

        // Install into a fresh engine that holds a stale extra key.
        let dst_dir = tempfile::tempdir().expect("dst tempdir");
        let (mut dst, dst_engine) = make_machine(&dst_dir, 3);
        let stale = flat_key(MetaCf::Meta, bucket, b"stale", &[]);
        dst_engine
            .apply(&[StoreOp::put(MetaCf::Meta, stale.clone(), b"old".as_slice())])
            .expect("seed stale");
        dst.install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");

        assert_eq!(
            dst_engine
                .get(MetaCf::Meta, &keep)
                .expect("get keep")
                .as_deref(),
            Some(&b"v1"[..])
        );
        assert!(
            dst_engine
                .get(MetaCf::Meta, &stale)
                .expect("get stale")
                .is_none(),
            "install must clear prior range state"
        );
        let (applied, _) = dst.applied_state().await.expect("applied state");
        assert_eq!(applied.map(|l| l.index), Some(2));

        // The persisted current snapshot survives and reloads.
        let current = dst.get_current_snapshot().await.expect("current snapshot");
        assert_eq!(
            current.map(|s| s.meta.snapshot_id),
            Some(snapshot.meta.snapshot_id.clone())
        );
    }

    fn split_entry(index: u64, op: SplitOp) -> Entry<MetaTypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(MetaEntry::Split(op)),
        }
    }

    /// A Split op applies deterministically: the parent narrows to the left
    /// half, a pending child is recorded for the right half, the child's
    /// bookkeeping is seeded, and replay of the same entry is a no-op (03 §2).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_narrows_parent_records_child_and_is_replay_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut machine, engine) = make_machine(&dir, 1);
        assert_eq!(machine.range, PartitionRange::full(Namespace::Flat));

        let op = SplitOp {
            child_group: 2,
            at_bucket: 5,
            at_routing_key: b"m".to_vec(),
            child_peers: vec![(1, "a:1".to_string())],
            bootstrap_node: 1,
            child_ino_tag: 7,
        };
        machine
            .apply(vec![split_entry(1, op.clone())])
            .await
            .expect("apply split");

        // Parent narrowed to [-inf, (5,"m")).
        assert_eq!(machine.range.start, None);
        assert_eq!(machine.range.end, Some((BucketId::new(5), b"m".to_vec())));

        // Pending child recorded for [(5,"m"), +inf) with the child's peers.
        let pending = read_pending_split(engine.as_ref(), 1)
            .expect("read pending")
            .expect("pending present");
        assert_eq!(pending.child_group, 2);
        assert_eq!(
            pending.child_range.start,
            Some((BucketId::new(5), b"m".to_vec()))
        );
        assert_eq!(pending.child_range.end, None);
        assert_eq!(pending.bootstrap_node, 1);

        // Child bookkeeping seeded: delq/ino counters at 0, ino tag = 7.
        assert_eq!(
            engine
                .get(MetaCf::Applied, &delq_seq_key(2))
                .expect("get")
                .as_deref(),
            Some(&0u64.to_be_bytes()[..])
        );
        assert_eq!(
            engine
                .get(MetaCf::Applied, &ino_tag_key(2))
                .expect("get")
                .as_deref(),
            Some(&7u32.to_be_bytes()[..])
        );

        // The narrowed range persists across a state-machine reload (restart).
        let reloaded = MetaStateMachine::new(
            engine.clone(),
            Arc::new(crate::ref_extractor::EpochRefExtractor),
            1,
            PartitionRange::full(Namespace::Flat),
            Arc::new(PartitionGuard::new()),
        )
        .expect("reload");
        assert_eq!(reloaded.range.end, Some((BucketId::new(5), b"m".to_vec())));

        // Replay of the same split entry is a no-op: the boundary is no longer
        // inside the (already-narrowed) parent range.
        machine
            .apply(vec![split_entry(1, op)])
            .await
            .expect("replay split");
        assert_eq!(machine.range.end, Some((BucketId::new(5), b"m".to_vec())));
    }
}
