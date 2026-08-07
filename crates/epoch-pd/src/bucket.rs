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

//! Bucket metadata and the bucket manager (PD bucket 表, 01 §2/§7).
//!
//! [`BucketManager`] owns the `bucket` column family of the PD state-machine
//! database and mirrors [`DiskManager`](crate::cluster::disk::DiskManager): it
//! applies replicated bucket commands (create) and keeps an in-memory index for
//! the read path. Records are the source of truth; the index is a cache rebuilt
//! by [`restore`](BucketManager::restore).
//!
//! Scope (M4): create + read/list. Bucket deletion data flow (DeleteRange +
//! orphan GC, 99-Q15) lands with the MetaNode data path (M5); only the identity
//! record exists here.
//!
//! Design: docs/design/03-metanode.md §2 (bucket-分区映射); docs/design/99-open-questions.md N5

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::BucketId;
use rocksdb::{DB, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::cluster::id_record;
use crate::raft::SmError;

/// The `bucket` column family.
pub(crate) const BUCKET_CF: &str = "bucket";

/// The key of the id-allocation counter record inside the `bucket` CF.
const COUNTER_KEY: &[u8] = b"__counter";

/// The bucket's namespace mode (03 §3): flat key dictionary order (S3-native)
/// or hierarchical directory semantics (FUSE-native).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NsMode {
    /// Flat key space (default): `ListObjectsV2` is one range scan.
    Flat,
    /// Hierarchical: directory semantics with atomic same-dir rename.
    Hier,
}

/// The bucket's metadata engine policy (03 §7): partitions of this bucket are
/// served from RocksDB (default) or the in-memory engine (AI hot buckets).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaEngine {
    /// RocksDB-backed partitions (capacity-first).
    Rocks,
    /// Memory-backed partitions (tail-latency-first; PD RAM-watermark guarded).
    Mem,
}

/// A registered bucket (identity + policy; 03 §4.3 inline 护栏的 bucket 覆盖项).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketMeta {
    /// PD-assigned bucket identity (never reused).
    pub bucket_id: BucketId,
    /// Unique bucket name (creation is idempotent by name).
    pub name: String,
    /// Namespace mode, fixed at creation (03 §3).
    pub ns_mode: NsMode,
    /// Inline threshold override (`None` = cluster default, 03 §4.3).
    pub inline_threshold: Option<u64>,
    /// Erasure code registry id for this bucket's objects.
    pub codemode_id: u16,
    /// Metadata engine policy for this bucket's partitions.
    pub engine: MetaEngine,
    /// Creation timestamp (leader-chosen at proposal, deterministic in apply).
    pub created_at: i64,
}

/// Replicated bucket-creation command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateBucket {
    /// Unique bucket name.
    pub name: String,
    /// Namespace mode (fixed at creation).
    pub ns_mode: NsMode,
    /// Inline threshold override.
    pub inline_threshold: Option<u64>,
    /// Erasure code registry id.
    pub codemode_id: u16,
    /// Metadata engine policy.
    pub engine: MetaEngine,
    /// Leader-chosen creation timestamp (apply stays deterministic, AGENTS §8).
    pub created_at: i64,
}

#[derive(Default)]
struct BucketIndex {
    buckets: BTreeMap<BucketId, BucketMeta>,
    names: BTreeMap<String, BucketId>,
    next_id: u64,
}

/// Owns the `bucket` column family and the bucket index (cloneable; clones
/// share the same database and index).
#[derive(Clone)]
pub struct BucketManager {
    db: Arc<DB>,
    index: Arc<RwLock<BucketIndex>>,
}

impl BucketManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted buckets.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(BucketIndex::default())),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, BucketIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, BucketIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuilds the in-memory index from the `bucket` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the column family is missing or a
    /// persisted record cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let cf = self.cf()?;
        let mut index = self.write_index();
        index.buckets.clear();
        index.names.clear();
        index.next_id = 1;
        for kv in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = kv.map_err(crate::raft::sm_read_err)?;
            if key.as_ref() == COUNTER_KEY {
                let raw: [u8; 8] = value
                    .as_ref()
                    .try_into()
                    .map_err(|_| crate::raft::sm_corrupt("bad bucket counter record"))?;
                index.next_id = u64::from_be_bytes(raw);
                continue;
            }
            let id_bytes: [u8; 8] = key
                .as_ref()
                .try_into()
                .map_err(|_| crate::raft::sm_corrupt("bad bucket key length"))?;
            let bucket: BucketMeta = serde_json::from_slice(&value)
                .map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
            index.names.insert(bucket.name.clone(), bucket.bucket_id);
            index
                .buckets
                .insert(BucketId::new(u64::from_be_bytes(id_bytes)), bucket);
        }
        Ok(())
    }

    /// Applies a bucket creation: allocates the next id and records the bucket.
    /// Idempotent by name — an existing name returns its original bucket
    /// without allocating (a retried proposal must not burn ids).
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized.
    pub(crate) fn apply_create(
        &self,
        batch: &mut WriteBatch,
        cmd: &CreateBucket,
    ) -> Result<BucketId, SmError> {
        if let Some(existing) = self.read_index().names.get(&cmd.name) {
            return Ok(*existing);
        }
        let mut index = self.write_index();
        // The ns bit convention (HIER_BUCKET_BIT, 03 §2/§4.1 归属不变量): hier
        // ids carry bit 63, flat ids never do — shared-CF range export stays
        // exact per namespace. The counter itself increments the low 63 bits.
        let raw_id = index.next_id
            | if cmd.ns_mode == NsMode::Hier {
                epoch_proto::consts::HIER_BUCKET_BIT
            } else {
                0
            };
        let bucket_id = BucketId::new(raw_id);
        let bucket = BucketMeta {
            bucket_id,
            name: cmd.name.clone(),
            ns_mode: cmd.ns_mode,
            inline_threshold: cmd.inline_threshold,
            codemode_id: cmd.codemode_id,
            engine: cmd.engine,
            created_at: cmd.created_at,
        };
        index.next_id = index
            .next_id
            .checked_add(1)
            .ok_or_else(|| crate::raft::sm_corrupt("bucket id counter overflow"))?;
        index.names.insert(bucket.name.clone(), bucket_id);
        index.buckets.insert(bucket_id, bucket.clone());
        let cf = self.cf()?;
        let value =
            serde_json::to_vec(&bucket).map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
        batch.put_cf(cf, bucket_id.get().to_be_bytes(), value);
        batch.put_cf(cf, COUNTER_KEY, index.next_id.to_be_bytes());
        Ok(bucket_id)
    }

    fn cf(&self) -> Result<&rocksdb::ColumnFamily, SmError> {
        id_record::open_cf(&self.db, BUCKET_CF)
    }

    /// The bucket with this id, if present.
    #[must_use]
    pub fn get(&self, bucket_id: BucketId) -> Option<BucketMeta> {
        self.read_index().buckets.get(&bucket_id).cloned()
    }

    /// The bucket with this name, if present.
    #[must_use]
    pub fn get_by_name(&self, name: &str) -> Option<BucketMeta> {
        let index = self.read_index();
        index
            .names
            .get(name)
            .and_then(|id| index.buckets.get(id).cloned())
    }

    /// Every registered bucket, in id order.
    #[must_use]
    pub fn list(&self) -> Vec<BucketMeta> {
        self.read_index().buckets.values().cloned().collect()
    }

    /// The number of registered buckets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_index().buckets.len()
    }

    /// Whether no bucket is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_index().buckets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_manager() -> (tempfile::TempDir, BucketManager) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[BUCKET_CF],
        )
        .expect("open db");
        (dir, BucketManager::new(Arc::new(db)))
    }

    fn cmd(name: &str) -> CreateBucket {
        CreateBucket {
            name: name.to_string(),
            ns_mode: NsMode::Flat,
            inline_threshold: None,
            codemode_id: 1,
            engine: MetaEngine::Rocks,
            created_at: 1_700_000_000,
        }
    }

    #[test]
    fn create_allocates_monotonic_ids_and_is_name_idempotent() {
        let (_dir, manager) = open_manager();
        let mut batch = WriteBatch::default();
        let first = manager
            .apply_create(&mut batch, &cmd("images"))
            .expect("create");
        let second = manager
            .apply_create(&mut batch, &cmd("ckpt"))
            .expect("create");
        assert_ne!(first, second);
        // A retried proposal for the same name reuses the id (no id burn).
        let again = manager
            .apply_create(&mut batch, &cmd("images"))
            .expect("idempotent create");
        assert_eq!(first, again);
        assert_eq!(manager.len(), 2);
        assert_eq!(
            manager.get_by_name("ckpt").map(|b| b.bucket_id),
            Some(second)
        );
    }

    #[test]
    fn hier_buckets_carry_the_namespace_bit() {
        let (_dir, manager) = open_manager();
        let mut batch = WriteBatch::default();
        let flat = manager
            .apply_create(&mut batch, &cmd("flat-bucket"))
            .expect("create flat");
        let mut hier_cmd = cmd("hier-bucket");
        hier_cmd.ns_mode = NsMode::Hier;
        let hier = manager
            .apply_create(&mut batch, &hier_cmd)
            .expect("create hier");
        assert_eq!(flat.get() & epoch_proto::consts::HIER_BUCKET_BIT, 0);
        assert_ne!(hier.get() & epoch_proto::consts::HIER_BUCKET_BIT, 0);
        // The counter advanced by one in the low 63 bits either way.
        assert_eq!(
            hier.get() & !epoch_proto::consts::HIER_BUCKET_BIT,
            flat.get() + 1
        );
    }

    #[test]
    fn restore_rebuilds_the_index() {
        let (_dir, manager) = open_manager();
        let db = manager.db.clone();
        let mut batch = WriteBatch::default();
        let id = manager
            .apply_create(&mut batch, &cmd("ds"))
            .expect("create");
        db.write(batch).expect("flush");

        let restored = BucketManager::new(db);
        restored.restore().expect("restore");
        assert_eq!(restored.get(id).map(|b| b.name), Some("ds".to_string()));
        assert_eq!(restored.get_by_name("ds").map(|b| b.bucket_id), Some(id));
        // A create after restore allocates past the restored counter.
        let mut batch = WriteBatch::default();
        let next = restored
            .apply_create(&mut batch, &cmd("ds2"))
            .expect("create");
        assert_ne!(next, id);
    }
}
