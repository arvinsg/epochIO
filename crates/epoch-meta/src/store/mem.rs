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

//! MemEngine: the in-memory [`MetaStore`] engine (03 §7 MemEngine 列, AI 高性能).
//!
//! An ordered `BTreeMap` per column family behind one `RwLock`, semantically
//! byte-identical to [`RocksEngine`](crate::store::rocks::RocksEngine): the
//! dual implementations validate each other (03 §7: trait 同时是分区迁移/快照
//! 的接口——双实现互为验证). Durability follows the cubefs recipe — the raft log
//! is the WAL, with periodic snapshots and log truncation (03 §7); this type
//! holds only the applied state, and the raft layer replays the log into it on
//! recovery, so [`apply`](MetaStore::apply) being idempotent is what makes
//! fuzzy-snapshot overlap replay safe.
//!
//! **experimental** (03 §7): default off, opt-in per bucket, promoted after
//! shadow-traffic validation (M9). The PD RAM-watermark guard is registered
//! there too; this type is the engine only.
//!
//! Design: docs/design/03-metanode.md §7

use std::collections::BTreeMap;
use std::sync::{PoisonError, RwLock};

use crate::store::keys::MetaCf;
use crate::store::{KeyRange, KvPage, MetaStore, MetaStoreError, StoreOp, StoreSnapshot};

/// The ordered in-memory store: one `BTreeMap` per column family, all under one
/// lock so an [`apply`](MetaStore::apply) batch commits atomically across CFs
/// and a [`snapshot`](MetaStore::snapshot) is a consistent point in time.
#[derive(Default)]
pub struct MemEngine {
    cfs: RwLock<Cfs>,
}

/// The per-CF ordered maps. A fixed set keyed by [`MetaCf`] (a small enum), so
/// a lookup is a match, not a hash.
#[derive(Default)]
struct Cfs {
    maps: BTreeMap<u8, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Cfs {
    fn map(&self, cf: MetaCf) -> Option<&BTreeMap<Vec<u8>, Vec<u8>>> {
        self.maps.get(&cf.tag())
    }

    fn map_mut(&mut self, cf: MetaCf) -> &mut BTreeMap<Vec<u8>, Vec<u8>> {
        self.maps.entry(cf.tag()).or_default()
    }
}

impl MemEngine {
    /// A fresh empty engine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Cfs> {
        self.cfs.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Cfs> {
        self.cfs.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl MetaStore for MemEngine {
    fn apply(&self, ops: &[StoreOp]) -> Result<(), MetaStoreError> {
        // One write lock for the whole batch → atomic across CFs, exactly like
        // RocksDB's WriteBatch. Puts overwrite and deletes tolerate absence, so
        // re-applying the same batch is a no-op (03 §7 idempotent).
        let mut cfs = self.write();
        for op in ops {
            match &op.value {
                Some(value) => {
                    cfs.map_mut(op.cf).insert(op.key.clone(), value.clone());
                }
                None => {
                    cfs.map_mut(op.cf).remove(&op.key);
                }
            }
        }
        Ok(())
    }

    fn get(&self, cf: MetaCf, key: &[u8]) -> Result<Option<Vec<u8>>, MetaStoreError> {
        Ok(self.read().map(cf).and_then(|m| m.get(key).cloned()))
    }

    fn scan(
        &self,
        cf: MetaCf,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<KvPage, MetaStoreError> {
        let cfs = self.read();
        let Some(map) = cfs.map(cf) else {
            return Ok(Vec::new());
        };
        Ok(map
            .range(start.to_vec()..end.to_vec())
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn snapshot(&self, ranges: &[KeyRange]) -> Result<StoreSnapshot, MetaStoreError> {
        // One read lock covers every range → a single consistent point in time,
        // matching RocksEngine's engine snapshot (03 §2 迁移导出期间写入不中断).
        let cfs = self.read();
        let mut out = StoreSnapshot::default();
        for range in ranges {
            let Some(map) = cfs.map(range.cf) else {
                continue;
            };
            for (key, value) in map.range(range.start.clone()..range.end.clone()) {
                out.entries.push((range.cf, key.clone(), value.clone()));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::keys::{bucket_prefix, flat_key};

    use epoch_proto::BucketId;

    #[test]
    fn apply_get_delete_round_trip() {
        let engine = MemEngine::new();
        let bucket = BucketId::new(1);
        let key = flat_key(MetaCf::Meta, bucket, b"a/b", &[]);
        engine
            .apply(&[StoreOp::put(MetaCf::Meta, key.clone(), b"head".as_slice())])
            .expect("put");
        assert_eq!(
            engine.get(MetaCf::Meta, &key).expect("get").as_deref(),
            Some(&b"head"[..])
        );
        engine
            .apply(&[StoreOp::delete(MetaCf::Meta, key.clone())])
            .expect("delete");
        assert!(engine.get(MetaCf::Meta, &key).expect("get").is_none());
    }

    #[test]
    fn apply_is_atomic_across_cfs_and_idempotent() {
        let engine = MemEngine::new();
        let bucket = BucketId::new(1);
        let batch = vec![
            StoreOp::put(
                MetaCf::Meta,
                flat_key(MetaCf::Meta, bucket, b"x", &[]),
                b"v".as_slice(),
            ),
            StoreOp::put(
                MetaCf::Delq,
                flat_key(MetaCf::Delq, bucket, b"x", &[]),
                b"d".as_slice(),
            ),
        ];
        engine.apply(&batch).expect("apply");
        engine.apply(&batch).expect("re-apply");
        assert_eq!(
            engine
                .get(MetaCf::Meta, &flat_key(MetaCf::Meta, bucket, b"x", &[]))
                .expect("get")
                .as_deref(),
            Some(&b"v"[..])
        );
    }

    #[test]
    fn scan_respects_range_and_limit_in_key_order() {
        let engine = MemEngine::new();
        let bucket = BucketId::new(1);
        for name in ["a/3", "a/1", "a/2", "b/1"] {
            engine
                .apply(&[StoreOp::put(
                    MetaCf::Meta,
                    flat_key(MetaCf::Meta, bucket, name.as_bytes(), &[]),
                    b"h".as_slice(),
                )])
                .expect("put");
        }
        let start = flat_key(MetaCf::Meta, bucket, b"a/", &[]);
        let end = flat_key(MetaCf::Meta, bucket, b"a0", &[]);
        let page = engine.scan(MetaCf::Meta, &start, &end, 2).expect("scan");
        let names: Vec<Vec<u8>> = page.iter().map(|(k, _)| k[9..].to_vec()).collect();
        assert_eq!(names, vec![b"a/1".to_vec(), b"a/2".to_vec()]);

        let prefix = bucket_prefix(MetaCf::Meta, bucket);
        let mut prefix_end = prefix.clone();
        *prefix_end.last_mut().expect("non-empty") += 1;
        let all = engine
            .scan(MetaCf::Meta, &prefix, &prefix_end, 16)
            .expect("scan all");
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn snapshot_is_a_consistent_point_in_time() {
        let engine = MemEngine::new();
        let bucket = BucketId::new(1);
        let range = KeyRange {
            cf: MetaCf::Meta,
            start: bucket_prefix(MetaCf::Meta, bucket),
            end: bucket_prefix(MetaCf::Meta, BucketId::new(2)),
        };
        engine
            .apply(&[StoreOp::put(
                MetaCf::Meta,
                flat_key(MetaCf::Meta, bucket, b"before", &[]),
                b"h".as_slice(),
            )])
            .expect("apply");
        let exported = engine
            .snapshot(std::slice::from_ref(&range))
            .expect("snapshot");
        engine
            .apply(&[StoreOp::put(
                MetaCf::Meta,
                flat_key(MetaCf::Meta, bucket, b"after", &[]),
                b"h".as_slice(),
            )])
            .expect("apply after");
        assert_eq!(exported.entries.len(), 1);
        assert_eq!(
            exported.entries[0].1,
            flat_key(MetaCf::Meta, bucket, b"before", &[])
        );
    }
}
