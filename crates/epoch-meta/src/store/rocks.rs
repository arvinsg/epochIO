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

//! RocksEngine: the default [`MetaStore`] engine.
//!
//! One shared RocksDB instance per node holds every partition of the Rocks
//! flavor (03 §2 同进程共享存储实例承载全部分区), column families per
//! [`MetaCf`]. Durability comes from the raft log, not the WAL: applies flush
//! with `disableWAL` (03 §8), so a crash may lose the unflushed tail — the
//! applied record written in the same batch caps how far the log replays, and
//! re-apply is idempotent (03 §7).
//!
//! (epoch-rocks StateMachine profile)

use std::path::Path;

use rocksdb::{ColumnFamily, Direction, IteratorMode, WriteBatch, WriteOptions};

use crate::store::keys::MetaCf;
use crate::store::{KeyRange, KvPage, MetaStore, MetaStoreError, StoreOp, StoreSnapshot};

/// The default metadata engine: one shared RocksDB over the [`MetaCf`] column
/// families (03 §7 RocksEngine column).
pub struct RocksEngine {
    db: rocksdb::DB,
}

impl RocksEngine {
    /// Opens (creating if absent) the shared state-machine database at `path`
    /// with every [`MetaCf`] column family.
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError::Rocks`] if RocksDB cannot open the database.
    pub fn open(path: &Path) -> Result<Self, MetaStoreError> {
        let names: Vec<&str> = MetaCf::ALL
            .iter()
            .chain(std::iter::once(&MetaCf::Applied))
            .map(|cf| cf.cf_name())
            .collect();
        let db = epoch_rocks::open_cfs(path, &epoch_rocks::state_machine_options(), &names)?;
        Ok(Self { db })
    }

    fn cf(&self, cf: MetaCf) -> Result<&ColumnFamily, MetaStoreError> {
        self.db
            .cf_handle(cf.cf_name())
            .ok_or(MetaStoreError::MissingCf(cf.cf_name()))
    }
}

impl MetaStore for RocksEngine {
    fn apply(&self, ops: &[StoreOp]) -> Result<(), MetaStoreError> {
        let mut batch = WriteBatch::default();
        for op in ops {
            let cf = self.cf(op.cf)?;
            match &op.value {
                Some(value) => batch.put_cf(cf, &op.key, value),
                None => batch.delete_cf(cf, &op.key),
            }
        }
        // 03 §8: no WAL — the raft log is the write-ahead log. Data and the
        // applied record share this batch, so crash recovery replays the log
        // from exactly the persisted apply position.
        let mut opts = WriteOptions::default();
        opts.disable_wal(true);
        self.db.write_opt(batch, &opts)?;
        Ok(())
    }

    fn get(&self, cf: MetaCf, key: &[u8]) -> Result<Option<Vec<u8>>, MetaStoreError> {
        Ok(self.db.get_cf(self.cf(cf)?, key)?)
    }

    fn scan(
        &self,
        cf: MetaCf,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<KvPage, MetaStoreError> {
        let cf = self.cf(cf)?;
        let mut out = Vec::new();
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(start, Direction::Forward))
        {
            let (key, value) = item?;
            if key.as_ref() >= end || out.len() >= limit {
                break;
            }
            out.push((key.to_vec(), value.to_vec()));
        }
        Ok(out)
    }

    fn snapshot(&self, ranges: &[KeyRange]) -> Result<StoreSnapshot, MetaStoreError> {
        // One engine snapshot for all ranges: the exported partition view is a
        // single consistent point in time even while applies continue (03 §2:
        // 迁移导出期间写入不中断).
        let snapshot = self.db.snapshot();
        let mut out = StoreSnapshot::default();
        for range in ranges {
            let cf = self.cf(range.cf)?;
            for item in
                snapshot.iterator_cf(cf, IteratorMode::From(&range.start, Direction::Forward))
            {
                let (key, value) = item?;
                if key.as_ref() >= range.end.as_slice() {
                    break;
                }
                out.entries.push((range.cf, key.to_vec(), value.to_vec()));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::keys::{bucket_prefix, flat_key, suffix};

    use epoch_proto::BucketId;

    fn head_key(bucket: BucketId, object: &[u8]) -> Vec<u8> {
        flat_key(MetaCf::Meta, bucket, object, &[])
    }

    #[test]
    fn apply_get_delete_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        let bucket = BucketId::new(1);
        let key = head_key(bucket, b"a/b");

        engine
            .apply(&[StoreOp::put(MetaCf::Meta, key.clone(), b"head".as_slice())])
            .expect("apply put");
        assert_eq!(
            engine.get(MetaCf::Meta, &key).expect("get").as_deref(),
            Some(&b"head"[..])
        );

        engine
            .apply(&[StoreOp::delete(MetaCf::Meta, key.clone())])
            .expect("apply delete");
        assert!(engine.get(MetaCf::Meta, &key).expect("get").is_none());
    }

    #[test]
    fn apply_is_atomic_with_applied_record_and_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        let bucket = BucketId::new(1);
        // The state machine tags its applied record with the partition id; the
        // store only sees a neutral op (03 §7).
        let applied_key = 7u64.to_be_bytes().to_vec();
        let batch = vec![
            StoreOp::put(MetaCf::Meta, head_key(bucket, b"x"), b"v1".as_slice()),
            StoreOp::put(MetaCf::Meta, head_key(bucket, b"y"), b"v2".as_slice()),
            StoreOp::put(MetaCf::Applied, applied_key.clone(), b"42".as_slice()),
        ];

        // Re-apply of the same raft batch must be a no-op (03 §7 replay).
        engine.apply(&batch).expect("apply");
        engine.apply(&batch).expect("re-apply");
        assert_eq!(
            engine
                .get(MetaCf::Applied, &applied_key)
                .expect("get applied")
                .as_deref(),
            Some(&b"42"[..])
        );
        assert_eq!(
            engine
                .get(MetaCf::Meta, &head_key(bucket, b"x"))
                .expect("get x")
                .as_deref(),
            Some(&b"v1"[..])
        );
    }

    #[test]
    fn scan_respects_range_and_limit_in_key_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        let bucket = BucketId::new(1);
        let other = BucketId::new(2);

        let ops: Vec<StoreOp> = ["a/3", "a/1", "a/2", "b/1"]
            .iter()
            .map(|o| {
                StoreOp::put(
                    MetaCf::Meta,
                    head_key(bucket, o.as_bytes()),
                    b"h".as_slice(),
                )
            })
            .chain(std::iter::once(StoreOp::put(
                MetaCf::Meta,
                head_key(other, b"a/0"),
                b"h".as_slice(),
            )))
            .collect();
        engine.apply(&ops).expect("apply");

        let start = head_key(bucket, b"a/");
        let end = head_key(bucket, b"a0"); // '0' > '/' → covers every "a/" key
        let page = engine
            .scan(MetaCf::Meta, &start, &end, 2)
            .expect("scan page");
        let names: Vec<Vec<u8>> = page.iter().map(|(k, _)| k[9..].to_vec()).collect();
        assert_eq!(names, vec![b"a/1".to_vec(), b"a/2".to_vec()]);

        let rest = engine
            .scan(MetaCf::Meta, &start, &end, 8)
            .expect("scan all");
        assert_eq!(rest.len(), 3, "bucket-2 key must stay out of range");

        // A bucket-wide scan uses the bucket prefix bounds (03 §4.1).
        let prefix = bucket_prefix(MetaCf::Meta, bucket);
        let mut prefix_end = prefix.clone();
        *prefix_end.last_mut().expect("prefix non-empty") += 1;
        let all = engine
            .scan(MetaCf::Meta, &prefix, &prefix_end, 16)
            .expect("bucket scan");
        assert_eq!(all.len(), 4);
    }

    #[test]
    fn snapshot_is_a_consistent_point_in_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        let bucket = BucketId::new(1);
        let range = KeyRange {
            cf: MetaCf::Meta,
            start: bucket_prefix(MetaCf::Meta, bucket),
            end: bucket_prefix(MetaCf::Meta, BucketId::new(2)),
        };

        engine
            .apply(&[StoreOp::put(
                MetaCf::Meta,
                head_key(bucket, b"before"),
                b"h".as_slice(),
            )])
            .expect("apply before");
        let exported = engine
            .snapshot(std::slice::from_ref(&range))
            .expect("snapshot");
        engine
            .apply(&[StoreOp::put(
                MetaCf::Meta,
                head_key(bucket, b"after"),
                b"h".as_slice(),
            )])
            .expect("apply after");

        assert_eq!(exported.entries.len(), 1);
        assert_eq!(exported.entries[0].1, head_key(bucket, b"before"));

        // Segment CF export of the same range coordinates (03 §4.1 逐 CF 子段):
        // the byte bounds are re-encoded per CF — the tag is the first byte.
        let seg_key = flat_key(MetaCf::MetaSeg, bucket, b"big", &suffix::seg_no(0));
        engine
            .apply(&[StoreOp::put(
                MetaCf::MetaSeg,
                seg_key.clone(),
                b"s".as_slice(),
            )])
            .expect("apply seg");
        let seg_export = engine
            .snapshot(&[KeyRange {
                cf: MetaCf::MetaSeg,
                start: bucket_prefix(MetaCf::MetaSeg, bucket),
                end: bucket_prefix(MetaCf::MetaSeg, BucketId::new(2)),
            }])
            .expect("seg snapshot");
        assert_eq!(
            seg_export.entries,
            vec![(MetaCf::MetaSeg, seg_key, b"s".to_vec())]
        );
    }

    #[test]
    fn data_survives_clean_reopen_after_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bucket = BucketId::new(1);
        let key = head_key(bucket, b"persist");
        {
            let engine = RocksEngine::open(dir.path()).expect("open");
            engine
                .apply(&[StoreOp::put(MetaCf::Meta, key.clone(), b"v".as_slice())])
                .expect("apply");
            // disableWAL (03 §8) leaves durability to the raft log; a clean
            // shutdown flush pins the tail so reopen sees it. Crash recovery
            // replays the log instead — covered by the raft layer's tests.
            engine.db.flush().expect("flush");
        }
        let engine = RocksEngine::open(dir.path()).expect("reopen");
        assert_eq!(
            engine.get(MetaCf::Meta, &key).expect("get").as_deref(),
            Some(&b"v"[..])
        );
    }
}
