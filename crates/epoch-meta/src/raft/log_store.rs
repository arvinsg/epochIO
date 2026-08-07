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

//! Durable multi-group raft log store over one shared RocksDB instance.
//!
//! Implements openraft's [`RaftLogStorage`] (and its [`RaftLogReader`]) for one
//! partition group; every group on the node shares the same isolated log
//! database (03 §8: log 独立 RocksDB 实例，与状态机隔离 — TiKV raftdb 方案),
//! keyed by a group-id prefix so log churn never crosses groups.
//!
//! ## Key layout
//!
//! | CF    | key bytes                       | value                     |
//! |-------|---------------------------------|---------------------------|
//! | `log` | group(u64 BE) + index(u64 BE)   | serialized [`Entry`]      |
//! | `meta`| `m` + group(u64 BE) + `v`       | serialized [`Vote`]       |
//! | `meta`| `m` + group(u64 BE) + `c`       | serialized committed id   |
//! | `meta`| `m` + group(u64 BE) + `p`       | serialized last-purged id |
//! | `meta`| `g` + group(u64 BE)             | serialized group registry |
//!
//! Big-endian indices keep a group's log keys in log order and all groups'
//! keyspaces disjoint. Appends/votes are fsync'd before acknowledging,
//! honoring raft's persist-before-ack contract (same as the PD store).
//!
//! Design: docs/design/03-metanode.md §8; docs/design/06-code-layout.md §9
//! (raft/log_store.rs)

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    AnyError, Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageError, StorageIOError,
    Vote,
};
use rocksdb::{ColumnFamily, DB, Direction, IteratorMode, WriteBatch, WriteOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::instrument;

use crate::MetaError;
use crate::raft::{MetaTypeConfig, NodeId};
use crate::store::MetaStoreError;

const LOG_CF: &str = "log";
const META_CF: &str = "meta";
const PER_GROUP_PREFIX: u8 = b'm';
const REGISTRY_PREFIX: u8 = b'g';
const VOTE_SUFFIX: u8 = b'v';
const COMMITTED_SUFFIX: u8 = b'c';
const PURGED_SUFFIX: u8 = b'p';

/// The per-group log key: `group(u64 BE) | index(u64 BE)`.
fn log_key(group: u64, index: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&group.to_be_bytes());
    key[8..].copy_from_slice(&index.to_be_bytes());
    key
}

/// The exclusive upper bound of a group's log keyspace: `(group + 1) | 0`.
fn log_keyspace_end(group: u64) -> [u8; 16] {
    log_key(group.wrapping_add(1), 0)
}

/// A per-group bookkeeping key in the meta CF: `m | group(u64 BE) | suffix`.
fn group_meta_key(group: u64, suffix: u8) -> [u8; 10] {
    let mut key = [0u8; 10];
    key[0] = PER_GROUP_PREFIX;
    key[1..9].copy_from_slice(&group.to_be_bytes());
    key[9] = suffix;
    key
}

/// A group registry key in the meta CF: `g | group(u64 BE)`.
fn registry_key(group: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = REGISTRY_PREFIX;
    key[1..].copy_from_slice(&group.to_be_bytes());
    key
}

fn log_index(key: &[u8]) -> Result<u64, StorageError<NodeId>> {
    key.get(8..16)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| corrupt("malformed raft log key"))
}

fn read_err(e: impl std::error::Error + 'static) -> StorageError<NodeId> {
    StorageIOError::read_logs(AnyError::new(&e)).into()
}

fn write_err(e: impl std::error::Error + 'static) -> StorageError<NodeId> {
    StorageIOError::write_logs(AnyError::new(&e)).into()
}

fn corrupt(msg: &str) -> StorageError<NodeId> {
    let io = std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    StorageIOError::read_logs(AnyError::new(&io)).into()
}

fn missing_cf(name: &str) -> StorageError<NodeId> {
    let io = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("missing raft log column family: {name}"),
    );
    StorageIOError::read_logs(AnyError::new(&io)).into()
}

fn sync_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StorageError<NodeId>> {
    serde_json::from_slice(bytes).map_err(read_err)
}

/// The shared raft-log database owner (one per MetaNode process).
///
/// Hands out per-group [`MetaLogStore`] handles and keeps the group registry
/// that restart recovery scans to reopen every group (03 §8; 07 §M5a kill -9
/// acceptance).
pub struct MetaLogDb {
    db: Arc<DB>,
}

impl MetaLogDb {
    /// Opens (creating if absent) the shared raft-log database at `path` with
    /// the [`raft_log_options`](epoch_rocks::raft_log_options) profile.
    ///
    /// # Errors
    ///
    /// Returns [`RocksError`](epoch_rocks::RocksError) if the database cannot
    /// be opened.
    pub fn open(path: &Path) -> Result<Self, epoch_rocks::RocksError> {
        let db = epoch_rocks::open_cfs(path, &epoch_rocks::raft_log_options(), &[LOG_CF, META_CF])?;
        Ok(Self { db: Arc::new(db) })
    }

    /// A log-store handle for `group` (cheap: shares the database).
    #[must_use]
    pub fn group_store(&self, group: u64) -> MetaLogStore {
        MetaLogStore {
            db: Arc::clone(&self.db),
            group,
        }
    }

    /// Persists the group registry record (serialized by the caller) marking
    /// `group` as owned by this node.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Store`] on a missing CF (open invariant violation)
    /// or [`MetaError::Rocks`] on write failure.
    pub fn register_group(&self, group: u64, record: &[u8]) -> Result<(), MetaError> {
        let cf = self
            .db
            .cf_handle(META_CF)
            .ok_or(MetaStoreError::MissingCf(META_CF))?;
        self.db
            .put_cf(&cf, registry_key(group), record)
            .map_err(epoch_rocks::RocksError::from)?;
        Ok(())
    }

    /// The `(group, registry record)` pairs persisted on this node, in group
    /// id order (restart recovery input).
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Store`] on a missing CF (open invariant violation)
    /// or [`MetaError::Rocks`] on read failure.
    pub fn registered_groups(&self) -> Result<Vec<(u64, Vec<u8>)>, MetaError> {
        let cf = self
            .db
            .cf_handle(META_CF)
            .ok_or(MetaStoreError::MissingCf(META_CF))?;
        let mut groups = Vec::new();
        let start = [REGISTRY_PREFIX];
        let end = [REGISTRY_PREFIX + 1];
        for item in self
            .db
            .iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward))
        {
            let (key, value) = item.map_err(epoch_rocks::RocksError::from)?;
            if key.as_ref() >= end.as_slice() {
                break;
            }
            let Some(id) = key.get(1..9) else { continue };
            let Ok(id) = <[u8; 8]>::try_from(id) else {
                continue;
            };
            groups.push((u64::from_be_bytes(id), value.to_vec()));
        }
        Ok(groups)
    }
}

/// Per-group raft log store over the shared instance (cloneable; the clone
/// shares the same database and serves as the [`RaftLogReader`]).
#[derive(Clone)]
pub struct MetaLogStore {
    db: Arc<DB>,
    group: u64,
}

impl MetaLogStore {
    fn log_cf(&self) -> Result<&ColumnFamily, StorageError<NodeId>> {
        self.db.cf_handle(LOG_CF).ok_or_else(|| missing_cf(LOG_CF))
    }

    fn meta_cf(&self) -> Result<&ColumnFamily, StorageError<NodeId>> {
        self.db
            .cf_handle(META_CF)
            .ok_or_else(|| missing_cf(META_CF))
    }

    fn read_meta<T: DeserializeOwned>(
        &self,
        suffix: u8,
    ) -> Result<Option<T>, StorageError<NodeId>> {
        let cf = self.meta_cf()?;
        match self
            .db
            .get_cf(cf, group_meta_key(self.group, suffix))
            .map_err(read_err)?
        {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }

    fn write_meta<T: Serialize>(
        &self,
        suffix: u8,
        value: Option<&T>,
        batch: &mut WriteBatch,
    ) -> Result<(), StorageError<NodeId>> {
        let cf = self.meta_cf()?;
        let key = group_meta_key(self.group, suffix);
        match value {
            Some(value) => batch.put_cf(cf, key, serde_json::to_vec(value).map_err(write_err)?),
            None => batch.delete_cf(cf, key),
        }
        Ok(())
    }

    fn read_purged(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.read_meta(PURGED_SUFFIX)
    }

    /// The id of the group's last present log entry, or `None` if empty.
    fn last_present_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let cf = self.log_cf()?;
        let mut iter = self.db.iterator_cf(
            cf,
            IteratorMode::From(&log_keyspace_end(self.group), Direction::Reverse),
        );
        match iter.next() {
            Some(kv) => {
                let (key, value) = kv.map_err(read_err)?;
                if key.first_chunk::<8>() != Some(&self.group.to_be_bytes()) {
                    return Ok(None);
                }
                let entry: Entry<MetaTypeConfig> = decode(&value)?;
                Ok(Some(entry.log_id))
            }
            None => Ok(None),
        }
    }
}

impl RaftLogReader<MetaTypeConfig> for MetaLogStore {
    #[instrument(skip(self), fields(group = self.group))]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetaTypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => i.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => i.saturating_add(1),
            Bound::Excluded(i) => *i,
            Bound::Unbounded => u64::MAX,
        };

        let cf = self.log_cf()?;
        let from = log_key(self.group, start);
        let keyspace_end = log_keyspace_end(self.group);
        let mut entries = Vec::new();
        for kv in self
            .db
            .iterator_cf(cf, IteratorMode::From(&from, Direction::Forward))
        {
            let (key, value) = kv.map_err(read_err)?;
            if key.as_ref() >= keyspace_end.as_slice() {
                break;
            }
            if log_index(&key)? >= end {
                break;
            }
            entries.push(decode(&value)?);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<MetaTypeConfig> for MetaLogStore {
    type LogReader = MetaLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<MetaTypeConfig>, StorageError<NodeId>> {
        let last_purged_log_id = self.read_purged()?;
        let last_log_id = self.last_present_log_id()?.or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        self.write_meta(VOTE_SUFFIX, Some(vote), &mut batch)?;
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.read_meta(VOTE_SUFFIX)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        self.write_meta(COMMITTED_SUFFIX, committed.as_ref(), &mut batch)?;
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.read_meta(COMMITTED_SUFFIX)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<MetaTypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<MetaTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let cf = self.log_cf()?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            let value = serde_json::to_vec(&entry).map_err(write_err)?;
            batch.put_cf(cf, log_key(self.group, entry.log_id.index), value);
        }
        // Persist before acknowledging: raft only re-examines the last log id,
        // so a lost tail after ack would silently break the consecutive-log
        // invariant.
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.log_cf()?;
        let keyspace_end = log_keyspace_end(self.group);
        let mut batch = WriteBatch::default();
        for kv in self.db.iterator_cf(
            cf,
            IteratorMode::From(&log_key(self.group, log_id.index), Direction::Forward),
        ) {
            let (key, _) = kv.map_err(write_err)?;
            if key.as_ref() >= keyspace_end.as_slice() {
                break;
            }
            batch.delete_cf(cf, key);
        }
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let cf = self.log_cf()?;
        let mut batch = WriteBatch::default();
        for kv in self.db.iterator_cf(
            cf,
            IteratorMode::From(&log_key(self.group, 0), Direction::Forward),
        ) {
            let (key, _) = kv.map_err(write_err)?;
            if key.as_ref() >= log_keyspace_end(self.group).as_slice() {
                break;
            }
            if log_index(&key)? > log_id.index {
                break;
            }
            batch.delete_cf(cf, key);
        }
        self.write_meta(PURGED_SUFFIX, Some(&log_id), &mut batch)?;
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorage;

    fn entry(index: u64) -> Entry<MetaTypeConfig> {
        Entry {
            log_id: LogId::new(openraft::LeaderId::new(1, 1), index),
            payload: openraft::EntryPayload::Blank,
        }
    }

    /// Writes entries directly into a group's log keyspace (append itself is
    /// covered by the openraft storage suite in tests/storage_suite.rs).
    fn seed_entries(db: &MetaLogDb, group: u64, indices: &[u64]) {
        let cf = db.db.cf_handle(LOG_CF).expect("log cf");
        for &index in indices {
            db.db
                .put_cf(
                    &cf,
                    log_key(group, index),
                    serde_json::to_vec(&entry(index)).expect("encode"),
                )
                .expect("seed entry");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn groups_share_instance_but_stay_disjoint() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = MetaLogDb::open(dir.path()).expect("open");
        let (mut one, mut two) = (db.group_store(1), db.group_store(2));
        seed_entries(&db, 1, &[1, 2]);
        seed_entries(&db, 2, &[1]);

        let state1 = one.get_log_state().await.expect("state g1");
        assert_eq!(state1.last_log_id.map(|l| l.index), Some(2));
        let state2 = two.get_log_state().await.expect("state g2");
        assert_eq!(state2.last_log_id.map(|l| l.index), Some(1));

        // Purging group 1 must not touch group 2's entries.
        one.purge(LogId::new(openraft::LeaderId::new(1, 1), 1))
            .await
            .expect("purge g1");
        let left1 = one.try_get_log_entries(1..).await.expect("read g1");
        assert_eq!(left1.len(), 1);
        assert_eq!(left1[0].log_id.index, 2);
        let left2 = two.try_get_log_entries(1..).await.expect("read g2");
        assert_eq!(left2.len(), 1);
        assert_eq!(
            one.read_purged().expect("purged g1").map(|l| l.index),
            Some(1)
        );
        assert!(two.read_purged().expect("purged g2").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn truncate_and_vote_are_per_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = MetaLogDb::open(dir.path()).expect("open");
        let (mut one, mut two) = (db.group_store(1), db.group_store(2));
        seed_entries(&db, 1, &[1, 2, 3]);
        seed_entries(&db, 2, &[1, 2]);

        one.truncate(LogId::new(openraft::LeaderId::new(1, 1), 2))
            .await
            .expect("truncate g1");
        let left1 = one.try_get_log_entries(1..).await.expect("read g1");
        assert_eq!(left1.len(), 1);
        assert_eq!(left1[0].log_id.index, 1);
        assert_eq!(
            two.try_get_log_entries(1..).await.expect("read g2").len(),
            2,
            "group 2 keyspace untouched"
        );

        one.save_vote(&Vote::new(1, 1)).await.expect("vote g1");
        two.save_vote(&Vote::new(2, 3)).await.expect("vote g2");
        assert_eq!(
            one.read_vote().await.expect("read g1"),
            Some(Vote::new(1, 1))
        );
        assert_eq!(
            two.read_vote().await.expect("read g2"),
            Some(Vote::new(2, 3))
        );
    }

    #[test]
    fn registry_round_trips_in_group_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = MetaLogDb::open(dir.path()).expect("open");
        db.register_group(9, b"range-9").expect("register 9");
        db.register_group(1, b"range-1").expect("register 1");
        let groups = db.registered_groups().expect("scan");
        assert_eq!(
            groups,
            vec![(1, b"range-1".to_vec()), (9, b"range-9".to_vec())]
        );
    }
}
