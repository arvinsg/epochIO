//! Durable raft log store backed by an isolated RocksDB instance.
//!
//! Implements openraft's [`RaftLogStorage`] (and its [`RaftLogReader`]) over a
//! dedicated RocksDB opened with the [`raft_log_options`](epoch_rocks::raft_log_options)
//! profile (Q1: the log lives apart from the state machine so log churn never
//! competes with state-machine compaction).
//!
//! ## Key layout (single default column family)
//!
//! | prefix | key bytes            | value                     |
//! |--------|----------------------|---------------------------|
//! | `l`    | `l` + index (u64 BE) | serialized [`Entry`]      |
//! | `v`    | `v`                  | serialized [`Vote`]       |
//! | `c`    | `c`                  | serialized committed id   |
//! | `p`    | `p`                  | serialized last-purged id |
//!
//! Big-endian indices keep `l`-prefixed keys in log order for range scans. All
//! writes are fsync'd before returning (append) or before the flush callback
//! fires, honoring raft's persist-before-ack contract.
//!
//! Design: docs/design/01-pd.md §2; docs/design/06-code-layout.md §8

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    AnyError, Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageError, StorageIOError,
    Vote,
};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};

use crate::raft::{NodeId, PdTypeConfig};

const VOTE_KEY: &[u8] = b"v";
const COMMITTED_KEY: &[u8] = b"c";
const PURGED_KEY: &[u8] = b"p";
const LOG_PREFIX: u8 = b'l';
/// One byte past [`LOG_PREFIX`]; the exclusive upper bound of the log key space.
const LOG_PREFIX_END: u8 = LOG_PREFIX + 1;

fn log_key(index: u64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = LOG_PREFIX;
    key[1..].copy_from_slice(&index.to_be_bytes());
    key
}

fn index_from_key(key: &[u8]) -> Result<u64, StorageError<NodeId>> {
    key.get(1..9)
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

fn sync_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

/// Raft log store over an isolated RocksDB instance (cloneable; the clone shares
/// the same underlying database and serves as the [`RaftLogReader`]).
#[derive(Clone)]
pub struct PdLogStore {
    db: Arc<DB>,
}

impl PdLogStore {
    /// Opens (creating if absent) the raft log database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`RocksError`](epoch_rocks::RocksError) if the database cannot be
    /// opened.
    pub fn open(path: &Path) -> Result<Self, epoch_rocks::RocksError> {
        let db = epoch_rocks::open_cfs(path, &epoch_rocks::raft_log_options(), &["default"])?;
        Ok(Self { db: Arc::new(db) })
    }

    fn read_purged(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self.db.get(PURGED_KEY).map_err(read_err)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(read_err)?)),
            None => Ok(None),
        }
    }

    /// The id of the last present log entry, or `None` if the log is empty.
    fn last_present_log_id(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let mut iter = self
            .db
            .iterator(IteratorMode::From(&[LOG_PREFIX_END], Direction::Reverse));
        match iter.next() {
            Some(kv) => {
                let (key, value) = kv.map_err(read_err)?;
                if key.first() != Some(&LOG_PREFIX) {
                    return Ok(None);
                }
                let entry: Entry<PdTypeConfig> =
                    serde_json::from_slice(&value).map_err(read_err)?;
                Ok(Some(entry.log_id))
            }
            None => Ok(None),
        }
    }
}

impl RaftLogReader<PdTypeConfig> for PdLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<PdTypeConfig>>, StorageError<NodeId>> {
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

        let mut entries = Vec::new();
        let from = log_key(start);
        for kv in self
            .db
            .iterator(IteratorMode::From(&from, Direction::Forward))
        {
            let (key, value) = kv.map_err(read_err)?;
            if key.first() != Some(&LOG_PREFIX) {
                break;
            }
            if index_from_key(&key)? >= end {
                break;
            }
            entries.push(serde_json::from_slice(&value).map_err(read_err)?);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<PdTypeConfig> for PdLogStore {
    type LogReader = PdLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<PdTypeConfig>, StorageError<NodeId>> {
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
        let bytes = serde_json::to_vec(vote).map_err(write_err)?;
        self.db
            .put_opt(VOTE_KEY, bytes, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        match self.db.get(VOTE_KEY).map_err(read_err)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(read_err)?)),
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        match committed {
            Some(log_id) => {
                let bytes = serde_json::to_vec(&log_id).map_err(write_err)?;
                self.db.put(COMMITTED_KEY, bytes).map_err(write_err)?;
            }
            None => self.db.delete(COMMITTED_KEY).map_err(write_err)?,
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self.db.get(COMMITTED_KEY).map_err(read_err)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(read_err)?)),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<PdTypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<PdTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = WriteBatch::default();
        for entry in entries {
            let value = serde_json::to_vec(&entry).map_err(write_err)?;
            batch.put(log_key(entry.log_id.index), value);
        }
        // Persist before acknowledging: raft only re-examines the last log id, so
        // a lost tail after ack would silently break the consecutive-log invariant.
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        for kv in self.db.iterator(IteratorMode::From(
            &log_key(log_id.index),
            Direction::Forward,
        )) {
            let (key, _) = kv.map_err(write_err)?;
            if key.first() != Some(&LOG_PREFIX) {
                break;
            }
            batch.delete(key);
        }
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut batch = WriteBatch::default();
        for kv in self
            .db
            .iterator(IteratorMode::From(&log_key(0), Direction::Forward))
        {
            let (key, _) = kv.map_err(write_err)?;
            if key.first() != Some(&LOG_PREFIX) {
                break;
            }
            if index_from_key(&key)? > log_id.index {
                break;
            }
            batch.delete(key);
        }
        let purged = serde_json::to_vec(&log_id).map_err(write_err)?;
        batch.put(PURGED_KEY, purged);
        self.db
            .write_opt(batch, &sync_write_options())
            .map_err(write_err)?;
        Ok(())
    }
}
