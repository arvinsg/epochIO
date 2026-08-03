//! Writer registry: the issued writer-token records, backed by the PD state
//! machine's `writer` column family.
//!
//! [`WriterManager`] mirrors the cluster managers (persistence mechanics in
//! [`id_record`](crate::cluster::id_record)): one record per token keyed by its
//! `u32` value, plus the monotonic token counter. Tokens are handed out from the
//! persisted counter (deterministic across replicas, AGENTS §8) and **never
//! reused** (01 §4.3). Registration always mints a new token — unlike node/disk
//! registration there is no idempotency-by-identity, because a rotating gateway
//! must get a distinct token.
//!
//! INVARIANT(design 01 §4.3): a token's status is one-way `Live → Dead`; a Dead
//! token never revives, which is what lets its blobs be garbage-collected.
//!
//! Design: docs/design/01-pd.md §4.3 (writer_token issuance / session)

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::{NodeId, WriterToken};
use rocksdb::{DB, WriteBatch};

use crate::cluster::RejectReason;
use crate::cluster::id_record;
use crate::raft::SmError;
use crate::writer::model::{MarkWriterDead, RegisterWriter, WriterRecord, WriterStatus};

/// The `writer` column family: one [`WriterRecord`] per token (4-byte BE key),
/// plus the token allocation counter (mechanics in [`id_record`]).
pub(crate) const WRITER_CF: &str = "writer";

/// In-memory writer index: issued records and the next token to allocate.
#[derive(Debug)]
struct WriterIndex {
    writers: BTreeMap<WriterToken, WriterRecord>,
    next_id: u32,
}

/// Owns the `writer` column family and the writer index (cloneable; clones share
/// the same database and in-memory index).
#[derive(Clone)]
pub struct WriterManager {
    db: Arc<DB>,
    index: Arc<RwLock<WriterIndex>>,
}

impl WriterManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted tokens.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(WriterIndex {
                writers: BTreeMap::new(),
                next_id: id_record::FIRST_ID,
            })),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, WriterIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, WriterIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuilds the in-memory index from the `writer` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the family is missing or a record
    /// cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let (records, next_id) = id_record::load_records::<WriterRecord>(&self.db, WRITER_CF)?;
        let mut index = self.write_index();
        index.writers = records.into_iter().map(|r| (r.token, r)).collect();
        index.next_id = next_id;
        Ok(())
    }

    /// Applies a writer registration: allocates the next token and records it
    /// `Live`. Stages durable writes into `batch`; the caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the token counter overflows or the
    /// record cannot be serialized.
    pub(crate) fn apply_register(
        &self,
        batch: &mut WriteBatch,
        cmd: &RegisterWriter,
    ) -> Result<WriterToken, SmError> {
        let cf = id_record::open_cf(&self.db, WRITER_CF)?;
        let mut index = self.write_index();

        let token = WriterToken::new(index.next_id);
        let next_id = id_record::advance_counter(index.next_id)?;
        let record = WriterRecord {
            token,
            node_id: cmd.node_id,
            status: WriterStatus::Live,
        };
        id_record::stage_put_record(batch, cf, token.get(), &record)?;
        id_record::stage_put_counter(batch, cf, next_id);
        index.next_id = next_id;
        index.writers.insert(token, record);
        Ok(token)
    }

    /// Applies a token retirement (`Live → Dead`). Returns `Ok(None)` when the
    /// token is now Dead (a token already Dead is an idempotent no-op) or
    /// `Ok(Some(RejectReason::NotFound))` when the token is unknown.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the updated record cannot be
    /// serialized.
    pub(crate) fn apply_mark_dead(
        &self,
        batch: &mut WriteBatch,
        cmd: &MarkWriterDead,
    ) -> Result<Option<RejectReason>, SmError> {
        let cf = id_record::open_cf(&self.db, WRITER_CF)?;
        let mut index = self.write_index();
        let Some(current) = index.writers.get(&cmd.token) else {
            return Ok(Some(RejectReason::NotFound));
        };
        if current.status == WriterStatus::Dead {
            return Ok(None);
        }

        let mut updated = *current;
        updated.status = WriterStatus::Dead;
        id_record::stage_put_record(batch, cf, cmd.token.get(), &updated)?;
        index.writers.insert(cmd.token, updated);
        Ok(None)
    }

    /// The writer records and next token for a snapshot. Records come out in
    /// token order (deterministic snapshot bytes, AGENTS §8).
    pub(crate) fn snapshot_view(&self) -> (Vec<WriterRecord>, u32) {
        let index = self.read_index();
        (index.writers.values().copied().collect(), index.next_id)
    }

    /// Replaces the full writer set from an installed snapshot: stages the record
    /// rewrite into `batch` and rebuilds the in-memory index. The caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the family is missing or a record cannot
    /// be (de)serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        writers: Vec<WriterRecord>,
        next_id: u32,
    ) -> Result<(), SmError> {
        let cf = id_record::open_cf(&self.db, WRITER_CF)?;
        id_record::stage_clear(batch, &self.db, cf)?;
        for record in &writers {
            id_record::stage_put_record(batch, cf, record.token.get(), record)?;
        }
        id_record::stage_put_counter(batch, cf, next_id);

        let mut index = self.write_index();
        index.writers = writers.into_iter().map(|r| (r.token, r)).collect();
        index.next_id = next_id;
        Ok(())
    }

    /// Looks up a writer record by token (read path).
    #[must_use]
    pub fn get(&self, token: WriterToken) -> Option<WriterRecord> {
        self.read_index().writers.get(&token).copied()
    }

    /// The `(token, node_id)` of every `Live` writer, in token order — the input
    /// to the liveness sweep, which retires those whose owning node's heartbeat
    /// has lapsed (01 §4.3).
    #[must_use]
    pub fn live_sessions(&self) -> Vec<(WriterToken, NodeId)> {
        self.read_index()
            .writers
            .values()
            .filter(|record| record.status == WriterStatus::Live)
            .map(|record| (record.token, record.node_id))
            .collect()
    }

    /// The number of issued tokens (read path).
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_index().writers.len()
    }

    /// Whether no token has been issued (read path).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_index().writers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::WriteBatch;
    use tempfile::TempDir;

    use super::*;

    fn open_manager(dir: &TempDir) -> WriterManager {
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[WRITER_CF],
        )
        .expect("open writer cf");
        let manager = WriterManager::new(Arc::new(db));
        manager.restore().expect("restore");
        manager
    }

    fn commit(manager: &WriterManager, batch: WriteBatch) {
        manager.db.write(batch).expect("flush batch");
    }

    fn register(manager: &WriterManager, node: u32) -> WriterToken {
        let mut batch = WriteBatch::default();
        let token = manager
            .apply_register(
                &mut batch,
                &RegisterWriter {
                    node_id: NodeId::new(node),
                },
            )
            .expect("register");
        commit(manager, batch);
        token
    }

    fn mark_dead(manager: &WriterManager, token: WriterToken) -> Option<RejectReason> {
        let mut batch = WriteBatch::default();
        let reject = manager
            .apply_mark_dead(&mut batch, &MarkWriterDead { token })
            .expect("mark dead");
        commit(manager, batch);
        reject
    }

    #[test]
    fn tokens_are_monotonic_and_never_reused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);

        let first = register(&manager, 1);
        let second = register(&manager, 1); // same node, still a fresh token
        assert_eq!(first, WriterToken::new(1));
        assert_eq!(second, WriterToken::new(2));
        assert_eq!(manager.len(), 2);

        // Retiring the first does not free its id for reuse.
        assert_eq!(mark_dead(&manager, first), None);
        let third = register(&manager, 1);
        assert_eq!(third, WriterToken::new(3));
    }

    #[test]
    fn mark_dead_is_idempotent_and_reports_unknown() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let token = register(&manager, 7);

        assert_eq!(mark_dead(&manager, token), None);
        assert_eq!(
            manager.get(token).expect("present").status,
            WriterStatus::Dead
        );
        // A second retirement is an idempotent no-op (still Dead, not revived).
        assert_eq!(mark_dead(&manager, token), None);
        assert_eq!(
            manager.get(token).expect("present").status,
            WriterStatus::Dead
        );

        // An unknown token is reported, not created.
        assert_eq!(
            mark_dead(&manager, WriterToken::new(999)),
            Some(RejectReason::NotFound)
        );
    }

    #[test]
    fn restore_rebuilds_records_and_counter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dead_token = {
            let manager = open_manager(&dir);
            let live = register(&manager, 1);
            let dead = register(&manager, 2);
            mark_dead(&manager, dead);
            assert_eq!(live, WriterToken::new(1));
            dead
        };

        let manager = open_manager(&dir);
        assert_eq!(manager.len(), 2);
        assert_eq!(
            manager.get(dead_token).expect("present").status,
            WriterStatus::Dead
        );
        assert_eq!(
            manager.live_sessions(),
            vec![(WriterToken::new(1), NodeId::new(1))]
        );
        // The counter survived: the next token continues the sequence.
        assert_eq!(register(&manager, 3), WriterToken::new(3));
    }

    #[test]
    fn snapshot_view_and_install_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = open_manager(&dir);
        let live = register(&source, 1);
        let dead = register(&source, 2);
        mark_dead(&source, dead);
        let (records, next_id) = source.snapshot_view();
        assert_eq!(records.len(), 2);
        assert_eq!(next_id, 3);

        let target_dir = tempfile::tempdir().expect("tempdir");
        let target = open_manager(&target_dir);
        register(&target, 99); // stale record to be replaced

        let mut batch = WriteBatch::default();
        target
            .stage_and_apply_snapshot(&mut batch, records, next_id)
            .expect("install snapshot");
        commit(&target, batch);

        assert_eq!(target.len(), 2);
        assert_eq!(
            target.get(live).expect("present").status,
            WriterStatus::Live
        );
        assert_eq!(
            target.get(dead).expect("present").status,
            WriterStatus::Dead
        );
        // Restore agrees with the installed in-memory state.
        target.restore().expect("restore");
        assert_eq!(target.len(), 2);
        assert_eq!(register(&target, 4), WriterToken::new(3));
    }
}
