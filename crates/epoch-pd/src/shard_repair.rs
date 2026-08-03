//! ShardRepair ticket registry (01 §6.3): the PD-side record of an in-flight
//! single-shard repair. Unlike a [`Job`](crate::job::types::Job), a ShardRepair
//! has **no coordinator, lease, or progress watermark** — it is a lightweight
//! "PD dispatched a rebuild of shard `(chunk, index)` to `target_node`" ticket.
//! The target node pulls its tickets, rebuilds the one shard in place from the
//! stripe survivors, and reports completion; PD then authorizes the shard-mapping
//! rebind against the ticket and clears it.
//!
//! This reconciles the two design statements that would otherwise conflict:
//! ShardRepair is "无 Job" (01 §6.3) yet a shard-mapping rebind "may only be
//! executed by PD raft, verifying the committer holds a valid Job **or a matching
//! ShardRepair ticket**" (01 §6.4 invariant 2). The ticket *is* the
//! authorization; the rebind mechanism itself is shared with the Job path
//! ([`ChunkManager::apply_rebind`](crate::chunk::ChunkManager::apply_rebind)).
//!
//! Determinism (AGENTS §8): `apply` reads no clock. A ticket's `target_node` and
//! `expected_epoch` are resolved from committed chunk/disk state at report apply
//! time (in [`PdState`](crate::state::PdState), the cross-manager aggregation
//! point), never from a report's claim — a stale gateway view cannot mis-target a
//! rebuild. Tickets are best-effort (a lost report is caught by the InspectRound
//! backstop, 01 §6.4 Q5), but replicating them is required: the rebind's apply
//! authorization must see the same ticket set on every replica.
//!
//! Mirrors [`JobManager`](crate::job::JobManager): raft applies replicated
//! commands (report / clear) while an in-memory index serves the node-pull RPC.
//! Keyed by `shard_prefix` (the epoch-zeroed stable shard identity, so a ticket
//! survives the very epoch bump its own rebind performs).
//!
//! Design: docs/design/01-pd.md §6.3 (dispatch); §6.4 (invariant 2 / Q5)

// The apply / snapshot methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::NodeId;
use rocksdb::{DB, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::chunk::model::ShardSlot;
use crate::raft::{SmError, sm_read_err, sm_write_err};

/// The `shard_repair` column family: one [`ShardRepairTicket`] per pending
/// single-shard repair, keyed by the 8-byte big-endian `shard_prefix`. No id
/// counter — tickets are keyed by the shard they repair, not an allocated id.
pub(crate) const SHARD_REPAIR_CF: &str = "shard_repair";

/// An in-flight single-shard repair PD has dispatched (01 §6.3). The target node
/// rebuilds shard `index` of `chunk_id` from the stripe survivors and reports
/// completion; the ticket authorizes the resulting rebind (01 §6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRepairTicket {
    /// The chunk whose shard is being repaired.
    pub chunk_id: epoch_proto::ChunkId,
    /// The shard index within the chunk (`0..N+M`).
    pub index: u8,
    /// The node currently hosting the shard slot — the only node authorized to
    /// commit this repair's rebind. Resolved from committed disk state at report
    /// apply time (never from the reporter's claim).
    pub target_node: NodeId,
    /// The slot epoch observed when the ticket was created; the rebuild reads
    /// from this epoch and the rebind's staleness fence must still match it.
    pub expected_epoch: u32,
}

impl ShardRepairTicket {
    /// The epoch-zeroed stable key (`chunk_id<<32 | index<<24`) — identical
    /// before and after the rebind's epoch bump, so a ticket keys the same slot
    /// across its own repair.
    #[must_use]
    pub fn shard_prefix(&self) -> u64 {
        shard_prefix(self.chunk_id, self.index)
    }
}

/// Report a bad/missing shard to PD (01 §6.3 / §6.4 Q5 fast path): a gateway's
/// heal-on-read observation or a scrub finding. Carries only the shard identity;
/// PD resolves the target node + epoch from committed state at apply time, so a
/// stale reporter view cannot mis-target the rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportShardRepair {
    /// The chunk whose shard is reported bad.
    pub chunk_id: epoch_proto::ChunkId,
    /// The reported shard index within the chunk (`0..N+M`).
    pub index: u8,
}

/// Commit a completed single-shard repair (01 §6.3 completion-receipt rebind):
/// the target node rebuilt the shard in place and asks PD to rebind the slot to
/// the rebuilt extent (epoch+1), authorized by the pending ticket rather than a
/// Job (01 §6.4 invariant 2). Mirrors
/// [`CommitShardMapping`](crate::chunk::model::CommitShardMapping) minus the
/// `job_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitShardRepair {
    /// The chunk whose shard slot is rebound.
    pub chunk_id: epoch_proto::ChunkId,
    /// The shard index within the chunk (`0..N+M`).
    pub index: u8,
    /// The slot epoch the target rebuilt from (the ticket + slot staleness fence).
    pub expected_epoch: u32,
    /// The disk the rebuilt extent lives on (equals the slot's current disk for
    /// an in-place ShardRepair rebuild).
    pub new_disk: epoch_proto::DiskId,
    /// The rebuilt extent's creation timestamp (its `ExtentId` derives from the
    /// new shard id + this ts, matching the DataNode's rebuilt extent).
    pub new_create_ts: i64,
    /// The node proposing the commit (must be the ticket's `target_node`).
    pub committer: NodeId,
}

/// The epoch-zeroed stable shard-slot key for `(chunk_id, index)`. Infallible:
/// `index` is a `u8` and epoch `0` never overflows the 24-bit field.
#[must_use]
pub fn shard_prefix(chunk_id: epoch_proto::ChunkId, index: u8) -> u64 {
    (u64::from(chunk_id.get()) << 32) | (u64::from(index) << epoch_proto::id::SHARD_EPOCH_BITS)
}

/// Why a ShardRepair completion commit was refused (01 §6.4). Distinct reasons so
/// the reporting node can tell "PD never dispatched this / it is already done"
/// (stop) from "another change moved the slot" (a stale ticket).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardRepairRejection {
    /// No ticket for this shard exists — PD did not dispatch it, or it was
    /// already completed and cleared (a duplicate/replayed commit).
    NoTicket,
    /// The committer is not the ticket's dispatched target node.
    NotTarget,
    /// The committer's rebuilt-from epoch does not match the ticket's.
    EpochMismatch,
}

/// Authorizes a ShardRepair completion commit from `committer` that rebuilt from
/// `commit_epoch`, against the pending `ticket`. `Ok(())` means the rebind may
/// apply; `Err` names the deterministic rejection. Pure — safe to call inside
/// `apply`. Mirrors [`authorize_commit`](crate::job::authorize_commit) for the
/// Job path.
///
/// # Errors
///
/// Returns the [`ShardRepairRejection`] describing why the commit is not
/// authorized.
pub fn authorize_commit(
    ticket: Option<&ShardRepairTicket>,
    committer: NodeId,
    commit_epoch: u32,
) -> Result<(), ShardRepairRejection> {
    let ticket = ticket.ok_or(ShardRepairRejection::NoTicket)?;
    if ticket.target_node != committer {
        return Err(ShardRepairRejection::NotTarget);
    }
    if ticket.expected_epoch != commit_epoch {
        return Err(ShardRepairRejection::EpochMismatch);
    }
    Ok(())
}

/// The outcome of applying a ShardRepair registry command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportOutcome {
    /// A new ticket was recorded.
    Created,
    /// A ticket for this shard already existed (idempotent re-report) — no-op.
    AlreadyPresent,
}

/// Owns the `shard_repair` column family and the in-memory ticket index
/// (cloneable; clones share the database and index).
#[derive(Clone)]
pub struct ShardRepairRegistry {
    db: Arc<DB>,
    /// `shard_prefix → ticket`, in prefix order (deterministic pull ordering).
    index: Arc<RwLock<BTreeMap<u64, ShardRepairTicket>>>,
}

impl ShardRepairRegistry {
    /// Creates a registry over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted tickets.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, BTreeMap<u64, ShardRepairTicket>> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, BTreeMap<u64, ShardRepairTicket>> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn cf(&self) -> Result<&rocksdb::ColumnFamily, SmError> {
        crate::cluster::id_record::open_cf(&self.db, SHARD_REPAIR_CF)
    }

    /// Rebuilds the in-memory index from the `shard_repair` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the family is missing or a record
    /// cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let cf = self.cf()?;
        let mut index = self.write_index();
        index.clear();
        for kv in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (_key, value) = kv.map_err(sm_read_err)?;
            let ticket: ShardRepairTicket = serde_json::from_slice(&value).map_err(sm_read_err)?;
            index.insert(ticket.shard_prefix(), ticket);
        }
        Ok(())
    }

    /// Records a dispatched repair ticket (idempotent by `shard_prefix`). Staging
    /// the durable write into `batch` (the caller flushes). Deterministic.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the record cannot be serialized.
    pub(crate) fn apply_report(
        &self,
        batch: &mut WriteBatch,
        ticket: ShardRepairTicket,
    ) -> Result<ReportOutcome, SmError> {
        let prefix = ticket.shard_prefix();
        let mut index = self.write_index();
        if index.contains_key(&prefix) {
            return Ok(ReportOutcome::AlreadyPresent);
        }
        let cf = self.cf()?;
        let bytes = serde_json::to_vec(&ticket).map_err(sm_write_err)?;
        batch.put_cf(cf, prefix.to_be_bytes(), bytes);
        index.insert(prefix, ticket);
        Ok(ReportOutcome::Created)
    }

    /// Clears a completed ticket by `shard_prefix` (idempotent). Staging the
    /// durable delete into `batch`.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the column family is missing.
    pub(crate) fn apply_clear(&self, batch: &mut WriteBatch, prefix: u64) -> Result<(), SmError> {
        let cf = self.cf()?;
        batch.delete_cf(cf, prefix.to_be_bytes());
        self.write_index().remove(&prefix);
        Ok(())
    }

    /// The ticket for `shard_prefix`, if pending.
    #[must_use]
    pub fn get(&self, prefix: u64) -> Option<ShardRepairTicket> {
        self.read_index().get(&prefix).copied()
    }

    /// Pending tickets targeting `node` — the node-pull delivery a DataNode polls
    /// (01 §6.3), in `shard_prefix` order.
    #[must_use]
    pub fn tickets_for_node(&self, node: NodeId) -> Vec<ShardRepairTicket> {
        self.read_index()
            .values()
            .filter(|t| t.target_node == node)
            .copied()
            .collect()
    }

    /// All pending tickets, in `shard_prefix` order (snapshot source).
    pub(crate) fn snapshot_view(&self) -> Vec<ShardRepairTicket> {
        self.read_index().values().copied().collect()
    }

    /// Replaces the full ticket set from an installed snapshot: stages the record
    /// rewrite into `batch` and rebuilds the index. The caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the family is missing or a record cannot
    /// be serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        tickets: Vec<ShardRepairTicket>,
    ) -> Result<(), SmError> {
        let cf = self.cf()?;
        for kv in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, _) = kv.map_err(sm_read_err)?;
            batch.delete_cf(cf, key);
        }
        let mut index = self.write_index();
        index.clear();
        for ticket in tickets {
            let prefix = ticket.shard_prefix();
            let bytes = serde_json::to_vec(&ticket).map_err(sm_write_err)?;
            batch.put_cf(cf, prefix.to_be_bytes(), bytes);
            index.insert(prefix, ticket);
        }
        Ok(())
    }
}

/// Resolves the [`ShardRepairTicket`] for a reported `(chunk_id, index)` from a
/// resolved shard slot and its owning node. Kept here (not in `PdState`) so the
/// ticket-construction rule lives with the ticket type.
#[must_use]
pub fn ticket_from_slot(
    chunk_id: epoch_proto::ChunkId,
    slot: &ShardSlot,
    target_node: NodeId,
) -> ShardRepairTicket {
    ShardRepairTicket {
        chunk_id,
        index: slot.index(),
        target_node,
        expected_epoch: slot.epoch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::ChunkId;

    fn registry() -> (ShardRepairRegistry, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(
            epoch_rocks::open_cfs(
                dir.path(),
                &epoch_rocks::state_machine_options(),
                &[SHARD_REPAIR_CF],
            )
            .expect("open db"),
        );
        (ShardRepairRegistry::new(db), dir)
    }

    fn ticket(chunk: u32, index: u8, node: u32, epoch: u32) -> ShardRepairTicket {
        ShardRepairTicket {
            chunk_id: ChunkId::new(chunk),
            index,
            target_node: NodeId::new(node),
            expected_epoch: epoch,
        }
    }

    fn report(reg: &ShardRepairRegistry, ticket: ShardRepairTicket) -> ReportOutcome {
        let mut batch = WriteBatch::default();
        let outcome = reg.apply_report(&mut batch, ticket).expect("report");
        reg.db.write(batch).expect("flush");
        outcome
    }

    fn clear(reg: &ShardRepairRegistry, prefix: u64) {
        let mut batch = WriteBatch::default();
        reg.apply_clear(&mut batch, prefix).expect("clear");
        reg.db.write(batch).expect("flush");
    }

    #[test]
    fn report_is_idempotent_by_shard_prefix() {
        let (reg, _dir) = registry();
        let t = ticket(1, 2, 7, 3);
        assert_eq!(report(&reg, t), ReportOutcome::Created);
        assert_eq!(
            report(&reg, ticket(1, 2, 9, 5)),
            ReportOutcome::AlreadyPresent,
            "same shard re-report is a no-op, first ticket wins"
        );
        // The stored ticket is the original (target/epoch unchanged by re-report).
        let stored = reg.get(t.shard_prefix()).expect("ticket");
        assert_eq!(stored.target_node, NodeId::new(7));
        assert_eq!(stored.expected_epoch, 3);
    }

    #[test]
    fn authorize_accepts_target_at_matching_epoch_only() {
        let t = ticket(1, 2, 7, 3);
        assert_eq!(authorize_commit(Some(&t), NodeId::new(7), 3), Ok(()));
        assert_eq!(
            authorize_commit(None, NodeId::new(7), 3),
            Err(ShardRepairRejection::NoTicket)
        );
        assert_eq!(
            authorize_commit(Some(&t), NodeId::new(9), 3),
            Err(ShardRepairRejection::NotTarget)
        );
        assert_eq!(
            authorize_commit(Some(&t), NodeId::new(7), 4),
            Err(ShardRepairRejection::EpochMismatch)
        );
    }

    #[test]
    fn tickets_for_node_filters_by_target() {
        let (reg, _dir) = registry();
        report(&reg, ticket(1, 0, 7, 0));
        report(&reg, ticket(1, 1, 8, 0));
        report(&reg, ticket(2, 0, 7, 0));
        let for_7 = reg.tickets_for_node(NodeId::new(7));
        assert_eq!(for_7.len(), 2, "two tickets target node 7");
        assert!(for_7.iter().all(|t| t.target_node == NodeId::new(7)));
    }

    #[test]
    fn clear_removes_ticket_and_is_idempotent() {
        let (reg, _dir) = registry();
        let t = ticket(1, 2, 7, 3);
        report(&reg, t);
        clear(&reg, t.shard_prefix());
        assert!(reg.get(t.shard_prefix()).is_none());
        // A second clear is a harmless no-op.
        clear(&reg, t.shard_prefix());
        assert!(reg.get(t.shard_prefix()).is_none());
    }

    #[test]
    fn restore_and_snapshot_round_trip_tickets() {
        let (reg, _dir) = registry();
        report(&reg, ticket(1, 0, 7, 0));
        report(&reg, ticket(2, 3, 8, 1));

        // restore() rebuilds the same index from the CF.
        reg.restore().expect("restore");
        assert_eq!(reg.snapshot_view().len(), 2);

        // Snapshot into a fresh registry over the same db and verify equality.
        let tickets = reg.snapshot_view();
        let restored = ShardRepairRegistry::new(reg.db.clone());
        let mut batch = WriteBatch::default();
        restored
            .stage_and_apply_snapshot(&mut batch, tickets)
            .expect("install");
        restored.db.write(batch).expect("flush");
        assert_eq!(restored.snapshot_view().len(), 2);
        assert_eq!(
            restored
                .get(shard_prefix(ChunkId::new(2), 3))
                .unwrap()
                .target_node,
            NodeId::new(8)
        );
    }
}
