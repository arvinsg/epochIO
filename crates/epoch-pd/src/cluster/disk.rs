//! Disk membership and the disk status state machine.
//!
//! [`DiskManager`] owns the `disk` column family of the PD state-machine
//! database, mirroring [`NodeManager`](super::node::NodeManager): it applies the
//! replicated disk commands (register / update-status) and keeps an in-memory
//! index for the read path. On-disk records are the source of truth; the index
//! is a cache rebuilt by [`restore`](DiskManager::restore).
//!
//! A disk belongs to a node; registration validates the owning node's existence
//! at the aggregation point ([`PdState::apply_command`](crate::state::PdState::apply_command)),
//! keeping this manager free of a cross-manager dependency. Only the disk's
//! durable identity (id / node / topology / total / status) lives here; the
//! heartbeat statistics (free, writable_extents) stay in leader memory and never
//! enter raft (Q18) — they arrive with heartbeat handling in a later phase.
//!
//! Design: docs/design/01-pd.md §1 (disk state machine); §3 (Disk model);
//! docs/design/02-datanode.md §1.8 (Broken reporting)

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::{DiskId, NodeId};
use rocksdb::{DB, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::cluster::RejectReason;
use crate::cluster::id_record;
use crate::raft::SmError;

/// The `disk` column family: one record per disk keyed by its id (4-byte BE),
/// plus the id allocation counter (persistence mechanics in [`id_record`]).
pub(crate) const DISK_CF: &str = "disk";

/// The lifecycle status of a disk (design 01 §1; 02 §1.8).
///
/// A freshly registered disk is [`Normal`](DiskStatus::Normal). An EIO on the
/// DataNode trips it to [`Broken`](DiskStatus::Broken) (reported to PD, 02 §1.8);
/// PD then advances it to [`Repairing`](DiskStatus::Repairing) and, once its
/// shards are rebuilt elsewhere, to [`Repaired`](DiskStatus::Repaired) (rejoins
/// as `Normal`) or [`Dropped`](DiskStatus::Dropped) (terminal, physically
/// removed). M4 registers disks as `Normal`; the broken/repair transitions are
/// driven in a later milestone (M7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskStatus {
    /// Healthy and serving.
    Normal,
    /// An I/O error tripped the disk; reported to PD (02 §1.8).
    Broken,
    /// PD is rebuilding the disk's shards elsewhere; its handle is dropped.
    Repairing,
    /// Repair finished; the disk may rejoin as `Normal`.
    Repaired,
    /// Operator-marked for decommission (01 §6.3 DropDisk): the source is still
    /// readable so shards are *copied* (not reconstructed) off it, but it is
    /// excluded from new-chunk placement. Advances to `Dropped` once drained.
    Draining,
    /// Removed from service (terminal).
    Dropped,
}

impl DiskStatus {
    /// Whether `self -> next` is a permitted status transition.
    ///
    /// A transition to the same status is an idempotent no-op and is always
    /// permitted. INVARIANT(design 01 §1): the lifecycle is
    /// `Normal → {Broken, Draining, Dropped}`, `Broken → {Repairing, Dropped}`,
    /// `Repairing → {Repaired, Dropped}`, `Repaired → {Normal, Dropped}`,
    /// `Draining → Dropped`, and `Dropped` is terminal.
    #[must_use]
    pub fn can_transition_to(self, next: DiskStatus) -> bool {
        use DiskStatus::{Broken, Draining, Dropped, Normal, Repaired, Repairing};

        if self == next {
            return true;
        }
        matches!(
            (self, next),
            (Normal, Broken | Draining | Dropped)
                | (Broken, Repairing | Dropped)
                | (Repairing, Repaired | Dropped)
                | (Repaired, Normal | Dropped)
                | (Draining, Dropped)
        )
    }
}

/// A registered disk (design 01 §3).
///
/// Holds only the disk's durable identity; `free` / `writable_extents` are
/// heartbeat statistics kept in leader memory (Q18), not stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Disk {
    /// PD-assigned disk identity.
    pub disk_id: DiskId,
    /// Owning node.
    pub node_id: NodeId,
    /// Availability zone (topology / anti-affinity).
    pub az: String,
    /// Rack (topology / anti-affinity).
    pub rack: String,
    /// Mount path on the owning node; the idempotency key with `node_id`.
    pub path: String,
    /// Total capacity in bytes (static, reported at registration).
    pub total: u64,
    /// Lifecycle status.
    pub status: DiskStatus,
}

/// Register a disk, or reclaim its id if `(node_id, path)` is already known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterDisk {
    /// Owning node (must already be registered).
    pub node_id: NodeId,
    /// Availability zone.
    pub az: String,
    /// Rack.
    pub rack: String,
    /// Mount path on the owning node; registration is idempotent by `(node_id, path)`.
    pub path: String,
    /// Total capacity in bytes.
    pub total: u64,
}

/// Drive a disk through a status transition (validated on apply).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateDiskStatus {
    /// Target disk.
    pub disk_id: DiskId,
    /// Requested new status.
    pub status: DiskStatus,
}

/// The in-memory disk index: the id→disk map plus the next id to allocate.
#[derive(Debug)]
struct DiskIndex {
    disks: BTreeMap<DiskId, Disk>,
    next_id: u32,
}

impl DiskIndex {
    fn find_by_node_path(&self, node_id: NodeId, path: &str) -> Option<DiskId> {
        self.disks
            .values()
            .find(|disk| disk.node_id == node_id && disk.path == path)
            .map(|disk| disk.disk_id)
    }
}

/// Owns the `disk` column family and the disk index (cloneable; clones share the
/// same database and in-memory index).
#[derive(Clone)]
pub struct DiskManager {
    db: Arc<DB>,
    index: Arc<RwLock<DiskIndex>>,
}

impl DiskManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted disks.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(DiskIndex {
                disks: BTreeMap::new(),
                next_id: id_record::FIRST_ID,
            })),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, DiskIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, DiskIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuilds the in-memory index from the `disk` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the column family is missing or a
    /// persisted record cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let (records, next_id) = id_record::load_records::<Disk>(&self.db, DISK_CF)?;
        let disks = records
            .into_iter()
            .map(|disk| (disk.disk_id, disk))
            .collect();
        let mut index = self.write_index();
        index.disks = disks;
        index.next_id = next_id;
        Ok(())
    }

    /// Applies a disk registration: idempotent by `(node_id, path)`, otherwise
    /// allocates the next id and records the disk as `Normal`. Stages durable
    /// writes into `batch` and updates the in-memory index; the caller flushes.
    ///
    /// The owning node's existence is validated by the caller before this runs.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized or
    /// the id counter would overflow.
    pub(crate) fn apply_register(
        &self,
        batch: &mut WriteBatch,
        cmd: &RegisterDisk,
    ) -> Result<DiskId, SmError> {
        let cf = id_record::open_cf(&self.db, DISK_CF)?;
        let mut index = self.write_index();
        if let Some(existing) = index.find_by_node_path(cmd.node_id, &cmd.path) {
            return Ok(existing);
        }

        let disk_id = DiskId::new(index.next_id);
        let next_id = id_record::advance_counter(index.next_id)?;
        let disk = Disk {
            disk_id,
            node_id: cmd.node_id,
            az: cmd.az.clone(),
            rack: cmd.rack.clone(),
            path: cmd.path.clone(),
            total: cmd.total,
            status: DiskStatus::Normal,
        };

        id_record::stage_put_record(batch, cf, disk_id.get(), &disk)?;
        id_record::stage_put_counter(batch, cf, next_id);
        index.next_id = next_id;
        index.disks.insert(disk_id, disk);
        Ok(disk_id)
    }

    /// Applies a disk status transition. Returns `Ok(None)` when applied (a
    /// same-status request is an idempotent no-op) or `Ok(Some(reason))` when
    /// rejected.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the updated record cannot be
    /// serialized.
    pub(crate) fn apply_update_status(
        &self,
        batch: &mut WriteBatch,
        cmd: &UpdateDiskStatus,
    ) -> Result<Option<RejectReason>, SmError> {
        let cf = id_record::open_cf(&self.db, DISK_CF)?;
        let mut index = self.write_index();
        let Some(current) = index.disks.get(&cmd.disk_id) else {
            return Ok(Some(RejectReason::NotFound));
        };
        if current.status == cmd.status {
            return Ok(None);
        }
        if !current.status.can_transition_to(cmd.status) {
            return Ok(Some(RejectReason::InvalidTransition));
        }

        let mut updated = current.clone();
        updated.status = cmd.status;
        id_record::stage_put_record(batch, cf, cmd.disk_id.get(), &updated)?;
        index.disks.insert(cmd.disk_id, updated);
        Ok(None)
    }

    /// The disk set and next id for a snapshot (design 01 §2). Disks come out in
    /// id order (deterministic snapshot bytes, AGENTS §8).
    pub(crate) fn snapshot_view(&self) -> (Vec<Disk>, u32) {
        let index = self.read_index();
        (index.disks.values().cloned().collect(), index.next_id)
    }

    /// Replaces the full disk set from an installed snapshot: stages the record
    /// rewrite into `batch` and rebuilds the in-memory index. The caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the column family is missing or a record
    /// cannot be (de)serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        disks: Vec<Disk>,
        next_id: u32,
    ) -> Result<(), SmError> {
        let cf = id_record::open_cf(&self.db, DISK_CF)?;
        id_record::stage_clear(batch, &self.db, cf)?;
        for disk in &disks {
            id_record::stage_put_record(batch, cf, disk.disk_id.get(), disk)?;
        }
        id_record::stage_put_counter(batch, cf, next_id);

        let mut index = self.write_index();
        index.disks = disks.into_iter().map(|disk| (disk.disk_id, disk)).collect();
        index.next_id = next_id;
        Ok(())
    }

    /// Looks up a disk by id (read path).
    #[must_use]
    pub fn get(&self, disk_id: DiskId) -> Option<Disk> {
        self.read_index().disks.get(&disk_id).cloned()
    }

    /// Every registered disk, in id order (placement candidate enumeration,
    /// 01 §4.1).
    #[must_use]
    pub fn list(&self) -> Vec<Disk> {
        self.read_index().disks.values().cloned().collect()
    }

    /// The number of registered disks (read path).
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_index().disks.len()
    }

    /// Whether no disk is registered (read path).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_index().disks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::WriteBatch;
    use tempfile::TempDir;

    use super::*;

    fn open_manager(dir: &TempDir) -> DiskManager {
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[DISK_CF],
        )
        .expect("open disk cf");
        let manager = DiskManager::new(Arc::new(db));
        manager.restore().expect("restore");
        manager
    }

    fn commit(manager: &DiskManager, batch: WriteBatch) {
        manager.db.write(batch).expect("flush batch");
    }

    fn register(manager: &DiskManager, node: NodeId, path: &str) -> DiskId {
        let mut batch = WriteBatch::default();
        let cmd = RegisterDisk {
            node_id: node,
            az: "az1".to_string(),
            rack: "r1".to_string(),
            path: path.to_string(),
            total: 1 << 40,
        };
        let disk_id = manager.apply_register(&mut batch, &cmd).expect("register");
        commit(manager, batch);
        disk_id
    }

    fn update_status(
        manager: &DiskManager,
        disk_id: DiskId,
        status: DiskStatus,
    ) -> Option<RejectReason> {
        let mut batch = WriteBatch::default();
        let reject = manager
            .apply_update_status(&mut batch, &UpdateDiskStatus { disk_id, status })
            .expect("update status");
        commit(manager, batch);
        reject
    }

    #[test]
    fn disk_status_transitions_follow_the_lifecycle() {
        use DiskStatus::{Broken, Dropped, Normal, Repaired, Repairing};

        // (from, to, permitted) — INVARIANT(design 01 §1).
        let cases = [
            ("normal->broken", Normal, Broken, true),
            ("normal->dropped", Normal, Dropped, true),
            ("normal->repairing", Normal, Repairing, false),
            ("normal->repaired", Normal, Repaired, false),
            ("broken->repairing", Broken, Repairing, true),
            ("broken->dropped", Broken, Dropped, true),
            ("broken->normal", Broken, Normal, false),
            ("broken->repaired", Broken, Repaired, false),
            ("repairing->repaired", Repairing, Repaired, true),
            ("repairing->dropped", Repairing, Dropped, true),
            ("repairing->normal", Repairing, Normal, false),
            ("repaired->normal", Repaired, Normal, true),
            ("repaired->dropped", Repaired, Dropped, true),
            ("repaired->broken", Repaired, Broken, false),
            ("dropped->normal", Dropped, Normal, false),
            ("dropped->repairing", Dropped, Repairing, false),
        ];
        for (name, from, to, permitted) in cases {
            assert_eq!(from.can_transition_to(to), permitted, "case {name}");
        }
    }

    #[test]
    fn same_status_transition_is_idempotent() {
        use DiskStatus::{Broken, Dropped, Normal, Repaired, Repairing};
        for status in [Normal, Broken, Repairing, Repaired, Dropped] {
            assert!(
                status.can_transition_to(status),
                "same-status {status:?} must be a permitted no-op"
            );
        }
    }

    #[test]
    fn disk_serde_round_trip() {
        let disk = Disk {
            disk_id: DiskId::new(5),
            node_id: NodeId::new(2),
            az: "az-a".to_string(),
            rack: "rack-1".to_string(),
            path: "/data/disk0".to_string(),
            total: 32 << 30,
            status: DiskStatus::Repairing,
        };
        let json = serde_json::to_string(&disk).expect("serialize");
        let back: Disk = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(disk, back);
    }

    #[test]
    fn register_allocates_sequential_ids_and_is_idempotent_by_node_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);

        let first = register(&manager, node1, "/data/0");
        let second = register(&manager, node1, "/data/1");
        assert_eq!(first, DiskId::new(1));
        assert_eq!(second, DiskId::new(2));

        // Same path on a different node is a distinct disk.
        let third = register(&manager, node2, "/data/0");
        assert_eq!(third, DiskId::new(3));

        // Re-registering the same (node, path) reclaims the existing id.
        assert_eq!(register(&manager, node1, "/data/0"), first);
        assert_eq!(manager.len(), 3);

        let disk = manager.get(first).expect("disk present");
        assert_eq!(disk.status, DiskStatus::Normal);
        assert_eq!(disk.node_id, node1);
        assert_eq!(disk.path, "/data/0");
    }

    #[test]
    fn update_status_applies_valid_and_rejects_invalid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let id = register(&manager, NodeId::new(1), "/data/0");

        // Valid: Normal -> Broken.
        assert_eq!(update_status(&manager, id, DiskStatus::Broken), None);
        assert_eq!(manager.get(id).expect("disk").status, DiskStatus::Broken);

        // Same status is an idempotent no-op (applied).
        assert_eq!(update_status(&manager, id, DiskStatus::Broken), None);

        // Invalid: Broken -> Normal is not permitted.
        assert_eq!(
            update_status(&manager, id, DiskStatus::Normal),
            Some(RejectReason::InvalidTransition)
        );
        assert_eq!(manager.get(id).expect("disk").status, DiskStatus::Broken);

        // Unknown disk.
        assert_eq!(
            update_status(&manager, DiskId::new(999), DiskStatus::Broken),
            Some(RejectReason::NotFound)
        );
    }

    #[test]
    fn restore_rebuilds_index_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = NodeId::new(1);
        let first = register(&open_manager(&dir), node, "/data/0");
        register(&open_manager(&dir), node, "/data/1");

        // A fresh manager over the same directory restores disks and the counter.
        let manager = open_manager(&dir);
        assert_eq!(manager.len(), 2);
        assert_eq!(manager.get(first).expect("disk").path, "/data/0");

        // The next allocation continues the sequence (counter was restored).
        assert_eq!(register(&manager, node, "/data/2"), DiskId::new(3));
    }

    #[test]
    fn snapshot_view_and_install_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node = NodeId::new(1);
        let source = open_manager(&dir);
        register(&source, node, "/data/0");
        register(&source, node, "/data/1");
        let (disks, next_id) = source.snapshot_view();
        assert_eq!(disks.len(), 2);
        assert_eq!(next_id, 3);

        // Install into a fresh manager that already holds a stale disk.
        let target_dir = tempfile::tempdir().expect("tempdir");
        let target = open_manager(&target_dir);
        register(&target, NodeId::new(9), "/stale/0");

        let mut batch = WriteBatch::default();
        target
            .stage_and_apply_snapshot(&mut batch, disks, next_id)
            .expect("install snapshot");
        commit(&target, batch);

        assert_eq!(target.len(), 2);
        assert_eq!(register(&target, node, "/data/2"), DiskId::new(3));

        // The stale record is gone from disk too: a restore agrees with memory.
        target.restore().expect("restore");
        assert_eq!(target.len(), 3);
    }
}
