//! Node membership and the node status state machine.
//!
//! [`NodeManager`] owns the `node` column family of the PD state-machine
//! database. It applies the replicated node commands (register / update-status /
//! remove) and keeps an in-memory index for the read path. The on-disk records
//! are the source of truth; the in-memory index is a cache rebuilt by
//! [`restore`](NodeManager::restore) on startup and after a snapshot install.
//!
//! Apply model (INVARIANT design 01 §2): each command mutates the in-memory
//! index and stages its durable writes into a shared [`WriteBatch`]; the state
//! machine flushes that batch once, after every command in the raft batch, with
//! `sync`. A failed flush is fatal — the raft engine shuts down and the next
//! start rebuilds the cache from disk, discarding the un-flushed mutations, so
//! cache and disk reconverge. Node ids are handed out from a persistent counter
//! (deterministic across replicas, AGENTS §8), never reused.
//!
//! Design: docs/design/01-pd.md §1 (node membership); §3 (Node model)

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see `raft` module): they run inside the raft state machine, so
// boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::NodeId;
use rocksdb::{DB, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::cluster::RejectReason;
use crate::cluster::id_record;
use crate::raft::SmError;

/// The `node` column family: one record per node keyed by its id (4-byte BE),
/// plus the id allocation counter (persistence mechanics in [`id_record`]).
pub(crate) const NODE_CF: &str = "node";

/// The set of roles a node runs, as a bitset over [`RoleSet::DATA`] /
/// [`RoleSet::GATEWAY`] / [`RoleSet::META`] (design 01 §3).
///
/// Hand-rolled rather than pulling in a bitflags dependency for three bits. The
/// bit values are a stable persisted encoding: never renumber them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RoleSet(u8);

impl RoleSet {
    /// Serves object data (DataNode).
    pub const DATA: RoleSet = RoleSet(1);
    /// Serves the S3 gateway.
    pub const GATEWAY: RoleSet = RoleSet(2);
    /// Serves metadata partitions (MetaNode).
    pub const META: RoleSet = RoleSet(4);

    /// The empty role set.
    #[must_use]
    pub const fn empty() -> Self {
        RoleSet(0)
    }

    /// The union of two role sets.
    #[must_use]
    pub const fn union(self, other: RoleSet) -> Self {
        RoleSet(self.0 | other.0)
    }

    /// Whether every role in `other` is present in `self`.
    #[must_use]
    pub const fn contains(self, other: RoleSet) -> bool {
        self.0 & other.0 == other.0
    }

    /// Whether no role is set.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The raw bit representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Wraps raw bits as a role set (the wire form in `pd.proto` and tests).
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        RoleSet(bits)
    }
}

/// The lifecycle status of a cluster node (design 01 §1).
///
/// A freshly registered node is [`Starting`](NodeStatus::Starting); the first
/// heartbeat promotes it to [`Live`](NodeStatus::Live) (heartbeat handling lands
/// in a later phase). Missed heartbeats move it to [`Offline`](NodeStatus::Offline)
/// and, if it stays down, to [`Lost`](NodeStatus::Lost).
/// [`Decommissioned`](NodeStatus::Decommissioned) is the terminal administrative
/// state; only a decommissioned node may be removed from the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Registered but not yet confirmed live by a heartbeat.
    Starting,
    /// Heartbeating and serving its roles.
    Live,
    /// Missed heartbeats; may recover to `Live`.
    Offline,
    /// Offline long enough to be presumed dead; eligible for decommissioning.
    Lost,
    /// Administratively removed from service (terminal).
    Decommissioned,
}

impl NodeStatus {
    /// Whether `self -> next` is a permitted status transition.
    ///
    /// A transition to the same status is an idempotent no-op and is always
    /// permitted. INVARIANT(design 01 §1): the lifecycle is
    /// `Starting → {Live, Offline, Decommissioned}`,
    /// `Live → {Offline, Decommissioned}`,
    /// `Offline → {Live, Lost, Decommissioned}`,
    /// `Lost → {Live, Decommissioned}`, and `Decommissioned` is terminal.
    #[must_use]
    pub fn can_transition_to(self, next: NodeStatus) -> bool {
        use NodeStatus::{Decommissioned, Live, Lost, Offline, Starting};

        if self == next {
            return true;
        }
        matches!(
            (self, next),
            (Starting, Live | Offline | Decommissioned)
                | (Live, Offline | Decommissioned)
                | (Offline, Live | Lost | Decommissioned)
                | (Lost, Live | Decommissioned)
        )
    }
}

/// A registered cluster node (design 01 §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// PD-assigned cluster identity.
    pub node_id: NodeId,
    /// Service address (`host:port`); the idempotency key for registration.
    pub addr: String,
    /// Availability zone (topology / anti-affinity).
    pub az: String,
    /// Rack (topology / anti-affinity).
    pub rack: String,
    /// Roles the node runs.
    pub roles: RoleSet,
    /// Lifecycle status.
    pub status: NodeStatus,
}

/// Register a node, or reclaim its existing id if `addr` is already known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterNode {
    /// Service address (`host:port`); registration is idempotent by this field.
    pub addr: String,
    /// Availability zone.
    pub az: String,
    /// Rack.
    pub rack: String,
    /// Roles the node runs.
    pub roles: RoleSet,
}

/// Drive a node through a status transition (validated on apply).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateNodeStatus {
    /// Target node.
    pub node_id: NodeId,
    /// Requested new status.
    pub status: NodeStatus,
}

/// Remove a decommissioned node's record from the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveNode {
    /// Target node (must be `Decommissioned`).
    pub node_id: NodeId,
}

/// The in-memory node index: the id→node map plus the next id to allocate.
#[derive(Debug)]
struct NodeIndex {
    nodes: BTreeMap<NodeId, Node>,
    next_id: u32,
}

impl NodeIndex {
    fn find_by_addr(&self, addr: &str) -> Option<NodeId> {
        self.nodes
            .values()
            .find(|node| node.addr == addr)
            .map(|node| node.node_id)
    }
}

/// Owns the `node` column family and the node index (cloneable; clones share the
/// same database and in-memory index).
#[derive(Clone)]
pub struct NodeManager {
    db: Arc<DB>,
    index: Arc<RwLock<NodeIndex>>,
}

impl NodeManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted nodes.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(NodeIndex {
                nodes: BTreeMap::new(),
                next_id: id_record::FIRST_ID,
            })),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, NodeIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, NodeIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuilds the in-memory index from the `node` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the column family is missing or a
    /// persisted record cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let (records, next_id) = id_record::load_records::<Node>(&self.db, NODE_CF)?;
        let nodes = records
            .into_iter()
            .map(|node| (node.node_id, node))
            .collect();
        let mut index = self.write_index();
        index.nodes = nodes;
        index.next_id = next_id;
        Ok(())
    }

    /// Applies a node registration: idempotent by address, otherwise allocates
    /// the next id and records the node in `Starting`. Stages durable writes
    /// into `batch` and updates the in-memory index; the caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized or
    /// the id counter would overflow.
    pub(crate) fn apply_register(
        &self,
        batch: &mut WriteBatch,
        cmd: &RegisterNode,
    ) -> Result<NodeId, SmError> {
        let cf = id_record::open_cf(&self.db, NODE_CF)?;
        let mut index = self.write_index();
        if let Some(existing) = index.find_by_addr(&cmd.addr) {
            return Ok(existing);
        }

        let node_id = NodeId::new(index.next_id);
        let next_id = id_record::advance_counter(index.next_id)?;
        let node = Node {
            node_id,
            addr: cmd.addr.clone(),
            az: cmd.az.clone(),
            rack: cmd.rack.clone(),
            roles: cmd.roles,
            status: NodeStatus::Starting,
        };

        id_record::stage_put_record(batch, cf, node_id.get(), &node)?;
        id_record::stage_put_counter(batch, cf, next_id);
        index.next_id = next_id;
        index.nodes.insert(node_id, node);
        Ok(node_id)
    }

    /// Applies a node status transition. Returns `Ok(None)` when applied (a
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
        cmd: &UpdateNodeStatus,
    ) -> Result<Option<RejectReason>, SmError> {
        let cf = id_record::open_cf(&self.db, NODE_CF)?;
        let mut index = self.write_index();
        let Some(current) = index.nodes.get(&cmd.node_id) else {
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
        id_record::stage_put_record(batch, cf, cmd.node_id.get(), &updated)?;
        index.nodes.insert(cmd.node_id, updated);
        Ok(None)
    }

    /// Applies a node removal, permitted only for a `Decommissioned` node.
    /// Returns `Ok(None)` when removed or `Ok(Some(reason))` when rejected.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the column family is missing.
    pub(crate) fn apply_remove(
        &self,
        batch: &mut WriteBatch,
        cmd: &RemoveNode,
    ) -> Result<Option<RejectReason>, SmError> {
        let cf = id_record::open_cf(&self.db, NODE_CF)?;
        let mut index = self.write_index();
        let Some(current) = index.nodes.get(&cmd.node_id) else {
            return Ok(Some(RejectReason::NotFound));
        };
        if current.status != NodeStatus::Decommissioned {
            return Ok(Some(RejectReason::NotRemovable));
        }

        batch.delete_cf(cf, id_record::record_key(cmd.node_id.get()));
        index.nodes.remove(&cmd.node_id);
        Ok(None)
    }

    /// The node set and next id for a snapshot (design 01 §2). Nodes come out in
    /// id order (deterministic snapshot bytes, AGENTS §8).
    pub(crate) fn snapshot_view(&self) -> (Vec<Node>, u32) {
        let index = self.read_index();
        (index.nodes.values().cloned().collect(), index.next_id)
    }

    /// Replaces the full node set from an installed snapshot: stages the record
    /// rewrite into `batch` and rebuilds the in-memory index. The caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the column family is missing or a record
    /// cannot be (de)serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        nodes: Vec<Node>,
        next_id: u32,
    ) -> Result<(), SmError> {
        let cf = id_record::open_cf(&self.db, NODE_CF)?;
        id_record::stage_clear(batch, &self.db, cf)?;
        for node in &nodes {
            id_record::stage_put_record(batch, cf, node.node_id.get(), node)?;
        }
        id_record::stage_put_counter(batch, cf, next_id);

        let mut index = self.write_index();
        index.nodes = nodes.into_iter().map(|node| (node.node_id, node)).collect();
        index.next_id = next_id;
        Ok(())
    }

    /// Looks up a node by id (read path).
    #[must_use]
    pub fn get(&self, node_id: NodeId) -> Option<Node> {
        self.read_index().nodes.get(&node_id).cloned()
    }

    /// Whether a node with `node_id` is registered (used to validate ownership
    /// of a disk at registration, without cloning the node record).
    #[must_use]
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.read_index().nodes.contains_key(&node_id)
    }

    /// The `(id, status)` of every registered node, in id order (read path for
    /// the liveness sweep). Returns statuses only, not full records, since the
    /// sweep derives transitions from status plus heartbeat staleness alone.
    #[must_use]
    pub fn statuses(&self) -> Vec<(NodeId, NodeStatus)> {
        self.read_index()
            .nodes
            .values()
            .map(|node| (node.node_id, node.status))
            .collect()
    }

    /// Every registered node, in id order (topology read for placement and the
    /// data-plane transport build).
    #[must_use]
    pub fn list(&self) -> Vec<Node> {
        self.read_index().nodes.values().cloned().collect()
    }

    /// The number of registered nodes (read path).
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_index().nodes.len()
    }

    /// Whether no node is registered (read path).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_index().nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::WriteBatch;
    use tempfile::TempDir;

    use super::*;

    fn open_manager(dir: &TempDir) -> NodeManager {
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[NODE_CF],
        )
        .expect("open node cf");
        let manager = NodeManager::new(Arc::new(db));
        manager.restore().expect("restore");
        manager
    }

    fn commit(manager: &NodeManager, batch: WriteBatch) {
        manager.db.write(batch).expect("flush batch");
    }

    fn register(manager: &NodeManager, addr: &str) -> NodeId {
        let mut batch = WriteBatch::default();
        let cmd = RegisterNode {
            addr: addr.to_string(),
            az: "az1".to_string(),
            rack: "r1".to_string(),
            roles: RoleSet::DATA,
        };
        let node_id = manager.apply_register(&mut batch, &cmd).expect("register");
        commit(manager, batch);
        node_id
    }

    fn update_status(
        manager: &NodeManager,
        node_id: NodeId,
        status: NodeStatus,
    ) -> Option<RejectReason> {
        let mut batch = WriteBatch::default();
        let reject = manager
            .apply_update_status(&mut batch, &UpdateNodeStatus { node_id, status })
            .expect("update status");
        commit(manager, batch);
        reject
    }

    #[test]
    fn node_status_transitions_follow_the_lifecycle() {
        use NodeStatus::{Decommissioned, Live, Lost, Offline, Starting};

        // (from, to, permitted) — INVARIANT(design 01 §1).
        let cases = [
            ("starting->live", Starting, Live, true),
            ("starting->offline", Starting, Offline, true),
            ("starting->decommissioned", Starting, Decommissioned, true),
            ("starting->lost", Starting, Lost, false),
            ("live->offline", Live, Offline, true),
            ("live->decommissioned", Live, Decommissioned, true),
            ("live->lost", Live, Lost, false),
            ("live->starting", Live, Starting, false),
            ("offline->live", Offline, Live, true),
            ("offline->lost", Offline, Lost, true),
            ("offline->decommissioned", Offline, Decommissioned, true),
            ("offline->starting", Offline, Starting, false),
            ("lost->live", Lost, Live, true),
            ("lost->decommissioned", Lost, Decommissioned, true),
            ("lost->offline", Lost, Offline, false),
            ("decommissioned->live", Decommissioned, Live, false),
            ("decommissioned->lost", Decommissioned, Lost, false),
        ];
        for (name, from, to, permitted) in cases {
            assert_eq!(from.can_transition_to(to), permitted, "case {name}");
        }
    }

    #[test]
    fn same_status_transition_is_idempotent() {
        use NodeStatus::{Decommissioned, Live, Lost, Offline, Starting};
        for status in [Starting, Live, Offline, Lost, Decommissioned] {
            assert!(
                status.can_transition_to(status),
                "same-status {status:?} must be a permitted no-op"
            );
        }
    }

    #[test]
    fn role_set_bit_operations() {
        let both = RoleSet::DATA.union(RoleSet::GATEWAY);
        assert!(both.contains(RoleSet::DATA));
        assert!(both.contains(RoleSet::GATEWAY));
        assert!(!both.contains(RoleSet::META));
        assert!(!both.is_empty());
        assert!(RoleSet::empty().is_empty());
        assert_eq!(RoleSet::DATA.union(RoleSet::META).bits(), 1 | 4);
    }

    #[test]
    fn node_serde_round_trip() {
        let node = Node {
            node_id: NodeId::new(7),
            addr: "10.0.0.1:9000".to_string(),
            az: "az-a".to_string(),
            rack: "rack-3".to_string(),
            roles: RoleSet::DATA.union(RoleSet::META),
            status: NodeStatus::Live,
        };
        let json = serde_json::to_string(&node).expect("serialize");
        let back: Node = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(node, back);
    }

    #[test]
    fn register_allocates_sequential_ids_and_is_idempotent_by_addr() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);

        let first = register(&manager, "node-a:1");
        let second = register(&manager, "node-b:1");
        assert_eq!(first, NodeId::new(1));
        assert_eq!(second, NodeId::new(2));

        // Re-registering the same address reclaims the existing id.
        assert_eq!(register(&manager, "node-a:1"), first);
        assert_eq!(manager.len(), 2);

        let node = manager.get(first).expect("node present");
        assert_eq!(node.status, NodeStatus::Starting);
        assert_eq!(node.addr, "node-a:1");
    }

    #[test]
    fn update_status_applies_valid_and_rejects_invalid() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let id = register(&manager, "node-a:1");

        // Valid: Starting -> Live.
        assert_eq!(update_status(&manager, id, NodeStatus::Live), None);
        assert_eq!(manager.get(id).expect("node").status, NodeStatus::Live);

        // Same status is an idempotent no-op (applied).
        assert_eq!(update_status(&manager, id, NodeStatus::Live), None);

        // Invalid: Live -> Lost is not permitted.
        assert_eq!(
            update_status(&manager, id, NodeStatus::Lost),
            Some(RejectReason::InvalidTransition)
        );
        assert_eq!(manager.get(id).expect("node").status, NodeStatus::Live);

        // Unknown node.
        assert_eq!(
            update_status(&manager, NodeId::new(999), NodeStatus::Live),
            Some(RejectReason::NotFound)
        );
    }

    #[test]
    fn remove_requires_decommissioned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let id = register(&manager, "node-a:1");

        let remove = |manager: &NodeManager| {
            let mut batch = WriteBatch::default();
            let reject = manager
                .apply_remove(&mut batch, &RemoveNode { node_id: id })
                .expect("remove");
            commit(manager, batch);
            reject
        };

        // Not yet decommissioned.
        assert_eq!(remove(&manager), Some(RejectReason::NotRemovable));

        assert_eq!(
            update_status(&manager, id, NodeStatus::Decommissioned),
            None
        );
        assert_eq!(remove(&manager), None);
        assert!(manager.get(id).is_none());
        assert!(manager.is_empty());
    }

    #[test]
    fn restore_rebuilds_index_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = register(&open_manager(&dir), "node-a:1");
        register(&open_manager(&dir), "node-b:1");

        // A fresh manager over the same directory restores nodes and the counter.
        let manager = open_manager(&dir);
        assert_eq!(manager.len(), 2);
        assert_eq!(manager.get(first).expect("node").addr, "node-a:1");

        // The next allocation continues the sequence (counter was restored).
        assert_eq!(register(&manager, "node-c:1"), NodeId::new(3));
    }

    #[test]
    fn snapshot_view_and_install_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = open_manager(&dir);
        register(&source, "node-a:1");
        register(&source, "node-b:1");
        let (nodes, next_id) = source.snapshot_view();
        assert_eq!(nodes.len(), 2);
        assert_eq!(next_id, 3);

        // Install into a fresh manager that already holds a stale node.
        let target_dir = tempfile::tempdir().expect("tempdir");
        let target = open_manager(&target_dir);
        register(&target, "stale:1");

        let mut batch = WriteBatch::default();
        target
            .stage_and_apply_snapshot(&mut batch, nodes, next_id)
            .expect("install snapshot");
        commit(&target, batch);

        assert_eq!(target.len(), 2);
        assert_eq!(register(&target, "node-c:1"), NodeId::new(3));

        // The stale record is gone from disk too: a restore agrees with memory.
        target.restore().expect("restore");
        assert_eq!(target.len(), 3);
    }
}
