//! openraft integration for the PD single raft group (design 01 §2, Q22/Q25).
//!
//! The integration is split into the four openraft building blocks, one per
//! submodule, mirroring the code-layout blueprint (06 §8):
//! [`log_store`] (durable log), [`state_machine`] (+ [`snapshot`]), and
//! [`network`] (peer RPC). [`PdTypeConfig`] binds the application types.
//!
//! ## Node identity
//!
//! The raft [`NodeId`] is the **PD replica id** (`u64`), a namespace distinct
//! from the cluster `NodeId` (`u32`, defined in `epoch-proto`) that PD assigns
//! to registered DataNodes / MetaNodes. PD replicas bootstrap the cluster before
//! any node registration, so they carry their own configured ids and addresses
//! ([`openraft::BasicNode`]).
//!
//! Design: docs/design/01-pd.md §2; docs/design/06-code-layout.md §8

// openraft's `StorageError` is intentionally large (its own crate allows this
// lint for the same reason); every storage-trait method here must return it, so
// boxing is not an option. Scope the allow to the raft subtree.
#![allow(clippy::result_large_err)]

pub mod grpc_service;
pub mod log_store;
pub mod network;
pub mod snapshot;
pub mod state_machine;

use std::sync::Arc;

use openraft::{AnyError, BasicNode, Config, Raft, StorageError, StorageIOError, TokioRuntime};

use crate::error::PdError;
use crate::journal::entry::{ApplyResult, PdEntry};
use crate::raft::log_store::PdLogStore;
use crate::raft::network::PdRaftNetworkFactory;
use crate::raft::state_machine::PdStateMachine;
use crate::state::PdState;

/// Raft replica id for a PD node (distinct from the cluster `NodeId` in `epoch-proto`).
pub type NodeId = u64;

/// The state-machine storage error returned across the PD apply / recovery paths.
///
/// An alias for openraft's [`StorageError`] keyed on the raft [`NodeId`]; naming
/// it keeps the cluster managers (which also import the cluster `NodeId`, a
/// distinct `u32`) free of an ambiguous second `NodeId` import.
pub(crate) type SmError = StorageError<NodeId>;

/// Maps a read-side failure (RocksDB or deserialization) to a state-machine
/// read error. Shared by [`state_machine`] and the cluster managers.
pub(crate) fn sm_read_err(e: impl std::error::Error + 'static) -> SmError {
    StorageIOError::read_state_machine(AnyError::new(&e)).into()
}

/// Maps a write-side failure (RocksDB or serialization) to a state-machine
/// write error. Shared by [`state_machine`] and the cluster managers.
pub(crate) fn sm_write_err(e: impl std::error::Error + 'static) -> SmError {
    StorageIOError::write_state_machine(AnyError::new(&e)).into()
}

/// A state-machine read error for a missing column family (open invariant).
pub(crate) fn missing_cf(name: &str) -> SmError {
    let io = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("missing state machine column family: {name}"),
    );
    StorageIOError::read_state_machine(AnyError::new(&io)).into()
}

/// A state-machine read error for a malformed persisted record.
pub(crate) fn sm_corrupt(msg: &str) -> SmError {
    let io = std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    StorageIOError::read_state_machine(AnyError::new(&io)).into()
}

openraft::declare_raft_types!(
    /// Application type binding for the PD raft group.
    pub PdTypeConfig:
        D = PdEntry,
        R = ApplyResult,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<Self>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

/// A running PD raft node.
pub type PdRaft = Raft<PdTypeConfig>;

/// Opens the raft stores under `dir` and starts a PD raft node **without**
/// forming or joining a cluster.
///
/// The log and state machine live in two independent RocksDB instances under
/// `dir/raft-log` and `dir/state-machine` (Q1: log churn must not compete with
/// state-machine compaction). The returned [`PdState`] is the read-side handle
/// shared with the running state machine. The caller initializes membership
/// exactly once on one replica (see [`Journal::initialize_cluster`]) or via
/// [`open_single_node`]; a restart replays the persisted membership and must not
/// re-initialize.
///
/// [`Journal::initialize_cluster`]: crate::journal::Journal::initialize_cluster
///
/// # Errors
///
/// Returns [`PdError::Rocks`] if a store cannot be opened, or [`PdError::Raft`]
/// if the raft engine fails to start.
pub async fn open_node(
    dir: &std::path::Path,
    node_id: NodeId,
) -> Result<(PdRaft, PdState), PdError> {
    let log_store = PdLogStore::open(&dir.join("raft-log"))?;
    let state = PdState::open(&dir.join("state-machine"))?;
    let state_machine = PdStateMachine::new(state.clone());

    let config = Arc::new(Config {
        cluster_name: "pd".to_string(),
        ..Default::default()
    });

    let raft = Raft::new(
        node_id,
        config,
        PdRaftNetworkFactory,
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| PdError::Raft(e.to_string()))?;

    Ok((raft, state))
}

/// Opens the raft stores under `dir` and starts a single-node PD raft group,
/// forming a one-member cluster on first start.
///
/// A thin wrapper over [`open_node`] that initializes a one-member cluster from
/// this node's own address. Multi-replica membership uses [`open_node`] plus
/// [`Journal::initialize_cluster`](crate::journal::Journal::initialize_cluster).
///
/// # Errors
///
/// Returns [`PdError::Rocks`] if a store cannot be opened, or [`PdError::Raft`]
/// if the raft engine fails to start or initialize.
pub async fn open_single_node(
    dir: &std::path::Path,
    node_id: NodeId,
    addr: impl Into<String>,
) -> Result<(PdRaft, PdState), PdError> {
    let (raft, state) = open_node(dir, node_id).await?;

    // Form the one-member cluster only on a pristine node; a restart replays
    // the persisted membership and must not re-initialize.
    let initialized = raft
        .is_initialized()
        .await
        .map_err(|e| PdError::Raft(e.to_string()))?;
    if !initialized {
        let mut members = std::collections::BTreeMap::new();
        members.insert(node_id, BasicNode::new(addr.into()));
        raft.initialize(members)
            .await
            .map_err(|e| PdError::Raft(e.to_string()))?;
    }

    Ok((raft, state))
}
