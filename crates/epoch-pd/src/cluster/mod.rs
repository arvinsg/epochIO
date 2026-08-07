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

//! Cluster membership: node and disk registration, status state machines, and
//! the in-memory topology indexes served to the read path.
//!
//! The managers own their RocksDB column families and apply replicated commands
//! dispatched from the state machine. INVARIANT(design 01 §2 / Q18): apply is
//! deterministic — it never reads wall-clock time, randomness, or heartbeat
//! statistics, so every replica derives the same state from the same log.
//!
//! Design: docs/design/01-pd.md §1 (cluster membership); §3 (Node / Disk model)

pub mod disk;
pub mod heartbeat;
pub(crate) mod id_record;
pub mod node;

pub use disk::{Disk, DiskManager, DiskStatus, RegisterDisk, UpdateDiskStatus};
pub use heartbeat::{
    Clock, DiskHeartbeat, DiskStats, HeartbeatReport, HeartbeatTracker, LivenessConfig,
    LivenessHandle, NodeStatsSnapshot, SystemClock,
};
pub use node::{
    Node, NodeManager, NodeStatus, RegisterNode, RemoveNode, RoleSet, UpdateNodeStatus,
};

use serde::{Deserialize, Serialize};

/// Why a replicated cluster mutation was rejected while being applied.
///
/// Carried by [`ApplyResult::Rejected`](crate::journal::entry::ApplyResult::Rejected)
/// so the proposing caller can tell a benign rejection from a storage failure.
/// A rejection is a normal, deterministic apply outcome — the command was valid
/// raft input but is not applicable to the current committed state — and is not
/// an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// The target node or disk id is not present in the cluster.
    NotFound,
    /// The requested status transition is not permitted from the current status
    /// (see [`NodeStatus::can_transition_to`] / [`DiskStatus::can_transition_to`]).
    InvalidTransition,
    /// A node removal was requested for a node that is not `Decommissioned`.
    NotRemovable,
    /// A disk registration referenced a node that is not present in the cluster.
    NodeNotFound,
    /// A chunk creation plan is malformed (wrong slot count, or a shard index is
    /// duplicated or out of range for the code mode).
    InvalidChunkPlan,
    /// A chunk creation plan referenced a disk that is not present or not
    /// `Normal` (usable) in the cluster.
    DiskUnavailable,
    /// A shard rebind would collocate two shards of one chunk in a single fault
    /// domain (node, or rack under rack-aware placement), turning EC redundancy
    /// into a correlated-failure risk (01 §4.1).
    AffinityViolation,
}
