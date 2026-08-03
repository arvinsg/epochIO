//! Job model (01 §6): the PD-side coarse-grained unit of the two-level
//! scheduler. PD tracks a small `Created → Running → Done` state machine per
//! Job plus a coordinator lease and a progress watermark; the actual task
//! expansion and execution is delegated to a DataNode coordinator
//! ([`epoch-worker`](../../../epoch_worker/index.html), 02 §3). PD's raft
//! proposal count is O(jobs), independent of shard/blob counts.
//!
//! Determinism (08 raft rule): every field that enters replicated state — the
//! job id, lease expiry, coordinator choice — is chosen *before* proposal and
//! carried in the command, so `apply` reads no clock/random. The lease-expiry
//! reassignment decision (which does need the clock) runs in a leader ticker
//! and proposes an explicit `AssignJob` with the computed expiry, mirroring the
//! writer-liveness pattern.
//!
//! Design: docs/design/01-pd.md §6; docs/design/06-code-layout.md §8

use epoch_proto::{DiskId, NodeId};
use serde::{Deserialize, Serialize};

/// The coarse-grained work unit PD tracks (01 §6.3). The *trigger* that creates
/// each kind (a broken disk, an ops mark, a skew snapshot, a periodic round) and
/// the subtask *execution* land in later M7 sub-phases; this phase carries the
/// definitions and the lifecycle bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobKind {
    /// Rebuild every shard of a broken disk onto healthy disks (01 §6.3).
    RepairDisk {
        /// The failed disk whose shards must be rebuilt elsewhere.
        disk_id: DiskId,
    },
    /// Drain a disk being decommissioned (source is still readable).
    DropDisk {
        /// The disk being retired.
        disk_id: DiskId,
    },
    /// Rebalance chunk placement off a skewed disk (01 §6.3): copy some of its
    /// shards to less-loaded disks. Carries the over-loaded source disk.
    Balance {
        /// The skewed disk to move shards off.
        disk_id: DiskId,
    },
    /// A stripe-presence inspection round (quorum-gap backstop, 01 §6.3).
    InspectRound,
    /// A garbage-collection round (per-token watermark reclaim, 01 §6.3).
    GcRound,
}

impl JobKind {
    /// The disk whose *host is suspect* for this kind of job, if any.
    ///
    /// Only the failure-driven kinds qualify: coordinating the repair of one's own
    /// broken disk means reading survivors through the node whose hardware is in
    /// doubt, and rebuilding onto it re-exposes the data to the same fault
    /// (01 §6.3 「坏盘节点之外」). `Balance` also carries a disk, but that disk is
    /// merely over-loaded — its node is healthy and makes a perfectly good
    /// coordinator, so it is deliberately not listed here.
    #[must_use]
    pub fn suspect_disk(&self) -> Option<DiskId> {
        match self {
            Self::RepairDisk { disk_id } | Self::DropDisk { disk_id } => Some(*disk_id),
            Self::Balance { .. } | Self::InspectRound | Self::GcRound => None,
        }
    }
}

/// The Job lifecycle (01 §6.1): `Created → Running → Done`. A Job never moves
/// backward; a coordinator failure keeps it `Running` and triggers reassignment
/// (a fresh `AssignJob`), not a state regression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    /// Recorded, no coordinator assigned yet.
    Created,
    /// Assigned to a coordinator with a live lease.
    Running,
    /// Completed (terminal).
    Done,
}

/// One tracked Job (01 §6.1). Lives in the replicated `job` CF; the coordinator's
/// per-subtask state is *not* here — it stays in coordinator memory and is
/// recomputed from `progress_watermark` on reassignment (01 §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    /// PD-assigned job id (never reused).
    pub id: u32,
    /// What the job does.
    pub kind: JobKind,
    /// Lifecycle state.
    pub state: JobState,
    /// The assigned coordinator DataNode, once `Running`.
    pub coordinator: Option<NodeId>,
    /// Lease expiry in epoch milliseconds (0 while `Created`). PD reassigns the
    /// job once `now > lease_expiry_millis` and at least one lease period has
    /// passed (01 §6.4).
    pub lease_expiry_millis: u64,
    /// Progress checkpoint the coordinator batch-commits (01 §6.1). Opaque to
    /// PD — its meaning is per-`JobKind` (e.g. shards processed); a reassigned
    /// coordinator recomputes remaining work from it.
    pub progress_watermark: u64,
}

impl Job {
    /// Whether the coordinator lease has expired relative to `now_millis`. A
    /// `Done` job never counts as lease-expired.
    #[must_use]
    pub fn lease_expired(&self, now_millis: u64) -> bool {
        self.state == JobState::Running && now_millis > self.lease_expiry_millis
    }

    /// Whether this job needs a coordinator assigned — either it has never had
    /// one (`Created`), or its lease lapsed (01 §6.4).
    ///
    /// INVARIANT(design 01 §6.1): every created Job must eventually be assigned.
    /// A job is born `Created` with `coordinator: None` and `lease_expiry_millis:
    /// 0`, and nothing else in PD proposes `Assign` — so the lease sweep is the
    /// *only* dispatch path and must cover both cases. Gating dispatch on
    /// `lease_expired` alone left every `Created` job permanently unassigned
    /// (`ListNodeJobs` filters `Running`, so no coordinator ever saw it), which
    /// silently disabled RepairDisk, DropDisk, Balance and InspectRound: a broken
    /// disk's job was created, never ran, and pinned the disk in `Repairing`
    /// forever.
    #[must_use]
    pub fn needs_coordinator(&self, now_millis: u64) -> bool {
        match self.state {
            JobState::Created => true,
            JobState::Running => now_millis > self.lease_expiry_millis,
            JobState::Done => false,
        }
    }
}
