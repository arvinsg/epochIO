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

//! The Job lease driver (01 §6.2/§6.4): a leader-gated periodic sweep that
//! reassigns Jobs whose coordinator lease has lapsed. When a coordinator dies,
//! its lease expires; PD waits at least one lease period, picks a fresh
//! coordinator, and proposes a new `Assign` — the new coordinator recomputes
//! remaining work from the Job's progress watermark (01 §6.2). Subtasks are
//! idempotent, so a brief dual-coordinator window is harmless.
//!
//! Mirrors the writer-liveness ticker: leadership is re-checked every tick, and
//! the reassignment decision (which needs the clock + a fresh lease expiry) is
//! made before proposal so `apply` stays deterministic (08).
//!
//! Coordinator *selection* is a pure function here ([`pick_coordinator`]); the
//! richer data-affinity choice (01 §6.3) lands with the subtask executors.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::cluster::heartbeat::Clock;
use crate::cluster::{Node, NodeStatus, RoleSet};
use crate::job::types::Job;
use crate::journal::Journal;

/// The coordinator lease length (01 §6.1: 30s). A lease is renewed by the
/// coordinator well within this window; a lapse triggers reassignment.
pub const JOB_LEASE_MILLIS: u64 = 30_000;

/// Owns the lease-sweep task and aborts it on drop (mirrors the other PD
/// tickers' handles).
#[derive(Debug)]
pub struct JobLeaseHandle {
    task: JoinHandle<()>,
}

impl JobLeaseHandle {
    /// Stops the task.
    pub fn stop(self) {}
}

impl Drop for JobLeaseHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the leader-gated Job-lease sweep. A follower tick is a no-op.
#[must_use]
pub fn spawn_job_lease(
    journal: Arc<Journal>,
    interval: Duration,
    clock: Arc<dyn Clock>,
) -> JobLeaseHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = sweep(&journal, clock.now_millis()).await {
                tracing::warn!(error = %err, "job lease sweep failed");
            }
        }
    });
    JobLeaseHandle { task }
}

/// Spawns the leader-gated repair/decommission/balance trigger (02 §1.8 /
/// 01 §6.3): each tick it (1) starts a RepairDisk job for every newly-`Broken`
/// disk and advances a completed repair `Repairing → Repaired`; (2) starts a
/// DropDisk job for every `Draining` disk and retires a drained disk to
/// `Dropped`; (3) starts a Balance job for the most-skewed disk beyond
/// `balance_tolerance`. Every sweep checks leadership, so a follower tick is a
/// no-op.
#[must_use]
pub fn spawn_repair_trigger(
    journal: Arc<Journal>,
    interval: Duration,
    balance_tolerance: f64,
) -> JobLeaseHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match journal.sweep_broken_disks().await {
                Ok(started) if started > 0 => {
                    tracing::info!(started, "repair trigger started RepairDisk jobs");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "repair trigger sweep failed"),
            }
            match journal.sweep_completed_repairs().await {
                Ok(advanced) if advanced > 0 => {
                    tracing::info!(advanced, "repair trigger advanced disks to Repaired");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "completed-repair sweep failed"),
            }
            match journal.sweep_draining_disks().await {
                Ok(started) if started > 0 => {
                    tracing::info!(started, "trigger started DropDisk jobs");
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(error = %err, "draining sweep failed"),
            }
            match journal.sweep_balance(balance_tolerance).await {
                Ok(true) => tracing::info!("trigger started a Balance job"),
                Ok(false) => {}
                Err(err) => tracing::warn!(error = %err, "balance sweep failed"),
            }
        }
    });
    JobLeaseHandle { task }
}

/// Spawns the leader-gated InspectRound trigger (01 §6.3/§6.4): each tick, start
/// a full-cluster stripe-presence scan if none is active. The `interval` is the
/// SLO knob — it bounds the maximum exposure window of a silently-missing shard
/// (default full round ≤ 7 days, §6.4). The sweep checks leadership, so a
/// follower tick is a no-op.
#[must_use]
pub fn spawn_inspect_trigger(journal: Arc<Journal>, interval: Duration) -> JobLeaseHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match journal.sweep_inspect_round().await {
                Ok(true) => tracing::info!("inspect trigger started an InspectRound job"),
                Ok(false) => {}
                Err(err) => tracing::warn!(error = %err, "inspect trigger sweep failed"),
            }
        }
    });
    JobLeaseHandle { task }
}

/// Spawns the leader-gated GcRound trigger (01 §6.3: PD 只发 Round 号). Each
/// tick maintains the replicated round bookkeeping — completing the round that
/// has run its `interval` and opening the next — so every DataNode's self-driven
/// scan observes a well-defined, monotonic round number. A follower tick is a
/// no-op.
///
/// The clock is read here (never inside `apply`), so the round's opening
/// timestamp stays deterministic across replicas.
#[must_use]
pub fn spawn_gc_trigger(
    journal: Arc<Journal>,
    interval: Duration,
    clock: Arc<dyn Clock>,
) -> JobLeaseHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let millis = interval.as_millis().min(u64::MAX as u128) as u64;
            match journal.sweep_gc_round(millis, clock.now_millis()).await {
                Ok(true) => tracing::info!("gc trigger opened a new GcRound"),
                Ok(false) => {}
                Err(err) => tracing::warn!(error = %err, "gc trigger sweep failed"),
            }
        }
    });
    JobLeaseHandle { task }
}

/// One dispatch sweep: assign a coordinator to every Job that needs one — never
/// assigned (`Created`) as well as those whose lease lapsed before `now_millis`.
///
/// Leader-gated (a follower call is a no-op) and idempotent, so the ticker can
/// simply re-run it. Exposed so tests can drive one deterministic sweep with a
/// fixed clock instead of racing the ticker.
///
/// # Errors
///
/// Returns [`PdError`](crate::error::PdError) only if reading replicated state
/// fails; a failed `Assign` propose is logged and retried on the next sweep.
pub async fn sweep_job_assignments(
    journal: &Journal,
    now_millis: u64,
) -> Result<(), crate::error::PdError> {
    sweep(journal, now_millis).await
}

/// One sweep: assign a coordinator to every Job that needs one — never-assigned
/// (`Created`) as well as those whose lease lapsed before `now_millis`.
async fn sweep(journal: &Journal, now_millis: u64) -> Result<(), crate::error::PdError> {
    if !journal.raft().metrics().borrow().state.is_leader() {
        return Ok(());
    }
    // Snapshot the pending jobs and topology before any await (§5).
    let pending = journal.state().jobs().needing_coordinator(now_millis);
    if pending.is_empty() {
        return Ok(());
    }
    let nodes = journal.state().nodes().list();
    // Resolve each job's excluded node (a RepairDisk/DropDisk coordinator must
    // not be the failing node itself, 01 §6.3) from committed disk state.
    let disks = journal.state().disks();
    let new_expiry = now_millis.saturating_add(JOB_LEASE_MILLIS);
    for job in pending {
        let exclude = job
            .kind
            .suspect_disk()
            .and_then(|disk_id| disks.get(disk_id))
            .map(|disk| disk.node_id);
        let Some(coordinator) = pick_coordinator(&job, &nodes, exclude) else {
            tracing::warn!(job = job.id, "no eligible coordinator; retry next sweep");
            continue;
        };
        // Best-effort: a failed propose is retried next sweep (the job still
        // needs a coordinator). Idempotent — reassigning the same node is fine.
        let _ = journal.assign_job(job.id, coordinator, new_expiry).await;
    }
    Ok(())
}

/// Picks a coordinator for `job` from the live DATA nodes (01 §6.3): the
/// lowest-id live DATA node, skipping `exclude`. Returns `None` when no eligible
/// node exists. Deterministic (id-ordered).
///
/// `exclude` carries the failing node for a disk-scoped job: coordinating the
/// repair of one's own broken disk means reading survivors through the very node
/// whose hardware is suspect, and rebuilding onto it re-exposes the data to the
/// same fault. The caller resolves it from committed disk state.
#[must_use]
pub fn pick_coordinator(
    job: &Job,
    nodes: &[Node],
    exclude: Option<epoch_proto::NodeId>,
) -> Option<epoch_proto::NodeId> {
    // `job` is accepted for the forthcoming per-kind coordinator affinity; the
    // framework selector is kind-agnostic beyond the exclusion.
    let _ = job;
    nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Live && n.roles.contains(RoleSet::DATA))
        .map(|n| n.node_id)
        .filter(|id| Some(*id) != exclude)
        .min_by_key(|id| id.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::RoleSet;
    use crate::job::types::JobKind;
    use epoch_proto::{DiskId, NodeId};

    fn node(id: u32, status: NodeStatus, roles: RoleSet) -> Node {
        Node {
            node_id: NodeId::new(id),
            addr: format!("10.0.0.{id}:9000"),
            az: "az1".to_string(),
            rack: "r1".to_string(),
            roles,
            status,
        }
    }

    fn job(kind: JobKind) -> Job {
        Job {
            id: 1,
            kind,
            state: crate::job::types::JobState::Running,
            coordinator: None,
            lease_expiry_millis: 0,
            progress_watermark: 0,
        }
    }

    #[test]
    fn picks_lowest_id_live_data_node() {
        let nodes = vec![
            node(3, NodeStatus::Live, RoleSet::DATA),
            node(1, NodeStatus::Lost, RoleSet::DATA), // not live
            node(2, NodeStatus::Live, RoleSet::DATA),
            node(4, NodeStatus::Live, RoleSet::META), // not a data node
        ];
        let chosen = pick_coordinator(&job(JobKind::GcRound), &nodes, None);
        assert_eq!(chosen, Some(NodeId::new(2)));
    }

    #[test]
    fn returns_none_when_no_live_data_node() {
        let nodes = vec![
            node(1, NodeStatus::Lost, RoleSet::DATA),
            node(2, NodeStatus::Live, RoleSet::META),
        ];
        assert_eq!(
            pick_coordinator(&job(JobKind::InspectRound), &nodes, None),
            None
        );
    }

    #[test]
    fn excludes_the_suspect_node_and_falls_through_to_the_next() {
        // The lowest-id node hosts the failing disk, so the next one coordinates.
        let nodes = vec![
            node(1, NodeStatus::Live, RoleSet::DATA),
            node(2, NodeStatus::Live, RoleSet::DATA),
        ];
        let repair = job(JobKind::RepairDisk {
            disk_id: DiskId::new(9),
        });
        assert_eq!(
            pick_coordinator(&repair, &nodes, Some(NodeId::new(1))),
            Some(NodeId::new(2)),
            "the failing node must not coordinate its own disk's repair"
        );
        // With no alternative, the sweep gets None and retries next tick rather
        // than assigning the suspect node.
        let only_suspect = vec![node(1, NodeStatus::Live, RoleSet::DATA)];
        assert_eq!(
            pick_coordinator(&repair, &only_suspect, Some(NodeId::new(1))),
            None
        );
    }

    #[test]
    fn suspect_disk_covers_failure_kinds_only() {
        let disk = DiskId::new(7);
        assert_eq!(
            JobKind::RepairDisk { disk_id: disk }.suspect_disk(),
            Some(disk)
        );
        assert_eq!(
            JobKind::DropDisk { disk_id: disk }.suspect_disk(),
            Some(disk)
        );
        // Balance's disk is merely over-loaded — its node is a fine coordinator.
        assert_eq!(JobKind::Balance { disk_id: disk }.suspect_disk(), None);
        assert_eq!(JobKind::InspectRound.suspect_disk(), None);
        assert_eq!(JobKind::GcRound.suspect_disk(), None);
    }
}
