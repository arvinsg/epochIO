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

//! Journal propose entry point over the PD raft node.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use epoch_proto::{ChunkId, CodeMode, DiskId, NodeId, WriterToken};
use epoch_rpc::ShardTransport;
use openraft::BasicNode;

use crate::bucket::CreateBucket;
use crate::chunk::model::{CommitChunk, CreateChunkStaging, RebumpStaging, SealChunk, SlotPlan};
use crate::cluster::heartbeat::{self, Clock, HeartbeatReport, HeartbeatTracker, LivenessConfig};
use crate::cluster::{
    DiskStatus, LivenessHandle, NodeStatus, RegisterDisk, RegisterNode, RemoveNode, RoleSet,
    UpdateDiskStatus, UpdateNodeStatus,
};
use crate::config_mgr::{DeleteConfig, PutConfig};
use crate::error::PdError;
use crate::journal::entry::{ApplyResult, PdEntry};
use crate::meta_mgr::CreatePartition;
use crate::raft::{self, PdRaft};
use crate::state::PdState;
use crate::writer::{self, MarkWriterDead, RegisterWriter};

/// Owns the running PD raft node and proposes replicated mutations through it.
///
/// Also holds the shared [`PdState`] read handle, whose in-memory indexes are
/// updated by the raft state machine on apply; reads go through [`state`](Self::state).
/// The [`HeartbeatTracker`] holds leader-only, non-replicated liveness state (Q18):
/// node heartbeats feed it, and the background sweep (see [`start_liveness`](Self::start_liveness))
/// derives status transitions from it.
#[derive(Clone)]
pub struct Journal {
    raft: PdRaft,
    state: PdState,
    heartbeats: HeartbeatTracker,
}

impl Journal {
    /// Opens the raft stores under `dir` and starts a single-node PD group.
    ///
    /// # Errors
    ///
    /// Returns [`PdError`] if the stores cannot be opened or the raft node fails
    /// to start or initialize.
    pub async fn open_single_node(
        dir: &Path,
        node_id: raft::NodeId,
        addr: impl Into<String>,
    ) -> Result<Self, PdError> {
        let (raft, state) = raft::open_single_node(dir, node_id, addr).await?;
        Ok(Self {
            raft,
            state,
            heartbeats: HeartbeatTracker::new(),
        })
    }

    /// Opens the raft stores under `dir` as a cluster member **without** forming
    /// or joining a cluster; call [`initialize_cluster`](Self::initialize_cluster)
    /// on exactly one member to bootstrap.
    ///
    /// # Errors
    ///
    /// Returns [`PdError`] if the stores cannot be opened or the raft node fails
    /// to start.
    pub async fn open_member(dir: &Path, node_id: raft::NodeId) -> Result<Self, PdError> {
        let (raft, state) = raft::open_node(dir, node_id).await?;
        Ok(Self {
            raft,
            state,
            heartbeats: HeartbeatTracker::new(),
        })
    }

    /// Bootstraps the raft group from `members` (replica id → advertised address)
    /// if this node has not been initialized yet. Idempotent: a node that already
    /// carries persisted membership (a restart) returns `Ok` without re-forming.
    ///
    /// Call on exactly one replica; openraft replicates the membership entry to
    /// the others, which must already be running so the entry can commit.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::Raft`] if the node cannot be initialized.
    pub async fn initialize_cluster(
        &self,
        members: BTreeMap<raft::NodeId, String>,
    ) -> Result<(), PdError> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|e| PdError::Raft(e.to_string()))?
        {
            return Ok(());
        }
        let nodes: BTreeMap<raft::NodeId, BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        self.raft
            .initialize(nodes)
            .await
            .map_err(|e| PdError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Proposes one mutation and waits for it to be committed and applied.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] if this node is not the leader (the caller
    /// should redirect and retry), or [`PdError::Raft`] on other raft failures.
    pub async fn propose(&self, entry: PdEntry) -> Result<ApplyResult, PdError> {
        // Every PD mutation funnels through this one propose, so it is the single
        // natural place to time raft consensus (08 §5.1).
        let start = std::time::Instant::now();
        let response = self.raft.client_write(entry).await?;
        crate::metrics::record_propose(start.elapsed().as_secs_f64());
        Ok(response.data)
    }

    /// Registers a node (idempotent by address), returning its assigned id.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn register_node(
        &self,
        addr: impl Into<String>,
        az: impl Into<String>,
        rack: impl Into<String>,
        roles: RoleSet,
    ) -> Result<NodeId, PdError> {
        let command = RegisterNode {
            addr: addr.into(),
            az: az.into(),
            rack: rack.into(),
            roles,
        };
        match self.propose(PdEntry::RegisterNode(command)).await? {
            ApplyResult::NodeRegistered { node_id } => Ok(node_id),
            other => Err(PdError::Raft(format!(
                "register_node: unexpected apply result {other:?}"
            ))),
        }
    }

    /// Requests a node status transition. The result is [`ApplyResult::Applied`]
    /// on success or [`ApplyResult::Rejected`] if the transition is not allowed.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn update_node_status(
        &self,
        node_id: NodeId,
        status: NodeStatus,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::UpdateNodeStatus(UpdateNodeStatus {
            node_id,
            status,
        }))
        .await
    }

    /// Removes a decommissioned node. The result is [`ApplyResult::Applied`] on
    /// success or [`ApplyResult::Rejected`] if the node is absent or not
    /// decommissioned.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn remove_node(&self, node_id: NodeId) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::RemoveNode(RemoveNode { node_id }))
            .await
    }

    /// Registers a disk (idempotent by `(node_id, path)`). The result is
    /// [`ApplyResult::DiskRegistered`] with the assigned id, or
    /// [`ApplyResult::Rejected`] with `NodeNotFound` if the owning node is
    /// unknown.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn register_disk(
        &self,
        node_id: NodeId,
        az: impl Into<String>,
        rack: impl Into<String>,
        path: impl Into<String>,
        total: u64,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::RegisterDisk(RegisterDisk {
            node_id,
            az: az.into(),
            rack: rack.into(),
            path: path.into(),
            total,
        }))
        .await
    }

    /// Requests a disk status transition. The result is [`ApplyResult::Applied`]
    /// on success or [`ApplyResult::Rejected`] if the disk is absent or the
    /// transition is not allowed.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn update_disk_status(
        &self,
        disk_id: DiskId,
        status: DiskStatus,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::UpdateDiskStatus(UpdateDiskStatus {
            disk_id,
            status,
        }))
        .await
    }

    /// The last heartbeat free bytes reported for a disk, or `None` if its
    /// owning node has not reported it (read by the writable-set filter, 01 §4.2).
    #[must_use]
    pub fn disk_free(&self, node_id: NodeId, disk_id: DiskId) -> Option<u64> {
        self.heartbeats
            .disk_stats(node_id, disk_id)
            .map(|stats| stats.free)
    }

    /// The last reported writable-extent count for a disk, or `0` if its owning
    /// node has not reported it — the creation watermark input (01 §4.1).
    #[must_use]
    pub fn disk_writable_extents(&self, node_id: NodeId, disk_id: DiskId) -> u32 {
        self.heartbeats
            .disk_stats(node_id, disk_id)
            .map_or(0, |stats| stats.writable_extents)
    }

    /// The full per-node heartbeat snapshot (last-seen + per-disk stats), in
    /// node-id order. Leader-only, volatile liveness state (Q18); the Web Console
    /// reads it for the data-node capacity/health view (08 §5). A follower's
    /// tracker is empty (heartbeats target the leader).
    #[must_use]
    pub fn heartbeat_snapshot(&self) -> Vec<heartbeat::NodeStatsSnapshot> {
        self.heartbeats.snapshot()
    }

    /// Borrows the shared read-side state (topology indexes updated on apply).
    #[must_use]
    pub fn state(&self) -> &PdState {
        &self.state
    }

    /// Issues a fresh, never-reused writer token to a gateway node. The result
    /// is [`ApplyResult::WriterRegistered`] with the token, or
    /// [`ApplyResult::Rejected`] with `NodeNotFound` if the gateway node is
    /// unknown (01 §4.3).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn register_writer(&self, node_id: NodeId) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::RegisterWriter(RegisterWriter { node_id }))
            .await
    }

    /// Retires a writer token (`Live → Dead`, one-way). The result is
    /// [`ApplyResult::Applied`] (also for an already-Dead token, idempotent) or
    /// [`ApplyResult::Rejected`] with `NotFound` if the token is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn mark_writer_dead(&self, token: WriterToken) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::MarkWriterDead(MarkWriterDead { token }))
            .await
    }

    /// Runs one writer-liveness sweep on the leader: retires every live token
    /// whose owning gateway's heartbeat has lapsed past `dead_after_millis`,
    /// returning how many retirements were proposed. A follower's sweep is a
    /// no-op. The running ticker that calls this periodically lands with the PD
    /// node role.
    ///
    /// `now_millis` is the leader's observation time; it never enters replicated
    /// state (Q18 / AGENTS §8). Each proposal is best-effort — a `NotLeader`
    /// result or a rejection is ignored, because the next sweep re-derives from
    /// committed state.
    ///
    /// # Errors
    ///
    /// Never returns an error today (proposals are best-effort); the `Result`
    /// leaves room for a future hard-failure signal without a signature change.
    pub async fn sweep_dead_writers(
        &self,
        now_millis: u64,
        dead_after_millis: u64,
    ) -> Result<usize, PdError> {
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(0);
        }
        // Derive the retirements first, releasing the heartbeat read before any
        // awaited proposal below (§5: no lock/borrow held across `.await`).
        let plan = writer::dead_writer_plan(
            &self.state.writers().live_sessions(),
            |node_id| self.heartbeats.staleness(node_id, now_millis),
            dead_after_millis,
        );
        let count = plan.len();
        for cmd in plan {
            let _ = self.propose(PdEntry::MarkWriterDead(cmd)).await;
        }
        Ok(count)
    }

    /// Starts a RepairDisk job for every disk a heartbeat flags Broken that is
    /// still `Normal` in replicated state (02 §1.8 / 01 §6.3). Leader-only.
    ///
    /// For each such disk: propose `UpdateDiskStatus(Broken)` → `create_job`
    /// (RepairDisk) → `UpdateDiskStatus(Repairing)`. Idempotent — a disk already
    /// past `Normal` is skipped, so a re-flagged disk never spawns a second job.
    /// Returns the number of repair jobs started this sweep.
    ///
    /// # Errors
    ///
    /// Never returns `Err` for a single disk's failure — each disk is best-effort
    /// and retried next sweep; the `Result` mirrors the other sweep helpers.
    pub async fn sweep_broken_disks(&self) -> Result<usize, PdError> {
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(0);
        }
        let broken = self.heartbeats.broken_disks();
        let mut started = 0;
        for (_node, disk_id) in broken {
            // Only a Normal disk is a fresh break; skip anything already in the
            // repair lifecycle (Broken/Repairing/Repaired/Dropped).
            match self.state().disks().get(disk_id) {
                Some(disk) if disk.status == crate::cluster::DiskStatus::Normal => {}
                _ => continue,
            }
            if self
                .update_disk_status(disk_id, crate::cluster::DiskStatus::Broken)
                .await
                .is_err()
            {
                continue; // retried next sweep
            }
            let _ = self
                .create_job(crate::job::types::JobKind::RepairDisk { disk_id })
                .await;
            let _ = self
                .update_disk_status(disk_id, crate::cluster::DiskStatus::Repairing)
                .await;
            started += 1;
        }
        Ok(started)
    }

    /// Advances a disk `Repairing → Repaired` once its RepairDisk job is `Done`
    /// and every shard it held has been rebound elsewhere (01 §1 disk lifecycle /
    /// §6.3). Leader-only.
    ///
    /// The completion signal is not the job's `Done` state alone but the stronger
    /// invariant that PD's own mapping no longer places any shard on the disk
    /// (`shard_slots_on_disk(disk_id)` empty) — each rebuilt shard's
    /// `CommitShardMapping` moved its slot off the broken disk (01 §6.4). A disk
    /// with shards still bound stays `Repairing` (a partial or in-flight repair).
    ///
    /// The disk is left `Repaired`, not returned to `Normal`: the DataNode's
    /// broken flag is sticky (a genuine hardware fault), so re-normalizing would
    /// loop back through `Broken` on the next heartbeat. Re-joining a physically
    /// replaced disk as `Normal` is an operator action (a follow-up admin path).
    ///
    /// # Errors
    ///
    /// Never returns `Err` for a single disk's failure — each is best-effort and
    /// retried next sweep; the `Result` mirrors [`sweep_broken_disks`](Self::sweep_broken_disks).
    pub async fn sweep_completed_repairs(&self) -> Result<usize, PdError> {
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(0);
        }
        // Snapshot the Done RepairDisk jobs' disks before any await (§5).
        let repaired_disks: Vec<DiskId> = self
            .state()
            .jobs()
            .list()
            .into_iter()
            .filter(|job| job.state == crate::job::types::JobState::Done)
            .filter_map(|job| match job.kind {
                crate::job::types::JobKind::RepairDisk { disk_id } => Some(disk_id),
                _ => None,
            })
            .collect();

        let mut advanced = 0;
        for disk_id in repaired_disks {
            // Only a disk still `Repairing` with no shards left bound to it is a
            // completed repair; anything else (already Repaired/Dropped, or shards
            // still bound) is skipped — idempotent and safe to re-run each sweep.
            match self.state().disks().get(disk_id) {
                Some(disk) if disk.status == crate::cluster::DiskStatus::Repairing => {}
                _ => continue,
            }
            if !self
                .state()
                .chunks()
                .shard_slots_on_disk(disk_id)
                .is_empty()
            {
                continue; // repair not fully rebound yet
            }
            if self
                .update_disk_status(disk_id, crate::cluster::DiskStatus::Repaired)
                .await
                .is_ok()
            {
                advanced += 1;
            }
        }
        Ok(advanced)
    }

    /// Starts a fresh InspectRound job if none is currently active (01 §6.3 /
    /// §6.4 correctness backstop). Leader-only.
    ///
    /// InspectRound is a periodic full-cluster stripe-presence scan; only one
    /// need run at a time (a coordinator walks every chunk in segments, resuming
    /// from its watermark on reassignment). This creates a new round only when no
    /// `Created`/`Running` InspectRound exists — a completed round's `Done` job
    /// does not block the next. Returns `true` if a round was started.
    ///
    /// # Errors
    ///
    /// Never returns `Err`: a failed create is retried next tick.
    pub async fn sweep_inspect_round(&self) -> Result<bool, PdError> {
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(false);
        }
        let active = self.state().jobs().list().into_iter().any(|job| {
            matches!(job.kind, crate::job::types::JobKind::InspectRound)
                && job.state != crate::job::types::JobState::Done
        });
        if active {
            return Ok(false);
        }
        Ok(self
            .create_job(crate::job::types::JobKind::InspectRound)
            .await
            .is_ok())
    }

    /// GcRound bookkeeping (01 §6.3: PD 只发 Round 号). GcRound is the only
    /// self-driven Job kind — every DataNode scans its own disk against the
    /// MetaNode keep-set (02 §3.2), with no coordinator pulling subtasks. So PD
    /// never assigns it a coordinator; instead this sweep maintains exactly one
    /// *open* GcRound whose replicated Job id is the round number.
    ///
    /// Each call: if no open GcRound exists, complete the previous round (if it
    /// has been open at least `interval` — enough wall-clock for a node round to
    /// have run) and open a new one. A completed round does not block the next
    /// (mirrors `sweep_inspect_round`). Returns `true` when a new round opened.
    ///
    /// `now_millis` is supplied by the caller so `apply` stays clock-free; the
    /// auto-complete check reads committed state only.
    ///
    /// # Errors
    ///
    /// Never returns `Err`: a failed propose is retried on the next tick.
    pub async fn sweep_gc_round(
        &self,
        interval_millis: u64,
        now_millis: u64,
    ) -> Result<bool, PdError> {
        use crate::job::types::{JobKind, JobState};
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(false);
        }
        let mut open_round: Option<crate::job::types::Job> = None;
        for job in self.state().jobs().list() {
            if matches!(job.kind, JobKind::GcRound) && job.state != JobState::Done {
                open_round = Some(job);
            }
        }
        if let Some(round) = open_round {
            // The round stays open for one full interval so every node's
            // self-driven scan can observe it; after that it is complete by
            // construction (each node's own round is idempotent, 02 §3.2).
            let opened_at = round.progress_watermark;
            if now_millis.saturating_sub(opened_at) < interval_millis {
                return Ok(false);
            }
            let _ = self.complete_job(round.id).await;
        }
        // Open the next round; its Job id is the round number, and we record the
        // opening wall-clock on the watermark so the next sweep knows when this
        // round has run its interval (the watermark is opaque, per-Kind state,
        // 01 §6.1 — GcRound uses it as "opened_at millis").
        match self.create_job(JobKind::GcRound).await {
            Ok(ApplyResult::JobCreated { job_id }) => {
                let _ = self
                    .advance_job_watermark(job_id, now_millis, now_millis)
                    .await;
                Ok(true)
            }
            // A concurrent create (or a rejected propose) is retried next tick.
            _ => Ok(false),
        }
    }

    /// Marks a disk for decommission (01 §6.3 DropDisk): transitions it
    /// `Normal → Draining` (excluded from placement, still readable). Leader-only.
    /// Idempotent — a disk already past `Normal` returns `false`.
    ///
    /// The DropDisk job that copies its shards off is started by
    /// [`sweep_draining_disks`](Self::sweep_draining_disks), mirroring how a
    /// broken disk's RepairDisk job is started by the sweep (so the trigger has a
    /// single owner and stays idempotent across retries).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn mark_disk_draining(&self, disk_id: DiskId) -> Result<bool, PdError> {
        match self.state().disks().get(disk_id) {
            Some(disk) if disk.status == DiskStatus::Normal => {}
            _ => return Ok(false),
        }
        Ok(matches!(
            self.update_disk_status(disk_id, DiskStatus::Draining)
                .await?,
            ApplyResult::Applied
        ))
    }

    /// Starts a DropDisk job for every `Draining` disk that has no active one,
    /// and advances a drained disk `Draining → Dropped` once its DropDisk job is
    /// `Done` and all its shards have been copied off (mirrors
    /// [`sweep_completed_repairs`](Self::sweep_completed_repairs)). Leader-only.
    ///
    /// # Errors
    ///
    /// Never returns `Err`: each disk is best-effort and retried next sweep.
    pub async fn sweep_draining_disks(&self) -> Result<usize, PdError> {
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(0);
        }
        // Snapshot state before any await (§5).
        let draining: Vec<DiskId> = self
            .state()
            .disks()
            .list()
            .into_iter()
            .filter(|d| d.status == DiskStatus::Draining)
            .map(|d| d.disk_id)
            .collect();
        let jobs = self.state().jobs().list();
        let mut started = 0;
        for disk_id in draining {
            let has_job = jobs.iter().any(|job| {
                matches!(job.kind, crate::job::types::JobKind::DropDisk { disk_id: d } if d == disk_id)
                    && job.state != crate::job::types::JobState::Done
            });
            let done = jobs.iter().any(|job| {
                matches!(job.kind, crate::job::types::JobKind::DropDisk { disk_id: d } if d == disk_id)
                    && job.state == crate::job::types::JobState::Done
            });
            if !has_job && !done {
                // No DropDisk job yet: start one.
                if self
                    .create_job(crate::job::types::JobKind::DropDisk { disk_id })
                    .await
                    .is_ok()
                {
                    started += 1;
                }
            } else if done
                && self
                    .state()
                    .chunks()
                    .shard_slots_on_disk(disk_id)
                    .is_empty()
            {
                // The drop completed and every shard was copied off → retire it.
                let _ = self.update_disk_status(disk_id, DiskStatus::Dropped).await;
            }
        }
        Ok(started)
    }

    /// Starts a Balance job for the most-skewed `Normal` disk when its shard
    /// count exceeds the fleet average by more than `tolerance` (e.g. 0.2 = 20%),
    /// if no Balance job is active (01 §6.3 容量均衡). Leader-only, at most one
    /// Balance job at a time. Returns `true` if a job was started.
    ///
    /// Skew is measured by committed shard count per disk (a deterministic proxy
    /// for capacity that needs no heartbeat statistics); the executor copies some
    /// of the source disk's shards to less-loaded disks via the shared DropDisk
    /// copy path.
    ///
    /// # Errors
    ///
    /// Never returns `Err`: a failed create is retried next tick.
    pub async fn sweep_balance(&self, tolerance: f64) -> Result<bool, PdError> {
        if !self.raft.metrics().borrow().state.is_leader() {
            return Ok(false);
        }
        let active = self.state().jobs().list().into_iter().any(|job| {
            matches!(job.kind, crate::job::types::JobKind::Balance { .. })
                && job.state != crate::job::types::JobState::Done
        });
        if active {
            return Ok(false);
        }
        // Per-`Normal`-disk committed shard counts (deterministic capacity proxy).
        let disks: Vec<DiskId> = self
            .state()
            .disks()
            .list()
            .into_iter()
            .filter(|d| d.status == DiskStatus::Normal)
            .map(|d| d.disk_id)
            .collect();
        if disks.len() < 2 {
            return Ok(false); // nowhere to move shards to
        }
        let counts: Vec<(DiskId, usize)> = disks
            .iter()
            .map(|&id| (id, self.state().chunks().shard_slots_on_disk(id).len()))
            .collect();
        let total: usize = counts.iter().map(|(_, c)| *c).sum();
        let avg = total as f64 / counts.len() as f64;
        // The most-loaded disk, and whether it exceeds the tolerance band.
        let Some(&(skewed, max)) = counts.iter().max_by_key(|(_, c)| *c) else {
            return Ok(false);
        };
        if avg <= 0.0 || (max as f64) <= avg * (1.0 + tolerance) {
            return Ok(false); // balanced enough
        }
        Ok(self
            .create_job(crate::job::types::JobKind::Balance { disk_id: skewed })
            .await
            .is_ok())
    }

    /// Begins two-phase chunk creation. The result is [`ApplyResult::ChunkStaged`]
    /// with the allocated chunk id, or [`ApplyResult::Rejected`] if the plan is
    /// malformed or a target disk is unavailable.
    ///
    /// `create_ts` and `epoch` are chosen by the caller (the leader) so apply
    /// stays deterministic (AGENTS §8).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn create_chunk_staging(
        &self,
        code_mode: CodeMode,
        slots: Vec<SlotPlan>,
        create_ts: i64,
        epoch: u32,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::CreateChunkStaging(CreateChunkStaging {
            code_mode,
            slots,
            create_ts,
            epoch,
        }))
        .await
    }

    /// Promotes a staging plan to a committed `Writable` chunk. The result is
    /// [`ApplyResult::Applied`] on success or [`ApplyResult::Rejected`] if no
    /// staging plan exists for the id.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn commit_chunk(&self, chunk_id: ChunkId) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::CommitChunk(CommitChunk { chunk_id }))
            .await
    }

    /// Bumps a staging plan's shard epoch on crash recovery. The result is
    /// [`ApplyResult::Applied`] on success or [`ApplyResult::Rejected`] if no
    /// staging plan exists for the id.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn rebump_staging(&self, chunk_id: ChunkId) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::RebumpStaging(RebumpStaging { chunk_id }))
            .await
    }

    /// Seals a committed chunk (01 §4.2): first the replicated status
    /// transition to `Sealed` (rejections surface as-is), then a best-effort
    /// `SealExtent` fanout to every shard's DataNode over the data-plane RPC
    /// (drain in-flight writes, reject new OPENs). Fanout failures are logged,
    /// not fatal — the replicated seal is the fence; the extent-level seals
    /// complete when the gateway-visible seal takes effect or on operator retry.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn seal_chunk(&self, chunk_id: ChunkId) -> Result<ApplyResult, PdError> {
        let result = self
            .propose(PdEntry::SealChunk(SealChunk { chunk_id }))
            .await?;
        if result != ApplyResult::Applied {
            return Ok(result);
        }
        // Fanout SealExtent to every shard's DataNode (best-effort). The
        // transport is rebuilt from committed topology; failures retry on the
        // operator's next attempt or via the gateway's Sealed-driven refresh.
        let Some(chunk) = self.state.chunks().get(chunk_id) else {
            return Ok(result);
        };
        let nodes = self.state.nodes().list();
        let transport = crate::chunk::driver::build_transport(&nodes);
        for slot in &chunk.shards {
            let Some(node_id) = self.state.disks().get(slot.disk_id).map(|d| d.node_id) else {
                tracing::warn!(disk = slot.disk_id.get(), "seal fanout: shard disk unknown");
                continue;
            };
            if let Err(err) = transport
                .seal(
                    node_id,
                    epoch_rpc::SealReq {
                        extent_id: slot.extent_id,
                    },
                )
                .await
            {
                tracing::warn!(
                    chunk = chunk_id.get(),
                    node = node_id.get(),
                    error = %err,
                    "seal fanout to data node failed"
                );
            }
        }
        Ok(result)
    }

    /// Creates a bucket (idempotent by name). The result is
    /// [`ApplyResult::BucketCreated`] with the (never-reused) bucket id.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn create_bucket(
        &self,
        name: impl Into<String>,
        ns_mode: crate::bucket::NsMode,
        inline_threshold: Option<u64>,
        codemode_id: u16,
        engine: crate::bucket::MetaEngine,
        created_at: i64,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::CreateBucket(CreateBucket {
            name: name.into(),
            ns_mode,
            inline_threshold,
            codemode_id,
            engine,
            created_at,
        }))
        .await
    }

    /// Tombstones a bucket for deletion (99-Q15): flips it to `Deleting`, after
    /// which the gateway refuses new writes. Returns [`ApplyResult::BucketDeleted`]
    /// with the id, or [`ApplyResult::Rejected`] if the name is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn tombstone_bucket(&self, name: &str) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::TombstoneBucket(crate::bucket::TombstoneBucket {
            name: name.to_string(),
        }))
        .await
    }

    /// Purges a tombstoned bucket's identity record after every partition has
    /// purged its records. Returns [`ApplyResult::BucketDeleted`] with the id, or
    /// [`ApplyResult::Rejected`] if the bucket is absent or not tombstoned.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn purge_bucket(&self, name: &str) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::PurgeBucket(crate::bucket::PurgeBucket {
            name: name.to_string(),
        }))
        .await
    }

    /// Creates a MetaNode range partition (01 §5). The result is
    /// [`ApplyResult::PartitionCreated`] with the (never-reused) partition id,
    /// or [`ApplyResult::Rejected`] with `NodeNotFound` if a peer is unknown
    /// or lacks the META role. The CreateRaftGroup fan-out to the chosen
    /// MetaNodes is driven by the service path after the proposal commits.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn create_partition(
        &self,
        ns: crate::bucket::NsMode,
        start: crate::meta_mgr::PartitionBound,
        end: crate::meta_mgr::PartitionBound,
        peers: Vec<NodeId>,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::CreatePartition(CreatePartition {
            ns,
            start,
            end,
            peers,
        }))
        .await
    }

    /// Splits a MetaNode range partition at `at` (03 §2 分裂 = 纯路由变更):
    /// narrows the parent to `[start, at)` and inserts the right child covering
    /// `[at, end)` with a fresh id, bumping both route epochs. The matching
    /// in-log split on the MetaNode side is driven separately (an admin push);
    /// this only mutates PD's route table.
    ///
    /// Returns [`ApplyResult::PartitionSplit`] with the child id, or
    /// [`ApplyResult::Rejected`] if `at` is not strictly inside the parent.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn split_partition(
        &self,
        parent_id: u64,
        at: crate::meta_mgr::PartitionBound,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::SplitPartition(crate::meta_mgr::SplitPartition {
            parent_id,
            at,
        }))
        .await
    }

    /// Migrates a MetaNode partition replica (`from` → `to`, 03 §2): swaps the
    /// voter in PD's route record and bumps the route epoch. Idempotent — a swap
    /// already reflected in `peers` is a no-op. The actual openraft membership
    /// change (data movement) is driven separately (an admin push).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn migrate_partition(
        &self,
        partition_id: u64,
        from: NodeId,
        to: NodeId,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::MigratePartition(
            crate::meta_mgr::MigratePartition {
                partition_id,
                from,
                to,
            },
        ))
        .await
    }

    /// Writes a cluster-level config KV entry (01 §1 配置中心).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn put_config(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::PutConfig(PutConfig {
            key: key.into(),
            value,
        }))
        .await
    }

    /// Creates a two-level-scheduler Job (01 §6): records it `Created` with a
    /// fresh id. Returns [`ApplyResult::JobCreated`] with the id.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn create_job(
        &self,
        kind: crate::job::types::JobKind,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::Job(crate::job::JobCommand::Create { kind }))
            .await
    }

    /// Assigns (or reassigns) a Job to a coordinator with a lease expiry
    /// (01 §6.1/§6.2). `lease_expiry_millis` is computed by the caller before
    /// proposal (the clock never enters `apply`).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn assign_job(
        &self,
        job_id: u32,
        coordinator: NodeId,
        lease_expiry_millis: u64,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::Job(crate::job::JobCommand::Assign {
            job_id,
            coordinator,
            lease_expiry_millis,
        }))
        .await
    }

    /// Renews a running Job's coordinator lease (01 §6.1).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn renew_job_lease(
        &self,
        job_id: u32,
        lease_expiry_millis: u64,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::Job(crate::job::JobCommand::RenewLease {
            job_id,
            lease_expiry_millis,
        }))
        .await
    }

    /// Advances a Job's progress watermark (batch checkpoint, 01 §6.1;
    /// monotonic — a lower value is ignored).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn advance_job_watermark(
        &self,
        job_id: u32,
        watermark: u64,
        lease_expiry_millis: u64,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::Job(crate::job::JobCommand::AdvanceWatermark {
            job_id,
            watermark,
            lease_expiry_millis,
        }))
        .await
    }

    /// Marks a Job `Done` (terminal, idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn complete_job(&self, job_id: u32) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::Job(crate::job::JobCommand::Complete { job_id }))
            .await
    }

    /// Commits a repair coordinator's shard-mapping rebind (01 §6.4): rebinds the
    /// slot to the rebuilt extent on `new_disk` and bumps its epoch, gated by the
    /// coordinator's Job authorization. Returns [`ApplyResult::ShardRebound`] with
    /// the new epoch, or [`ApplyResult::Rejected`] if unauthorized / stale.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn commit_shard_mapping(
        &self,
        cmd: crate::chunk::model::CommitShardMapping,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::CommitShardMapping(cmd)).await
    }

    /// Reports a bad/missing shard, creating a ShardRepair ticket (01 §6.3): the
    /// heal-on-read / scrub fast path. PD resolves the target node + epoch from
    /// committed state. Returns [`ApplyResult::ShardRepairReported`], or
    /// [`ApplyResult::Rejected`] if the reported shard is unknown.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn report_shard_repair(
        &self,
        cmd: crate::shard_repair::ReportShardRepair,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::ReportShardRepair(cmd)).await
    }

    /// Commits a completed single-shard repair (01 §6.3): rebinds the slot to the
    /// rebuilt extent and bumps its epoch, gated by a matching ShardRepair ticket
    /// (not a Job, 01 §6.4 invariant 2). Returns [`ApplyResult::ShardRebound`]
    /// with the new epoch, or [`ApplyResult::Rejected`] if unauthorized / stale.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn commit_shard_repair(
        &self,
        cmd: crate::shard_repair::CommitShardRepair,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::CommitShardRepair(cmd)).await
    }

    /// Deletes a cluster-level config KV entry (idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn delete_config(&self, key: impl Into<String>) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::DeleteConfig(DeleteConfig { key: key.into() }))
            .await
    }

    /// Stores (creates or replaces) an access-key credential (01 §6 认证 N5).
    ///
    /// # Errors
    ///
    /// Returns [`PdError::NotLeader`] or [`PdError::Raft`] as [`propose`](Self::propose).
    pub async fn put_credential(
        &self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        allowed_buckets: Option<Vec<String>>,
        role: crate::credential::ConsoleRole,
    ) -> Result<ApplyResult, PdError> {
        self.propose(PdEntry::PutCredential(crate::credential::PutCredential {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            allowed_buckets,
            role,
        }))
        .await
    }

    /// Records a node heartbeat into the leader-only tracker (non-replicated).
    ///
    /// `now_millis` is the leader's observation time; it never enters replicated
    /// state (Q18 / AGENTS §8). The background sweep turns staleness into status
    /// transitions.
    pub fn record_heartbeat(&self, report: &HeartbeatReport, now_millis: u64) {
        self.heartbeats.record(report, now_millis);
    }

    /// Refreshes a node's last-seen time from a writer heartbeat (01 §4.3):
    /// liveness proof only, no capacity report (Q18: leader memory, not raft).
    pub fn record_writer_seen(&self, node_id: epoch_proto::NodeId, now_millis: u64) {
        self.heartbeats.touch(node_id, now_millis);
    }

    /// Records a writer token's GC commit watermark `W(t)` from its heartbeat
    /// (Q27, leader memory only — never raft). A GcRound reads it to decide which
    /// `(token, seq ≤ W)` blobs are reclaimable.
    pub fn record_writer_watermark(&self, token: epoch_proto::WriterToken, watermark: i64) {
        self.heartbeats.record_watermark(token, watermark);
    }

    /// The recorded GC commit watermark for `token`, or `-1` if unreported.
    #[must_use]
    pub fn writer_watermark(&self, token: epoch_proto::WriterToken) -> i64 {
        self.heartbeats.watermark(token)
    }

    /// The live-token table a GcRound needs (01 §6.3 / Q27): every `Live` writer
    /// token, its owning node, and its reported commit watermark `W(t)`. A blob
    /// `(t, s)` is reclaimable iff `t` is absent here (dead + grace, caught by
    /// the caller) or `s ≤ W(t)`. Leader-memory (Q18): the watermarks are
    /// volatile, so this is only meaningful on the leader.
    #[must_use]
    pub fn live_writers_with_watermark(&self) -> Vec<(epoch_proto::WriterToken, NodeId, i64)> {
        self.state()
            .writers()
            .live_sessions()
            .into_iter()
            .map(|(token, node)| (token, node, self.heartbeats.watermark(token)))
            .collect()
    }

    /// Spawns the background liveness ticker over this journal's raft node and
    /// heartbeat tracker, returning a handle that stops it on drop.
    ///
    /// The ticker is leader-only at sweep time; a follower's sweep is a no-op.
    #[must_use]
    pub fn start_liveness(&self, config: LivenessConfig, clock: Arc<dyn Clock>) -> LivenessHandle {
        heartbeat::spawn_liveness(
            self.raft.clone(),
            self.state.clone(),
            self.heartbeats.clone(),
            config,
            clock,
        )
    }

    /// Spawns the background writer-liveness sweep: on each tick, the leader
    /// retires `Live` tokens whose gateway heartbeat lapsed past
    /// `dead_after_millis` ([`sweep_dead_writers`](Self::sweep_dead_writers)).
    ///
    /// INVARIANT(design 01 §4.3): a Dead token never revives — the sweep is the
    /// PD half of the liveness contract; the gateway half is its session's
    /// stale gate. The returned handle owns the task (§5: no detached task).
    #[must_use]
    pub fn start_writer_liveness(
        &self,
        sweep_interval: std::time::Duration,
        dead_after_millis: u64,
        clock: Arc<dyn Clock>,
    ) -> WriterLivenessHandle {
        let journal = self.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(sweep_interval);
            loop {
                ticker.tick().await;
                match journal
                    .sweep_dead_writers(clock.now_millis(), dead_after_millis)
                    .await
                {
                    Ok(0) | Err(_) => {}
                    Ok(retired) => tracing::info!(retired, "retired stale writer tokens"),
                }
            }
        });
        WriterLivenessHandle { task }
    }

    /// Borrows the underlying raft handle (metrics, leadership, triggers).
    #[must_use]
    pub fn raft(&self) -> &PdRaft {
        &self.raft
    }

    /// Stops the raft node, releasing its background tasks and store locks.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::Raft`] if the raft core task cannot be joined.
    pub async fn shutdown(&self) -> Result<(), PdError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| PdError::Raft(e.to_string()))
    }
}

/// Owns the background writer-liveness sweep task and aborts it on drop.
pub struct WriterLivenessHandle {
    task: tokio::task::JoinHandle<()>,
}

impl WriterLivenessHandle {
    /// Stops the sweep, aborting the background task.
    pub fn stop(self) {
        self.task.abort();
    }
}

impl Drop for WriterLivenessHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
