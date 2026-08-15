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

//! Watermark-driven chunk creation loop (design 01 §4.1).
//!
//! The PD leader keeps every `Normal` disk's writable-extent watermark at or
//! above the target K: a periodic sweep finds disks below target and creates
//! new chunks placed on the lowest-watermark disks (topology anti-affinity), so
//! a freshly added disk — watermark 0 — is filled first and every disk can
//! accept writes concurrently.
//!
//! Each creation runs the two-phase protocol (01 §4.1): staging propose →
//! per-slot `create_extent` on the owning DataNode (data-plane RPC, with the
//! staged deterministic identity) → commit propose. A failed creation is
//! rebumped (epoch jump) and retried on the next sweep; staging plans left by a
//! leader crash are resumed the same way, so no manual cleanup is ever needed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use epoch_proto::{CodeMode, DiskId};
use epoch_rpc::{CreateExtentReq, RemoteTransport, ShardTransport};
use tokio::task::JoinHandle;

use crate::chunk::model::SlotPlan;
use crate::chunk::placement::{AntiAffinity, DiskCandidate, plan_placement};
use crate::cluster::heartbeat::Clock;
use crate::cluster::{DiskStatus, Node, NodeStatus, RoleSet};
use crate::error::PdError;
use crate::journal::{ApplyResult, Journal};

/// Tuning for the watermark-driven creation loop (01 §4.1).
#[derive(Debug, Clone, Copy)]
pub struct PlacementConfig {
    /// Sweep interval.
    pub interval: Duration,
    /// Per-disk writable-extent target watermark (K; 01 §4.1).
    pub writable_target: u32,
    /// Creation cap per sweep (backpressure against creation bursts).
    pub max_creations_per_cycle: usize,
    /// Topology anti-affinity enforced by placement.
    pub affinity: AntiAffinity,
    /// Extent-creation attempts per staged chunk per sweep (rebump + retry).
    pub max_create_attempts: u32,
    /// Free bytes a disk must report to receive a new chunk (01 §4.1).
    ///
    /// Shares the writable-set publish threshold's value: a disk that would be
    /// pulled from the writable set for lack of space should not be given new
    /// chunks either, and two different numbers would let placement fill disks
    /// that publishing then immediately hides.
    pub min_free_bytes: u64,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            writable_target: 8,
            max_creations_per_cycle: 4,
            affinity: AntiAffinity::HostAware,
            max_create_attempts: 3,
            min_free_bytes: crate::chunk::writable_set::DEFAULT_MIN_FREE_BYTES,
        }
    }
}

/// Owns the background placement task and aborts it on drop (mirrors
/// [`crate::cluster::LivenessHandle`]).
#[derive(Debug)]
pub struct PlacementHandle {
    task: JoinHandle<()>,
}

impl PlacementHandle {
    /// Stops the task, aborting it.
    pub fn stop(self) {}
}

impl Drop for PlacementHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the watermark-driven creation loop over a shared journal.
///
/// Leader-scoped at sweep time: a follower's sweep is a no-op (same contract as
/// the liveness ticker; the role is re-checked every tick so no lifecycle
/// plumbing is needed). The data-node transport is rebuilt per sweep from the
/// committed topology — sweeps are infrequent and short, so per-sweep
/// connections beat maintaining a live endpoint registry (M9 优化点).
#[must_use]
pub fn spawn_placement(
    journal: Arc<Journal>,
    code_mode: CodeMode,
    config: PlacementConfig,
    clock: Arc<dyn Clock>,
) -> PlacementHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            if !is_leader(&journal) {
                continue;
            }
            if let Err(err) = sweep(&journal, &code_mode, &config, &clock).await {
                tracing::warn!(error = %err, "placement sweep failed");
            }
        }
    });
    PlacementHandle { task }
}

fn is_leader(journal: &Journal) -> bool {
    journal.raft().metrics().borrow().state.is_leader()
}

/// One sweep: finish anything staged, then create to cover the watermark deficit.
async fn sweep(
    journal: &Journal,
    code_mode: &CodeMode,
    config: &PlacementConfig,
    clock: &Arc<dyn Clock>,
) -> Result<(), PdError> {
    let nodes = journal.state().nodes().list();
    resume_staging(journal, &nodes).await?;

    let mut candidates = gather_candidates(journal, &nodes, config);
    tracing::debug!(
        candidates = candidates.len(),
        nodes = nodes.len(),
        "placement sweep"
    );
    if candidates.is_empty() {
        return Ok(());
    }
    let transport = build_transport(&nodes);

    let mut creations = 0;
    while creations < config.max_creations_per_cycle {
        let Some(chosen) = plan_placement(&candidates, code_mode.shards_total(), config.affinity)
        else {
            break;
        };
        tracing::info!(?chosen, "placement plan");
        create_chunk(journal, code_mode, &chosen, &transport, config, clock).await?;
        creations += 1;
        // Chosen disks just gained a writable extent (optimistic accounting;
        // heartbeats converge the truth — 01 §4.1 事后一致).
        for disk_id in &chosen {
            if let Some(candidate) = candidates.iter_mut().find(|c| &c.disk_id == disk_id) {
                candidate.writable_extents += 1;
            }
        }
        candidates.retain(|c| c.writable_extents < config.writable_target);
        if candidates.is_empty() {
            break;
        }
    }
    Ok(())
}

/// Disks below the target watermark, with their live-node topology. A disk with
/// no heartbeat report counts as watermark 0 (a fresh disk is filled first).
fn gather_candidates(
    journal: &Journal,
    nodes: &[Node],
    config: &PlacementConfig,
) -> Vec<DiskCandidate> {
    let mut out = Vec::new();
    for disk in journal.state().disks().list() {
        if disk.status != DiskStatus::Normal {
            continue;
        }
        let Some(node) = nodes.iter().find(|n| n.node_id == disk.node_id) else {
            continue;
        };
        if node.status != NodeStatus::Live || !node.roles.contains(RoleSet::DATA) {
            continue;
        }
        // A disk with no room must not receive a new chunk (01 §4.1 满盘不再进
        // 候选集). Without this, placement *prefers* a full disk — its writable
        // extent count is low, which the watermark reads as "underused" — so new
        // chunks pile onto the disks that have the least space left.
        //
        // `None` (no heartbeat yet) is admitted optimistically, matching the
        // publish filter's rule: a newly registered disk should be usable
        // immediately, and a transient over-commit is caught by the write path's
        // OutOfSpace/ChunkFull eviction.
        match journal.disk_free(disk.node_id, disk.disk_id) {
            Some(free) if free < config.min_free_bytes => continue,
            _ => {}
        }
        let writable_extents = journal.disk_writable_extents(disk.node_id, disk.disk_id);
        if writable_extents < config.writable_target {
            out.push(DiskCandidate {
                disk_id: disk.disk_id,
                node_id: disk.node_id,
                rack: disk.rack.clone(),
                writable_extents,
            });
        }
    }
    out
}

/// Builds the sweep's data-plane transport from committed DATA-node topology.
/// Nodes with unparsable addresses are skipped (they cannot serve creation).
pub(crate) fn build_transport(nodes: &[Node]) -> RemoteTransport {
    let mut endpoints = HashMap::new();
    for node in nodes {
        if node.status != NodeStatus::Live || !node.roles.contains(RoleSet::DATA) {
            continue;
        }
        match node.addr.parse::<SocketAddr>() {
            Ok(addr) => {
                endpoints.insert(node.node_id, addr);
            }
            Err(_) => {
                tracing::warn!(
                    node = node.node_id.get(),
                    addr = %node.addr,
                    "skipping node with bad data-plane address"
                );
            }
        }
    }
    RemoteTransport::new(endpoints)
}

/// Resumes every pending staging plan (previous leader's leftovers or a failed
/// creation from an earlier sweep): re-create every slot extent (idempotent)
/// and commit, or rebump so the next sweep retries with fresh ids.
async fn resume_staging(journal: &Journal, nodes: &[Node]) -> Result<(), PdError> {
    let pending = journal.state().chunks().pending_staging();
    if pending.is_empty() {
        return Ok(());
    }
    let transport = build_transport(nodes);
    for staging in pending {
        let created = create_extents_for(&staging.shards, journal, &transport).await;
        if created == staging.shards.len() {
            journal.commit_chunk(staging.chunk_id).await?;
            continue;
        }
        tracing::warn!(
            chunk = staging.chunk_id.get(),
            created,
            total = staging.shards.len(),
            "staging resume incomplete; rebumping for next sweep"
        );
        journal.rebump_staging(staging.chunk_id).await?;
    }
    Ok(())
}

/// One chunk creation: staging → per-slot extent creation → commit; on failure,
/// rebump (epoch jump) and retry within this sweep's attempt budget.
async fn create_chunk(
    journal: &Journal,
    code_mode: &CodeMode,
    chosen: &[DiskId],
    transport: &RemoteTransport,
    config: &PlacementConfig,
    clock: &Arc<dyn Clock>,
) -> Result<(), PdError> {
    let slots: Vec<SlotPlan> = chosen
        .iter()
        .enumerate()
        .map(|(index, &disk_id)| SlotPlan {
            index: u8::try_from(index).expect("slot index fits u8"),
            disk_id,
        })
        .collect();
    let create_ts = i64::try_from(clock.now_millis()).unwrap_or(i64::MAX);
    let chunk_id = match journal
        .create_chunk_staging(*code_mode, slots, create_ts, 0)
        .await?
    {
        ApplyResult::ChunkStaged { chunk_id } => chunk_id,
        ApplyResult::Rejected(reason) => {
            tracing::warn!(?reason, "chunk staging rejected by state machine");
            return Ok(());
        }
        other => {
            return Err(PdError::Raft(format!(
                "unexpected staging apply result: {other:?}"
            )));
        }
    };

    for attempt in 1..=config.max_create_attempts {
        let Some(staging) = journal.state().chunks().staging(chunk_id) else {
            return Err(PdError::Raft(format!(
                "staging plan {} vanished mid-creation",
                chunk_id.get()
            )));
        };
        let created = create_extents_for(&staging.shards, journal, transport).await;
        if created == staging.shards.len() {
            journal.commit_chunk(chunk_id).await?;
            return Ok(());
        }
        tracing::warn!(
            chunk = chunk_id.get(),
            attempt,
            created,
            total = staging.shards.len(),
            "extent creation incomplete; rebumping"
        );
        journal.rebump_staging(chunk_id).await?;
    }
    // Attempts exhausted: the plan stays pending and the next sweep retries
    // with fresh ids (rebump above already advanced the epoch).
    Err(PdError::Raft(format!(
        "chunk {} extent creation exhausted {} attempts",
        chunk_id.get(),
        config.max_create_attempts
    )))
}

/// Creates every slot's extent on its owning DataNode, returning how many
/// succeeded. Extent creation is idempotent per staged identity, so resuming a
/// half-finished plan is safe (01 §4.1).
async fn create_extents_for(
    shards: &[crate::chunk::model::ShardSlot],
    journal: &Journal,
    transport: &RemoteTransport,
) -> usize {
    let mut created = 0;
    for slot in shards {
        let Some(node_id) = journal
            .state()
            .disks()
            .get(slot.disk_id)
            .map(|disk| disk.node_id)
        else {
            tracing::warn!(disk = slot.disk_id.get(), "slot disk is not registered");
            continue;
        };
        let req = CreateExtentReq {
            shard_id: slot.shard_id(),
            create_ts: slot.extent_id.create_ts(),
        };
        match transport.create_extent(node_id, req).await {
            Ok(extent_id) if extent_id == slot.extent_id => created += 1,
            Ok(extent_id) => {
                tracing::warn!(
                    node = node_id.get(),
                    staged = ?slot.extent_id,
                    returned = ?extent_id,
                    "extent id mismatch on creation"
                );
            }
            Err(err) => {
                tracing::warn!(
                    node = node_id.get(),
                    shard = ?slot.shard_id(),
                    error = %err,
                    "create_extent failed"
                );
            }
        }
    }
    created
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::heartbeat::HeartbeatReport;
    use crate::cluster::{DiskHeartbeat, NodeStatus};

    #[test]
    fn config_defaults_match_design_watermark() {
        let config = PlacementConfig::default();
        assert_eq!(config.writable_target, 8);
        assert_eq!(config.interval, Duration::from_secs(5));
        assert!(config.max_creations_per_cycle > 0);
        assert!(config.max_create_attempts >= 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn candidates_skip_non_normal_disks_and_non_live_nodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Arc::new(
            Journal::open_single_node(dir.path(), 1, "127.0.0.1:9001")
                .await
                .expect("open journal"),
        );
        // Two live DATA nodes (one disk each) + one Lost node (one disk).
        let n1 = journal
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("register n1");
        let n2 = journal
            .register_node("10.0.0.2:9000", "az1", "r2", RoleSet::DATA)
            .await
            .expect("register n2");
        journal
            .register_disk(n1, "az1", "r1", "/data/d1", 1 << 40)
            .await
            .expect("disk 1");
        journal
            .register_disk(n2, "az1", "r2", "/data/d2", 1 << 40)
            .await
            .expect("disk 2");
        journal
            .update_node_status(n1, NodeStatus::Live)
            .await
            .expect("mark n1 live");
        journal
            .update_node_status(n2, NodeStatus::Lost)
            .await
            .expect("mark n2 lost");

        // Disk 1 reports a below-target watermark; disk 2 is excluded (node lost).
        journal.record_heartbeat(
            &HeartbeatReport {
                node_id: n1,
                disks: vec![DiskHeartbeat {
                    disk_id: epoch_proto::DiskId::new(1),
                    free: 1 << 39,
                    used: 1 << 39,
                    writable_extents: 3,
                    broken: false,
                }],
            },
            1_000,
        );

        let nodes = journal.state().nodes().list();
        let config = PlacementConfig::default();
        let candidates = gather_candidates(&journal, &nodes, &config);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].disk_id, epoch_proto::DiskId::new(1));
        assert_eq!(candidates[0].writable_extents, 3);

        journal.shutdown().await.expect("shutdown");
    }

    /// INVARIANT(design 01 §4.1): a disk with no room must leave the candidate
    /// set. Without the free-space gate, placement actively *prefers* a full disk:
    /// a full disk has few writable extents, which the watermark reads as
    /// "underused", so new chunks pile onto exactly the disks that are out of
    /// space.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_disk_below_the_free_threshold_is_not_a_candidate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Arc::new(
            Journal::open_single_node(dir.path(), 1, "127.0.0.1:9003")
                .await
                .expect("open journal"),
        );
        journal
            .raft()
            .wait(Some(Duration::from_secs(10)))
            .state(openraft::ServerState::Leader, "leader")
            .await
            .expect("leader");

        let node = journal
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("register");
        journal
            .register_disk(node, "az1", "r1", "/data/full", 1 << 40)
            .await
            .expect("disk");
        journal
            .update_node_status(node, NodeStatus::Live)
            .await
            .expect("live");

        let config = PlacementConfig::default();
        let disk_id = epoch_proto::DiskId::new(1);
        let report = |free: u64| HeartbeatReport {
            node_id: node,
            disks: vec![DiskHeartbeat {
                disk_id,
                free,
                used: 1 << 39,
                // The lowest watermark: without the gate this disk looks the most
                // "underused" and is picked first.
                writable_extents: 0,
                broken: false,
            }],
        };

        // Plenty of room: a candidate.
        journal.record_heartbeat(&report(config.min_free_bytes * 4), 1_000);
        let nodes = journal.state().nodes().list();
        assert_eq!(
            gather_candidates(&journal, &nodes, &config).len(),
            1,
            "a disk with room is a candidate"
        );

        // Below the threshold: excluded, despite the lowest possible watermark.
        journal.record_heartbeat(&report(config.min_free_bytes / 2), 2_000);
        assert!(
            gather_candidates(&journal, &nodes, &config).is_empty(),
            "a disk below the free-space threshold must not receive new chunks"
        );

        journal.shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn candidates_treat_unreported_disks_as_watermark_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Arc::new(
            Journal::open_single_node(dir.path(), 1, "127.0.0.1:9001")
                .await
                .expect("open journal"),
        );
        let n1 = journal
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("register n1");
        journal
            .register_disk(n1, "az1", "r1", "/data/d1", 1 << 40)
            .await
            .expect("disk 1");
        journal
            .update_node_status(n1, NodeStatus::Live)
            .await
            .expect("mark n1 live");

        let nodes = journal.state().nodes().list();
        let config = PlacementConfig::default();
        let candidates = gather_candidates(&journal, &nodes, &config);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].writable_extents, 0);

        journal.shutdown().await.expect("shutdown");
    }
}
