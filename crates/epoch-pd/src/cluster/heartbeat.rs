//! Node liveness: heartbeat tracking and the background sweep that drives
//! missed-heartbeat status transitions through raft.
//!
//! Heartbeat state is **leader-only, in-memory, and never replicated** (Q18):
//! the last-seen timestamp and per-disk statistics a node reports are volatile
//! observations, not committed cluster state. Only the *derived* status
//! transition (`Live` → `Offline` → `Lost`, or recovery back to `Live`) is
//! proposed through raft, so every replica converges on the same membership
//! from the committed log. INVARIANT(design 01 §2 / AGENTS §8): the wall clock
//! is read only on the leader to decide *when* to propose, and never enters a
//! replicated apply.
//!
//! Design: docs/design/01-pd.md §2 (heartbeat / liveness)

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use epoch_proto::{DiskId, NodeId};
use openraft::ServerState;
use tokio::task::JoinHandle;

use crate::cluster::{NodeStatus, UpdateNodeStatus};
use crate::journal::entry::PdEntry;
use crate::raft::PdRaft;
use crate::state::PdState;

/// Volatile per-disk capacity statistics from the latest heartbeat (design 01 §2).
///
/// Never persisted or replicated: the placement path (a later phase) reads these
/// live to pick write targets, but they are rebuilt from heartbeats after any
/// leader change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskStats {
    /// Free bytes reported for the disk.
    pub free: u64,
    /// Used bytes reported for the disk.
    pub used: u64,
    /// Number of extents currently accepting writes.
    pub writable_extents: u32,
    /// The DataNode has tripped this disk to Broken (02 §1.8); the repair
    /// trigger transitions it and starts a RepairDisk job.
    pub broken: bool,
}

/// The latest heartbeat observed for one node: when it arrived and the per-disk
/// statistics it carried.
#[derive(Debug, Clone)]
struct NodeHeartbeat {
    last_seen_millis: u64,
    disks: HashMap<DiskId, DiskStats>,
}

/// One disk's slice of a [`HeartbeatReport`].
///
/// No `serde` derive: heartbeats travel in-process today; the gRPC wire form
/// arrives with the transport phase, which defines its own encoding.
#[derive(Debug, Clone)]
pub struct DiskHeartbeat {
    /// The reporting disk.
    pub disk_id: DiskId,
    /// Free bytes.
    pub free: u64,
    /// Used bytes.
    pub used: u64,
    /// Extents currently accepting writes.
    pub writable_extents: u32,
    /// The disk has tripped to Broken on the DataNode (02 §1.8).
    pub broken: bool,
}

/// A node's heartbeat: liveness plus the capacity of each of its disks.
///
/// The in-process input to [`HeartbeatTracker::record`]; the gRPC intake in a
/// later phase decodes the wire message into this before recording it.
#[derive(Debug, Clone)]
pub struct HeartbeatReport {
    /// The reporting node.
    pub node_id: NodeId,
    /// Per-disk statistics.
    pub disks: Vec<DiskHeartbeat>,
}

/// One node's slice of [`HeartbeatTracker::snapshot`]: when its last heartbeat
/// arrived and the per-disk stats it carried (08 §5 console read). Disks are in
/// id order.
#[derive(Debug, Clone)]
pub struct NodeStatsSnapshot {
    /// The node this snapshot is for.
    pub node_id: NodeId,
    /// Millis (epoch) of the node's last heartbeat.
    pub last_seen_millis: u64,
    /// `(disk_id, stats)` for each disk the last heartbeat reported.
    pub disks: Vec<(DiskId, DiskStats)>,
}

/// Tracks the latest heartbeat per node (leader-only, in-memory).
///
/// Cloneable; clones share the same map so the sweep task and the heartbeat
/// intake observe the same state.
#[derive(Clone, Default)]
pub struct HeartbeatTracker {
    inner: Arc<RwLock<HashMap<NodeId, NodeHeartbeat>>>,
    /// Per-writer-token GC commit watermark `W(t)` (Q27), reported on writer
    /// heartbeats. Leader-only, volatile (Q18): it never enters raft; a GcRound
    /// reads it to decide which `(token, seq ≤ W)` blobs are reclaimable. Absent
    /// = the token has not reported (treated as `-1`, nothing reclaimable).
    watermarks: Arc<RwLock<HashMap<epoch_proto::WriterToken, i64>>>,
}

impl HeartbeatTracker {
    /// An empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a heartbeat, replacing the node's last-seen time and its full
    /// disk-stats set with the reported one.
    pub(crate) fn record(&self, report: &HeartbeatReport, now_millis: u64) {
        let disks = report
            .disks
            .iter()
            .map(|disk| {
                (
                    disk.disk_id,
                    DiskStats {
                        free: disk.free,
                        used: disk.used,
                        writable_extents: disk.writable_extents,
                        broken: disk.broken,
                    },
                )
            })
            .collect();
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        guard.insert(
            report.node_id,
            NodeHeartbeat {
                last_seen_millis: now_millis,
                disks,
            },
        );
    }

    /// Refreshes only the node's last-seen time, preserving its disk stats.
    /// Used by writer heartbeats (01 §4.3), which prove liveness but carry no
    /// capacity report. A never-seen node gets an empty stats entry.
    pub(crate) fn touch(&self, node_id: NodeId, now_millis: u64) {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        guard
            .entry(node_id)
            .and_modify(|hb| hb.last_seen_millis = now_millis)
            .or_insert_with(|| NodeHeartbeat {
                last_seen_millis: now_millis,
                disks: HashMap::new(),
            });
    }

    /// Records a writer token's GC commit watermark `W(t)` (Q27), reported on a
    /// writer heartbeat. Monotonic: a lower value than recorded is ignored (an
    /// out-of-order/stale heartbeat can never rewind the watermark, which would
    /// wrongly expose an already-committed blob to GC).
    pub(crate) fn record_watermark(&self, token: epoch_proto::WriterToken, watermark: i64) {
        let mut guard = self
            .watermarks
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = guard.entry(token).or_insert(i64::MIN);
        if watermark > *entry {
            *entry = watermark;
        }
    }

    /// The recorded commit watermark for `token`, or `-1` (nothing reclaimable)
    /// if it has not reported one.
    #[must_use]
    pub fn watermark(&self, token: epoch_proto::WriterToken) -> i64 {
        self.watermarks
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&token)
            .copied()
            .unwrap_or(-1)
    }

    /// Milliseconds since `node_id` was last seen, or [`u64::MAX`] if it has
    /// never heartbeat (treated as maximally stale). Saturates at 0 if `now` is
    /// behind the recorded time (clock ran backwards).
    #[must_use]
    pub fn staleness(&self, node_id: NodeId, now_millis: u64) -> u64 {
        let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        match guard.get(&node_id) {
            Some(hb) => now_millis.saturating_sub(hb.last_seen_millis),
            None => u64::MAX,
        }
    }

    /// A bulk snapshot of every node's last heartbeat: last-seen millis + its
    /// per-disk stats. The console reads this once per refresh to render node /
    /// disk capacity and staleness without a call per `(node, disk)` (08 §5).
    /// Nodes come out in id order (deterministic); a node that has never
    /// heartbeat is simply absent (the caller shows "等待心跳", 08 §4).
    #[must_use]
    pub fn snapshot(&self) -> Vec<NodeStatsSnapshot> {
        let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        let mut out: Vec<NodeStatsSnapshot> = guard
            .iter()
            .map(|(node, hb)| {
                let mut disks: Vec<(DiskId, DiskStats)> =
                    hb.disks.iter().map(|(d, s)| (*d, *s)).collect();
                disks.sort_by_key(|(d, _)| d.get());
                NodeStatsSnapshot {
                    node_id: *node,
                    last_seen_millis: hb.last_seen_millis,
                    disks,
                }
            })
            .collect();
        out.sort_by_key(|s| s.node_id.get());
        out
    }

    /// The latest reported statistics for one disk, if the owning node has
    /// heartbeat and included it (read by the placement path in a later phase).
    #[must_use]
    pub fn disk_stats(&self, node_id: NodeId, disk_id: DiskId) -> Option<DiskStats> {
        let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        guard
            .get(&node_id)
            .and_then(|hb| hb.disks.get(&disk_id).copied())
    }

    /// Every `(node, disk)` a heartbeat currently flags as Broken (02 §1.8) —
    /// the repair trigger's input. Sorted for deterministic iteration.
    #[must_use]
    pub fn broken_disks(&self) -> Vec<(NodeId, DiskId)> {
        let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        let mut out: Vec<(NodeId, DiskId)> = guard
            .iter()
            .flat_map(|(node, hb)| {
                hb.disks
                    .iter()
                    .filter(|(_, stats)| stats.broken)
                    .map(move |(disk, _)| (*node, *disk))
            })
            .collect();
        out.sort_by_key(|(n, d)| (n.get(), d.get()));
        out
    }
}

/// Source of wall-clock time in milliseconds since the Unix epoch.
///
/// Injected so the liveness sweep is testable with a controllable clock. The
/// clock is read only on the leader to decide when to propose a status change
/// and never feeds a replicated apply (AGENTS §8).
pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since the Unix epoch.
    fn now_millis(&self) -> u64;
}

/// A [`Clock`] backed by the system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or(0);
        u64::try_from(millis).unwrap_or(u64::MAX)
    }
}

/// Timing for the liveness sweep.
#[derive(Debug, Clone, Copy)]
pub struct LivenessConfig {
    /// How often the sweep runs.
    pub sweep_interval: Duration,
    /// A node stale (no heartbeat) longer than this becomes `Offline`.
    pub offline_after_millis: u64,
    /// An `Offline` node stale longer than this becomes `Lost`.
    pub lost_after_millis: u64,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            sweep_interval: Duration::from_secs(5),
            offline_after_millis: 30_000,
            lost_after_millis: 300_000,
        }
    }
}

/// The status a node should move to given how stale its heartbeat is, or `None`
/// to stay put.
///
/// INVARIANT(design 01 §1): every returned transition is one the node status
/// state machine permits, so the proposed [`UpdateNodeStatus`] is never rejected
/// for an illegal transition:
/// - a fresh heartbeat recovers `Starting` / `Offline` / `Lost` to `Live`;
/// - staleness past `offline_after` drops `Live` / `Starting` to `Offline`;
/// - an `Offline` node stale past `lost_after` becomes `Lost`;
/// - `Decommissioned` is terminal and never moves.
///
/// A `Live` node whose staleness already exceeds `lost_after` first drops to
/// `Offline` and reaches `Lost` only on a later sweep (multi-cycle convergence),
/// respecting the state machine's `Live ↛ Lost` restriction.
#[must_use]
pub(crate) fn next_status(
    current: NodeStatus,
    staleness: u64,
    offline_after: u64,
    lost_after: u64,
) -> Option<NodeStatus> {
    use NodeStatus::{Decommissioned, Live, Lost, Offline, Starting};

    match current {
        Decommissioned => None,
        _ if staleness <= offline_after => (current != Live).then_some(Live),
        Live | Starting => Some(Offline),
        Offline if staleness > lost_after => Some(Lost),
        Offline | Lost => None,
    }
}

/// Runs one liveness sweep on the leader: derives each node's next status from
/// its heartbeat staleness and proposes the transitions through raft.
///
/// Leader-only: heartbeats target the leader, so a follower's map is empty and
/// must not drive transitions. Each proposal is best-effort — a `NotLeader`
/// result or a rejected transition is ignored, because the next sweep re-derives
/// from committed state and converges; structured telemetry for dropped
/// proposals arrives with the gRPC transport phase.
pub(crate) async fn sweep(
    raft: &PdRaft,
    state: &PdState,
    heartbeats: &HeartbeatTracker,
    now_millis: u64,
    config: &LivenessConfig,
) {
    if !matches!(raft.metrics().borrow().state, ServerState::Leader) {
        return;
    }

    // Derive the transitions first, releasing the read locks before any awaited
    // proposal below (§5: no lock held across `.await`).
    let transitions: Vec<UpdateNodeStatus> = state
        .nodes()
        .statuses()
        .into_iter()
        .filter_map(|(node_id, current)| {
            let staleness = heartbeats.staleness(node_id, now_millis);
            next_status(
                current,
                staleness,
                config.offline_after_millis,
                config.lost_after_millis,
            )
            .map(|status| UpdateNodeStatus { node_id, status })
        })
        .collect();

    for transition in transitions {
        // Best-effort: a NotLeader result or a rejected transition is dropped;
        // the next sweep re-derives from committed state and converges.
        let _ = raft
            .client_write(PdEntry::UpdateNodeStatus(transition))
            .await;
    }
}

/// Spawns the background liveness ticker and returns a handle that owns it.
///
/// The task ticks at `config.sweep_interval` and runs [`sweep`] each tick. It is
/// owned by the returned [`LivenessHandle`] (§5: no detached task) — dropping or
/// [`stop`](LivenessHandle::stop)-ping the handle aborts it.
pub(crate) fn spawn_liveness(
    raft: PdRaft,
    state: PdState,
    heartbeats: HeartbeatTracker,
    config: LivenessConfig,
    clock: Arc<dyn Clock>,
) -> LivenessHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(config.sweep_interval);
        loop {
            ticker.tick().await;
            sweep(&raft, &state, &heartbeats, clock.now_millis(), &config).await;
        }
    });
    LivenessHandle { task }
}

/// Owns the background liveness ticker task and aborts it on drop.
pub struct LivenessHandle {
    task: JoinHandle<()>,
}

impl LivenessHandle {
    /// Stops the ticker, aborting the background task.
    pub fn stop(self) {
        self.task.abort();
    }
}

impl Drop for LivenessHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OFFLINE_AFTER: u64 = 100;
    const LOST_AFTER: u64 = 500;

    #[test]
    fn next_status_derivation_table() {
        use NodeStatus::{Decommissioned, Live, Lost, Offline, Starting};

        // (name, current, staleness, expected next status).
        let cases = [
            ("fresh starting -> live", Starting, 0, Some(Live)),
            ("fresh offline -> live", Offline, 50, Some(Live)),
            ("fresh lost -> live", Lost, 100, Some(Live)),
            ("fresh live stays", Live, 0, None),
            (
                "boundary offline_after is still fresh",
                Live,
                OFFLINE_AFTER,
                None,
            ),
            ("live goes offline", Live, OFFLINE_AFTER + 1, Some(Offline)),
            (
                "starting goes offline",
                Starting,
                OFFLINE_AFTER + 1,
                Some(Offline),
            ),
            (
                "offline within lost window stays",
                Offline,
                LOST_AFTER,
                None,
            ),
            (
                "offline past lost -> lost",
                Offline,
                LOST_AFTER + 1,
                Some(Lost),
            ),
            ("lost stays lost when stale", Lost, LOST_AFTER + 1, None),
            ("never-seen live -> offline", Live, u64::MAX, Some(Offline)),
            ("never-seen offline -> lost", Offline, u64::MAX, Some(Lost)),
            (
                "never-seen starting -> offline",
                Starting,
                u64::MAX,
                Some(Offline),
            ),
            (
                "decommissioned never moves (fresh)",
                Decommissioned,
                0,
                None,
            ),
            (
                "decommissioned never moves (stale)",
                Decommissioned,
                u64::MAX,
                None,
            ),
        ];

        for (name, current, staleness, expected) in cases {
            let got = next_status(current, staleness, OFFLINE_AFTER, LOST_AFTER);
            assert_eq!(got, expected, "case {name}");
            if let Some(next) = got {
                // INVARIANT(design 01 §1): the derived transition must be legal.
                assert!(
                    current.can_transition_to(next),
                    "case {name}: {current:?} -> {next:?} must be a permitted transition"
                );
            }
        }
    }

    fn report(node_id: u32, disks: &[(u32, u64, u64, u32)]) -> HeartbeatReport {
        HeartbeatReport {
            node_id: NodeId::new(node_id),
            disks: disks
                .iter()
                .map(|&(disk_id, free, used, writable_extents)| DiskHeartbeat {
                    disk_id: DiskId::new(disk_id),
                    free,
                    used,
                    writable_extents,
                    broken: false,
                })
                .collect(),
        }
    }

    #[test]
    fn record_updates_last_seen_and_replaces_disk_stats() {
        let tracker = HeartbeatTracker::new();
        let node = NodeId::new(1);

        // A never-seen node is maximally stale and has no disk stats.
        assert_eq!(tracker.staleness(node, 1_000), u64::MAX);
        assert_eq!(tracker.disk_stats(node, DiskId::new(1)), None);

        tracker.record(&report(1, &[(1, 900, 100, 4)]), 1_000);
        assert_eq!(tracker.staleness(node, 1_250), 250);
        assert_eq!(
            tracker.disk_stats(node, DiskId::new(1)),
            Some(DiskStats {
                free: 900,
                used: 100,
                writable_extents: 4,
                broken: false,
            })
        );

        // A later heartbeat fully replaces the previous disk set.
        tracker.record(&report(1, &[(2, 500, 500, 1)]), 2_000);
        assert_eq!(tracker.staleness(node, 2_000), 0);
        assert_eq!(tracker.disk_stats(node, DiskId::new(1)), None);
        assert_eq!(
            tracker.disk_stats(node, DiskId::new(2)),
            Some(DiskStats {
                free: 500,
                used: 500,
                writable_extents: 1,
                broken: false,
            })
        );
    }

    #[test]
    fn staleness_saturates_when_clock_runs_backwards() {
        let tracker = HeartbeatTracker::new();
        let node = NodeId::new(1);
        tracker.record(&report(1, &[]), 2_000);
        assert_eq!(tracker.staleness(node, 1_000), 0);
    }

    #[test]
    fn snapshot_returns_all_nodes_and_disks_matching_single_key_reads() {
        let tracker = HeartbeatTracker::new();
        tracker.record(&report(1, &[(1, 900, 100, 4), (2, 500, 500, 1)]), 1_000);
        tracker.record(&report(2, &[(3, 100, 0, 2)]), 1_500);

        let snap = tracker.snapshot();
        assert_eq!(snap.len(), 2, "both nodes present");
        // Node order is ascending id.
        assert_eq!(snap[0].node_id, NodeId::new(1));
        assert_eq!(snap[0].last_seen_millis, 1_000);
        assert_eq!(snap[0].disks.len(), 2);
        // Disk order is ascending id, and each entry equals the single-key read.
        assert_eq!(snap[0].disks[0].0, DiskId::new(1));
        assert_eq!(
            Some(snap[0].disks[0].1),
            tracker.disk_stats(NodeId::new(1), DiskId::new(1))
        );
        assert_eq!(snap[1].node_id, NodeId::new(2));
        assert_eq!(snap[1].disks[0].0, DiskId::new(3));
        // An empty tracker snapshots to nothing (the console shows 等待心跳).
        assert!(HeartbeatTracker::new().snapshot().is_empty());
    }

    #[test]
    fn writer_watermark_is_monotonic_and_defaults_to_minus_one() {
        let tracker = HeartbeatTracker::new();
        let token = epoch_proto::WriterToken::new(7);
        // Unreported → -1 (nothing reclaimable).
        assert_eq!(tracker.watermark(token), -1);

        tracker.record_watermark(token, 5);
        assert_eq!(tracker.watermark(token), 5);
        // A higher watermark advances it.
        tracker.record_watermark(token, 9);
        assert_eq!(tracker.watermark(token), 9);
        // A lower (stale/out-of-order) report never rewinds it — else an
        // already-committed blob could be re-exposed to GC.
        tracker.record_watermark(token, 3);
        assert_eq!(tracker.watermark(token), 9);
    }
}
