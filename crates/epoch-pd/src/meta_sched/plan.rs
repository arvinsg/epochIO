//! Pure scheduling decisions for MetaNode partitions (01 §5): given a snapshot
//! of the route table + leader reports + node liveness, decide which partitions
//! to **split** (grown past the size threshold) and which replicas to
//! **migrate** (off a failed node, or to rebalance a skewed load).
//!
//! Every function here is pure and deterministic — no clock, no I/O, no raft.
//! The [`driver`](super::driver) turns these decisions into proposals and admin
//! pushes. Keeping the policy pure makes it directly table-testable (AGENTS
//! §10.1) and keeps the replicated side free of scheduling heuristics.
//!
//! The balance heuristic mirrors the "hunger"/tolerance model of a PD load
//! balancer: a node is overloaded when its partition count exceeds its fair
//! quota plus a tolerance band, and a move only fires when the source/target
//! gap exceeds twice the tolerance (anti-thrash). Migration targets respect
//! partition-level anti-affinity: no two replicas of one partition on one node.
//!
//! Design: docs/design/01-pd.md §5 (分区分裂/迁移触发)

use std::collections::BTreeMap;

use epoch_proto::NodeId;

use crate::cluster::NodeStatus;

/// A flattened per-partition snapshot the planner consumes: the replicated
/// route record (id / peers / epoch / interval shape) joined with the leader's
/// last reported size. Built by the driver from [`MetaPartitionManager`] +
/// leader reports.
///
/// [`MetaPartitionManager`]: crate::meta_mgr::MetaPartitionManager
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLoad {
    /// Partition (== raft group) id.
    pub partition_id: u64,
    /// Current replica voter set.
    pub peers: Vec<NodeId>,
    /// Route epoch at snapshot time (used to detect staleness before execute).
    pub epoch: u64,
    /// Total live object bytes as last reported by the leader (0 if no report).
    pub total_bytes: u64,
    /// Whether the interval is a single point and therefore cannot be split
    /// (a split boundary must be strictly inside a non-empty interval, 03 §2).
    pub splittable: bool,
}

/// A MetaNode's identity + liveness + topology, for migrate target selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaNodeInfo {
    /// Node id.
    pub node_id: NodeId,
    /// Liveness (only `Live` nodes are valid migration targets).
    pub status: NodeStatus,
    /// Rack, for topology-aware target preference.
    pub rack: String,
}

/// Tuning for the partition scheduler (01 §5). Every field has a production
/// default; overridable via `[scheduler]` config.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchedulerConfig {
    /// Split when a partition's `total_bytes` exceeds this (soft size cap).
    pub split_threshold_bytes: u64,
    /// Ceiling on migrations planned per sweep (backpressure; migrations move
    /// data, so keep concurrent moves bounded).
    pub max_concurrent_migrations: usize,
    /// Balance tolerance ratio: a node may carry up to `quota * (1 + ratio)`
    /// partitions before it is "overloaded" (anti-thrash headroom).
    pub migrate_tolerant_ratio: f64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            // 4 GiB soft cap per partition (01 §5 分裂阈值; tunable). Chosen so a
            // partition stays comfortably inside the raft snapshot/scan budget.
            split_threshold_bytes: 4 << 30,
            max_concurrent_migrations: 2,
            migrate_tolerant_ratio: 0.2,
        }
    }
}

/// A decision to split one partition. The boundary is deliberately absent: the
/// MetaNode that owns the data picks the median routing key (03 §2), so PD only
/// names the partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitDecision {
    /// The partition to split.
    pub parent_id: u64,
    /// The parent's route epoch when the decision was made (staleness guard).
    pub epoch: u64,
}

/// A decision to move one partition replica from `from` to `to`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrateDecision {
    /// The partition whose replica set changes.
    pub partition_id: u64,
    /// The voter being removed.
    pub from: NodeId,
    /// The voter being added.
    pub to: NodeId,
    /// Why the move was chosen (observability + priority in the controller).
    pub reason: MigrateReason,
    /// The partition's route epoch when the decision was made.
    pub epoch: u64,
}

/// Why a migration was planned — drives operator priority (evacuation of a dead
/// node outranks opportunistic balancing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateReason {
    /// A replica sits on a node that is no longer `Live` (must move).
    Evacuate,
    /// The source node is overloaded relative to its fair quota (optional).
    Balance,
}

/// Partitions whose reported size exceeds the split threshold and whose
/// interval can still be split. Returned in ascending partition-id order for
/// determinism.
#[must_use]
pub fn plan_splits(loads: &[PartitionLoad], cfg: &SchedulerConfig) -> Vec<SplitDecision> {
    let mut out: Vec<SplitDecision> = loads
        .iter()
        .filter(|p| p.splittable && p.total_bytes > cfg.split_threshold_bytes)
        .map(|p| SplitDecision {
            parent_id: p.partition_id,
            epoch: p.epoch,
        })
        .collect();
    out.sort_by_key(|d| d.parent_id);
    out
}

/// Migration decisions: first evacuate every replica off a non-`Live` node,
/// then (within the remaining budget) rebalance replicas off overloaded nodes.
/// Evacuations are emitted first so they win the per-sweep budget.
#[must_use]
pub fn plan_migrations(
    loads: &[PartitionLoad],
    nodes: &[MetaNodeInfo],
    cfg: &SchedulerConfig,
) -> Vec<MigrateDecision> {
    let live: Vec<&MetaNodeInfo> = nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Live)
        .collect();
    if live.is_empty() {
        return Vec::new();
    }

    // Projected replica count per node — updated as we plan moves so we never
    // pile the whole sweep onto one target (the "pending influence" idea).
    let mut load_by_node: BTreeMap<NodeId, i64> = live.iter().map(|n| (n.node_id, 0)).collect();
    for p in loads {
        for peer in &p.peers {
            *load_by_node.entry(*peer).or_insert(0) += 1;
        }
    }

    let mut decisions = Vec::new();
    plan_evacuations(loads, nodes, &live, &mut load_by_node, &mut decisions);
    if decisions.len() < cfg.max_concurrent_migrations {
        plan_balance(loads, &live, cfg, &mut load_by_node, &mut decisions);
    }
    decisions.truncate(cfg.max_concurrent_migrations);
    decisions
}

/// Every replica on a node that is not `Live` must move to a live node that
/// does not already hold a replica of the same partition (anti-affinity).
fn plan_evacuations(
    loads: &[PartitionLoad],
    nodes: &[MetaNodeInfo],
    live: &[&MetaNodeInfo],
    load_by_node: &mut BTreeMap<NodeId, i64>,
    decisions: &mut Vec<MigrateDecision>,
) {
    let dead: std::collections::BTreeSet<NodeId> = nodes
        .iter()
        .filter(|n| n.status != NodeStatus::Live)
        .map(|n| n.node_id)
        .collect();
    if dead.is_empty() {
        return;
    }
    for p in loads {
        for &from in &p.peers {
            if !dead.contains(&from) {
                continue;
            }
            let Some(to) = pick_hungriest_target(p, live, load_by_node) else {
                continue; // no eligible target this sweep; retried next sweep
            };
            *load_by_node.entry(to).or_insert(0) += 1;
            // The dead node's slot frees up on removal (route epoch bump).
            if let Some(count) = load_by_node.get_mut(&from) {
                *count -= 1;
            }
            decisions.push(MigrateDecision {
                partition_id: p.partition_id,
                from,
                to,
                reason: MigrateReason::Evacuate,
                epoch: p.epoch,
            });
        }
    }
}

/// Rebalance: move a replica off the most-overloaded node to the most-hungry
/// node when the gap exceeds twice the tolerance band (anti-thrash).
fn plan_balance(
    loads: &[PartitionLoad],
    live: &[&MetaNodeInfo],
    cfg: &SchedulerConfig,
    load_by_node: &mut BTreeMap<NodeId, i64>,
    decisions: &mut Vec<MigrateDecision>,
) {
    let total_replicas: i64 = load_by_node.values().sum();
    let quota = (total_replicas as f64) / (live.len() as f64);
    let tolerance = (quota * cfg.migrate_tolerant_ratio).max(1.0);

    // Consider partitions in id order for determinism.
    let mut ordered: Vec<&PartitionLoad> = loads.iter().collect();
    ordered.sort_by_key(|p| p.partition_id);

    for p in ordered {
        if decisions.len() >= cfg.max_concurrent_migrations {
            return;
        }
        // The most-loaded peer of this partition is the move candidate.
        let Some(&from) = p
            .peers
            .iter()
            .max_by_key(|peer| load_by_node.get(peer).copied().unwrap_or(0))
        else {
            continue;
        };
        let from_load = load_by_node.get(&from).copied().unwrap_or(0);
        if (from_load as f64) <= quota + tolerance {
            continue; // source not overloaded
        }
        let Some(to) = pick_hungriest_target(p, live, load_by_node) else {
            continue;
        };
        let to_load = load_by_node.get(&to).copied().unwrap_or(0);
        // Anti-thrash: only move if the imbalance is worth it (gap > 2×tol).
        if (from_load - to_load) as f64 <= 2.0 * tolerance {
            continue;
        }
        *load_by_node.entry(to).or_insert(0) += 1;
        if let Some(count) = load_by_node.get_mut(&from) {
            *count -= 1;
        }
        decisions.push(MigrateDecision {
            partition_id: p.partition_id,
            from,
            to,
            reason: MigrateReason::Balance,
            epoch: p.epoch,
        });
    }
}

/// The live node with the fewest projected replicas that does **not** already
/// hold a replica of `p` (partition-level anti-affinity), tie-broken by node id
/// for determinism.
fn pick_hungriest_target(
    p: &PartitionLoad,
    live: &[&MetaNodeInfo],
    load_by_node: &BTreeMap<NodeId, i64>,
) -> Option<NodeId> {
    live.iter()
        .filter(|n| !p.peers.contains(&n.node_id))
        .min_by(|a, b| {
            let la = load_by_node.get(&a.node_id).copied().unwrap_or(0);
            let lb = load_by_node.get(&b.node_id).copied().unwrap_or(0);
            la.cmp(&lb).then(a.node_id.get().cmp(&b.node_id.get()))
        })
        .map(|n| n.node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u32, status: NodeStatus) -> MetaNodeInfo {
        MetaNodeInfo {
            node_id: NodeId::new(id),
            status,
            rack: "r1".to_string(),
        }
    }

    fn load(id: u64, peers: &[u32], total_bytes: u64, splittable: bool) -> PartitionLoad {
        PartitionLoad {
            partition_id: id,
            peers: peers.iter().map(|&n| NodeId::new(n)).collect(),
            epoch: 1,
            total_bytes,
            splittable,
        }
    }

    #[test]
    fn split_fires_only_above_threshold_and_when_splittable() {
        let cfg = SchedulerConfig {
            split_threshold_bytes: 1000,
            ..SchedulerConfig::default()
        };
        let loads = vec![
            load(1, &[1, 2, 3], 2000, true),  // over, splittable -> split
            load(2, &[1, 2, 3], 500, true),   // under -> no
            load(3, &[1, 2, 3], 2000, false), // over but point interval -> no
            load(4, &[1, 2, 3], 1000, true),  // exactly at threshold -> no (strict >)
        ];
        let splits = plan_splits(&loads, &cfg);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].parent_id, 1);
        assert_eq!(splits[0].epoch, 1);
    }

    #[test]
    fn evacuation_moves_replicas_off_a_dead_node() {
        let cfg = SchedulerConfig::default();
        let nodes = vec![
            node(1, NodeStatus::Live),
            node(2, NodeStatus::Live),
            node(3, NodeStatus::Lost), // dead
            node(4, NodeStatus::Live),
        ];
        // Two partitions both hold a replica on the dead node 3.
        let loads = vec![load(1, &[1, 2, 3], 0, true), load(2, &[1, 2, 3], 0, true)];
        let mut cfg = cfg;
        cfg.max_concurrent_migrations = 8;
        let moves = plan_migrations(&loads, &nodes, &cfg);
        assert_eq!(moves.len(), 2, "both replicas on the dead node evacuate");
        for m in &moves {
            assert_eq!(m.from, NodeId::new(3));
            assert_eq!(m.reason, MigrateReason::Evacuate);
            assert_eq!(m.to, NodeId::new(4), "only node 4 is a non-peer live node");
        }
    }

    #[test]
    fn evacuation_respects_the_migration_budget() {
        let nodes = vec![
            node(1, NodeStatus::Live),
            node(2, NodeStatus::Live),
            node(3, NodeStatus::Lost),
            node(4, NodeStatus::Live),
            node(5, NodeStatus::Live),
        ];
        let loads = vec![
            load(1, &[1, 2, 3], 0, true),
            load(2, &[1, 2, 3], 0, true),
            load(3, &[1, 2, 3], 0, true),
        ];
        let cfg = SchedulerConfig {
            max_concurrent_migrations: 2,
            ..SchedulerConfig::default()
        };
        let moves = plan_migrations(&loads, &nodes, &cfg);
        assert_eq!(moves.len(), 2, "capped at the per-sweep budget");
    }

    #[test]
    fn no_target_when_all_live_nodes_already_hold_the_partition() {
        let nodes = vec![
            node(1, NodeStatus::Live),
            node(2, NodeStatus::Live),
            node(3, NodeStatus::Lost),
        ];
        // Every live node already holds partition 1 — no anti-affinity-safe target.
        let loads = vec![load(1, &[1, 2, 3], 0, true)];
        let cfg = SchedulerConfig::default();
        let moves = plan_migrations(&loads, &nodes, &cfg);
        assert!(moves.is_empty(), "no eligible target: evacuation deferred");
    }

    #[test]
    fn balance_moves_off_an_overloaded_node_only_past_the_gap() {
        // 4 live nodes; nodes 1/2/3 carry a heavy skew, node 4 holds nothing.
        let nodes = vec![
            node(1, NodeStatus::Live),
            node(2, NodeStatus::Live),
            node(3, NodeStatus::Live),
            node(4, NodeStatus::Live),
        ];
        // Six partitions all on [1,2,3]: node1=node2=node3=6, node4=0.
        // quota = 18/4 = 4.5, tol = max(0.9, 1.0) = 1.0, overload > 5.5 → 6 fires;
        // gap 6-0 = 6 > 2*tol = 2.
        let loads: Vec<PartitionLoad> = (1..=6).map(|id| load(id, &[1, 2, 3], 0, true)).collect();
        let cfg = SchedulerConfig {
            max_concurrent_migrations: 1,
            migrate_tolerant_ratio: 0.2,
            ..SchedulerConfig::default()
        };
        let moves = plan_migrations(&loads, &nodes, &cfg);
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].reason, MigrateReason::Balance);
        assert_eq!(moves[0].to, NodeId::new(4), "hungriest node is the target");
    }

    #[test]
    fn balance_does_not_thrash_a_nearly_even_spread() {
        let nodes = vec![
            node(1, NodeStatus::Live),
            node(2, NodeStatus::Live),
            node(3, NodeStatus::Live),
        ];
        // Perfectly even: every node carries 2 replicas.
        let loads = vec![
            load(1, &[1, 2], 0, true),
            load(2, &[2, 3], 0, true),
            load(3, &[1, 3], 0, true),
        ];
        let cfg = SchedulerConfig::default();
        let moves = plan_migrations(&loads, &nodes, &cfg);
        assert!(moves.is_empty(), "an even spread must not be rebalanced");
    }

    #[test]
    fn evacuation_is_planned_before_balance() {
        let nodes = vec![
            node(1, NodeStatus::Live),
            node(2, NodeStatus::Live),
            node(3, NodeStatus::Lost),
            node(4, NodeStatus::Live),
        ];
        let loads = vec![load(1, &[1, 2, 3], 0, true)];
        let cfg = SchedulerConfig {
            max_concurrent_migrations: 1,
            ..SchedulerConfig::default()
        };
        let moves = plan_migrations(&loads, &nodes, &cfg);
        assert_eq!(moves.len(), 1);
        assert_eq!(
            moves[0].reason,
            MigrateReason::Evacuate,
            "evacuation wins the budget over balance"
        );
    }
}
