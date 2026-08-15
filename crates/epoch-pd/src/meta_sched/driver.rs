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

//! The partition-scheduler driver (01 §5): a leader-gated periodic sweep that
//! gathers the current partition layout, runs the pure planner, admits the
//! resulting decisions through the [`OperatorController`], and executes each —
//! proposing the route-table change to PD raft and pushing the matching admin
//! directive to the owning MetaNode(s).
//!
//! It mirrors the chunk placement loop ([`crate::chunk::driver::spawn_placement`]):
//! a `tokio::interval` loop that re-checks leadership every tick (a follower
//! tick is a no-op) and aborts on handle drop. The MetaNode-facing side is
//! abstracted behind [`MetaAdmin`] so the driver is unit-testable without a
//! live gRPC cluster; the production impl (gRPC push) is assembled in the node
//! role.
//!
//! Execution split (why two sides):
//! - **split**: PD does not know the median routing key. The driver pushes
//!   `SplitGroup(parent)`; the MetaNode leader picks the boundary from its key
//!   space (03 §2) and returns it, then the driver proposes the matching
//!   PD-side route narrow. The controller slot clears when the parent's route
//!   epoch advances.
//! - **migrate**: the driver proposes the PD-side voter swap, then pushes
//!   `PrepareMigrateTarget` + `MigrateGroupMember` so the MetaNode performs the
//!   openraft membership change (data movement).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::error::PdError;
use crate::journal::{ApplyResult, Journal};
use crate::meta_mgr::{MetaPartition, PartitionBound};
use crate::meta_sched::operator::{OpKind, OperatorController};
use crate::meta_sched::plan::{
    MetaNodeInfo, PartitionLoad, SchedulerConfig, plan_migrations, plan_splits,
};

/// The MetaNode-facing admin surface the driver drives (01 §5 PD→MetaNode).
/// Abstracted so the scheduler is testable without a gRPC cluster; the
/// production impl pushes over the `MetaNode` admin RPCs (assembled in the node
/// role). Every method is best-effort with its own retry — a failed push is
/// retried on the next sweep (the operation stays in flight until its route
/// epoch advances), exactly like `CreateRaftGroup` reconciliation.
#[async_trait]
pub trait MetaAdmin: Send + Sync {
    /// Asks the parent group's leader to choose a split boundary: a median
    /// routing key strictly inside its range (03 §2). Returns the chosen
    /// boundary, or `None` if the group cannot be split now (empty/single-key
    /// range, or not reachable this sweep — retried next sweep).
    async fn suggest_split_point(
        &self,
        parent: &MetaPartition,
    ) -> Result<Option<PartitionBound>, PdError>;

    /// Tells the parent group's leader to perform the deterministic in-log
    /// split at `at`, deriving the PD-assigned `child_group` with `child_ino_tag`
    /// (03 §2/§8). Idempotent — a replayed split already outside the narrowed
    /// live range is a no-op.
    async fn apply_split(
        &self,
        parent: &MetaPartition,
        child_group: u64,
        at: &PartitionBound,
        child_ino_tag: u32,
    ) -> Result<(), PdError>;

    /// Ensures the target node hosts the (un-initialized) group so it can
    /// receive replication, then drives the source leader's membership swap
    /// (AddLearner → Promote → RemovePeer, 03 §2).
    async fn migrate_group_member(
        &self,
        partition: &MetaPartition,
        from: epoch_proto::NodeId,
        to: epoch_proto::NodeId,
    ) -> Result<(), PdError>;
}

/// Owns the scheduler task and aborts it on drop (mirrors
/// [`crate::chunk::driver::PlacementHandle`]).
#[derive(Debug)]
pub struct SchedulerHandle {
    task: JoinHandle<()>,
}

impl SchedulerHandle {
    /// Stops the scheduler task.
    pub fn stop(self) {}
}

impl Drop for SchedulerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the leader-gated partition-scheduler loop. A follower tick is a
/// no-op (leadership is re-checked every tick, same contract as the placement
/// loop), so no lifecycle plumbing is needed on a PD failover.
#[must_use]
pub fn spawn_meta_scheduler(
    journal: Arc<Journal>,
    admin: Arc<dyn MetaAdmin>,
    config: SchedulerConfig,
    interval: Duration,
) -> SchedulerHandle {
    let task = tokio::spawn(async move {
        let mut controller = OperatorController::new(config.max_concurrent_migrations);
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if !is_leader(&journal) {
                continue;
            }
            if let Err(err) = sweep(&journal, admin.as_ref(), &config, &mut controller).await {
                tracing::warn!(error = %err, "partition scheduler sweep failed");
            }
        }
    });
    SchedulerHandle { task }
}

fn is_leader(journal: &Journal) -> bool {
    journal.raft().metrics().borrow().state.is_leader()
}

/// One sweep: drop stale in-flight ops, gather the layout, plan, admit, execute.
async fn sweep(
    journal: &Journal,
    admin: &dyn MetaAdmin,
    config: &SchedulerConfig,
    controller: &mut OperatorController,
) -> Result<(), PdError> {
    let partitions = journal.state().partitions();
    let live_partitions = partitions.list();

    // Clear finished/stale slots: a running op whose partition's route epoch
    // moved past what it was admitted at is done (or was superseded); free the
    // slot so the partition can be planned again.
    let current_epochs: std::collections::HashMap<u64, u64> = live_partitions
        .iter()
        .map(|p| (p.partition_id, p.epoch))
        .collect();
    let running_ids: Vec<u64> = controller
        .running()
        .iter()
        .map(|op| op.partition_id)
        .collect();
    for pid in running_ids {
        match current_epochs.get(&pid) {
            Some(&epoch) if controller.is_stale(pid, epoch) => controller.complete(pid),
            None => controller.complete(pid), // partition gone (e.g. split away)
            _ => {}
        }
    }

    let loads = gather_loads(&live_partitions, partitions);
    let nodes = gather_meta_nodes(journal);

    let splits = plan_splits(&loads, config);
    let migrations = plan_migrations(&loads, &nodes, config);
    let admitted = controller.admit(&splits, &migrations);
    if admitted.is_empty() {
        return Ok(());
    }

    for op in admitted {
        let Some(partition) = partitions.get(op.partition_id) else {
            controller.complete(op.partition_id);
            continue;
        };
        let result = match op.kind {
            OpKind::Split => execute_split(journal, admin, &partition).await,
            OpKind::Migrate { from, to } => {
                execute_migrate(journal, admin, &partition, from, to).await
            }
        };
        if let Err(err) = result {
            // Free the slot so the next sweep re-plans; the change is either
            // already applied (idempotent) or will be retried.
            tracing::warn!(
                partition = op.partition_id,
                error = %err,
                "scheduler op failed; will re-plan next sweep"
            );
            controller.complete(op.partition_id);
        }
    }
    Ok(())
}

/// Joins each route record with its leader's last reported size into the pure
/// planner's input. A partition with no leader report contributes size 0 (it
/// simply won't cross the split threshold yet).
///
/// `pub` so the console's metadata-node view can reuse the same partition-load
/// join the scheduler uses (08 §5), rather than re-deriving it.
pub fn gather_loads(
    partitions: &[MetaPartition],
    manager: &crate::meta_mgr::MetaPartitionManager,
) -> Vec<PartitionLoad> {
    partitions
        .iter()
        .map(|p| {
            let total_bytes = manager
                .leader(p.partition_id)
                .map(|r| r.total_bytes)
                .unwrap_or(0);
            PartitionLoad {
                partition_id: p.partition_id,
                peers: p.peers.clone(),
                epoch: p.epoch,
                total_bytes,
                splittable: is_splittable(p),
            }
        })
        .collect()
}

/// A partition is splittable when its interval is not a single point — i.e.
/// there is room for a boundary strictly inside `[start, end)` (03 §2). An
/// unbounded side always leaves room.
fn is_splittable(p: &MetaPartition) -> bool {
    if p.start.unbounded || p.end.unbounded {
        return true;
    }
    (p.start.bucket, &p.start.routing_key) != (p.end.bucket, &p.end.routing_key)
}

/// Every registered MetaNode's identity + liveness for target selection.
fn gather_meta_nodes(journal: &Journal) -> Vec<MetaNodeInfo> {
    journal
        .state()
        .nodes()
        .list()
        .into_iter()
        .filter(|n| n.roles.contains(crate::cluster::RoleSet::META))
        .map(|n| MetaNodeInfo {
            node_id: n.node_id,
            status: n.status,
            rack: n.rack,
        })
        .collect()
}

/// Executes a split (three-step handshake, 01 §5 / 03 §2):
/// 1. ask the MetaNode leader for a boundary (it holds the data);
/// 2. propose the PD-side route narrow + child insert, which allocates the
///    child id and ino tag;
/// 3. push `ApplySplit` so the parent group performs the matching in-log split.
///
/// The controller slot clears when the parent's route epoch advances on a later
/// sweep. A failure at any step frees the slot and the next sweep re-plans.
async fn execute_split(
    journal: &Journal,
    admin: &dyn MetaAdmin,
    partition: &MetaPartition,
) -> Result<(), PdError> {
    let Some(at) = admin.suggest_split_point(partition).await? else {
        return Ok(()); // MetaNode deferred; retry next sweep
    };
    let (child_id, child_ino_tag) = match journal
        .split_partition(partition.partition_id, at.clone())
        .await?
    {
        ApplyResult::PartitionSplit {
            child_id,
            child_ino_tag,
        } => (child_id, child_ino_tag),
        ApplyResult::Rejected(reason) => {
            tracing::warn!(?reason, parent = partition.partition_id, "split rejected");
            return Ok(());
        }
        other => return Err(PdError::Raft(format!("unexpected split result: {other:?}"))),
    };
    // The parent's peers are the child's peers (same replica set; a later
    // migrate may rebalance). Drive the in-log split on the parent group.
    admin
        .apply_split(partition, child_id, &at, child_ino_tag)
        .await?;
    tracing::info!(
        parent = partition.partition_id,
        child = child_id,
        child_ino_tag,
        "partition split driven"
    );
    Ok(())
}

/// Executes a migrate: propose the PD-side voter swap, then push the MetaNode
/// membership change (data movement). Idempotent — a swap already reflected in
/// the route record is a raft no-op, and the MetaNode membership steps are
/// themselves idempotent.
async fn execute_migrate(
    journal: &Journal,
    admin: &dyn MetaAdmin,
    partition: &MetaPartition,
    from: epoch_proto::NodeId,
    to: epoch_proto::NodeId,
) -> Result<(), PdError> {
    match journal
        .migrate_partition(partition.partition_id, from, to)
        .await?
    {
        ApplyResult::PartitionMigrated { .. } | ApplyResult::Applied => {}
        ApplyResult::Rejected(reason) => {
            tracing::warn!(
                ?reason,
                partition = partition.partition_id,
                "migrate rejected"
            );
            return Ok(());
        }
        other => {
            return Err(PdError::Raft(format!(
                "unexpected migrate result: {other:?}"
            )));
        }
    }
    // Re-read the post-swap membership so the target-preparation push carries
    // the new voter set.
    let updated = journal
        .state()
        .partitions()
        .get(partition.partition_id)
        .unwrap_or_else(|| partition.clone());
    admin.migrate_group_member(&updated, from, to).await?;
    tracing::info!(
        partition = partition.partition_id,
        from = from.get(),
        to = to.get(),
        "partition migrate driven"
    );
    Ok(())
}
