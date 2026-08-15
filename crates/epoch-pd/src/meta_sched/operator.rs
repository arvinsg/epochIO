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

//! Operator controller (01 §5): serializes the scheduler's decisions into
//! at-most-one in-flight operation per partition, with a bounded number of
//! concurrent migrations and a priority order so evacuations outrank balance
//! moves outrank splits.
//!
//! The controller is intentionally small — it is the "who may run now" gate,
//! not an execution engine. The [`driver`](super::driver) asks it to admit a
//! batch of decisions each sweep; the controller returns only those it will
//! allow to start this sweep and records them as running. When the driver later
//! observes an operation finished (the route epoch advanced past what the op
//! would produce), it clears the slot. A partition whose route epoch changed
//! out from under a pending decision is rejected (staleness guard, the
//! ConfVerChanged idea): the next sweep re-plans on fresh state.

use std::collections::HashMap;

use crate::meta_sched::plan::{MigrateDecision, MigrateReason, SplitDecision};

/// The kind of operation queued against a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// Split the partition (MetaNode picks the boundary).
    Split,
    /// Migrate a replica `from` → `to`.
    Migrate {
        /// Voter removed.
        from: epoch_proto::NodeId,
        /// Voter added.
        to: epoch_proto::NodeId,
    },
}

/// One in-flight scheduler operation against a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionOp {
    /// The partition (== raft group) id.
    pub partition_id: u64,
    /// What to do.
    pub kind: OpKind,
    /// The route epoch when the op was admitted (staleness guard).
    pub origin_epoch: u64,
    /// Scheduling priority (higher runs first when contending for the budget).
    pub priority: OpPriority,
}

/// Priority ladder: evacuation of a failed node outranks opportunistic balance,
/// which outranks a split (splits are zero-IO and never urgent). Ordered so a
/// numerically larger value is more urgent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OpPriority {
    /// A split (zero IO, lowest urgency).
    Split = 0,
    /// A rebalancing migration.
    Balance = 1,
    /// Evacuating a replica off a non-live node (highest urgency).
    Evacuate = 2,
}

/// Serializes scheduler decisions: at most one op per partition, a bounded
/// number of concurrent migrations, priority-ordered admission.
#[derive(Debug, Default)]
pub struct OperatorController {
    /// Currently in-flight ops, keyed by partition id (at most one each).
    running: HashMap<u64, PartitionOp>,
    /// Ceiling on concurrent migration ops (splits are unbounded — zero IO).
    max_concurrent_migrations: usize,
}

impl OperatorController {
    /// A controller allowing `max_concurrent_migrations` in-flight migrations.
    #[must_use]
    pub fn new(max_concurrent_migrations: usize) -> Self {
        Self {
            running: HashMap::new(),
            max_concurrent_migrations,
        }
    }

    /// The number of in-flight migration ops.
    fn running_migrations(&self) -> usize {
        self.running
            .values()
            .filter(|op| matches!(op.kind, OpKind::Migrate { .. }))
            .count()
    }

    /// Whether a partition already has an op in flight.
    #[must_use]
    pub fn is_running(&self, partition_id: u64) -> bool {
        self.running.contains_key(&partition_id)
    }

    /// The set of in-flight ops (observation / tests).
    #[must_use]
    pub fn running(&self) -> Vec<PartitionOp> {
        let mut ops: Vec<PartitionOp> = self.running.values().copied().collect();
        ops.sort_by_key(|op| op.partition_id);
        ops
    }

    /// Admits a batch of decisions for this sweep, returning the ops the driver
    /// should start now. Decisions for a partition that already has an op in
    /// flight are skipped; migrations beyond the concurrency budget are
    /// deferred to a later sweep. Admitted ops are recorded as running.
    ///
    /// Evacuations are considered before balance moves before splits (priority
    /// order), so the scarce migration budget goes to the most urgent work.
    pub fn admit(
        &mut self,
        splits: &[SplitDecision],
        migrations: &[MigrateDecision],
    ) -> Vec<PartitionOp> {
        let mut admitted = Vec::new();

        // Migrations first, in priority order (Evacuate before Balance), so the
        // migration budget is spent on the most urgent moves.
        let mut ordered: Vec<&MigrateDecision> = migrations.iter().collect();
        ordered.sort_by(|a, b| {
            migrate_priority(b.reason)
                .cmp(&migrate_priority(a.reason))
                .then(a.partition_id.cmp(&b.partition_id))
        });
        for m in ordered {
            if self.is_running(m.partition_id) {
                continue;
            }
            if self.running_migrations() >= self.max_concurrent_migrations {
                continue; // budget exhausted; retried next sweep
            }
            let op = PartitionOp {
                partition_id: m.partition_id,
                kind: OpKind::Migrate {
                    from: m.from,
                    to: m.to,
                },
                origin_epoch: m.epoch,
                priority: migrate_priority(m.reason),
            };
            self.running.insert(m.partition_id, op);
            admitted.push(op);
        }

        // Splits are zero-IO: admit every one whose partition is free.
        for s in splits {
            if self.is_running(s.parent_id) {
                continue;
            }
            let op = PartitionOp {
                partition_id: s.parent_id,
                kind: OpKind::Split,
                origin_epoch: s.epoch,
                priority: OpPriority::Split,
            };
            self.running.insert(s.parent_id, op);
            admitted.push(op);
        }

        admitted
    }

    /// Clears the in-flight slot for `partition_id` — call when the op finished
    /// (route epoch advanced) or is being abandoned as stale.
    pub fn complete(&mut self, partition_id: u64) {
        self.running.remove(&partition_id);
    }

    /// Whether the op recorded for `partition_id` is stale: the partition's
    /// current route epoch differs from the epoch the op was admitted at, so
    /// some other change landed and the op must be dropped and re-planned. A
    /// partition with no in-flight op is never stale.
    #[must_use]
    pub fn is_stale(&self, partition_id: u64, current_epoch: u64) -> bool {
        self.running
            .get(&partition_id)
            .is_some_and(|op| op.origin_epoch != current_epoch)
    }
}

/// Maps a migration reason to its operator priority.
fn migrate_priority(reason: MigrateReason) -> OpPriority {
    match reason {
        MigrateReason::Evacuate => OpPriority::Evacuate,
        MigrateReason::Balance => OpPriority::Balance,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::NodeId;

    fn split(id: u64, epoch: u64) -> SplitDecision {
        SplitDecision {
            parent_id: id,
            epoch,
        }
    }

    fn migrate(id: u64, from: u32, to: u32, reason: MigrateReason) -> MigrateDecision {
        MigrateDecision {
            partition_id: id,
            from: NodeId::new(from),
            to: NodeId::new(to),
            reason,
            epoch: 1,
        }
    }

    #[test]
    fn at_most_one_op_per_partition() {
        let mut ctrl = OperatorController::new(4);
        let admitted = ctrl.admit(&[split(1, 1)], &[migrate(1, 2, 3, MigrateReason::Evacuate)]);
        // Partition 1 gets exactly one op (the migration, admitted first).
        assert_eq!(admitted.len(), 1);
        assert!(matches!(admitted[0].kind, OpKind::Migrate { .. }));
        assert!(ctrl.is_running(1));
    }

    #[test]
    fn migration_budget_caps_concurrent_moves() {
        let mut ctrl = OperatorController::new(2);
        let migrations = vec![
            migrate(1, 9, 3, MigrateReason::Evacuate),
            migrate(2, 9, 4, MigrateReason::Evacuate),
            migrate(3, 9, 5, MigrateReason::Evacuate),
        ];
        let admitted = ctrl.admit(&[], &migrations);
        assert_eq!(admitted.len(), 2, "third migration deferred by the budget");
    }

    #[test]
    fn splits_are_not_capped_by_the_migration_budget() {
        let mut ctrl = OperatorController::new(0); // no migrations allowed
        let splits = vec![split(1, 1), split(2, 1), split(3, 1)];
        let admitted = ctrl.admit(&splits, &[]);
        assert_eq!(admitted.len(), 3, "splits are zero-IO, never budget-capped");
    }

    #[test]
    fn evacuation_admitted_before_balance() {
        let mut ctrl = OperatorController::new(1);
        let migrations = vec![
            migrate(1, 9, 3, MigrateReason::Balance),
            migrate(2, 9, 4, MigrateReason::Evacuate),
        ];
        let admitted = ctrl.admit(&[], &migrations);
        assert_eq!(admitted.len(), 1);
        assert_eq!(
            admitted[0].partition_id, 2,
            "evacuation wins the single migration slot"
        );
        assert_eq!(admitted[0].priority, OpPriority::Evacuate);
    }

    #[test]
    fn a_running_partition_is_not_re_admitted() {
        let mut ctrl = OperatorController::new(4);
        ctrl.admit(&[], &[migrate(1, 9, 3, MigrateReason::Evacuate)]);
        let again = ctrl.admit(&[split(1, 1)], &[migrate(1, 9, 3, MigrateReason::Evacuate)]);
        assert!(again.is_empty(), "partition 1 already has an op in flight");
        // After completion, it can be admitted again.
        ctrl.complete(1);
        let after = ctrl.admit(&[split(1, 1)], &[]);
        assert_eq!(after.len(), 1);
    }

    #[test]
    fn stale_when_epoch_advanced_under_a_running_op() {
        let mut ctrl = OperatorController::new(4);
        ctrl.admit(&[split(1, 5)], &[]);
        assert!(!ctrl.is_stale(1, 5), "same epoch is fresh");
        assert!(ctrl.is_stale(1, 6), "epoch advanced -> stale");
        assert!(!ctrl.is_stale(99, 6), "no op -> never stale");
    }
}
