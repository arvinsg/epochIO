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

//! MetaNode partition scheduler (01 §5): the PD-leader background driver that
//! keeps the metadata partition layout healthy — splitting partitions that
//! outgrow the size threshold and migrating replicas off failed or overloaded
//! nodes. PD only *decides and proposes*; the deterministic split executes
//! inside the MetaNode parent group's raft log (03 §2) and the data movement
//! rides openraft membership changes, both driven by admin RPCs.
//!
//! Layering mirrors the chunk placement loop ([`crate::chunk::driver`]): a pure
//! decision layer ([`plan`]) feeds a serializing controller ([`operator`]),
//! which a leader-gated ticker ([`driver`]) turns into proposals + pushes.

pub mod driver;
pub mod operator;
pub mod plan;

pub use driver::{MetaAdmin, SchedulerHandle, gather_loads, spawn_meta_scheduler};
pub use operator::{OpKind, OpPriority, OperatorController, PartitionOp};
pub use plan::{
    MetaNodeInfo, MigrateDecision, MigrateReason, PartitionLoad, SchedulerConfig, SplitDecision,
    plan_migrations, plan_splits,
};
