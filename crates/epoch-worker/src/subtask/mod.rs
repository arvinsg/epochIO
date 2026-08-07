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

//! The idempotent subtask contract (01 §6.2 / 02 §3.2): the unit a Job
//! coordinator expands its Job into and dispatches. Every subtask must be
//! idempotent — re-running it (after a coordinator crash + reassignment, or a
//! brief dual-coordinator window) has no additional effect — which is the whole
//! basis of the two-level scheduler's crash safety.
//!
//! This module defines the trait and its result vocabulary. Concrete executors
//! (`migrate` / `shard_repair` / `inspect` / `gc`) land in later M7 sub-phases
//! with the data-plane wiring they need (seal fence, shard rebind, EC rebuild);
//! this phase supplies the contract the coordinator drives against and a test
//! stub proving the framework.
//!
//! Design: docs/design/01-pd.md §6.2; docs/design/02-datanode.md §3.2

use async_trait::async_trait;

/// A subtask execution error (transient — the coordinator retries or the Job is
/// reassigned). Terminal correctness failures surface as their own variants as
/// concrete executors are added.
#[derive(Debug, thiserror::Error)]
pub enum SubtaskError {
    /// A dependency was not ready (peer unreachable, source not sealed yet);
    /// the coordinator retries on the next pass.
    #[error("subtask not ready: {0}")]
    NotReady(String),
    /// The subtask hit an unrecoverable condition for this attempt.
    #[error("subtask failed: {0}")]
    Failed(String),
}

/// One idempotent unit of Job work (01 §6.2). `execute` must be safe to call
/// more than once; `is_done` lets the coordinator skip already-satisfied work
/// when re-expanding a Job from its progress watermark after reassignment.
#[async_trait]
pub trait Subtask: Send + Sync {
    /// A stable identifier for this subtask within its Job (e.g. a shard id),
    /// used for progress accounting and de-duplication.
    fn id(&self) -> u64;

    /// Whether the subtask's effect is already present (idempotent skip). A
    /// coordinator checks this before `execute` so a re-expansion after a crash
    /// does not redo completed work.
    async fn is_done(&self) -> Result<bool, SubtaskError>;

    /// Performs the subtask's work (idempotent). Returns once the effect is
    /// durably in place.
    async fn execute(&self) -> Result<(), SubtaskError>;
}
