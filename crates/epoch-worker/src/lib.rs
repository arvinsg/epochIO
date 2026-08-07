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

//! epoch-worker (L3): task executors and the Job coordinator.
//!
//! The lower tier of the two-level scheduler: a coordinator expands a PD Job
//! into idempotent per-shard/per-extent subtasks, dispatches them (with data
//! affinity), aggregates results, and batches progress back to PD.
//!
//! Design: docs/design/02-datanode.md §3; docs/design/01-pd.md §6;
//! docs/design/06-code-layout.md §11
//!
//! M7 (first phase) delivers the coordinator *framework* — the expand → run →
//! checkpoint loop over an idempotent [`Subtask`] contract, parameterized by a
//! [`JobExpander`] and a [`ProgressSink`]. Concrete subtask executors (repair /
//! migrate / inspect / gc) and the gRPC sink land with their data-plane wiring
//! in later M7 sub-phases.

pub mod coordinator;
pub mod gc;
pub mod inspect;
pub mod repair;
pub mod subtask;

pub use coordinator::{JobExpander, ProgressSink, run_job};
pub use gc::{GcBackend, GcSubtask, LiveTokens, orphans};
pub use inspect::{InspectBackend, InspectSubtask};
pub use repair::rebuild_shard_body;
pub use subtask::{Subtask, SubtaskError};
