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

//! RepairDisk data-plane execution (M7, 01 §6.3 / 04 §5): the rebuild primitive
//! and (later sub-steps) the RepairDisk `JobExpander` + per-shard `Subtask`.

pub mod rebuild;
pub mod shard_repair;
pub mod subtask;

pub use rebuild::rebuild_shard_body;
pub use shard_repair::ShardRepairTask;
pub use subtask::{ChunkLayout, CommitOutcome, RepairBackend, RepairSubtask};
