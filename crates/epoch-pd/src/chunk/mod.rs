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

//! Chunk lifecycle: EC-group creation (watermark-driven placement + two-phase
//! staging), the committed chunk model, and crash-recovery of half-created
//! plans.
//!
//! [`placement`] is the pure disk-selection policy; [`model`] holds the
//! replicated chunk / staging records and creation commands; [`manager`] owns
//! the backing column families and applies the commands. Per-chunk capacity
//! (`free` / `used`) is heartbeat-derived leader memory (Q18) and is not part of
//! this replicated state.
//!
//! Design: docs/design/01-pd.md §3 (Chunk model); §4.1 (creation / recovery)

pub mod driver;
pub mod manager;
pub mod model;
pub mod placement;
pub mod writable_set;

pub use driver::{PlacementConfig, PlacementHandle, spawn_placement};
pub use manager::ChunkManager;
pub use model::{
    Chunk, ChunkStatus, ChunkStatusHistogram, CommitChunk, CommitShardMapping, CreateChunkStaging,
    RebumpStaging, ShardSlot, SlotPlan, StagingChunk,
};
pub use placement::{AntiAffinity, DiskCandidate, plan_placement};
pub use writable_set::{DiskHealth, WritableThreshold, is_publishable};
