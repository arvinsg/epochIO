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

//! epoch-store (L2): the Extent storage engine.
//!
//! Superblock, extent files, per-disk index, blob write/read, compaction, QoS
//! and local scrub. Read addressing is by `shard_id` (with epoch fence);
//! DataNode resolves the current extent locally (compaction rebind never
//! reaches PD).
//!
//! Design: docs/design/02-datanode.md §1; docs/design/06-code-layout.md §6
//!
//! M2 delivers the single-node core end to end: superblock/disk, extent files,
//! index, blob write/read, compaction, Background-QoS and scrub
//! (docs/design/07-iteration-plan.md).

pub mod compact;
pub mod disk;
pub mod error;
pub mod extent;
pub mod handler;
pub mod index;
pub mod meta_merge;
mod metrics;
pub mod qos;
pub mod read;
pub mod scrub;
pub mod service;
pub mod superblock;
pub mod write;

mod codec;
mod io_pool;
mod writer;

#[cfg(test)]
mod testutil;

pub use compact::CompactOutcome;
pub use disk::{Disk, DiskError, space_stats};
pub use error::StoreError;
pub use handler::EngineHandler;
pub use index::{BlobIndex, DiskIndex, ExtentMeta, IndexError};
pub use qos::{IoClass, QosConfig};
pub use read::read_blob;
pub use scrub::ScrubReport;
pub use service::StorageEngine;
pub use superblock::{SUPERBLOCK_SIZE, Superblock, SuperblockError};
pub use write::{WriteOutcome, write_blob};
