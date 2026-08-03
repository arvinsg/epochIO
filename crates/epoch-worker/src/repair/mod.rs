//! RepairDisk data-plane execution (M7, 01 §6.3 / 04 §5): the rebuild primitive
//! and (later sub-steps) the RepairDisk `JobExpander` + per-shard `Subtask`.
//!
//! Design: docs/design/01-pd.md §6.3; docs/design/04-ec-io.md §5

pub mod rebuild;
pub mod shard_repair;
pub mod subtask;

pub use rebuild::rebuild_shard_body;
pub use shard_repair::ShardRepairTask;
pub use subtask::{ChunkLayout, CommitOutcome, RepairBackend, RepairSubtask};
