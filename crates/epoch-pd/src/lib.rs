//! epoch-pd (L3): the Placement Driver service.
//!
//! Single raft group with modular propose: cluster/disk/node membership,
//! chunk placement (disk-watermark driven), writer_token issuance, MetaNode
//! partition management, and coarse-grained Job scheduling.
//!
//! M4 builds this incrementally. The scaffold provides the openraft integration
//! ([`raft`]) — log store, state machine, snapshot, and peer network — and the
//! [`journal`] propose entry point over a single-node group. Cluster membership
//! ([`cluster`]) is the first domain manager; its state is shared with the read
//! path through [`state::PdState`]. Chunk / writer registry / bucket managers
//! land in later phases.
//!
//! Design: docs/design/01-pd.md; docs/design/06-code-layout.md §8

pub mod bucket;
pub mod chunk;
pub mod cluster;
pub mod config_mgr;
pub mod console;
pub mod credential;
pub mod error;
pub mod job;
pub mod journal;
pub mod meta_mgr;
pub mod meta_sched;
pub mod raft;
pub mod service;
pub mod shard_repair;
pub mod state;
pub mod writer;

pub use bucket::{BucketManager, BucketMeta, MetaEngine, NsMode};
pub use chunk::{
    AntiAffinity, Chunk, ChunkManager, ChunkStatus, CommitShardMapping, DiskCandidate,
    PlacementConfig, PlacementHandle, ShardSlot, SlotPlan, StagingChunk, WritableThreshold,
    plan_placement, spawn_placement,
};
pub use cluster::{
    Clock, DiskHeartbeat, HeartbeatReport, LivenessConfig, LivenessHandle, SystemClock,
};
pub use cluster::{Disk, DiskStatus, Node, NodeStatus, RejectReason, RoleSet};
pub use config_mgr::ConfigManager;
pub use error::PdError;
pub use job::types::{Job, JobKind, JobState};
pub use job::{JobCommand, JobManager, JobOutcome, sweep_job_assignments};
pub use journal::{ApplyResult, Journal, PdEntry, WriterLivenessHandle};
pub use meta_mgr::{
    CreatePartition, LeaderReport, MetaPartition, MetaPartitionManager, PartitionBound,
};
pub use raft::grpc_service::PdRaftPeerService;
pub use raft::{NodeId, PdRaft, PdTypeConfig};
pub use service::PdControlService;
pub use shard_repair::{ShardRepairRegistry, ShardRepairTicket};
pub use state::PdState;
pub use writer::DEFAULT_WRITER_DEAD_AFTER_MILLIS;
pub use writer::{WriterManager, WriterRecord, WriterStatus};
