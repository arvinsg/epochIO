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

//! Replicated command (`D`) and application response (`R`) for the PD raft group.
//!
//! Every PD mutation is proposed as one [`PdEntry`] and, once committed, applied
//! serially by the state machine (INVARIANT(design 01 §2 / Q18): apply is
//! single-threaded and never reads wall-clock time or heartbeat statistics, so
//! the replicated result is deterministic from the committed command alone).
//!
//! The enum grows one variant per module (chunk / disk / node / config / bucket
//! / meta_partition / job / kv, design 01 §2) as those managers land; the node
//! (cluster membership) variants arrive first alongside [`PdEntry::Noop`], which
//! exercises and health-checks the propose→apply path.

use epoch_proto::{BucketId, ChunkId, DiskId, NodeId, WriterToken};
use serde::{Deserialize, Serialize};

use crate::bucket::CreateBucket;
use crate::chunk::model::{CommitChunk, CreateChunkStaging, RebumpStaging, SealChunk};
use crate::cluster::RejectReason;
use crate::cluster::disk::{RegisterDisk, UpdateDiskStatus};
use crate::cluster::node::{RegisterNode, RemoveNode, UpdateNodeStatus};
use crate::config_mgr::{DeleteConfig, PutConfig};
use crate::credential::PutCredential;
use crate::meta_mgr::{CreatePartition, MigratePartition, SplitPartition};
use crate::shard_repair::{CommitShardRepair, ReportShardRepair};
use crate::writer::model::{MarkWriterDead, RegisterWriter};

/// A single replicated PD mutation (openraft application data `D`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PdEntry {
    /// A no-op mutation: advances the applied log id without changing PD state.
    /// Used to exercise and health-check the raft propose→apply path.
    Noop,
    /// Register a node, or reclaim its id if its address is already known.
    RegisterNode(RegisterNode),
    /// Transition a node to a new lifecycle status.
    UpdateNodeStatus(UpdateNodeStatus),
    /// Remove a decommissioned node from the cluster.
    RemoveNode(RemoveNode),
    /// Register a disk, or reclaim its id if its `(node, path)` is already known.
    RegisterDisk(RegisterDisk),
    /// Transition a disk to a new lifecycle status.
    UpdateDiskStatus(UpdateDiskStatus),
    /// Begin two-phase chunk creation: allocate an id and write a staging plan.
    CreateChunkStaging(CreateChunkStaging),
    /// Promote a staging plan to a committed `Writable` chunk.
    CommitChunk(CommitChunk),
    /// Bump a staging plan's shard epoch on crash recovery.
    RebumpStaging(RebumpStaging),
    /// Seal a committed chunk (the migration/decommission fence, 01 §4.2).
    SealChunk(SealChunk),
    /// Create a bucket (idempotent by name; allocates a never-reused id).
    CreateBucket(CreateBucket),
    /// Tombstone a bucket for deletion (99-Q15): flips it to `Deleting`.
    TombstoneBucket(crate::bucket::TombstoneBucket),
    /// Purge a tombstoned bucket's identity record after its records are gone.
    PurgeBucket(crate::bucket::PurgeBucket),
    /// Create a MetaNode range partition (01 §5; peers chosen by the leader).
    CreatePartition(CreatePartition),
    /// Split a MetaNode range partition at a boundary (03 §2; route-table
    /// narrow + child insert, matching the MetaNode's in-log split).
    SplitPartition(SplitPartition),
    /// Migrate a MetaNode partition replica (03 §2; voter-set swap + epoch
    /// bump, matching the MetaNode's openraft membership change).
    MigratePartition(MigratePartition),
    /// Write a cluster-level config KV entry (01 §1 配置中心).
    PutConfig(PutConfig),
    /// Store (create or replace) an access-key credential (01 §6 认证 N5).
    PutCredential(PutCredential),
    /// Delete a cluster-level config KV entry (idempotent).
    DeleteConfig(DeleteConfig),
    /// Issue a fresh writer token to a gateway node (never reused).
    RegisterWriter(RegisterWriter),
    /// Retire a writer token whose session heartbeat has lapsed (one-way).
    MarkWriterDead(MarkWriterDead),
    /// A two-level-scheduler Job lifecycle command (01 §6: create / assign /
    /// renew lease / advance watermark / complete).
    Job(crate::job::JobCommand),
    /// Rebind one shard slot to a rebuilt extent (01 §6.4): a repair coordinator's
    /// shard-mapping commit, gated by Job authorization + epoch match.
    CommitShardMapping(crate::chunk::model::CommitShardMapping),
    /// Report a bad/missing shard, creating a ShardRepair ticket (01 §6.3): the
    /// heal-on-read / scrub fast path. PD resolves the target node + epoch from
    /// committed state at apply.
    ReportShardRepair(ReportShardRepair),
    /// Commit a completed single-shard repair (01 §6.3): rebind the slot to the
    /// rebuilt extent (epoch+1), gated by a matching ShardRepair ticket rather
    /// than a Job (01 §6.4 invariant 2).
    CommitShardRepair(CommitShardRepair),
}

/// The result of applying one committed entry (openraft application response `R`).
///
/// A [`Rejected`](ApplyResult::Rejected) outcome is a normal, deterministic
/// result (the command was valid raft input but not applicable to current
/// state), distinct from a storage error; the proposing caller inspects it to
/// react (retry, surface a 4xx, …). The rejection vocabulary is borrowed in
/// spirit from curvine's `ApplyOutcome`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
    /// The entry was applied successfully with no returned value.
    #[default]
    Applied,
    /// A node registration was applied; carries the node's (possibly reclaimed) id.
    NodeRegistered {
        /// The assigned or reclaimed cluster node id.
        node_id: NodeId,
    },
    /// A disk registration was applied; carries the disk's (possibly reclaimed) id.
    DiskRegistered {
        /// The assigned or reclaimed disk id.
        disk_id: DiskId,
    },
    /// A chunk staging plan was written; carries the allocated chunk id.
    ChunkStaged {
        /// The allocated chunk id.
        chunk_id: ChunkId,
    },
    /// A writer token was issued; carries the assigned (never-reused) token.
    WriterRegistered {
        /// The assigned writer token.
        token: WriterToken,
    },
    /// A bucket was created (or reclaimed by name); carries its id.
    BucketCreated {
        /// The assigned (never-reused) bucket id.
        bucket_id: BucketId,
    },
    /// A bucket was tombstoned or purged; carries its id. `Deleted` covers both
    /// the tombstone (write-refusing) and the final purge steps.
    BucketDeleted {
        /// The bucket's id.
        bucket_id: BucketId,
    },
    /// A partition was created (or an exact-range retry reclaimed); carries its id.
    PartitionCreated {
        /// The assigned (never-reused) partition id (== its raft group id).
        partition_id: u64,
    },
    /// A partition was split; carries the new right-child partition id.
    PartitionSplit {
        /// The assigned (never-reused) child partition id (== its raft group id).
        child_id: u64,
        /// The child's ino partition tag (03 §6.5): PD-allocated, unique across
        /// the namespace so child inodes never collide with a sibling's.
        child_ino_tag: u32,
    },
    /// A partition's replica set was migrated; carries the new voter set.
    PartitionMigrated {
        /// The partition's new voter node ids after the swap.
        peers: Vec<NodeId>,
    },
    /// A Job was created; carries its assigned (never-reused) id.
    JobCreated {
        /// The assigned job id.
        job_id: u32,
    },
    /// A shard slot was rebound to a rebuilt extent; carries the new slot epoch
    /// (01 §6.4 invariant 5: rebind bumps the epoch, gateways invalidate by it).
    ShardRebound {
        /// The slot's epoch after the rebind (old + 1).
        new_epoch: u32,
    },
    /// A ShardRepair ticket was recorded (or an existing one matched) for a
    /// reported bad shard (01 §6.3). `created` is false for an idempotent
    /// re-report of an already-pending shard.
    ShardRepairReported {
        /// Whether a new ticket was created (false = one already existed).
        created: bool,
    },
    /// The mutation was rejected without changing state.
    Rejected(RejectReason),
}
