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

//! Chunk domain model: the EC-group record PD replicates, its shard slots, the
//! lifecycle status, and the two-phase creation commands.
//!
//! A [`Chunk`] is a committed EC group of `N+M` [`ShardSlot`]s (each shard is
//! peer-equal; there is no per-group allocator, 01 §4.3). Creation is two-phase
//! (01 §4.1): a [`CreateChunkStaging`] writes a [`StagingChunk`] plan and
//! allocates the chunk id, extents are created out-of-band, then [`CommitChunk`]
//! promotes the plan to a committed `Writable` chunk. [`RebumpStaging`] bumps the
//! shard epoch on crash recovery so a retried extent creation cannot collide
//! with a half-created one (01 §4.1: "epoch jump +3").
//!
//! Only the chunk's durable identity lives here; per-chunk `free` / `used` are
//! heartbeat-derived capacity statistics kept in leader memory (Q18), not
//! replicated — they arrive with the writable-set path in a later phase.
//!
//! INVARIANT(design 01 §3): `shards.len() == code_mode.shards_total()`, slot `i`
//! holds shard index `i`, and `shard_prefix` is the epoch-zeroed stable identity
//! (`chunk_id<<32 | index<<24`).

use epoch_proto::{ChunkId, CodeMode, DiskId, ExtentId, NodeId, ShardId};
use serde::{Deserialize, Serialize};

/// The lifecycle status of a committed chunk (design 01 §3).
///
/// M4 only ever creates chunks as [`Writable`](ChunkStatus::Writable); the
/// `Writable ⇄ Full` capacity transitions and the `Sealed` migration/repair
/// fence are driven in later milestones (seal lands with the writable-set path;
/// migration/repair with M7), so no transition-validation function is defined
/// yet — it arrives with its first driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkStatus {
    /// Accepting new blob writes (creation state).
    Writable,
    /// Full; may return to `Writable` after compaction reclaims space.
    Full,
    /// Fenced for migration / repair / decommission; rejects writes.
    Sealed,
    /// Being migrated to new shard bindings.
    Migrating,
    /// Unusable (e.g. too many shards lost).
    Broken,
}

/// A count of committed chunks by lifecycle status (08 §5 console COUNTS). All
/// fields are counts, never a chunk list — the overview + Chunk admin pages
/// render the distribution without enumerating 100k-scale chunks (§5 red line).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkStatusHistogram {
    /// Total committed chunks.
    pub total: u64,
    /// Chunks accepting writes.
    pub writable: u64,
    /// Full chunks.
    pub full: u64,
    /// Sealed (migration/repair/decommission fence).
    pub sealed: u64,
    /// Chunks being migrated.
    pub migrating: u64,
    /// Broken chunks (below quorum / unusable).
    pub broken: u64,
}

/// One shard slot of a chunk: its stable identity, current epoch, and the disk /
/// extent it is bound to (design 01 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSlot {
    /// Epoch-zeroed stable identity `chunk_id<<32 | index<<24` (secondary-index key).
    pub shard_prefix: u64,
    /// Current epoch; bumped when the slot is re-bound to a new extent (01 §4.4).
    pub epoch: u32,
    /// Disk the shard's extent lives on.
    pub disk_id: DiskId,
    /// The bound on-disk extent container.
    pub extent_id: ExtentId,
}

impl ShardSlot {
    /// The full shard id (`shard_prefix | epoch`).
    ///
    /// Composing by OR is exact because `shard_prefix` has its epoch bits zeroed
    /// and `epoch` was range-checked (24-bit) when the slot was created.
    #[must_use]
    pub fn shard_id(self) -> ShardId {
        ShardId::from_raw(self.shard_prefix | u64::from(self.epoch))
    }

    /// The shard index within the EC stripe (`0..N+M`).
    #[must_use]
    pub fn index(self) -> u8 {
        self.shard_id().index()
    }
}

/// A committed chunk: an EC group of `N+M` shard slots (design 01 §3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// PD-assigned EC-group id.
    pub chunk_id: ChunkId,
    /// Erasure-code parameters (published to gateways for encoding).
    pub code_mode: CodeMode,
    /// Lifecycle status.
    pub status: ChunkStatus,
    /// The `N+M` shard slots, slot `i` holding shard index `i`.
    pub shards: Vec<ShardSlot>,
}

/// An in-flight chunk creation plan awaiting extent creation + commit (01 §4.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingChunk {
    /// The allocated chunk id (final id once committed).
    pub chunk_id: ChunkId,
    /// Erasure-code parameters chosen for the chunk.
    pub code_mode: CodeMode,
    /// The planned shard slots (disk + extent bindings).
    pub shards: Vec<ShardSlot>,
}

/// One shard slot's disk assignment, chosen by placement before proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotPlan {
    /// Shard index within the stripe (`0..N+M`).
    pub index: u8,
    /// Disk the shard's extent will be created on.
    pub disk_id: DiskId,
}

/// Begin two-phase chunk creation: allocate a chunk id and write a staging plan.
///
/// `create_ts` and `epoch` are chosen by the leader before proposal so apply
/// stays deterministic (AGENTS §8): the extent ids are derived from them, never
/// from a clock read inside apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateChunkStaging {
    /// Erasure-code parameters (its `shards_total` fixes the slot count).
    pub code_mode: CodeMode,
    /// Per-slot disk assignment; must cover indices `0..shards_total` exactly once.
    pub slots: Vec<SlotPlan>,
    /// Leader-chosen extent creation timestamp (embedded in each `ExtentId`).
    pub create_ts: i64,
    /// Starting shard epoch (0 for a fresh plan).
    pub epoch: u32,
}

/// Promote a staging plan to a committed `Writable` chunk (01 §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitChunk {
    /// The staged chunk to commit.
    pub chunk_id: ChunkId,
}

/// Seal a committed chunk: the migration/decommission fence (01 §4.2). A
/// sealed chunk rejects new writes; gateways hitting `Sealed` rewrite blobs to
/// a different chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealChunk {
    /// The chunk to seal.
    pub chunk_id: ChunkId,
}

/// Bump a staging plan's shard epoch (by [`EPOCH_JUMP`]) on crash recovery, so a
/// retried extent creation gets fresh shard / extent ids (01 §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebumpStaging {
    /// The staged chunk to re-plan.
    pub chunk_id: ChunkId,
}

/// Epoch increment applied on each crash-recovery retry of a staging chunk.
///
/// A jump of 3 (rather than 1) leaves margin so that even a few successive
/// crashes cannot reuse a shard/extent id that an earlier attempt may have
/// half-created on a DataNode (01 §4.1).
pub const EPOCH_JUMP: u32 = 3;

/// Rebind one shard slot of a committed chunk to a freshly-rebuilt extent on a
/// healthy disk (01 §6.4 invariant 2: shard-mapping rebind is the *only* thing a
/// repair coordinator changes, and it goes through PD raft). The apply verifies
/// the committer holds a valid Job at the expected epoch
/// ([`authorize_commit`](crate::job::authorize_commit)) before repointing the
/// slot and bumping its epoch (01 §6.4 invariant 5).
///
/// `expected_epoch` is the slot's current epoch as the coordinator saw it; a
/// mismatch means another change landed and the commit is rejected (the
/// ConfVerChanged guard).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitShardMapping {
    /// The Job authorizing this rebind (must be `Running`, owned by `committer`).
    pub job_id: u32,
    /// The chunk whose shard slot is rebound.
    pub chunk_id: ChunkId,
    /// The shard index within the chunk (`0..N+M`).
    pub index: u8,
    /// The slot epoch the coordinator observed (staleness guard).
    pub expected_epoch: u32,
    /// The healthy disk the rebuilt extent lives on.
    pub new_disk: DiskId,
    /// The rebuilt extent's creation timestamp (its `ExtentId` is derived from
    /// the new shard id + this ts, matching the DataNode's created extent).
    pub new_create_ts: i64,
    /// The coordinator proposing the commit (must own `job_id`).
    pub committer: NodeId,
}

#[cfg(test)]
mod tests {
    use epoch_proto::CodeModeId;

    use super::*;

    #[test]
    fn shard_slot_reconstructs_id_and_index() {
        let chunk_id = ChunkId::new(42);
        let index = 5u8;
        let epoch = 7u32;
        let shard_id = ShardId::new(chunk_id, index, epoch);
        let slot = ShardSlot {
            shard_prefix: shard_id.shard_prefix(),
            epoch,
            disk_id: DiskId::new(9),
            extent_id: ExtentId::new(shard_id, 1_700_000_000_000),
        };
        assert_eq!(slot.shard_id(), shard_id);
        assert_eq!(slot.index(), index);
        assert_eq!(slot.shard_id().chunk_id(), chunk_id);
    }

    #[test]
    fn chunk_serde_round_trip() {
        let chunk_id = ChunkId::new(1);
        let shard_id = ShardId::new(chunk_id, 0, 0);
        let chunk = Chunk {
            chunk_id,
            code_mode: CodeMode {
                id: CodeModeId::new(1),
                data: 2,
                parity: 1,
                stripe_size: 1 << 20,
                blob_size: 32 << 20,
                write_quorum: None,
            },
            status: ChunkStatus::Writable,
            shards: vec![ShardSlot {
                shard_prefix: shard_id.shard_prefix(),
                epoch: 0,
                disk_id: DiskId::new(3),
                extent_id: ExtentId::new(shard_id, 123),
            }],
        };
        let json = serde_json::to_string(&chunk).expect("serialize");
        let back: Chunk = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(chunk, back);
    }
}
