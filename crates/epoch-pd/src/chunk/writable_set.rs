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

//! Writable-set publish filter: which committed `Writable` chunks PD advertises
//! to gateways on `GetWritableChunks` (design 01 §4.2).
//!
//! The set is a **soft, leaderside** projection over committed chunk identity
//! plus volatile per-disk liveness (Q18): a chunk is dropped when any of its
//! shards sits on a failed disk, or on a disk whose *known* free has fallen
//! below the threshold. A disk that has not heartbeat yet is kept optimistically
//! — a freshly created chunk must be writable immediately, and transient
//! over-publish is caught downstream by the `ChunkFull` / `DiskBroken` write
//! errors (01 §4.2: "过创建/欠创建均无害 + ChunkFull 兜底").
//!
//! This is pure decision logic (no I/O, no clock): the caller supplies a disk
//! health lookup built from committed disk state and the heartbeat tracker, so
//! the policy is directly unit-testable.
//!
//! Design: docs/design/01-pd.md §4.2 (writable set)

use epoch_proto::DiskId;

use crate::chunk::model::{Chunk, ChunkStatus};

/// Default soft free-space threshold below which a shard's disk is dropped from
/// the published set. A tunable cluster config takes this over with the PD
/// config manager (a later milestone); until then it is a fixed 1 GiB margin.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 1 << 30;

/// The free-space threshold for keeping a chunk in the writable set.
#[derive(Debug, Clone, Copy)]
pub struct WritableThreshold {
    /// A shard whose disk reports free below this is evicted (only when the
    /// free is known; an un-reported disk is not evicted on this ground).
    pub min_free_bytes: u64,
}

impl Default for WritableThreshold {
    fn default() -> Self {
        Self {
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
        }
    }
}

/// The liveness of one shard's disk, as seen by the writable-set filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskHealth {
    /// The disk is `Normal`. `free` is its last heartbeat free bytes, or `None`
    /// if it has not reported yet (kept optimistically).
    Usable { free: Option<u64> },
    /// The disk is registered but not `Normal` (broken / repairing / dropped):
    /// a hard eviction.
    Failed,
    /// The disk is unknown to the cluster — a committed shard whose disk is
    /// missing is not safe to publish.
    Unknown,
}

impl DiskHealth {
    /// Whether a shard on this disk keeps its chunk publishable under `threshold`.
    fn keeps_publishable(self, threshold: WritableThreshold) -> bool {
        match self {
            DiskHealth::Usable { free: Some(free) } => free >= threshold.min_free_bytes,
            DiskHealth::Usable { free: None } => true,
            DiskHealth::Failed | DiskHealth::Unknown => false,
        }
    }
}

/// Whether a committed chunk stays in the published writable set: it must be
/// `Writable` and every shard's disk must keep it publishable (design 01 §4.2:
/// "free 低于阈值 / 组内盘故障 → 摘除").
///
/// `health` maps a shard's disk id to its current [`DiskHealth`]; the caller
/// builds it from committed disk state and the heartbeat tracker. Taken by
/// reference so one lookup can be reused across a whole candidate set.
pub fn is_publishable(
    chunk: &Chunk,
    threshold: WritableThreshold,
    health: &impl Fn(DiskId) -> DiskHealth,
) -> bool {
    chunk.status == ChunkStatus::Writable
        && chunk
            .shards
            .iter()
            .all(|slot| health(slot.disk_id).keeps_publishable(threshold))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use epoch_proto::{ChunkId, CodeMode, CodeModeId, ExtentId, ShardId};

    use super::*;
    use crate::chunk::model::ShardSlot;

    fn code_mode() -> CodeMode {
        CodeMode {
            id: CodeModeId::new(1),
            data: 2,
            parity: 1,
            stripe_size: 1 << 20,
            blob_size: 32 << 20,
            write_quorum: None,
        }
    }

    /// A chunk with three shards on disks 10/20/30.
    fn chunk(status: ChunkStatus) -> Chunk {
        let chunk_id = ChunkId::new(1);
        let shards = [10u32, 20, 30]
            .into_iter()
            .enumerate()
            .map(|(index, disk)| {
                let index = u8::try_from(index).expect("small");
                let shard_id = ShardId::new(chunk_id, index, 0);
                ShardSlot {
                    shard_prefix: shard_id.shard_prefix(),
                    epoch: 0,
                    disk_id: DiskId::new(disk),
                    extent_id: ExtentId::new(shard_id, 1),
                }
            })
            .collect();
        Chunk {
            chunk_id,
            code_mode: code_mode(),
            status,
            shards,
        }
    }

    /// Builds a health lookup from an explicit per-disk map.
    fn health_map(entries: &[(u32, DiskHealth)]) -> impl Fn(DiskId) -> DiskHealth + '_ {
        let map: HashMap<u32, DiskHealth> = entries.iter().copied().collect();
        move |disk_id: DiskId| {
            map.get(&disk_id.get())
                .copied()
                .unwrap_or(DiskHealth::Unknown)
        }
    }

    const THRESHOLD: WritableThreshold = WritableThreshold {
        min_free_bytes: 1000,
    };

    #[test]
    fn publishes_when_all_disks_usable_with_enough_free() {
        let health = health_map(&[
            (10, DiskHealth::Usable { free: Some(1000) }), // exactly at threshold
            (20, DiskHealth::Usable { free: Some(5000) }),
            (30, DiskHealth::Usable { free: None }), // un-reported, kept
        ]);
        assert!(is_publishable(
            &chunk(ChunkStatus::Writable),
            THRESHOLD,
            &health
        ));
    }

    #[test]
    fn evicts_on_failed_disk() {
        let health = health_map(&[
            (10, DiskHealth::Usable { free: Some(5000) }),
            (20, DiskHealth::Failed),
            (30, DiskHealth::Usable { free: Some(5000) }),
        ]);
        assert!(!is_publishable(
            &chunk(ChunkStatus::Writable),
            THRESHOLD,
            &health
        ));
    }

    #[test]
    fn evicts_on_known_low_free() {
        let health = health_map(&[
            (10, DiskHealth::Usable { free: Some(5000) }),
            (20, DiskHealth::Usable { free: Some(999) }), // just below threshold
            (30, DiskHealth::Usable { free: Some(5000) }),
        ]);
        assert!(!is_publishable(
            &chunk(ChunkStatus::Writable),
            THRESHOLD,
            &health
        ));
    }

    #[test]
    fn evicts_on_unknown_disk() {
        // Disk 30 is absent from the map -> Unknown -> evicted.
        let health = health_map(&[
            (10, DiskHealth::Usable { free: Some(5000) }),
            (20, DiskHealth::Usable { free: Some(5000) }),
        ]);
        assert!(!is_publishable(
            &chunk(ChunkStatus::Writable),
            THRESHOLD,
            &health
        ));
    }

    #[test]
    fn evicts_non_writable_chunk() {
        let health = health_map(&[
            (10, DiskHealth::Usable { free: Some(5000) }),
            (20, DiskHealth::Usable { free: Some(5000) }),
            (30, DiskHealth::Usable { free: Some(5000) }),
        ]);
        // Same healthy disks, but a Full chunk is never published.
        assert!(!is_publishable(
            &chunk(ChunkStatus::Full),
            THRESHOLD,
            &health
        ));
    }
}
