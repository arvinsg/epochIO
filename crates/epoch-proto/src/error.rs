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

//! Cross-component error codes.
//!
//! This enum is the contract that drives cache invalidation across the gateway,
//! data plane and metadata plane. Design: draft/design/06-code-layout.md §1;
//! draft/design/02-datanode.md §2.1.

/// Errors exchanged across epochIO components.
///
/// Several variants double as *cache-invalidation signals* on the gateway:
/// - [`Sealed`](EpochError::Sealed) / [`ChunkFull`](EpochError::ChunkFull) /
///   [`DiskBroken`](EpochError::DiskBroken) evict the chunk from the writable set;
/// - [`ShardNotFound`](EpochError::ShardNotFound) refreshes the chunk→shards map
///   (epoch bump);
/// - [`NotLeader`](EpochError::NotLeader) / [`PartitionMoved`](EpochError::PartitionMoved)
///   refresh the metadata routing table.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum EpochError {
    /// The chunk is sealed (migration/repair/decommission fence); writes rejected.
    #[error("chunk is sealed")]
    Sealed,
    /// The chunk has no writable capacity left.
    #[error("chunk is full")]
    ChunkFull,
    /// The target disk is broken.
    #[error("disk is broken")]
    DiskBroken,
    /// The addressed shard slot has no current extent binding.
    #[error("shard not found")]
    ShardNotFound,
    /// The addressed raft group replica is not the leader.
    #[error("not leader")]
    NotLeader,
    /// The routing table is stale; the partition moved to another group.
    #[error("partition moved")]
    PartitionMoved,
    /// The target extent is rebuilding and cannot yet serve the request.
    #[error("target extent is rebuilding")]
    Rebuilding,
    /// A blob with the same id already exists (idempotent retry success).
    #[error("blob already exists")]
    AlreadyExists,
    /// The peer is at capacity for this request class (e.g. the data-plane
    /// per-connection stream cap, 02 §5); retriable after backoff.
    #[error("peer busy")]
    Busy,
    /// The target disk is out of space (02 §1.7).
    ///
    /// Deliberately distinct from [`DiskBroken`](EpochError::DiskBroken): a full
    /// disk is healthy hardware and must NOT trigger repair. Conflating the two
    /// makes a filling cluster respond to exhaustion by starting rebuilds, which
    /// consume the capacity that ran out — a positive feedback loop where every
    /// disk appears to fail at once. The gateway evicts the chunk from the
    /// writable set (as with `ChunkFull`) but raises no repair.
    #[error("disk is out of space")]
    OutOfSpace,
    /// An internal error with no more specific cross-component meaning. Carries
    /// no payload on the wire; the origin logs the detail (§9.3).
    #[error("internal error")]
    Internal,
}

impl EpochError {
    /// Encodes this error as its stable data-plane wire code (02 §5 `ErrorResp`).
    ///
    /// The codes are a permanent protocol contract: never reuse or renumber a
    /// value. Errors with no distinct cross-component meaning collapse to
    /// [`Internal`](EpochError::Internal).
    #[must_use]
    pub const fn wire_code(&self) -> u16 {
        match self {
            EpochError::Sealed => 1,
            EpochError::ChunkFull => 2,
            EpochError::DiskBroken => 3,
            EpochError::ShardNotFound => 4,
            EpochError::NotLeader => 5,
            EpochError::PartitionMoved => 6,
            EpochError::Rebuilding => 7,
            EpochError::AlreadyExists => 8,
            EpochError::Busy => 10,
            EpochError::OutOfSpace => 11,
            EpochError::Internal => 9,
        }
    }

    /// Decodes a data-plane wire code back into an error. An unrecognized code
    /// maps to [`Internal`](EpochError::Internal) rather than failing, so a
    /// newer peer's added code degrades gracefully on an older reader.
    #[must_use]
    pub const fn from_wire_code(code: u16) -> Self {
        match code {
            1 => EpochError::Sealed,
            2 => EpochError::ChunkFull,
            3 => EpochError::DiskBroken,
            4 => EpochError::ShardNotFound,
            5 => EpochError::NotLeader,
            6 => EpochError::PartitionMoved,
            7 => EpochError::Rebuilding,
            8 => EpochError::AlreadyExists,
            10 => EpochError::Busy,
            11 => EpochError::OutOfSpace,
            _ => EpochError::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_stable() {
        assert_eq!(EpochError::Sealed.to_string(), "chunk is sealed");
        assert_eq!(EpochError::NotLeader.to_string(), "not leader");
        assert_eq!(EpochError::AlreadyExists.to_string(), "blob already exists");
        assert_eq!(EpochError::Internal.to_string(), "internal error");
    }

    #[test]
    fn variants_are_comparable() {
        assert_eq!(EpochError::ChunkFull, EpochError::ChunkFull);
        assert_ne!(EpochError::ChunkFull, EpochError::Sealed);
    }

    #[test]
    fn wire_codes_round_trip() {
        for err in [
            EpochError::Sealed,
            EpochError::ChunkFull,
            EpochError::DiskBroken,
            EpochError::ShardNotFound,
            EpochError::NotLeader,
            EpochError::PartitionMoved,
            EpochError::Rebuilding,
            EpochError::AlreadyExists,
            EpochError::Busy,
            EpochError::OutOfSpace,
            EpochError::Internal,
        ] {
            assert_eq!(EpochError::from_wire_code(err.wire_code()), err);
        }
    }

    #[test]
    fn unknown_wire_code_maps_to_internal() {
        // 9 is Internal's own code; anything unassigned also degrades to Internal.
        assert_eq!(EpochError::from_wire_code(9), EpochError::Internal);
        assert_eq!(EpochError::from_wire_code(0), EpochError::Internal);
        assert_eq!(EpochError::from_wire_code(60_000), EpochError::Internal);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn error_serde_round_trips() {
        for err in [
            EpochError::Sealed,
            EpochError::NotLeader,
            EpochError::ShardNotFound,
            EpochError::Internal,
        ] {
            let json = serde_json::to_string(&err).unwrap();
            assert_eq!(serde_json::from_str::<EpochError>(&json).unwrap(), err);
        }
    }
}
