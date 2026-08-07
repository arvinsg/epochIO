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

//! ID types — the workspace's identity vocabulary and their bit layouts.
//!
//! Layouts (docs/design/00-overview.md §4):
//! ```text
//! ChunkId  u32
//! BlobId   u64 = writer_token(32) | seq(32)
//! ShardId  u64 = chunk_id(32) | index(8) | epoch(24)
//! ExtentId [16]byte = shard_id(8B BE) | create_ts(8B BE)
//! ```
//!
//! Design: docs/design/00-overview.md §4; docs/design/06-code-layout.md §1

/// Bits reserved for the shard index within a [`ShardId`].
pub const SHARD_INDEX_BITS: u32 = 8;
/// Bits reserved for the epoch within a [`ShardId`].
pub const SHARD_EPOCH_BITS: u32 = 24;
/// Largest epoch representable in a [`ShardId`] (24-bit field).
pub const MAX_SHARD_EPOCH: u32 = (1 << SHARD_EPOCH_BITS) - 1;

/// EC group identifier, assigned by PD at chunk creation (low frequency).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ChunkId(u32);

impl ChunkId {
    /// Wraps a raw `u32`.
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }
    /// Returns the raw `u32`.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for ChunkId {
    fn from(raw: u32) -> Self {
        Self(raw)
    }
}

/// Globally unique writer identity issued by PD (the high 32 bits of a [`BlobId`]).
///
/// Never reused; a gateway instance holds one per registration. Design: 01 §4.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct WriterToken(u32);

impl WriterToken {
    /// Wraps a raw `u32`.
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }
    /// Returns the raw `u32`.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for WriterToken {
    fn from(raw: u32) -> Self {
        Self(raw)
    }
}

/// Disk identifier assigned by PD at disk registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct DiskId(u32);

impl DiskId {
    /// Wraps a raw `u32`.
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }
    /// Returns the raw `u32`.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for DiskId {
    fn from(raw: u32) -> Self {
        Self(raw)
    }
}

/// Cluster node identifier assigned by PD at node registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct NodeId(u32);

impl NodeId {
    /// Wraps a raw `u32`.
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }
    /// Returns the raw `u32`.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for NodeId {
    fn from(raw: u32) -> Self {
        Self(raw)
    }
}

/// MetaNode range-partition identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct PartitionId(u64);

impl PartitionId {
    /// Wraps a raw `u64`.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
    /// Returns the raw `u64`.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for PartitionId {
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

/// Bucket identifier assigned by PD at bucket creation (never reused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct BucketId(u64);

impl BucketId {
    /// Wraps a raw `u64`.
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }
    /// Returns the raw `u64`.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for BucketId {
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

/// Writer-scoped blob identifier: `writer_token(32) | seq(32)`.
///
/// Constructed locally by the gateway with no allocation point. Globally unique
/// by construction: tokens are never reused and `seq` is monotonic within a
/// token. Design: docs/design/00-overview.md §4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct BlobId(u64);

impl BlobId {
    /// Composes a blob id from its writer token and sequence number.
    pub const fn new(token: WriterToken, seq: u32) -> Self {
        Self(((token.get() as u64) << 32) | seq as u64)
    }
    /// Wraps a raw `u64` (e.g. decoded from a metadata slice).
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
    /// Returns the raw `u64`.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
    /// Extracts the writer token (high 32 bits).
    pub const fn writer_token(self) -> WriterToken {
        WriterToken::new((self.0 >> 32) as u32)
    }
    /// Extracts the sequence number (low 32 bits).
    pub const fn seq(self) -> u32 {
        self.0 as u32
    }
    /// Big-endian encoding used for order-preserving index keys (`b{blob_id BE}`).
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// Shard slot identity: `chunk_id(32) | index(8) | epoch(24)`.
///
/// `epoch` increments when a shard is re-bound to a new extent by repair or
/// migration; it never enters object metadata (clients hold epoch-free Slices).
/// Design: docs/design/00-overview.md §4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ShardId(u64);

impl ShardId {
    /// Composes a shard id, panicking on epoch overflow.
    ///
    /// `epoch` is a 24-bit field: PD's `ShardSlot.epoch` is a `u32` in memory
    /// (01 §3) but is encoded here to 24 bits. Prefer [`try_new`](Self::try_new)
    /// on any path that applies replicated state — a panic inside raft apply is
    /// fatal (AGENTS §8).
    ///
    /// # Panics
    /// Panics if `epoch` exceeds [`MAX_SHARD_EPOCH`]: silently masking could
    /// alias two different epochs onto one shard identity.
    pub fn new(chunk_id: ChunkId, index: u8, epoch: u32) -> Self {
        Self::try_new(chunk_id, index, epoch).expect("shard epoch exceeds 24-bit max")
    }

    /// Composes a shard id, returning [`EpochError::Internal`](crate::EpochError::Internal)
    /// if `epoch` exceeds [`MAX_SHARD_EPOCH`].
    ///
    /// Use this on replicated-state paths (PD raft apply, 01 §4.4 epoch bump):
    /// the epoch is derived from persisted state, so an overflow must surface as
    /// a recoverable error rather than a panic (AGENTS §8).
    pub fn try_new(chunk_id: ChunkId, index: u8, epoch: u32) -> Result<Self, crate::EpochError> {
        if epoch > MAX_SHARD_EPOCH {
            return Err(crate::EpochError::Internal);
        }
        let raw = (u64::from(chunk_id.get()) << 32)
            | (u64::from(index) << SHARD_EPOCH_BITS)
            | u64::from(epoch);
        Ok(Self(raw))
    }
    /// Wraps a raw `u64`.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
    /// Returns the raw `u64`.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
    /// Extracts the owning chunk id.
    pub const fn chunk_id(self) -> ChunkId {
        ChunkId::new((self.0 >> 32) as u32)
    }
    /// Extracts the shard index within the EC stripe (`0..N+M`).
    pub const fn index(self) -> u8 {
        ((self.0 >> SHARD_EPOCH_BITS) & 0xFF) as u8
    }
    /// Extracts the epoch (24-bit).
    pub const fn epoch(self) -> u32 {
        (self.0 & MAX_SHARD_EPOCH as u64) as u32
    }
    /// The epoch-zeroed stable identity `chunk_id<<32 | index<<24`, used as the
    /// per-disk `s{shard_prefix}` index key. Design: docs/design/01-pd.md §3.
    pub const fn shard_prefix(self) -> u64 {
        self.0 & !(MAX_SHARD_EPOCH as u64)
    }
    /// Big-endian encoding (embedded in [`ExtentId`] and self-describing records).
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// On-disk extent container identity: `shard_id(8B BE) | create_ts(8B BE)`.
///
/// Big-endian layout makes extents sort by shard first, then by creation time.
/// Used as the `e{extent_id}` index key and the extent file name.
/// Design: docs/design/00-overview.md §4; docs/design/02-datanode.md §1.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ExtentId([u8; 16]);

impl ExtentId {
    /// Composes an extent id from its shard slot and creation timestamp.
    pub fn new(shard_id: ShardId, create_ts: i64) -> Self {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&shard_id.to_be_bytes());
        bytes[8..].copy_from_slice(&create_ts.to_be_bytes());
        Self(bytes)
    }
    /// Wraps the raw 16 bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
    /// Borrows the raw 16 bytes (key/file-name encoding).
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
    /// Extracts the owning shard slot (high 8 bytes).
    pub fn shard_id(&self) -> ShardId {
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.0[..8]);
        ShardId::from_raw(u64::from_be_bytes(b))
    }
    /// Extracts the creation timestamp (low 8 bytes).
    pub fn create_ts(&self) -> i64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.0[8..]);
        i64::from_be_bytes(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_ids_wrap_and_unwrap() {
        assert_eq!(ChunkId::new(42).get(), 42);
        assert_eq!(ChunkId::from(7).get(), 7);
        assert_eq!(WriterToken::new(u32::MAX).get(), u32::MAX);
        assert_eq!(DiskId::from(1).get(), 1);
        assert_eq!(NodeId::from(2).get(), 2);
        assert_eq!(PartitionId::from(u64::MAX).get(), u64::MAX);
    }

    #[test]
    fn blob_id_round_trip() {
        for (token, seq) in [
            (0u32, 0u32),
            (u32::MAX, u32::MAX),
            (1, 2),
            (0xDEAD_BEEF, 0x0102_0304),
        ] {
            let blob = BlobId::new(WriterToken::new(token), seq);
            assert_eq!(blob.writer_token().get(), token);
            assert_eq!(blob.seq(), seq);
            assert_eq!(BlobId::from_raw(blob.as_u64()), blob);
            assert_eq!(u64::from_be_bytes(blob.to_be_bytes()), blob.as_u64());
        }
    }

    #[test]
    fn shard_id_round_trip_and_prefix() {
        for (chunk, index, epoch) in [
            (0u32, 0u8, 0u32),
            (u32::MAX, 255, MAX_SHARD_EPOCH),
            (7, 3, 42),
        ] {
            let shard = ShardId::new(ChunkId::new(chunk), index, epoch);
            assert_eq!(shard.chunk_id().get(), chunk);
            assert_eq!(shard.index(), index);
            assert_eq!(shard.epoch(), epoch);
            assert_eq!(ShardId::from_raw(shard.as_u64()), shard);

            let prefix_only = ShardId::new(ChunkId::new(chunk), index, 0);
            assert_eq!(shard.shard_prefix(), prefix_only.as_u64());
            assert_eq!(shard.shard_prefix() & u64::from(MAX_SHARD_EPOCH), 0);
        }
    }

    #[test]
    fn shard_id_accessors_mask_each_field() {
        // A fully-saturated raw exercises the field masks: epoch is only 24 bits.
        let shard = ShardId::from_raw(u64::MAX);
        assert_eq!(shard.chunk_id().get(), u32::MAX);
        assert_eq!(shard.index(), u8::MAX);
        assert_eq!(shard.epoch(), MAX_SHARD_EPOCH);
        assert_eq!(MAX_SHARD_EPOCH, (1 << 24) - 1);
        assert_eq!(shard.shard_prefix(), !u64::from(MAX_SHARD_EPOCH));
    }

    #[test]
    #[should_panic(expected = "exceeds 24-bit max")]
    fn shard_id_new_rejects_epoch_overflow() {
        let _ = ShardId::new(ChunkId::new(1), 0, MAX_SHARD_EPOCH + 1);
    }

    #[test]
    fn shard_id_try_new_matches_new_and_rejects_overflow() {
        // Valid epoch: try_new agrees with the panicking constructor.
        let ok = ShardId::try_new(ChunkId::new(7), 3, MAX_SHARD_EPOCH)
            .expect("max epoch is representable");
        assert_eq!(ok, ShardId::new(ChunkId::new(7), 3, MAX_SHARD_EPOCH));

        // Overflow surfaces as a recoverable error, never a panic (AGENTS §8).
        assert_eq!(
            ShardId::try_new(ChunkId::new(1), 0, MAX_SHARD_EPOCH + 1),
            Err(crate::EpochError::Internal)
        );
    }

    #[test]
    fn extent_id_round_trip_and_big_endian_order() {
        let shard = ShardId::new(ChunkId::new(9), 4, 100);
        let early = ExtentId::new(shard, 1000);
        assert_eq!(early.shard_id(), shard);
        assert_eq!(early.create_ts(), 1000);
        assert_eq!(ExtentId::from_bytes(*early.as_bytes()), early);

        // Same shard, later timestamp sorts after (BE ordering).
        let late = ExtentId::new(shard, 2000);
        assert!(early < late);

        // Shard id is the high-order sort key.
        let other_shard = ExtentId::new(ShardId::new(ChunkId::new(10), 0, 0), 0);
        assert!(early < other_shard);
    }

    #[cfg(feature = "serde")]
    fn json_round_trip<T>(v: &T) -> T
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        serde_json::from_str(&serde_json::to_string(v).unwrap()).unwrap()
    }

    #[cfg(feature = "serde")]
    #[test]
    fn ids_serde_round_trip_transparently() {
        // Transparent newtypes serialize as their bare inner value.
        assert_eq!(serde_json::to_string(&ChunkId::new(42)).unwrap(), "42");
        assert_eq!(
            serde_json::to_string(&BlobId::new(WriterToken::new(1), 2)).unwrap(),
            ((1u64 << 32) | 2).to_string()
        );

        // Round-trip every id type (u32 / u64 / [u8;16] inner shapes).
        assert_eq!(
            json_round_trip(&ChunkId::new(0xDEAD_BEEF)),
            ChunkId::new(0xDEAD_BEEF)
        );
        assert_eq!(json_round_trip(&WriterToken::new(7)), WriterToken::new(7));
        assert_eq!(json_round_trip(&DiskId::new(3)), DiskId::new(3));
        assert_eq!(json_round_trip(&NodeId::new(11)), NodeId::new(11));
        assert_eq!(
            json_round_trip(&PartitionId::new(u64::MAX)),
            PartitionId::new(u64::MAX)
        );
        let blob = BlobId::new(WriterToken::new(0xABCD), 0x1234);
        assert_eq!(json_round_trip(&blob), blob);
        let shard = ShardId::new(ChunkId::new(9), 4, 100);
        assert_eq!(json_round_trip(&shard), shard);
        let extent = ExtentId::new(shard, 1234);
        assert_eq!(json_round_trip(&extent), extent);
    }
}
