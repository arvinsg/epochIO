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

use epoch_proto::{BlobId, EpochError, ExtentId, ShardId};

/// Header `flags` bit set on a `ReadShardResp` that carries shard data (a hit).
/// Absent means the shard has no such blob (a miss, empty payload).
pub const FLAG_READ_HIT: u8 = 0x01;

/// Data-plane protocol opcode. Values are a permanent wire contract: never
/// reuse or renumber. The set covers write (create/open/data/end→ack), read,
/// seal, and delete (02 §1.6 tombstone); list arrives with later milestones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Pcode {
    /// Create (or ensure) the writable extent backing a shard.
    CreateExtent = 1,
    /// Reply to [`CreateExtent`](Pcode::CreateExtent): the extent id.
    CreateExtentResp = 2,
    /// Open a blob write stream on a shard.
    Open = 3,
    /// A blob data frame (payload is one bitrot-framed shard body chunk).
    Data = 4,
    /// End a blob write stream (frame count + whole-body CRC to verify).
    End = 5,
    /// Success reply to [`End`](Pcode::End): the shard is fsynced and committed.
    CommitAck = 6,
    /// Read one blob's shard body.
    ReadShard = 7,
    /// Reply to [`ReadShard`](Pcode::ReadShard); [`FLAG_READ_HIT`] marks a hit.
    ReadShardResp = 8,
    /// Error reply carrying an [`EpochError`] wire code.
    Error = 9,
    /// Success reply to [`Open`](Pcode::Open): the stream is established and
    /// data frames may follow (lets an OPEN rejection fail fast, 04 §3.1).
    OpenAck = 10,
    /// Seal an extent: drain in-flight writes then reject further writes (01 §4.2).
    Seal = 11,
    /// Success reply to [`Seal`](Pcode::Seal): the extent is sealed.
    SealResp = 12,
    /// Delete one blob: tombstone its index record (02 §1.6; driven by the
    /// MetaNode delete queue, 03 §8).
    DeleteBlob = 13,
    /// Success reply to [`DeleteBlob`](Pcode::DeleteBlob): the blob is
    /// tombstoned (or was absent / already tombstoned — idempotent).
    DeleteBlobResp = 14,
    /// Abort an in-flight blob write stream: the server drops its reassembly
    /// state without committing. No reply — best-effort cleanup sent when the
    /// client abandons a stream (sticky shard failure / timeout, 04 §3.3), so
    /// abandoned streams do not leak server memory on a long-lived connection.
    Abort = 15,
    /// List the blob ids present in an extent (repair enumeration, 01 §6.3): a
    /// coordinator lists a surviving shard's blobs to learn what the broken
    /// shard held (EC stripe families share blob ids).
    ListBlobs = 16,
    /// Reply to [`ListBlobs`](Pcode::ListBlobs): the extent's blob ids.
    ListBlobsResp = 17,
}

impl Pcode {
    /// The wire value of this opcode.
    #[must_use]
    pub const fn to_u16(self) -> u16 {
        self as u16
    }

    /// Parses a wire opcode, or `None` if unrecognized (dispatch replies
    /// [`Error`](Pcode::Error) rather than dropping the frame).
    #[must_use]
    pub const fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::CreateExtent),
            2 => Some(Self::CreateExtentResp),
            3 => Some(Self::Open),
            4 => Some(Self::Data),
            5 => Some(Self::End),
            6 => Some(Self::CommitAck),
            7 => Some(Self::ReadShard),
            8 => Some(Self::ReadShardResp),
            9 => Some(Self::Error),
            10 => Some(Self::OpenAck),
            11 => Some(Self::Seal),
            12 => Some(Self::SealResp),
            13 => Some(Self::DeleteBlob),
            14 => Some(Self::DeleteBlobResp),
            15 => Some(Self::Abort),
            16 => Some(Self::ListBlobs),
            17 => Some(Self::ListBlobsResp),
            _ => None,
        }
    }
}

/// A control-struct decode failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    /// The payload is shorter than the fixed control struct it should hold.
    #[error("{what} payload too short: {got} bytes (need {need})")]
    Truncated {
        /// The struct being decoded.
        what: &'static str,
        /// Bytes available.
        got: usize,
        /// Bytes required.
        need: usize,
    },
}

/// `CreateExtent` request: the shard whose writable extent to ensure, with the
/// caller-chosen creation timestamp that fixes the extent's identity
/// (`ExtentId = shard_id | create_ts`). PD drives chunk creation with
/// deterministic ids (01 §4.1); local provisioning passes its own clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateExtentReq {
    /// Shard slot to back with a writable extent.
    pub shard_id: ShardId,
    /// Creation timestamp embedded in the extent id (deterministic identity).
    pub create_ts: i64,
}

impl CreateExtentReq {
    /// Encodes to its fixed 16-byte control form.
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&self.shard_id.as_u64().to_le_bytes());
        b[8..16].copy_from_slice(&self.create_ts.to_le_bytes());
        b
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<16>(buf, "CreateExtentReq")?;
        Ok(Self {
            shard_id: ShardId::from_raw(le_u64(&a, 0)),
            create_ts: le_u64(&a, 8) as i64,
        })
    }
}

/// `CreateExtentResp`: the id of the (possibly pre-existing) writable extent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateExtentResp {
    /// Extent now backing the shard.
    pub extent_id: ExtentId,
}

impl CreateExtentResp {
    /// Encodes to its fixed 16-byte control form (opaque extent identity).
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        *self.extent_id.as_bytes()
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<16>(buf, "CreateExtentResp")?;
        Ok(Self {
            extent_id: ExtentId::from_bytes(a),
        })
    }
}

/// `Seal` request: the extent to seal (drain in-flight writes, then reject
/// further writes; 01 §4.2 / Q17).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealReq {
    /// Extent to seal.
    pub extent_id: ExtentId,
}

impl SealReq {
    /// Encodes to its fixed 16-byte control form (opaque extent identity).
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        *self.extent_id.as_bytes()
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<16>(buf, "SealReq")?;
        Ok(Self {
            extent_id: ExtentId::from_bytes(a),
        })
    }
}

/// `SealResp`: an empty success ack for [`SealReq`]; the extent is now sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealResp;

impl SealResp {
    /// Encodes to its empty control form (success carries no payload).
    #[must_use]
    pub fn encode(&self) -> [u8; 0] {
        []
    }

    /// Decodes from control bytes (any empty/ignored payload is a success ack).
    ///
    /// # Errors
    ///
    /// Never fails; the signature mirrors the other control decoders.
    pub fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
        Ok(SealResp)
    }
}

/// `DeleteBlob` request: tombstone `blob_id` on `extent_id` (02 §1.6).
///
/// The MetaNode delete queue drives this RPC (03 §8); it is idempotent — an
/// absent or already-tombstoned blob is a success, so at-least-once delivery
/// is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteBlobReq {
    /// Extent holding the blob's index record.
    pub extent_id: ExtentId,
    /// Blob to tombstone.
    pub blob_id: BlobId,
}

impl DeleteBlobReq {
    /// Encodes to its fixed 24-byte control form (extent id | blob id BE).
    #[must_use]
    pub fn encode(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[..16].copy_from_slice(self.extent_id.as_bytes());
        buf[16..].copy_from_slice(&self.blob_id.to_be_bytes());
        buf
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 24 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<24>(buf, "DeleteBlobReq")?;
        let mut extent = [0u8; 16];
        extent.copy_from_slice(&a[..16]);
        Ok(Self {
            extent_id: ExtentId::from_bytes(extent),
            blob_id: BlobId::from_raw(u64::from_be_bytes(
                a[16..].try_into().expect("24-byte struct"),
            )),
        })
    }
}

/// `DeleteBlobResp`: an empty success ack for [`DeleteBlobReq`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteBlobResp;

impl DeleteBlobResp {
    /// Encodes to its empty control form (success carries no payload).
    #[must_use]
    pub fn encode(&self) -> [u8; 0] {
        []
    }

    /// Decodes from control bytes (any empty/ignored payload is a success ack).
    ///
    /// # Errors
    ///
    /// Never fails; the signature mirrors the other control decoders.
    pub fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
        Ok(DeleteBlobResp)
    }
}

/// `Open` request: begin writing `blob_id`'s shard on slot `shard_id`.
///
/// `shard_id` already embeds the chunk id (`shard_id.chunk_id()`), so the chunk
/// is not sent separately — a redundant field could contradict the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenReq {
    /// Blob being written (constructed by the gateway as `token|seq`).
    pub blob_id: BlobId,
    /// Shard slot receiving this blob's shard.
    pub shard_id: ShardId,
}

impl OpenReq {
    /// Encodes to its fixed 16-byte control form.
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&self.blob_id.as_u64().to_le_bytes());
        b[8..16].copy_from_slice(&self.shard_id.as_u64().to_le_bytes());
        b
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<16>(buf, "OpenReq")?;
        Ok(Self {
            blob_id: BlobId::from_raw(le_u64(&a, 0)),
            shard_id: ShardId::from_raw(le_u64(&a, 8)),
        })
    }
}

/// `End` request: close a blob stream after `frame_count` data frames, with the
/// whole-body CRC32C the server verifies before committing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndReq {
    /// Number of `Data` frames the client sent for this blob's shard.
    pub frame_count: u32,
    /// CRC32C over the concatenated shard body (integrity gate).
    pub blob_crc: u32,
}

impl EndReq {
    /// Encodes to its fixed 8-byte control form.
    #[must_use]
    pub fn encode(&self) -> [u8; 8] {
        let mut b = [0u8; 8];
        b[0..4].copy_from_slice(&self.frame_count.to_le_bytes());
        b[4..8].copy_from_slice(&self.blob_crc.to_le_bytes());
        b
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 8 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<8>(buf, "EndReq")?;
        Ok(Self {
            frame_count: le_u32(&a, 0),
            blob_crc: le_u32(&a, 4),
        })
    }
}

/// `ReadShard` request: read `blob_id`'s shard on slot `shard_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadShardReq {
    /// Shard slot to read from.
    pub shard_id: ShardId,
    /// Blob whose shard body to return.
    pub blob_id: BlobId,
}

impl ReadShardReq {
    /// Encodes to its fixed 16-byte control form.
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&self.shard_id.as_u64().to_le_bytes());
        b[8..16].copy_from_slice(&self.blob_id.as_u64().to_le_bytes());
        b
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<16>(buf, "ReadShardReq")?;
        Ok(Self {
            shard_id: ShardId::from_raw(le_u64(&a, 0)),
            blob_id: BlobId::from_raw(le_u64(&a, 8)),
        })
    }
}

/// `ListBlobs` request: enumerate the blob ids present in `extent_id` (repair
/// enumeration, 01 §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListBlobsReq {
    /// The extent whose blob ids to list.
    pub extent_id: ExtentId,
}

impl ListBlobsReq {
    /// Encodes to its fixed 16-byte control form (the extent id).
    #[must_use]
    pub fn encode(&self) -> [u8; 16] {
        *self.extent_id.as_bytes()
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 16 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<16>(buf, "ListBlobsReq")?;
        Ok(Self {
            extent_id: ExtentId::from_bytes(a),
        })
    }
}

/// `ListBlobsResp` reply: the extent's blob ids as a packed `u64` LE array
/// (one 8-byte little-endian id each; the count is the payload length / 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBlobsResp {
    /// The blob ids present in the extent, ascending.
    pub blob_ids: Vec<BlobId>,
}

impl ListBlobsResp {
    /// Encodes to a variable payload: `blob_ids.len() * 8` bytes, each id LE.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.blob_ids.len() * 8);
        for id in &self.blob_ids {
            out.extend_from_slice(&id.as_u64().to_le_bytes());
        }
        out
    }

    /// Decodes from a packed `u64` LE array.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf`'s length is not a multiple of 8.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if !buf.len().is_multiple_of(8) {
            return Err(CodecError::Truncated {
                what: "ListBlobsResp",
                need: buf.len().next_multiple_of(8),
                got: buf.len(),
            });
        }
        let blob_ids = buf
            .chunks_exact(8)
            .map(|c| BlobId::from_raw(u64::from_le_bytes(c.try_into().expect("8 bytes"))))
            .collect();
        Ok(Self { blob_ids })
    }
}

/// `Error` reply: an [`EpochError`] as its stable wire code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorResp {
    /// The [`EpochError::wire_code`] of the error.
    pub code: u16,
}

impl ErrorResp {
    /// Builds a reply from an error.
    #[must_use]
    pub fn from_error(err: &EpochError) -> Self {
        Self {
            code: err.wire_code(),
        }
    }

    /// The error this reply denotes (unknown codes degrade to `Internal`).
    #[must_use]
    pub fn error(&self) -> EpochError {
        EpochError::from_wire_code(self.code)
    }

    /// Encodes to its fixed 2-byte control form.
    #[must_use]
    pub fn encode(&self) -> [u8; 2] {
        self.code.to_le_bytes()
    }

    /// Decodes from control bytes.
    ///
    /// # Errors
    ///
    /// [`CodecError::Truncated`] if `buf` is shorter than 2 bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let a = take::<2>(buf, "ErrorResp")?;
        Ok(Self {
            code: u16::from_le_bytes(a),
        })
    }
}

/// Copies the leading `[u8; N]` out of `buf`, or [`CodecError::Truncated`].
fn take<const N: usize>(buf: &[u8], what: &'static str) -> Result<[u8; N], CodecError> {
    buf.first_chunk::<N>()
        .copied()
        .ok_or(CodecError::Truncated {
            what,
            got: buf.len(),
            need: N,
        })
}

/// Reads a little-endian `u64` at `off`. `off + 8` is in bounds by construction
/// (callers read within a buffer already length-checked by [`take`]).
fn le_u64(buf: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[off..off + 8]);
    u64::from_le_bytes(a)
}

/// Reads a little-endian `u32` at `off` (in bounds by construction, see [`le_u64`]).
fn le_u32(buf: &[u8], off: usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&buf[off..off + 4]);
    u32::from_le_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::{ChunkId, WriterToken};

    #[test]
    fn pcode_round_trips_and_rejects_unknown() {
        for pc in [
            Pcode::CreateExtent,
            Pcode::CreateExtentResp,
            Pcode::Open,
            Pcode::Data,
            Pcode::End,
            Pcode::CommitAck,
            Pcode::ReadShard,
            Pcode::ReadShardResp,
            Pcode::Error,
            Pcode::OpenAck,
            Pcode::Seal,
            Pcode::SealResp,
            Pcode::DeleteBlob,
            Pcode::DeleteBlobResp,
        ] {
            assert_eq!(Pcode::from_u16(pc.to_u16()), Some(pc));
        }
        assert_eq!(Pcode::from_u16(0), None);
        assert_eq!(Pcode::from_u16(99), None);
        assert_eq!(Pcode::from_u16(u16::MAX), None);
    }

    #[test]
    fn delete_blob_req_round_trip() {
        let req = DeleteBlobReq {
            extent_id: ExtentId::new(ShardId::new(ChunkId::new(3), 1, 0), 42),
            blob_id: BlobId::from_raw(0xDEAD_BEEF),
        };
        assert_eq!(DeleteBlobReq::decode(&req.encode()), Ok(req));
        assert!(DeleteBlobReq::decode(&req.encode()[..23]).is_err());
        assert_eq!(DeleteBlobResp::decode(&[]), Ok(DeleteBlobResp));
    }

    #[test]
    fn seal_req_round_trip() {
        let ext = ExtentId::new(ShardId::new(ChunkId::new(2), 5, 9), 987_654);
        let req = SealReq { extent_id: ext };
        assert_eq!(SealReq::decode(&req.encode()), Ok(req));
    }

    #[test]
    fn seal_resp_decodes_from_empty_payload() {
        assert_eq!(SealResp::decode(&SealResp.encode()), Ok(SealResp));
        // Any payload is accepted as a success ack.
        assert_eq!(SealResp::decode(&[1, 2, 3]), Ok(SealResp));
    }

    #[test]
    fn create_extent_req_round_trip() {
        for shard in [
            ShardId::from_raw(0),
            ShardId::new(ChunkId::new(7), 4, 100),
            ShardId::from_raw(u64::MAX),
        ] {
            let req = CreateExtentReq {
                shard_id: shard,
                create_ts: 1_700_000_000,
            };
            assert_eq!(CreateExtentReq::decode(&req.encode()), Ok(req));
        }
    }

    #[test]
    fn create_extent_resp_round_trip() {
        let ext = ExtentId::new(ShardId::new(ChunkId::new(1), 0, 3), 123_456);
        let resp = CreateExtentResp { extent_id: ext };
        assert_eq!(CreateExtentResp::decode(&resp.encode()), Ok(resp));
    }

    #[test]
    fn open_req_round_trip() {
        let req = OpenReq {
            blob_id: BlobId::new(WriterToken::new(0xDEAD_BEEF), 0x0102_0304),
            shard_id: ShardId::new(ChunkId::new(9), 4, 100),
        };
        assert_eq!(OpenReq::decode(&req.encode()), Ok(req));
        // Field order is stable (blob then shard); a swapped buffer decodes differently.
        let bytes = req.encode();
        let decoded = OpenReq::decode(&bytes).unwrap();
        assert_eq!(decoded.blob_id, req.blob_id);
        assert_eq!(decoded.shard_id, req.shard_id);
    }

    #[test]
    fn end_req_round_trip() {
        for req in [
            EndReq {
                frame_count: 0,
                blob_crc: 0,
            },
            EndReq {
                frame_count: 33,
                blob_crc: 0xABCD_1234,
            },
            EndReq {
                frame_count: u32::MAX,
                blob_crc: u32::MAX,
            },
        ] {
            assert_eq!(EndReq::decode(&req.encode()), Ok(req));
        }
    }

    #[test]
    fn read_shard_req_round_trip() {
        let req = ReadShardReq {
            shard_id: ShardId::new(ChunkId::new(2), 1, 5),
            blob_id: BlobId::new(WriterToken::new(11), 22),
        };
        assert_eq!(ReadShardReq::decode(&req.encode()), Ok(req));
    }

    #[test]
    fn list_blobs_req_round_trip() {
        let req = ListBlobsReq {
            extent_id: ExtentId::new(ShardId::new(ChunkId::new(3), 2, 7), 1_700_000_000_000),
        };
        assert_eq!(ListBlobsReq::decode(&req.encode()), Ok(req));
    }

    #[test]
    fn list_blobs_resp_round_trips_and_rejects_ragged() {
        let resp = ListBlobsResp {
            blob_ids: vec![
                BlobId::new(WriterToken::new(1), 1),
                BlobId::new(WriterToken::new(1), 2),
                BlobId::new(WriterToken::new(9), 300),
            ],
        };
        assert_eq!(ListBlobsResp::decode(&resp.encode()), Ok(resp));
        // Empty is valid (an empty extent).
        let empty = ListBlobsResp { blob_ids: vec![] };
        assert_eq!(ListBlobsResp::decode(&empty.encode()), Ok(empty));
        // A payload whose length is not a multiple of 8 is rejected.
        assert!(matches!(
            ListBlobsResp::decode(&[0u8; 12]),
            Err(CodecError::Truncated { .. })
        ));
    }

    #[test]
    fn error_resp_maps_epoch_errors() {
        for err in [
            EpochError::Sealed,
            EpochError::ChunkFull,
            EpochError::ShardNotFound,
            EpochError::Internal,
        ] {
            let resp = ErrorResp::from_error(&err);
            assert_eq!(ErrorResp::decode(&resp.encode()), Ok(resp));
            assert_eq!(resp.error(), err);
        }
    }

    #[test]
    fn truncated_control_structs_are_rejected() {
        assert!(matches!(
            OpenReq::decode(&[0u8; 15]),
            Err(CodecError::Truncated {
                what: "OpenReq",
                got: 15,
                need: 16
            })
        ));
        assert!(matches!(
            EndReq::decode(&[0u8; 7]),
            Err(CodecError::Truncated { need: 8, .. })
        ));
        assert!(matches!(
            ErrorResp::decode(&[]),
            Err(CodecError::Truncated { need: 2, .. })
        ));
    }
}
