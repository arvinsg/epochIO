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

//! On-disk blob record framing: a self-describing 32-byte header and 8-byte
//! footer wrapping one shard's bitrot-framed data body (02 §1.2).
//!
//! ```text
//! record = header(32B) + body(bitrot frames) + footer(8B), 4 KiB-aligned
//!   header: crc32c(4B) | magic(4B) | blob_id(8B) | shard_id(8B) | size(4B) | pad(4B)
//!   footer: magic(4B) | body_crc32c(4B)
//! ```
//!
//! The header carries `blob_id`/`shard_id`/`size`, so a crash-recovery scan (or
//! GC reconciliation) can rebuild the index directly from the file stream. On-
//! disk scalars are little-endian; only order-sensitive index keys use
//! big-endian (see `epoch-proto`). Body framing (`[BLAKE3(32B) | unit]*`) is
//! produced by the write path via `epoch-ec`; this module is body-agnostic.

use epoch_proto::{BlobId, ShardId};

use crate::codec::read_array;

/// Fixed record-header length: 32 bytes.
pub const RECORD_HEADER_LEN: usize = 32;
/// Fixed record-footer length: 8 bytes.
pub const RECORD_FOOTER_LEN: usize = 8;
/// Records are padded to this alignment on disk (02 §1.2).
pub const RECORD_ALIGN: usize = 4096;

const HEADER_MAGIC: [u8; 4] = *b"EPRH";
const FOOTER_MAGIC: [u8; 4] = *b"EPRF";

// Header field offsets (little-endian).
const H_CRC: usize = 0; //    [0..4)   crc32c over [4..32)
const H_MAGIC: usize = 4; //  [4..8)   magic
const H_BLOB: usize = 8; //   [8..16)  blob_id  u64
const H_SHARD: usize = 16; // [16..24) shard_id u64
const H_SIZE: usize = 24; //  [24..28) body_len u32
const H_PAD: usize = 28; //   [28..32) reserved

// The checksum covers every header byte after the checksum field itself.
const HEADER_CRC_COVERAGE: std::ops::RangeFrom<usize> = H_MAGIC..;

/// A record decode/encode failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RecordError {
    /// The buffer is shorter than the fixed structure it should hold.
    #[error("record buffer too short: {got} bytes (need {need})")]
    Truncated {
        /// Bytes available.
        got: usize,
        /// Bytes required.
        need: usize,
    },
    /// The header magic did not match.
    #[error("record header magic mismatch")]
    BadHeaderMagic,
    /// The footer magic did not match.
    #[error("record footer magic mismatch")]
    BadFooterMagic,
    /// The header CRC32C did not match the recomputed value.
    #[error("record header checksum mismatch (stored {stored:#010x}, computed {computed:#010x})")]
    HeaderChecksum {
        /// CRC read from the header.
        stored: u32,
        /// CRC recomputed over the header field region.
        computed: u32,
    },
    /// The body length does not fit the on-disk `u32` size field.
    #[error("record body too large: {0} bytes")]
    BodyTooLarge(usize),
}

/// Self-describing header of one on-disk blob record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    /// Blob this record stores a shard of.
    pub blob_id: BlobId,
    /// Shard slot this record belongs to.
    pub shard_id: ShardId,
    /// Length of the bitrot-framed data body that follows the header.
    pub body_len: u32,
}

impl RecordHeader {
    /// Builds a header, validating that `body_len` fits the on-disk `u32` field.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::BodyTooLarge`] if `body_len` exceeds [`u32::MAX`].
    pub fn new(blob_id: BlobId, shard_id: ShardId, body_len: usize) -> Result<Self, RecordError> {
        let body_len = u32::try_from(body_len).map_err(|_| RecordError::BodyTooLarge(body_len))?;
        Ok(Self {
            blob_id,
            shard_id,
            body_len,
        })
    }

    /// Encodes this header into its fixed 32-byte on-disk form (CRC computed).
    #[must_use]
    pub fn encode(&self) -> [u8; RECORD_HEADER_LEN] {
        let mut buf = [0u8; RECORD_HEADER_LEN];
        buf[H_MAGIC..H_BLOB].copy_from_slice(&HEADER_MAGIC);
        buf[H_BLOB..H_SHARD].copy_from_slice(&self.blob_id.as_u64().to_le_bytes());
        buf[H_SHARD..H_SIZE].copy_from_slice(&self.shard_id.as_u64().to_le_bytes());
        buf[H_SIZE..H_PAD].copy_from_slice(&self.body_len.to_le_bytes());
        let crc = crc32c::crc32c(&buf[HEADER_CRC_COVERAGE]);
        buf[H_CRC..H_MAGIC].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decodes and validates a record header (magic then CRC32C).
    ///
    /// # Errors
    ///
    /// [`RecordError::Truncated`] if `buf` is too short, [`RecordError::BadHeaderMagic`]
    /// if the magic is wrong, or [`RecordError::HeaderChecksum`] on CRC mismatch.
    pub fn decode(buf: &[u8]) -> Result<Self, RecordError> {
        let Some(buf) = buf.first_chunk::<RECORD_HEADER_LEN>() else {
            return Err(RecordError::Truncated {
                got: buf.len(),
                need: RECORD_HEADER_LEN,
            });
        };
        if read_array::<4>(buf, H_MAGIC) != HEADER_MAGIC {
            return Err(RecordError::BadHeaderMagic);
        }
        let stored = u32::from_le_bytes(read_array::<4>(buf, H_CRC));
        let computed = crc32c::crc32c(&buf[HEADER_CRC_COVERAGE]);
        if stored != computed {
            return Err(RecordError::HeaderChecksum { stored, computed });
        }
        Ok(Self {
            blob_id: BlobId::from_raw(u64::from_le_bytes(read_array::<8>(buf, H_BLOB))),
            shard_id: ShardId::from_raw(u64::from_le_bytes(read_array::<8>(buf, H_SHARD))),
            body_len: u32::from_le_bytes(read_array::<4>(buf, H_SIZE)),
        })
    }

    /// Total on-disk bytes this record occupies, padded to [`RECORD_ALIGN`].
    #[must_use]
    pub fn on_disk_len(&self) -> usize {
        record_on_disk_len(self.body_len)
    }
}

/// Encodes a record footer (`magic || body_crc`).
#[must_use]
pub fn encode_footer(body_crc: u32) -> [u8; RECORD_FOOTER_LEN] {
    let mut buf = [0u8; RECORD_FOOTER_LEN];
    buf[0..4].copy_from_slice(&FOOTER_MAGIC);
    buf[4..8].copy_from_slice(&body_crc.to_le_bytes());
    buf
}

/// Decodes a record footer, returning the stored body CRC32C.
///
/// # Errors
///
/// [`RecordError::Truncated`] if `buf` is too short or [`RecordError::BadFooterMagic`]
/// if the magic is wrong.
pub fn decode_footer(buf: &[u8]) -> Result<u32, RecordError> {
    let Some(buf) = buf.first_chunk::<RECORD_FOOTER_LEN>() else {
        return Err(RecordError::Truncated {
            got: buf.len(),
            need: RECORD_FOOTER_LEN,
        });
    };
    if read_array::<4>(buf, 0) != FOOTER_MAGIC {
        return Err(RecordError::BadFooterMagic);
    }
    Ok(u32::from_le_bytes(read_array::<4>(buf, 4)))
}

/// CRC32C of a record body, stored in the footer and checked on recovery.
#[must_use]
pub fn body_crc(body: &[u8]) -> u32 {
    crc32c::crc32c(body)
}

/// On-disk bytes a record with `body_len` bytes occupies, padded to
/// [`RECORD_ALIGN`] so the next record starts aligned (02 §1.2).
#[must_use]
pub fn record_on_disk_len(body_len: u32) -> usize {
    let raw = RECORD_HEADER_LEN + body_len as usize + RECORD_FOOTER_LEN;
    raw.next_multiple_of(RECORD_ALIGN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::{ChunkId, WriterToken};

    fn sample() -> RecordHeader {
        RecordHeader {
            blob_id: BlobId::new(WriterToken::new(0xDEAD_BEEF), 0x0102_0304),
            shard_id: ShardId::new(ChunkId::new(9), 4, 100),
            body_len: 1_048_576,
        }
    }

    #[test]
    fn header_round_trip_all_fields() {
        for hdr in [
            sample(),
            RecordHeader {
                blob_id: BlobId::from_raw(0),
                shard_id: ShardId::from_raw(0),
                body_len: 0,
            },
            RecordHeader {
                blob_id: BlobId::from_raw(u64::MAX),
                shard_id: ShardId::from_raw(u64::MAX),
                body_len: u32::MAX,
            },
        ] {
            let bytes = hdr.encode();
            assert_eq!(bytes.len(), RECORD_HEADER_LEN);
            assert_eq!(RecordHeader::decode(&bytes), Ok(hdr));
        }
    }

    #[test]
    fn new_rejects_oversized_body() {
        let big = usize::try_from(u32::MAX).unwrap() + 1;
        assert_eq!(
            RecordHeader::new(BlobId::from_raw(1), ShardId::from_raw(2), big),
            Err(RecordError::BodyTooLarge(big))
        );
    }

    #[test]
    fn header_short_buffer_is_truncated() {
        assert_eq!(
            RecordHeader::decode(&[0u8; RECORD_HEADER_LEN - 1]),
            Err(RecordError::Truncated {
                got: RECORD_HEADER_LEN - 1,
                need: RECORD_HEADER_LEN,
            })
        );
    }

    #[test]
    fn header_bad_magic_is_rejected() {
        // All-zero bytes never carry the header magic — this is exactly what a
        // tail-truncation scan sees past the last written record.
        assert_eq!(
            RecordHeader::decode(&[0u8; RECORD_HEADER_LEN]),
            Err(RecordError::BadHeaderMagic)
        );
    }

    #[test]
    fn header_corruption_fails_checksum() {
        let mut bytes = sample().encode();
        bytes[H_SIZE] ^= 0xFF; // flip a covered byte
        assert!(matches!(
            RecordHeader::decode(&bytes),
            Err(RecordError::HeaderChecksum { .. })
        ));
    }

    #[test]
    fn footer_round_trip() {
        let bytes = encode_footer(0xABCD_1234);
        assert_eq!(bytes.len(), RECORD_FOOTER_LEN);
        assert_eq!(decode_footer(&bytes), Ok(0xABCD_1234));
    }

    #[test]
    fn footer_bad_magic_is_rejected() {
        assert_eq!(
            decode_footer(&[0u8; RECORD_FOOTER_LEN]),
            Err(RecordError::BadFooterMagic)
        );
    }

    #[test]
    fn on_disk_len_pads_to_alignment() {
        assert_eq!(record_on_disk_len(0), RECORD_ALIGN);
        let exact_fill = (RECORD_ALIGN - RECORD_HEADER_LEN - RECORD_FOOTER_LEN) as u32;
        assert_eq!(record_on_disk_len(exact_fill), RECORD_ALIGN);
        assert_eq!(record_on_disk_len(exact_fill + 1), 2 * RECORD_ALIGN);
    }
}
