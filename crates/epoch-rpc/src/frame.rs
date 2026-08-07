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

//! The fixed 50-byte data-plane frame header (02 §5).
//!
//! ```text
//! header(50B):
//!   magic(2B) | ver(1B) | flags(1B) | pcode(u16) | stream_id(u32)
//!   | seq(u32) | session(u64) | payload_len(u32) | trace_id(u64)
//!   | deadline_ms(u32) | header_crc32c(4B) | reserved(8B)
//! payload: the pcode's compact control struct + the raw data body
//! ```
//!
//! Only the header is checksummed (crc32c); the payload body is never hashed
//! wholesale — a `DATA` frame's body is already a bitrot frame carrying its own
//! BLAKE3 (02 §5, 04 §1.2), so re-hashing it here would be wasted CPU.
//!
//! The header treats `pcode` as an opaque `u16`: an unrecognized code is a
//! dispatch concern (reply `ErrorResp`), not a framing error, so decoding still
//! yields `payload_len` and the frame can be consumed. Multi-byte scalars are
//! little-endian, matching the on-disk codecs (`epoch-store`); only order-
//! sensitive index keys use big-endian (`epoch-proto`).
//!
//! Design: docs/design/02-datanode.md §5

/// Fixed frame-header length: 50 bytes.
pub const FRAME_HEADER_LEN: usize = 50;

/// Frame protocol magic (`"ER"`, epochIO Rpc).
const FRAME_MAGIC: [u8; 2] = *b"ER";
/// Current frame protocol version.
pub const PROTOCOL_VERSION: u8 = 1;

// Header field offsets (little-endian). The crc field is zeroed while the
// checksum is computed, so the checksum covers every other byte including the
// reserved tail.
const O_MAGIC: usize = 0; //     [0..2)
const O_VER: usize = 2; //       [2..3)
const O_FLAGS: usize = 3; //     [3..4)
const O_PCODE: usize = 4; //     [4..6)   u16
const O_STREAM: usize = 6; //    [6..10)  u32
const O_SEQ: usize = 10; //      [10..14) u32
const O_SESSION: usize = 14; //  [14..22) u64
const O_PAYLEN: usize = 22; //   [22..26) u32
const O_TRACE: usize = 26; //    [26..34) u64
const O_DEADLINE: usize = 34; // [34..38) u32
const O_CRC: usize = 38; //      [38..42) u32
const O_RESERVED: usize = 42; // [42..50)

/// A frame-header decode failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    /// The buffer is shorter than a full header.
    #[error("frame header too short: {got} bytes (need {need})")]
    Truncated {
        /// Bytes available.
        got: usize,
        /// Bytes required.
        need: usize,
    },
    /// The header magic did not match.
    #[error("frame header magic mismatch")]
    BadMagic,
    /// The header carried an unsupported protocol version.
    #[error("unsupported frame protocol version: {got}")]
    Version {
        /// The version byte read from the header.
        got: u8,
    },
    /// The header CRC32C did not match the recomputed value.
    #[error("frame header checksum mismatch (stored {stored:#010x}, computed {computed:#010x})")]
    HeaderChecksum {
        /// CRC read from the header.
        stored: u32,
        /// CRC recomputed over the header with the CRC field zeroed.
        computed: u32,
    },
}

/// The decoded fields of a data-plane frame header.
///
/// The envelope fields (magic, version, header CRC, reserved tail) are handled
/// entirely by [`encode`](FrameHeader::encode)/[`decode`](FrameHeader::decode)
/// and are not represented here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Protocol opcode (interpreted by the codec layer, not by framing).
    pub pcode: u16,
    /// Per-frame flag bits (stream role hints; interpreted by the codec layer).
    pub flags: u8,
    /// Stream this frame belongs to (multiplexes one connection).
    pub stream_id: u32,
    /// Monotonic sequence within the stream.
    pub seq: u32,
    /// Session/connection correlation id.
    pub session: u64,
    /// Length of the payload that follows this header, in bytes.
    pub payload_len: u32,
    /// Distributed-trace id propagated end to end (0 if untraced).
    pub trace_id: u64,
    /// Request deadline in milliseconds (0 if none).
    pub deadline_ms: u32,
}

impl FrameHeader {
    /// Encodes this header into its fixed 50-byte wire form (CRC computed over
    /// the whole header with the CRC field held at zero).
    #[must_use]
    pub fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut b = [0u8; FRAME_HEADER_LEN];
        b[O_MAGIC..O_VER].copy_from_slice(&FRAME_MAGIC);
        b[O_VER] = PROTOCOL_VERSION;
        b[O_FLAGS] = self.flags;
        b[O_PCODE..O_STREAM].copy_from_slice(&self.pcode.to_le_bytes());
        b[O_STREAM..O_SEQ].copy_from_slice(&self.stream_id.to_le_bytes());
        b[O_SEQ..O_SESSION].copy_from_slice(&self.seq.to_le_bytes());
        b[O_SESSION..O_PAYLEN].copy_from_slice(&self.session.to_le_bytes());
        b[O_PAYLEN..O_TRACE].copy_from_slice(&self.payload_len.to_le_bytes());
        b[O_TRACE..O_DEADLINE].copy_from_slice(&self.trace_id.to_le_bytes());
        b[O_DEADLINE..O_CRC].copy_from_slice(&self.deadline_ms.to_le_bytes());
        // The crc field [O_CRC..O_RESERVED) and reserved tail stay zero while the
        // checksum is computed over the full buffer, so both are covered.
        let crc = crc32c::crc32c(&b);
        b[O_CRC..O_RESERVED].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Decodes and validates a header: magic, then version, then CRC32C.
    ///
    /// # Errors
    ///
    /// [`FrameError::Truncated`] if `buf` is shorter than [`FRAME_HEADER_LEN`],
    /// [`FrameError::BadMagic`] on a magic mismatch, [`FrameError::Version`] on
    /// an unsupported version, or [`FrameError::HeaderChecksum`] on CRC
    /// mismatch.
    pub fn decode(buf: &[u8]) -> Result<Self, FrameError> {
        let Some(buf) = buf.first_chunk::<FRAME_HEADER_LEN>() else {
            return Err(FrameError::Truncated {
                got: buf.len(),
                need: FRAME_HEADER_LEN,
            });
        };
        if read_array::<2>(buf, O_MAGIC) != FRAME_MAGIC {
            return Err(FrameError::BadMagic);
        }
        let ver = buf[O_VER];
        if ver != PROTOCOL_VERSION {
            return Err(FrameError::Version { got: ver });
        }
        let stored = u32::from_le_bytes(read_array::<4>(buf, O_CRC));
        let mut zeroed = *buf;
        zeroed[O_CRC..O_RESERVED].copy_from_slice(&[0u8; 4]);
        let computed = crc32c::crc32c(&zeroed);
        if stored != computed {
            return Err(FrameError::HeaderChecksum { stored, computed });
        }
        Ok(Self {
            pcode: u16::from_le_bytes(read_array::<2>(buf, O_PCODE)),
            flags: buf[O_FLAGS],
            stream_id: u32::from_le_bytes(read_array::<4>(buf, O_STREAM)),
            seq: u32::from_le_bytes(read_array::<4>(buf, O_SEQ)),
            session: u64::from_le_bytes(read_array::<8>(buf, O_SESSION)),
            payload_len: u32::from_le_bytes(read_array::<4>(buf, O_PAYLEN)),
            trace_id: u64::from_le_bytes(read_array::<8>(buf, O_TRACE)),
            deadline_ms: u32::from_le_bytes(read_array::<4>(buf, O_DEADLINE)),
        })
    }
}

/// Copies a fixed `[u8; N]` field out of the header buffer at `off`. Offsets
/// come from the module's const table and are always in bounds; a violation is
/// a programming error and panics on the slice index.
fn read_array<const N: usize>(buf: &[u8; FRAME_HEADER_LEN], off: usize) -> [u8; N] {
    let mut out = [0u8; N];
    out.copy_from_slice(&buf[off..off + N]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> FrameHeader {
        FrameHeader {
            pcode: 0x1234,
            flags: 0xA5,
            stream_id: 0xDEAD_BEEF,
            seq: 7,
            session: 0x0102_0304_0506_0708,
            payload_len: 1_048_576,
            trace_id: 0xCAFE_F00D_1234_5678,
            deadline_ms: 5_000,
        }
    }

    fn all_zero() -> FrameHeader {
        FrameHeader {
            pcode: 0,
            flags: 0,
            stream_id: 0,
            seq: 0,
            session: 0,
            payload_len: 0,
            trace_id: 0,
            deadline_ms: 0,
        }
    }

    fn all_max() -> FrameHeader {
        FrameHeader {
            pcode: u16::MAX,
            flags: u8::MAX,
            stream_id: u32::MAX,
            seq: u32::MAX,
            session: u64::MAX,
            payload_len: u32::MAX,
            trace_id: u64::MAX,
            deadline_ms: u32::MAX,
        }
    }

    #[test]
    fn round_trip_all_fields() {
        for hdr in [sample(), all_zero(), all_max()] {
            let bytes = hdr.encode();
            assert_eq!(bytes.len(), FRAME_HEADER_LEN);
            assert_eq!(FrameHeader::decode(&bytes), Ok(hdr));
        }
    }

    #[test]
    fn header_len_is_fifty() {
        assert_eq!(FRAME_HEADER_LEN, 50);
        assert_eq!(sample().encode().len(), 50);
    }

    #[test]
    fn short_buffer_is_truncated() {
        assert_eq!(
            FrameHeader::decode(&[0u8; FRAME_HEADER_LEN - 1]),
            Err(FrameError::Truncated {
                got: FRAME_HEADER_LEN - 1,
                need: FRAME_HEADER_LEN,
            })
        );
    }

    #[test]
    fn bad_magic_is_rejected() {
        // An all-zero buffer never carries the magic.
        assert_eq!(
            FrameHeader::decode(&[0u8; FRAME_HEADER_LEN]),
            Err(FrameError::BadMagic)
        );
    }

    #[test]
    fn unsupported_version_is_rejected_before_checksum() {
        let mut bytes = sample().encode();
        bytes[O_VER] = PROTOCOL_VERSION + 1;
        assert_eq!(
            FrameHeader::decode(&bytes),
            Err(FrameError::Version {
                got: PROTOCOL_VERSION + 1,
            })
        );
    }

    #[test]
    fn corruption_fails_checksum() {
        let mut bytes = sample().encode();
        bytes[O_PAYLEN] ^= 0xFF; // flip a covered field
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(FrameError::HeaderChecksum { .. })
        ));
    }

    #[test]
    fn reserved_tail_is_covered_by_checksum() {
        // Proves the reserved bytes are inside the CRC coverage, not ignored.
        let mut bytes = sample().encode();
        bytes[O_RESERVED] ^= 0xFF;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(FrameError::HeaderChecksum { .. })
        ));
    }
}
