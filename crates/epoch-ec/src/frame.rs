//! Bitrot frames: each frame is `BLAKE3(data)(32B) || data`.
//!
//! reed-solomon-simd does not detect intra-shard corruption, so every stored
//! shard unit is wrapped in a frame whose hash is checked on read and scrub;
//! corrupted shards are dropped before EC reconstruction.
//!
//! Design: docs/design/04-ec-io.md §1.2

use crate::layout::HASH_LEN;

/// A bitrot verification failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    /// A frame's stored hash did not match its recomputed data hash.
    #[error("bitrot check failed at frame index {index}")]
    HashMismatch {
        /// Zero-based frame index within the shard body.
        index: usize,
    },
    /// The body ended mid-frame (fewer than `HASH_LEN` header bytes remain).
    #[error("truncated frame at byte offset {offset}")]
    Truncated {
        /// Byte offset within the shard body where the truncated frame begins.
        offset: usize,
    },
}

/// Physical length of a frame carrying `data_len` data bytes.
#[must_use]
pub fn frame_len(data_len: usize) -> usize {
    HASH_LEN + data_len
}

/// Appends one bitrot frame (`hash || data`) to `out`.
pub fn write_frame(data: &[u8], out: &mut Vec<u8>) {
    let hash = blake3::hash(data);
    out.extend_from_slice(hash.as_bytes());
    out.extend_from_slice(data);
}

/// Splits a frame into (hash, data) and returns the data iff the hash matches.
fn frame_data(frame: &[u8]) -> Option<&[u8]> {
    let (hash, data) = frame.split_at_checked(HASH_LEN)?;
    (blake3::hash(data).as_bytes().as_slice() == hash).then_some(data)
}

/// Verifies a single frame (`hash(32) || data`) and returns the data slice.
pub fn verify_frame(frame: &[u8]) -> Result<&[u8], FrameError> {
    if frame.len() < HASH_LEN {
        return Err(FrameError::Truncated { offset: 0 });
    }
    frame_data(frame).ok_or(FrameError::HashMismatch { index: 0 })
}

/// Verifies every frame in a shard body framed at `unit` (the final frame may
/// be shorter) and returns the total verified data length.
///
/// Design: docs/design/04-ec-io.md §1.2 (frame boundaries derived from `unit`).
pub fn verify_shard_body(body: &[u8], unit: usize) -> Result<usize, FrameError> {
    if body.is_empty() {
        return Ok(0);
    }
    debug_assert!(unit > 0, "unit must be > 0 for a non-empty shard body");

    let mut pos = 0usize;
    let mut data_total = 0usize;
    let mut index = 0usize;
    while pos < body.len() {
        let remaining = body.len() - pos;
        if remaining <= HASH_LEN {
            return Err(FrameError::Truncated { offset: pos });
        }
        let data_len = core::cmp::min(unit, remaining - HASH_LEN);
        let frame = &body[pos..pos + HASH_LEN + data_len];
        if frame_data(frame).is_none() {
            return Err(FrameError::HashMismatch { index });
        }
        pos += HASH_LEN + data_len;
        data_total += data_len;
        index += 1;
    }
    Ok(data_total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let data = b"hello bitrot frame";
        let mut buf = Vec::new();
        write_frame(data, &mut buf);
        assert_eq!(buf.len(), frame_len(data.len()));
        assert_eq!(verify_frame(&buf).unwrap(), data);
    }

    #[test]
    fn empty_data_frame_round_trip() {
        let mut buf = Vec::new();
        write_frame(&[], &mut buf);
        assert_eq!(buf.len(), HASH_LEN);
        assert_eq!(verify_frame(&buf).unwrap(), b"");
    }

    #[test]
    fn corrupted_data_is_detected() {
        let mut buf = Vec::new();
        write_frame(b"payload", &mut buf);
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert_eq!(
            verify_frame(&buf),
            Err(FrameError::HashMismatch { index: 0 })
        );
    }

    #[test]
    fn corrupted_hash_is_detected() {
        let mut buf = Vec::new();
        write_frame(b"payload", &mut buf);
        buf[0] ^= 0xFF;
        assert_eq!(
            verify_frame(&buf),
            Err(FrameError::HashMismatch { index: 0 })
        );
    }

    #[test]
    fn truncated_frame_is_rejected() {
        assert_eq!(
            verify_frame(&[0u8; 10]),
            Err(FrameError::Truncated { offset: 0 })
        );
    }

    #[test]
    fn shard_body_multi_frame_ok() {
        let unit = 64;
        let mut body = Vec::new();
        write_frame(&[1u8; 64], &mut body);
        write_frame(&[2u8; 64], &mut body);
        write_frame(&[3u8; 10], &mut body); // short final frame
        assert_eq!(verify_shard_body(&body, unit).unwrap(), 64 + 64 + 10);
    }

    #[test]
    fn shard_body_detects_corruption_with_index() {
        let unit = 64;
        let mut body = Vec::new();
        write_frame(&[1u8; 64], &mut body);
        write_frame(&[2u8; 64], &mut body);
        let corrupt_at = HASH_LEN + 64 + HASH_LEN + 5;
        body[corrupt_at] ^= 0xFF;
        assert_eq!(
            verify_shard_body(&body, unit),
            Err(FrameError::HashMismatch { index: 1 })
        );
    }
}
