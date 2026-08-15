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

//! HTTP byte-range resolution for GET (RFC 9110 §14.1.2).
//!
//! Pure and side-effect free: turning a client's `Range` header plus the object
//! size into a concrete byte window, and selecting the minimum set of blobs that
//! window touches. A ranged GET must fetch only the overlapping blobs — an object
//! is chunked into 32 MiB blobs, so reading a 1 MiB footer of a 10 GiB object
//! should decode one blob, not all of them.
//!
//! INVARIANT(design 04 §4): a ranged GET must return exactly the requested bytes
//! with status 206 and a matching `Content-Range`, or fail. Answering a range
//! request with the whole object under 200 is worse than refusing it: clients that
//! trust the status code (every parallel-download tool: `aws s3 cp`, s3fs,
//! columnar readers seeking a footer) slice the response as if it were the window
//! they asked for and silently assemble corrupt data.

use crate::code::{BlobDesc, ObjectLayout};

/// A resolved, inclusive byte window within an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// First byte offset (inclusive).
    pub start: u64,
    /// Last byte offset (inclusive). Always `>= start`.
    pub end: u64,
}

impl ByteRange {
    /// The window's length in bytes.
    ///
    /// Named `byte_count` rather than `len`: a resolved window always spans at
    /// least one byte, so the `is_empty` companion `len` implies would be a
    /// constant `false`.
    #[must_use]
    pub fn byte_count(&self) -> u64 {
        self.end - self.start + 1
    }

    /// The `Content-Range` header value for an object of `total` bytes.
    #[must_use]
    pub fn content_range(&self, total: u64) -> String {
        format!("bytes {}-{}/{}", self.start, self.end, total)
    }
}

/// Why a range request cannot be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeError {
    /// The range lies entirely outside the object (416 Range Not Satisfiable).
    Unsatisfiable,
}

/// Resolves an `int`/`suffix` range against an object of `size` bytes.
///
/// Follows RFC 9110: an int range's `last` is clamped to the final byte (a client
/// may ask past the end), a suffix range longer than the object yields the whole
/// object, and a first-byte position at or past the end is unsatisfiable. A range
/// against an empty object is always unsatisfiable — there is no byte to return.
///
/// # Errors
///
/// [`RangeError::Unsatisfiable`] when no byte of the object is covered.
pub fn resolve(
    size: u64,
    first: u64,
    last: Option<u64>,
    suffix: bool,
) -> Result<ByteRange, RangeError> {
    if size == 0 {
        return Err(RangeError::Unsatisfiable);
    }
    let final_byte = size - 1;
    if suffix {
        // `first` carries the suffix length. A zero-length suffix selects nothing.
        if first == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        // A suffix longer than the object is the whole object (RFC 9110).
        let start = size.saturating_sub(first);
        return Ok(ByteRange {
            start,
            end: final_byte,
        });
    }
    if first > final_byte {
        return Err(RangeError::Unsatisfiable);
    }
    // An absent or past-the-end `last` clamps to the object's final byte.
    let end = last.map_or(final_byte, |l| l.min(final_byte));
    if end < first {
        return Err(RangeError::Unsatisfiable);
    }
    Ok(ByteRange { start: first, end })
}

/// The blobs a byte window touches, plus how to trim the decoded bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangePlan {
    /// The overlapping blobs, in object order (never empty).
    pub blobs: Vec<BlobDesc>,
    /// Bytes to drop from the front of the concatenated blobs.
    pub skip: usize,
    /// Bytes to keep after skipping (equals the window length).
    pub take: usize,
}

/// Selects the minimum set of blobs covering `range` and the trim offsets within
/// them.
///
/// Returns `None` if the layout's blobs do not cover `range` — a truncated or
/// inconsistent slice list, which must fail rather than serve short data.
#[must_use]
pub fn plan(layout: &ObjectLayout, range: ByteRange) -> Option<RangePlan> {
    let mut blobs = Vec::new();
    let mut offset: u64 = 0;
    let mut skip: Option<usize> = None;
    for blob in &layout.blobs {
        let blob_len = blob.len as u64;
        let blob_end = offset + blob_len; // exclusive
        // Overlap test on half-open [offset, blob_end) vs inclusive [start, end].
        if blob_end > range.start && offset <= range.end {
            if skip.is_none() {
                // Distance from this blob's start to the window's start.
                skip = Some(usize::try_from(range.start - offset).ok()?);
            }
            blobs.push(blob.clone());
        }
        offset = blob_end;
        if offset > range.end {
            break;
        }
    }
    // The blobs must actually reach the window's end.
    if blobs.is_empty() || offset <= range.end {
        return None;
    }
    Some(RangePlan {
        blobs,
        skip: skip?,
        take: usize::try_from(range.byte_count()).ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code::{ChunkPlacement, CodeMode};
    use epoch_proto::{BlobId, ChunkId, WriterToken};

    fn code() -> CodeMode {
        CodeMode {
            data: 2,
            parity: 1,
            stripe_size: 1 << 20,
            blob_size: 100,
            write_quorum: None,
        }
    }

    /// A layout of `lens` blobs, so ranges can be planned against known offsets.
    fn layout(lens: &[usize]) -> ObjectLayout {
        let size: u64 = lens.iter().map(|l| *l as u64).sum();
        let blobs = lens
            .iter()
            .enumerate()
            .map(|(i, &len)| BlobDesc {
                blob_id: BlobId::new(WriterToken::new(1), i as u32),
                len,
                chunk: ChunkPlacement {
                    chunk_id: ChunkId::new(i as u32 + 1),
                    shards: Vec::new(),
                },
                code: code(),
            })
            .collect();
        ObjectLayout {
            size,
            code: code(),
            blobs,
        }
    }

    #[test]
    fn int_range_resolves_and_clamps_past_the_end() {
        assert_eq!(
            resolve(100, 10, Some(19), false),
            Ok(ByteRange { start: 10, end: 19 })
        );
        // `last` past the final byte clamps (RFC 9110).
        assert_eq!(
            resolve(100, 90, Some(500), false),
            Ok(ByteRange { start: 90, end: 99 })
        );
        // Open-ended range runs to the final byte.
        assert_eq!(
            resolve(100, 50, None, false),
            Ok(ByteRange { start: 50, end: 99 })
        );
        // A single byte is a valid window.
        assert_eq!(
            resolve(100, 0, Some(0), false),
            Ok(ByteRange { start: 0, end: 0 })
        );
    }

    #[test]
    fn suffix_range_resolves_and_saturates() {
        assert_eq!(
            resolve(100, 10, None, true),
            Ok(ByteRange { start: 90, end: 99 })
        );
        // A suffix longer than the object is the whole object, not an error.
        assert_eq!(
            resolve(100, 500, None, true),
            Ok(ByteRange { start: 0, end: 99 })
        );
        assert_eq!(resolve(100, 0, None, true), Err(RangeError::Unsatisfiable));
    }

    #[test]
    fn unsatisfiable_ranges_are_rejected() {
        // First byte at or past the end.
        assert_eq!(
            resolve(100, 100, Some(200), false),
            Err(RangeError::Unsatisfiable)
        );
        // Inverted range.
        assert_eq!(
            resolve(100, 50, Some(10), false),
            Err(RangeError::Unsatisfiable)
        );
        // An empty object has no byte to serve.
        assert_eq!(resolve(0, 0, None, false), Err(RangeError::Unsatisfiable));
    }

    #[test]
    fn plan_selects_only_the_overlapping_blobs() {
        // Four 100-byte blobs: offsets 0..99, 100..199, 200..299, 300..399.
        let layout = layout(&[100, 100, 100, 100]);
        // A window inside blob 2 alone.
        let plan = plan(
            &layout,
            ByteRange {
                start: 210,
                end: 240,
            },
        )
        .expect("plan");
        assert_eq!(plan.blobs.len(), 1, "one blob covers the window");
        assert_eq!(plan.blobs[0].chunk.chunk_id, ChunkId::new(3));
        assert_eq!(plan.skip, 10, "trim to the window inside that blob");
        assert_eq!(plan.take, 31);
    }

    #[test]
    fn plan_spans_a_blob_boundary() {
        let layout = layout(&[100, 100, 100, 100]);
        // Straddles blobs 0 and 1 — the case a naive per-blob implementation gets
        // wrong, and the one every parallel downloader hits.
        let plan = plan(
            &layout,
            ByteRange {
                start: 95,
                end: 104,
            },
        )
        .expect("plan");
        assert_eq!(plan.blobs.len(), 2);
        assert_eq!(plan.blobs[0].chunk.chunk_id, ChunkId::new(1));
        assert_eq!(plan.blobs[1].chunk.chunk_id, ChunkId::new(2));
        assert_eq!(plan.skip, 95);
        assert_eq!(plan.take, 10);
    }

    #[test]
    fn plan_covers_exact_blob_edges() {
        let layout = layout(&[100, 100]);
        // Exactly the first blob.
        let p = plan(&layout, ByteRange { start: 0, end: 99 }).expect("plan");
        assert_eq!(p.blobs.len(), 1);
        assert_eq!((p.skip, p.take), (0, 100));
        // Exactly the second blob.
        let p = plan(
            &layout,
            ByteRange {
                start: 100,
                end: 199,
            },
        )
        .expect("plan");
        assert_eq!(p.blobs.len(), 1);
        assert_eq!(p.blobs[0].chunk.chunk_id, ChunkId::new(2));
        assert_eq!((p.skip, p.take), (0, 100));
        // The whole object.
        let p = plan(&layout, ByteRange { start: 0, end: 199 }).expect("plan");
        assert_eq!(p.blobs.len(), 2);
        assert_eq!((p.skip, p.take), (0, 200));
        // The final byte only.
        let p = plan(
            &layout,
            ByteRange {
                start: 199,
                end: 199,
            },
        )
        .expect("plan");
        assert_eq!(p.blobs.len(), 1);
        assert_eq!((p.skip, p.take), (99, 1));
    }

    #[test]
    fn plan_rejects_a_layout_that_does_not_cover_the_window() {
        // A truncated slice list must fail rather than serve short data.
        let layout = layout(&[100]);
        assert_eq!(
            plan(
                &layout,
                ByteRange {
                    start: 50,
                    end: 150
                }
            ),
            None
        );
        assert_eq!(
            plan(
                &layout,
                ByteRange {
                    start: 100,
                    end: 100
                }
            ),
            None
        );
    }

    #[test]
    fn plan_handles_uneven_final_blob() {
        // Last blob short (the usual case: size is not a blob multiple).
        let layout = layout(&[100, 30]);
        let p = plan(
            &layout,
            ByteRange {
                start: 120,
                end: 129,
            },
        )
        .expect("plan");
        assert_eq!(p.blobs.len(), 1);
        assert_eq!(p.blobs[0].chunk.chunk_id, ChunkId::new(2));
        assert_eq!((p.skip, p.take), (20, 10));
    }

    #[test]
    fn content_range_header_formats_rfc_style() {
        let r = ByteRange { start: 10, end: 19 };
        assert_eq!(r.content_range(100), "bytes 10-19/100");
        assert_eq!(r.byte_count(), 10);
    }
}
