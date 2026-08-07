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

//! The data-reference extraction seam (03 §12.2 通用化缝).
//!
//! When apply captures an old (overwritten or deleted) metadata value into the
//! delete queue, the state machine core never interprets the value's `content`
//! — all slice interpretation lives behind the [`RefExtractor`] seam, so the
//! same domain state machine can serve a different product with a different
//! reference model (03 §12.2: epochIO extracts `Slice{chunk_id, blob_ids}`,
//! curvine would extract block ids).
//!
//! Design: docs/design/03-metanode.md §12.2 (RefExtractor trait), §4.2 (Slice),
//! §8 (PendingDelete)

use epoch_proto::{BlobId, ChunkId};
use serde::{Deserialize, Serialize};

use crate::store::MetaCf;

/// One data reference: the blob ids of one EC stripe of the object
/// (03 §4.2 `Slice{chunk_id, blob_ids(varint delta), blob_size}`).
///
/// v1 encodes values as serde_json like the rest of the metadata model; the
/// varint-delta blob-id packing of 03 §4.2 is a registered encoding
/// optimization, not a layout change (the type set stays identical).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slice {
    /// The chunk (EC stripe group) this slice references.
    pub chunk_id: ChunkId,
    /// One blob id per shard of the stripe, in shard order.
    pub blob_ids: Vec<BlobId>,
    /// The uniform blob size of this slice's blobs (last stripe may differ —
    /// carried per slice for that reason).
    pub blob_size: u32,
}

/// A captured batch of references awaiting tombstoning (03 §8 delq value:
/// `PendingDeleteSlice{slices[], enqueue_ts}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingDelete {
    /// The references to tombstone, in object order.
    pub slices: Vec<Slice>,
    /// Proposer wall-clock milliseconds, carried inside the raft entry: the
    /// apply path never reads a clock (deterministic apply, same discipline
    /// as PD's Q18), yet every replica enqueues the identical entry.
    pub enqueue_ts: i64,
}

/// Extracts data references from captured old metadata values (03 §12.2).
///
/// Implementations must be total and non-failing: a value that holds no
/// references (inline content, a tombstone, a malformed blob that will be
/// dealt with by inspection tooling) yields an empty vec, never a panic —
/// apply must always make progress.
pub trait RefExtractor: Send + Sync {
    /// The references held by one old value of column family `cf`
    /// (`ObjectHead` in `meta`, `SliceSegment` in `meta_seg`); empty for
    /// values holding none.
    fn extract(&self, cf: MetaCf, value: &[u8]) -> Vec<Slice>;
}

/// The epochIO extractor: decodes the flat-namespace value model
/// (`ns_flat`) and pulls out its slices.
pub struct EpochRefExtractor;

impl RefExtractor for EpochRefExtractor {
    fn extract(&self, cf: MetaCf, value: &[u8]) -> Vec<Slice> {
        match cf {
            MetaCf::Meta => serde_json::from_slice::<crate::ns_flat::ObjectHead>(value)
                .map(|head| head.into_slices())
                .unwrap_or_default(),
            MetaCf::Fs => match serde_json::from_slice::<crate::ns_hier::FsRecord>(value) {
                Ok(crate::ns_hier::FsRecord::File(file)) => file.into_slices(),
                // Directory entries and sentinels hold no data references.
                _ => Vec::new(),
            },
            MetaCf::MetaSeg => serde_json::from_slice::<crate::ns_flat::SliceSegment>(value)
                .map(|segment| segment.slices)
                .unwrap_or_default(),
            MetaCf::Upload => serde_json::from_slice::<crate::ns_flat::multipart::PartMeta>(value)
                .map(|part| part.slices)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ns_flat::{ContentHead, ObjectHead, SliceSegment, StorageClass};

    fn slice(chunk: u32, blobs: &[u64]) -> Slice {
        Slice {
            chunk_id: ChunkId::new(chunk),
            blob_ids: blobs.iter().map(|&b| BlobId::from_raw(b)).collect(),
            blob_size: 32 << 20,
        }
    }

    #[test]
    fn extracts_slices_from_heads_and_segments() {
        let extractor = EpochRefExtractor;
        let inline = ObjectHead {
            size: 3,
            etag: [0; 16],
            mtime: 0,
            storage: StorageClass::Standard,
            content: ContentHead::Inline(b"abc".to_vec()),
            seg_count: 0,
            http: Default::default(),
        };
        let value = serde_json::to_vec(&inline).expect("encode");
        assert!(extractor.extract(MetaCf::Meta, &value).is_empty());

        let with_slices = ObjectHead {
            content: ContentHead::Slices(vec![slice(1, &[10, 11])]),
            ..inline.clone()
        };
        let value = serde_json::to_vec(&with_slices).expect("encode");
        assert_eq!(
            extractor.extract(MetaCf::Meta, &value),
            vec![slice(1, &[10, 11])]
        );

        let segment = SliceSegment {
            slices: vec![slice(2, &[20])],
        };
        let value = serde_json::to_vec(&segment).expect("encode");
        assert_eq!(
            extractor.extract(MetaCf::MetaSeg, &value),
            vec![slice(2, &[20])]
        );

        // Multipart parts (upload CF) yield their slices too.
        let part = crate::ns_flat::multipart::PartMeta {
            size: 1,
            etag: [0; 16],
            slices: vec![slice(3, &[30])],
        };
        let value = serde_json::to_vec(&part).expect("encode");
        assert_eq!(
            extractor.extract(MetaCf::Upload, &value),
            vec![slice(3, &[30])]
        );

        // Malformed values and non-content CFs yield nothing, never a panic.
        assert!(extractor.extract(MetaCf::Meta, b"not json").is_empty());
        assert!(extractor.extract(MetaCf::Delq, &value).is_empty());
    }
}
