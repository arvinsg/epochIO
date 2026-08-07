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

//! The gateway↔MetaNode object bridge (M6): mapping between the gateway's
//! read-back [`ObjectLayout`] and the MetaNode's `SliceRef` list (03 §4.2).
//!
//! A PUT's [`ObjectLayout`] carries per-blob placement the gateway wrote to; the
//! MetaNode persists only the *reference* — `Slice{chunk_id, blob_ids,
//! blob_size}` — because a blob's shard endpoints are derivable from its chunk
//! via PD (`ChunkMap`), and re-derived on read so a repair/migration that
//! re-binds shards is transparent (01 §4.4 epoch 失效). So:
//!
//! - **write** ([`layout_to_slices`]): one blob → one slice (`blob_ids = [blob
//!   id]`, `blob_size = blob logical length`). The MetaNode stores the list.
//! - **read** ([`slices_to_layout`]): each slice's `chunk_id` is resolved to
//!   live shard endpoints via [`ChunkMap`], rebuilding the [`ObjectLayout`] a
//!   GET replays.
//!
//! Design: docs/design/04-ec-io.md §3.1; docs/design/03-metanode.md §4.2

use epoch_client::{ChunkMap, ClientError};
use epoch_proto::grpc::meta::SliceRef;
use epoch_proto::{BlobId, ChunkId, ShardId};

use crate::code::{BlobDesc, ChunkPlacement, CodeMode, ObjectLayout};
use crate::error::GatewayError;

/// Maps an object's layout to the MetaNode slice list (one blob → one slice).
/// The blob's logical length rides in `blob_size` so the read path can bound
/// each blob's bytes without a separate length record.
#[must_use]
pub fn layout_to_slices(layout: &ObjectLayout) -> Vec<SliceRef> {
    layout
        .blobs
        .iter()
        .map(|blob| SliceRef {
            chunk_id: blob.chunk.chunk_id.get(),
            blob_ids: vec![blob.blob_id.as_u64()],
            blob_size: blob.len as u32,
        })
        .collect()
}

/// Rebuilds the read-back [`ObjectLayout`] from a MetaNode slice list, resolving
/// each slice's chunk to its current shard endpoints via `chunk_map` (01 §4.4:
/// placement is re-derived on read, so a re-bound shard is transparent).
///
/// `code`/`size` come from the object head (the gateway's configured code mode
/// and the head's size). A slice is expected to reference exactly one blob (the
/// gateway writes one blob per slice); a malformed slice is an internal error.
///
/// # Errors
///
/// - [`GatewayError::Pd`] if a chunk cannot be resolved;
/// - [`GatewayError::ShardCount`] if a resolved chunk's shard count does not
///   match the code mode;
/// - [`GatewayError::Writer`] for a malformed slice (not exactly one blob id).
pub async fn slices_to_layout(
    slices: &[SliceRef],
    code: CodeMode,
    size: u64,
    chunk_map: &ChunkMap,
) -> Result<ObjectLayout, GatewayError> {
    let total = code.total();
    let mut blobs = Vec::with_capacity(slices.len());
    for slice in slices {
        let [blob_raw] = slice.blob_ids.as_slice() else {
            return Err(GatewayError::Writer(format!(
                "slice for chunk {} references {} blobs, expected exactly 1",
                slice.chunk_id,
                slice.blob_ids.len()
            )));
        };
        let chunk_id = ChunkId::new(slice.chunk_id);
        let placement = resolve_placement(chunk_id, total, chunk_map)
            .await
            .map_err(|e| GatewayError::Pd(e.to_string()))?;
        blobs.push(BlobDesc {
            blob_id: BlobId::from_raw(*blob_raw),
            len: slice.blob_size as usize,
            chunk: placement,
        });
    }
    Ok(ObjectLayout { size, code, blobs })
}

/// Resolves a chunk to its read placement (shard slots + hosting nodes) via the
/// chunk map, composing each slot's id with its current epoch (01 §4.4).
async fn resolve_placement(
    chunk_id: ChunkId,
    total: usize,
    chunk_map: &ChunkMap,
) -> Result<ChunkPlacement, ClientError> {
    let slots = chunk_map.get(chunk_id).await?;
    if slots.shards.len() != total {
        return Err(ClientError::Internal(format!(
            "chunk {} resolved to {} shards, expected {total}",
            chunk_id.get(),
            slots.shards.len()
        )));
    }
    let shards = slots
        .shards
        .iter()
        .map(|s| (ShardId::new(chunk_id, s.index, s.epoch), s.node_id))
        .collect();
    Ok(ChunkPlacement { chunk_id, shards })
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::NodeId;

    fn layout() -> ObjectLayout {
        ObjectLayout {
            size: 100,
            code: CodeMode::new(2, 1, 1 << 20, 32 << 20).expect("code"),
            blobs: vec![
                BlobDesc {
                    blob_id: BlobId::from_raw(10),
                    len: 64,
                    chunk: ChunkPlacement {
                        chunk_id: ChunkId::new(5),
                        shards: vec![
                            (ShardId::new(ChunkId::new(5), 0, 1), NodeId::new(1)),
                            (ShardId::new(ChunkId::new(5), 1, 1), NodeId::new(2)),
                            (ShardId::new(ChunkId::new(5), 2, 1), NodeId::new(3)),
                        ],
                    },
                },
                BlobDesc {
                    blob_id: BlobId::from_raw(11),
                    len: 36,
                    chunk: ChunkPlacement {
                        chunk_id: ChunkId::new(6),
                        shards: Vec::new(),
                    },
                },
            ],
        }
    }

    #[test]
    fn layout_to_slices_maps_one_blob_per_slice() {
        let slices = layout_to_slices(&layout());
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].chunk_id, 5);
        assert_eq!(slices[0].blob_ids, vec![10]);
        assert_eq!(slices[0].blob_size, 64);
        assert_eq!(slices[1].chunk_id, 6);
        assert_eq!(slices[1].blob_ids, vec![11]);
        assert_eq!(slices[1].blob_size, 36);
    }
}
