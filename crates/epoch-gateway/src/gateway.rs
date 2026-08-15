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

//! [`Gateway`]: the assembled write/read path against a real cluster (M4) —
//! PD-driven writable-chunk selection with seal/error-driven **rewrite on a
//! different chunk** (04 §3.3), per-blob placement, and the writer-token
//! session for blob ids.
//!
//! Assembly: PD client, topology, chunk map, writable set, and the writer
//! session come from `epoch-client`; the transport is an
//! `Arc<dyn ShardTransport>` (a [`epoch_client::MuxTransport`] when co-located
//! with a data node, Q21).
//!
//! Design: docs/design/04-ec-io.md §3; docs/design/02-datanode.md §2.1/§2.3

use std::sync::Arc;

use epoch_client::{TokenSource, WritableSet, WriterSession};
use epoch_proto::{BlobId, EpochError, NodeId, ShardId};
use epoch_rpc::ShardTransport;

use crate::code::{BlobDesc, ChunkPlacement, CodeMode, ObjectLayout};
use crate::error::GatewayError;
use crate::get::ReadObject;
use crate::put::write_blob;

/// Blob-write attempts before giving up (each on a fresh chunk, 04 §3.3).
const MAX_BLOB_ATTEMPTS: usize = 3;

/// The assembled gateway write/read path. Cheap to clone (all state is shared).
pub struct Gateway<S: TokenSource> {
    code: CodeMode,
    local_node: NodeId,
    writable_set: WritableSet,
    /// Shared with the session's heartbeat driver (01 §4.3): the driver
    /// refreshes the liveness watermark this gateway's minting gates on.
    writer: Arc<WriterSession<S>>,
    transport: Arc<dyn ShardTransport>,
}

impl<S: TokenSource> Gateway<S> {
    /// Assembles the path over its injected pieces.
    #[must_use]
    pub fn new(
        code: CodeMode,
        local_node: NodeId,
        writable_set: WritableSet,
        writer: Arc<WriterSession<S>>,
        transport: Arc<dyn ShardTransport>,
    ) -> Self {
        Self {
            code,
            local_node,
            writable_set,
            writer,
            transport,
        }
    }

    /// Writes `data` as an erasure-coded object and returns its layout.
    ///
    /// Each blob is written to a chunk picked from the PD-published writable
    /// set (locality-affined, 02 §2.3); a blob that cannot reach quorum on a
    /// chunk (seal, disk failure, …) is **rewritten whole** on a freshly picked
    /// chunk after evicting the failing one (04 §3.3).
    ///
    /// # Errors
    ///
    /// - [`GatewayError::NoWritableChunk`] if the set is empty;
    /// - [`GatewayError::Writer`] if the session cannot mint ids;
    /// - [`GatewayError::QuorumNotMet`] if every attempt failed.
    pub async fn put_object(&self, data: &[u8]) -> Result<ObjectLayout, GatewayError> {
        let code = &self.code;
        let ec = code.erasure()?;
        let quorum = code.write_quorum();
        let mut blobs = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + code.blob_size).min(data.len());
            let slice = &data[offset..end];
            let blob_id = self.next_blob_id().await?;
            let placement = self
                .write_blob_with_rewrite(&ec, blob_id, slice, quorum)
                .await?;
            blobs.push(BlobDesc {
                blob_id,
                len: slice.len(),
                chunk: placement,
                code: self.code,
            });
            offset = end;
        }
        Ok(ObjectLayout {
            size: data.len() as u64,
            code: self.code,
            blobs,
        })
    }

    /// Reads and reconstructs a whole object from its layout (04 §4), returning
    /// the bytes plus any heal-on-read reports the caller forwards to PD.
    pub async fn get_object(&self, layout: &ObjectLayout) -> Result<ReadObject, GatewayError> {
        crate::get::get_object(self.transport.clone(), layout).await
    }

    /// The writer session (heartbeat recording and token inspection). Shared:
    /// pass a clone to [`WriterSession::spawn_heartbeat`] to drive liveness.
    pub fn writer_session(&self) -> &Arc<WriterSession<S>> {
        &self.writer
    }

    /// Marks every blob of `layout` resolved on the writer session (its object
    /// metadata has committed to MetaNode, or the write was abandoned), so they
    /// no longer count as in-flight for the GC commit watermark (Q27). The caller
    /// invokes this after the MetaNode commit that references the layout.
    pub fn resolve_layout(&self, layout: &ObjectLayout) {
        for blob in &layout.blobs {
            self.writer.resolve_blob(blob.blob_id);
        }
    }

    async fn next_blob_id(&self) -> Result<BlobId, GatewayError> {
        self.writer
            .next_blob_id(now_millis())
            .await
            .map_err(|e| GatewayError::Writer(e.to_string()))
    }

    /// Writes one blob, evicting and retrying on a fresh chunk until quorum.
    async fn write_blob_with_rewrite(
        &self,
        ec: &epoch_ec::Erasure,
        blob_id: BlobId,
        slice: &[u8],
        quorum: usize,
    ) -> Result<ChunkPlacement, GatewayError> {
        let mut last_committed = 0;
        // Whether the *last* attempt failed because targets had no room: it
        // changes the error the client sees from "internal" to "out of space",
        // which is the difference between "retry" and "add capacity".
        let mut last_out_of_space = false;
        for _ in 0..MAX_BLOB_ATTEMPTS {
            let Some(writable) = self.writable_set.pick(Some(self.local_node)).await else {
                return Err(GatewayError::NoWritableChunk);
            };
            let placement = placement_of(&writable);
            if placement.shards.len() != self.code.total() {
                self.writable_set.evict(writable.chunk_id);
                continue;
            }
            let (committed, errors) =
                write_blob(&self.transport, &self.code, &placement, ec, blob_id, slice).await?;
            if committed >= quorum {
                return Ok(placement);
            }
            last_committed = committed;
            // Whatever the cause (Sealed / ChunkFull / DiskBroken / OutOfSpace /
            // a slow shard), a chunk that cannot reach quorum leaves the writable
            // set and the blob is rewritten elsewhere. `OutOfSpace` needs no
            // special case here — it is a "write somewhere else" signal exactly
            // like `ChunkFull`, and deliberately *not* a disk-failure signal
            // (02 §1.7: a full disk must not trigger repair).
            let out_of_space = errors.contains(&EpochError::OutOfSpace);
            last_out_of_space = out_of_space;
            tracing::debug!(
                chunk = writable.chunk_id.get(),
                committed,
                error_count = errors.len(),
                out_of_space,
                "blob quorum unmet; evicting chunk and rewriting whole blob"
            );
            self.writable_set.evict(writable.chunk_id);
        }
        if last_out_of_space {
            return Err(GatewayError::OutOfSpace);
        }
        Err(GatewayError::QuorumNotMet {
            need: quorum,
            got: last_committed,
        })
    }
}

/// Projects a picked writable chunk to the gateway's placement view (shard ids
/// composed with their current epoch, 01 §4.4).
fn placement_of(writable: &epoch_client::WritableChunk) -> ChunkPlacement {
    ChunkPlacement {
        chunk_id: writable.chunk_id,
        shards: writable
            .slots
            .shards
            .iter()
            .map(|shard| {
                (
                    ShardId::new(writable.chunk_id, shard.index, shard.epoch),
                    shard.node_id,
                )
            })
            .collect(),
    }
}

/// Local wall-clock milliseconds for the writer session's liveness gate.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
