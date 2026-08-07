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

//! The data-plane trait seam: what a server implements ([`ShardHandler`]) and
//! what a client drives ([`ShardTransport`] + [`WriteStream`]), plus the
//! in-process [`LocalTransport`] that bypasses the network for a co-located
//! store (Q21).
//!
//! A blob write is a stream: [`ShardTransport::open_write`] → repeated
//! [`WriteStream::send_frame`] → [`WriteStream::finish`] (which resolves once
//! the shard is fsynced and committed, 02 §1.7 / Q16).
//!
//! Design: docs/design/02-datanode.md §2/§5; docs/design/04-ec-io.md §3.1

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use epoch_proto::{EpochError, ExtentId, NodeId};

use crate::codec::{
    CreateExtentReq, DeleteBlobReq, EndReq, ListBlobsReq, OpenReq, ReadShardReq, SealReq,
};

/// Server side of the data plane: a data node's storage engine implements this
/// so [`crate::server::Server`] can dispatch decoded frames to it.
#[async_trait]
pub trait ShardHandler: Send + Sync {
    /// Ensures a writable extent backs `shard`, returning its id. The extent's
    /// identity is `req.shard_id | req.create_ts` (PD-chosen for deterministic
    /// chunk creation, 01 §4.1).
    async fn create_extent(&self, req: CreateExtentReq) -> Result<ExtentId, EpochError>;

    /// Resolves (and validates) the writable extent a blob write will target.
    /// Called once per stream at OPEN, before any data is received.
    async fn open_blob(&self, req: OpenReq) -> Result<ExtentId, EpochError>;

    /// Commits a fully-received shard body to `extent` under `blob` (called at
    /// END). `frames` is the number of data frames the transport actually
    /// received; implementations MUST apply the integrity gate —
    /// `frames == end.frame_count && crc32c(body) == end.blob_crc` — before
    /// committing (04 §3.1: the index write is the shard-level commit point and
    /// must only follow a verified body). Returns once durable.
    async fn commit_blob(
        &self,
        extent: ExtentId,
        blob: epoch_proto::BlobId,
        frames: u32,
        end: EndReq,
        body: Bytes,
    ) -> Result<(), EpochError>;

    /// Reads a blob's shard body, or `None` if the shard has no such blob.
    async fn read_shard(&self, req: ReadShardReq) -> Result<Option<Bytes>, EpochError>;

    /// Seals an extent: drains in-flight writes, then rejects further writes
    /// (reads are unaffected). Idempotent — sealing an already-sealed extent
    /// succeeds (01 §4.2 / Q17).
    async fn seal_extent(&self, req: SealReq) -> Result<(), EpochError>;

    /// Tombstones one blob (02 §1.6): sets the index tombstone flag and
    /// accounts its bytes as deleted. Idempotent — an absent or
    /// already-tombstoned blob is a success (at-least-once delete, 03 §8).
    async fn delete_blob(&self, req: DeleteBlobReq) -> Result<(), EpochError>;

    /// Lists the blob ids present in an extent (repair enumeration, 01 §6.3):
    /// a coordinator lists a surviving shard's blobs to learn what the broken
    /// shard held. Tombstoned blobs are omitted (they must not be rebuilt).
    async fn list_blobs(&self, req: ListBlobsReq) -> Result<Vec<epoch_proto::BlobId>, EpochError>;
}

/// Client side of the data plane: the gateway drives shards through this,
/// regardless of whether the target is remote or co-located.
#[async_trait]
pub trait ShardTransport: Send + Sync {
    /// Ensures a writable extent backs `req`'s shard on `node`.
    async fn create_extent(
        &self,
        node: NodeId,
        req: CreateExtentReq,
    ) -> Result<ExtentId, EpochError>;

    /// Opens a blob write stream to `node`'s shard.
    async fn open_write(
        &self,
        node: NodeId,
        req: OpenReq,
    ) -> Result<Box<dyn WriteStream>, EpochError>;

    /// Reads a blob's shard body from `node`, or `None` if absent.
    async fn read_shard(
        &self,
        node: NodeId,
        req: ReadShardReq,
    ) -> Result<Option<Bytes>, EpochError>;

    /// Seals `node`'s extent (drain in-flight writes, then reject further
    /// writes). Idempotent (01 §4.2 / Q17).
    async fn seal(&self, node: NodeId, req: SealReq) -> Result<(), EpochError>;

    /// Tombstones one blob on `node` (02 §1.6). Idempotent — an absent or
    /// already-tombstoned blob is a success (at-least-once delete, 03 §8).
    async fn delete_blob(&self, node: NodeId, req: DeleteBlobReq) -> Result<(), EpochError>;

    /// Lists the blob ids present in `node`'s extent (repair enumeration,
    /// 01 §6.3). Tombstoned blobs are omitted.
    async fn list_blobs(
        &self,
        node: NodeId,
        req: ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError>;
}

/// A single blob's shard write stream (one per shard per blob).
#[async_trait]
pub trait WriteStream: Send {
    /// Sends one data frame (a bitrot-framed shard chunk). Frames are delivered
    /// and applied in call order.
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), EpochError>;

    /// Finishes the stream: sends END and resolves once the peer has committed
    /// the shard (or errors).
    async fn finish(self: Box<Self>, end: EndReq) -> Result<(), EpochError>;
}

/// An in-process [`ShardTransport`] that calls a co-located [`ShardHandler`]
/// directly, skipping framing and the network entirely (Q21).
pub struct LocalTransport {
    handler: Arc<dyn ShardHandler>,
}

impl LocalTransport {
    /// Wraps a co-located handler.
    #[must_use]
    pub fn new(handler: Arc<dyn ShardHandler>) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl ShardTransport for LocalTransport {
    async fn create_extent(
        &self,
        _node: NodeId,
        req: CreateExtentReq,
    ) -> Result<ExtentId, EpochError> {
        self.handler.create_extent(req).await
    }

    async fn open_write(
        &self,
        _node: NodeId,
        req: OpenReq,
    ) -> Result<Box<dyn WriteStream>, EpochError> {
        let extent = self.handler.open_blob(req).await?;
        Ok(Box::new(LocalWriteStream {
            handler: self.handler.clone(),
            extent,
            blob: req.blob_id,
            buf: BytesMut::new(),
            frames: 0,
        }))
    }

    async fn read_shard(
        &self,
        _node: NodeId,
        req: ReadShardReq,
    ) -> Result<Option<Bytes>, EpochError> {
        self.handler.read_shard(req).await
    }

    async fn seal(&self, _node: NodeId, req: SealReq) -> Result<(), EpochError> {
        self.handler.seal_extent(req).await
    }

    async fn delete_blob(&self, _node: NodeId, req: DeleteBlobReq) -> Result<(), EpochError> {
        self.handler.delete_blob(req).await
    }

    async fn list_blobs(
        &self,
        _node: NodeId,
        req: ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        self.handler.list_blobs(req).await
    }
}

/// A [`WriteStream`] that accumulates frames in memory and commits directly to
/// the co-located handler on finish. No wire is involved, but the shared
/// integrity gate in the handler still runs (it catches caller-side frame
/// counting bugs, 04 §3.1).
struct LocalWriteStream {
    handler: Arc<dyn ShardHandler>,
    extent: ExtentId,
    blob: epoch_proto::BlobId,
    buf: BytesMut,
    frames: u32,
}

#[async_trait]
impl WriteStream for LocalWriteStream {
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), EpochError> {
        self.buf.extend_from_slice(&frame);
        self.frames += 1;
        Ok(())
    }

    async fn finish(self: Box<Self>, end: EndReq) -> Result<(), EpochError> {
        self.handler
            .commit_blob(self.extent, self.blob, self.frames, end, self.buf.freeze())
            .await
    }
}
