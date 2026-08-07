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

//! [`EngineHandler`]: the adapter that lets a data node's [`StorageEngine`]
//! serve the data-plane RPC by implementing [`epoch_rpc::ShardHandler`].
//!
//! The engine already speaks the cross-component [`EpochError`] and owns all
//! ordering, durability and concurrency, so this adapter is intentionally thin —
//! it maps each RPC verb to one engine call:
//!   - `CreateExtent` → [`StorageEngine::create_extent`];
//!   - `Open`         → [`StorageEngine::writable_extent`] (resolve the target);
//!   - `commit_blob`  → the shared integrity gate (frame count + CRC, 04 §3.1)
//!     then [`StorageEngine::write`];
//!   - `read_shard`   → [`StorageEngine::read_shard`].
//!
//! Design: docs/design/02-datanode.md §5; docs/design/06-code-layout.md §6

use async_trait::async_trait;
use bytes::Bytes;
use epoch_proto::{BlobId, EpochError, ExtentId};
use epoch_rpc::{
    CreateExtentReq, DeleteBlobReq, EndReq, ListBlobsReq, OpenReq, ReadShardReq, SealReq,
    ShardHandler,
};

use crate::service::StorageEngine;

/// Serves the data-plane RPC by delegating to a co-located [`StorageEngine`].
#[derive(Debug, Clone)]
pub struct EngineHandler {
    engine: StorageEngine,
}

impl EngineHandler {
    /// Wraps `engine` as a data-plane handler.
    #[must_use]
    pub fn new(engine: StorageEngine) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl ShardHandler for EngineHandler {
    async fn create_extent(&self, req: CreateExtentReq) -> Result<ExtentId, EpochError> {
        self.engine
            .create_extent_at(req.shard_id, req.create_ts)
            .await
    }

    async fn open_blob(&self, req: OpenReq) -> Result<ExtentId, EpochError> {
        self.engine.writable_extent(req.shard_id).await
    }

    async fn commit_blob(
        &self,
        extent: ExtentId,
        blob: BlobId,
        frames: u32,
        end: EndReq,
        body: Bytes,
    ) -> Result<(), EpochError> {
        // The shared integrity gate (04 §3.1): the body is committed only after
        // the declared frame count and CRC verify — on every path, including
        // the in-process LocalTransport.
        if frames != end.frame_count || crc32c::crc32c(&body) != end.blob_crc {
            tracing::warn!(
                blob = ?blob,
                frames,
                expected_frames = end.frame_count,
                "blob integrity mismatch on end, rejecting"
            );
            return Err(EpochError::Internal);
        }
        // Both Written and AlreadyExists are success: the blob is durable under
        // its id (idempotent retry, 02 §1.4).
        self.engine.write(extent, blob, body).await.map(|_| ())
    }

    async fn read_shard(&self, req: ReadShardReq) -> Result<Option<Bytes>, EpochError> {
        Ok(self
            .engine
            .read_shard(req.shard_id, req.blob_id)
            .await?
            .map(Bytes::from))
    }

    async fn seal_extent(&self, req: SealReq) -> Result<(), EpochError> {
        self.engine.seal_extent(req.extent_id).await
    }

    async fn delete_blob(&self, req: DeleteBlobReq) -> Result<(), EpochError> {
        // `delete` (not `index.tombstone_blob`) so the shard's *current* binding
        // is resolved under the stripe lock: the deleter's extent id can predate
        // a compaction rebind (02 §1.6 INVARIANT). The bool is intentionally
        // dropped — an already-absent blob is a success for at-least-once
        // delivery (03 §8).
        self.engine
            .delete(req.extent_id, req.blob_id)
            .await
            .map(|_| ())
    }

    async fn list_blobs(&self, req: ListBlobsReq) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        self.engine.list_live_blobs(req.extent_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{blob, fresh_engine, shard};

    /// INVARIANT(design 02 §1.6): the RPC delete verb must resolve the shard's
    /// *current* extent. The MetaNode deleter's extent id comes from a PD
    /// placement read that can predate a compaction rebind; if the handler
    /// tombstoned the caller's (retired) extent, the RPC would report success,
    /// the delq record would be consumed, and the blob would stay live forever.
    ///
    /// This test drives the production path (`ShardHandler::delete_blob`) rather
    /// than the engine method directly — the defect it guards was invisible to
    /// engine-level tests precisely because they took the safe path.
    #[tokio::test]
    async fn rpc_delete_with_a_stale_extent_id_tombstones_the_current_extent() {
        let (_dir, engine) = fresh_engine();
        let handler = EngineHandler::new(engine.clone());
        let stale = engine.create_extent(shard()).await.expect("create");
        for seq in 0..3u32 {
            engine
                .write(stale, blob(seq), Bytes::from(vec![seq as u8; 1000]))
                .await
                .expect("write");
        }
        // Retire `stale`: compaction copies the live blobs into a new extent and
        // rebinds the shard to it.
        engine.delete(stale, blob(0)).await.expect("pre-delete");
        let outcome = engine.compact_extent(stale).await.expect("compact");
        assert_ne!(outcome.new_extent, stale, "the shard was rebound");

        // A deleter still holding the pre-compaction id issues the RPC.
        handler
            .delete_blob(DeleteBlobReq {
                extent_id: stale,
                blob_id: blob(1),
            })
            .await
            .expect("delete_blob");

        // The tombstone landed on the live extent, not the retired one.
        let live = engine
            .list_live_blobs(outcome.new_extent)
            .await
            .expect("list");
        assert!(
            !live.contains(&blob(1)),
            "blob(1) must be tombstoned on the current extent; found {live:?}"
        );
        assert!(
            live.contains(&blob(2)),
            "unrelated blobs stay live; found {live:?}"
        );
    }

    /// At-least-once delivery (03 §8): a repeated delete is a success, and an
    /// unknown blob is not an error — the deleter must be able to retry.
    #[tokio::test]
    async fn rpc_delete_is_idempotent() {
        let (_dir, engine) = fresh_engine();
        let handler = EngineHandler::new(engine.clone());
        let extent = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent, blob(0), Bytes::from_static(b"x"))
            .await
            .expect("write");

        let req = DeleteBlobReq {
            extent_id: extent,
            blob_id: blob(0),
        };
        handler.delete_blob(req).await.expect("first delete");
        handler.delete_blob(req).await.expect("repeat delete");
        handler
            .delete_blob(DeleteBlobReq {
                extent_id: extent,
                blob_id: blob(99),
            })
            .await
            .expect("unknown blob is a success");
    }
}
