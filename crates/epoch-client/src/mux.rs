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

//! [`MuxTransport`]: routes each shard op to the in-process [`LocalTransport`]
//! when the target is the co-located node, and to the remote [`RemoteTransport`]
//! otherwise (Q21: the gateway bypasses the network for its own node's shards).
//!
//! Design: docs/design/02-datanode.md §2; docs/design/99-open-questions.md (Q21)

use std::sync::Arc;

use epoch_proto::{EpochError, ExtentId, NodeId};
use epoch_rpc::{
    CreateExtentReq, DeleteBlobReq, LocalTransport, OpenReq, ReadShardReq, RemoteTransport,
    SealReq, ShardHandler, ShardTransport, WriteStream,
};

/// A [`ShardTransport`] that is local for one node and remote for the rest.
pub struct MuxTransport {
    local_node: NodeId,
    local: LocalTransport,
    remote: RemoteTransport,
}

impl MuxTransport {
    /// Builds the mux: ops for `local_node` go to the co-located `handler`
    /// in-process; everything else goes over the remote transport.
    #[must_use]
    pub fn new(
        local_node: NodeId,
        handler: Arc<dyn ShardHandler>,
        remote: RemoteTransport,
    ) -> Self {
        Self {
            local_node,
            local: LocalTransport::new(handler),
            remote,
        }
    }
}

#[async_trait::async_trait]
impl ShardTransport for MuxTransport {
    async fn create_extent(
        &self,
        node: NodeId,
        req: CreateExtentReq,
    ) -> Result<ExtentId, EpochError> {
        if node == self.local_node {
            self.local.create_extent(node, req).await
        } else {
            self.remote.create_extent(node, req).await
        }
    }

    async fn open_write(
        &self,
        node: NodeId,
        req: OpenReq,
    ) -> Result<Box<dyn WriteStream>, EpochError> {
        if node == self.local_node {
            self.local.open_write(node, req).await
        } else {
            self.remote.open_write(node, req).await
        }
    }

    async fn read_shard(
        &self,
        node: NodeId,
        req: ReadShardReq,
    ) -> Result<Option<bytes::Bytes>, EpochError> {
        if node == self.local_node {
            self.local.read_shard(node, req).await
        } else {
            self.remote.read_shard(node, req).await
        }
    }

    async fn seal(&self, node: NodeId, req: SealReq) -> Result<(), EpochError> {
        if node == self.local_node {
            self.local.seal(node, req).await
        } else {
            self.remote.seal(node, req).await
        }
    }

    async fn delete_blob(&self, node: NodeId, req: DeleteBlobReq) -> Result<(), EpochError> {
        if node == self.local_node {
            self.local.delete_blob(node, req).await
        } else {
            self.remote.delete_blob(node, req).await
        }
    }

    async fn list_blobs(
        &self,
        node: NodeId,
        req: epoch_rpc::ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        if node == self.local_node {
            self.local.list_blobs(node, req).await
        } else {
            self.remote.list_blobs(node, req).await
        }
    }
}
