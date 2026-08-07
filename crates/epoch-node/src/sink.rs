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

//! The epochIO [`DeleteSink`] implementation: routes tombstones from the
//! MetaNode delete queue to the DataNode shard hosts (03 §12.2 通用化缝 —
//! the sink is injected by the assembly layer so epoch-meta never depends on
//! the DataNode client; 06 §9 v0.10).
//!
//! Resolution: `chunk_id` → PD `GetChunk` → every shard's extent and disk →
//! `DeleteBlob` RPC **fanned out to all shards** (02 §1: a blob is striped
//! across the chunk's shards under one id, so it is tombstoned everywhere;
//! per-shard failures are retried and re-tombstoned idempotently, 03 §8).
//!
//! Design: docs/design/03-metanode.md §8/§12.2; docs/design/02-datanode.md §1.6

use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use epoch_client::{PdClient, Topology};
use epoch_meta::deleter::{DeleteSink, DeleteSinkError};
use epoch_proto::{BlobId, ChunkId, ExtentId, NodeId};
use epoch_rpc::{DeleteBlobReq, RemoteTransport, ShardTransport};

/// Tombstones blobs via PD chunk lookup + the data-plane transport.
pub struct DataNodeDeleteSink {
    pd: PdClient,
    topology: Arc<Topology>,
    /// Refreshed alongside the topology (nodes join/leave; a static endpoint
    /// map learned at startup could miss every DataNode on a cold start).
    transport: RwLock<Arc<RemoteTransport>>,
}

impl DataNodeDeleteSink {
    /// Builds the sink over a PD client, its node topology (kept fresh by the
    /// caller), and the initial endpoint map. The caller refreshes the
    /// transport via [`refresh_transport`](Self::refresh_transport) as the
    /// topology converges.
    #[must_use]
    pub fn new(pd: PdClient, topology: Arc<Topology>, transport: Arc<RemoteTransport>) -> Self {
        Self {
            pd,
            topology,
            transport: RwLock::new(transport),
        }
    }

    /// Rebuilds the transport from the topology's current serving endpoints.
    pub fn refresh_transport(
        &self,
        endpoints: std::collections::HashMap<NodeId, std::net::SocketAddr>,
    ) {
        *self
            .transport
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Arc::new(RemoteTransport::new(endpoints));
    }

    fn transport(&self) -> Arc<RemoteTransport> {
        Arc::clone(
            &self
                .transport
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Maps any lookup/transport failure to a retryable sink error (the entry
/// stays queued for the next sweep, 03 §8).
fn retryable(err: impl std::fmt::Display) -> DeleteSinkError {
    DeleteSinkError::Retryable(err.to_string())
}

#[async_trait]
impl DeleteSink for DataNodeDeleteSink {
    async fn delete_blob(&self, chunk_id: ChunkId, blob_id: BlobId) -> Result<(), DeleteSinkError> {
        let chunk = self.pd.get_chunk(chunk_id).await.map_err(retryable)?;
        // One blob is striped across every shard of the chunk under one id
        // (02 §1): tombstone it on each of them. A shard that fails — including
        // one whose disk is not in the current topology snapshot — is retried
        // on the next sweep (tombstone is idempotent, 02 §1.6). At M5a such
        // gaps are transient (topology refreshes, nodes rejoin); skipping a
        // *permanently* broken disk and letting repair re-tombstone it lands
        // with M7 (see the DeleteSink trait doc).
        for shard in &chunk.shards {
            let node = self
                .topology
                .disk_node(epoch_proto::DiskId::new(shard.disk_id))
                .ok_or_else(|| retryable(format!("disk {} unknown", shard.disk_id)))?;
            let extent_id = ExtentId::from_bytes(
                shard
                    .extent_id
                    .as_slice()
                    .try_into()
                    .map_err(|_| retryable("malformed extent id in chunk view"))?,
            );
            self.transport()
                .delete_blob(node.node_id, DeleteBlobReq { extent_id, blob_id })
                .await
                .map_err(retryable)?;
        }
        Ok(())
    }
}

/// The topology refresh interval for the sink's disk→node resolution (01 §7:
/// gateways cache with periodic refresh; the sink shares that discipline).
pub const SINK_TOPOLOGY_REFRESH: Duration = Duration::from_secs(5);
