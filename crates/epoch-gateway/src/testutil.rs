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

//! Shared gateway test fixtures (AGENTS §9.1a: one `testutil` per crate).
//!
//! Builds a small cluster of real [`StorageEngine`]s, each behind an
//! [`EngineHandler`] reached in-process through a per-node [`LocalTransport`],
//! multiplexed by a [`RoutingTransport`]. A per-node [`Fault`] decorator injects
//! write failures, read misses, and bitrot so the orchestration's quorum and
//! reconstruction paths can be exercised without the network.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use epoch_proto::{BlobId, ChunkId, DiskId, EpochError, ExtentId, NodeId, ShardId};
use epoch_rpc::{
    CreateExtentReq, DeleteBlobReq, EndReq, LocalTransport, OpenReq, ReadShardReq, SealReq,
    ShardHandler, ShardTransport, WriteStream,
};
use epoch_store::{Disk, EngineHandler, StorageEngine, Superblock};
use tempfile::TempDir;

use crate::code::{ChunkPlacement, CodeMode};

const CLUSTER: u128 = 0x00C0_FFEE;
// Small so the per-extent fallocate reservation stays cheap on Linux CI.
const TEST_EXTENT_SIZE: u64 = 8 * 1024 * 1024;

/// A fault injected at one node's shard handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Healthy node.
    None,
    /// The write path is down: create/open/commit all fail.
    FailWrite,
    /// The read path returns a miss for every blob.
    DropRead,
    /// The read path returns a body with a flipped byte (silent bitrot).
    CorruptRead,
    /// The commit stalls far past the shard write timeout (a wedged node).
    SlowCommit,
}

/// Wraps an [`EngineHandler`] to inject a [`Fault`] on the RPC verbs.
struct FaultyHandler {
    inner: EngineHandler,
    fault: Fault,
}

#[async_trait]
impl ShardHandler for FaultyHandler {
    async fn create_extent(&self, req: CreateExtentReq) -> Result<ExtentId, EpochError> {
        if self.fault == Fault::FailWrite {
            return Err(EpochError::DiskBroken);
        }
        self.inner.create_extent(req).await
    }

    async fn open_blob(&self, req: OpenReq) -> Result<ExtentId, EpochError> {
        if self.fault == Fault::FailWrite {
            return Err(EpochError::DiskBroken);
        }
        self.inner.open_blob(req).await
    }

    async fn commit_blob(
        &self,
        extent: ExtentId,
        blob: BlobId,
        frames: u32,
        end: EndReq,
        body: Bytes,
    ) -> Result<(), EpochError> {
        match self.fault {
            Fault::FailWrite => return Err(EpochError::DiskBroken),
            Fault::SlowCommit => {
                // Far past any shard timeout: simulates a wedged node whose
                // socket stays open (04 §3.3 slow-shard case).
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            _ => {}
        }
        self.inner
            .commit_blob(extent, blob, frames, end, body)
            .await
    }

    async fn read_shard(&self, req: ReadShardReq) -> Result<Option<Bytes>, EpochError> {
        match self.fault {
            Fault::DropRead => Ok(None),
            Fault::CorruptRead => Ok(self.inner.read_shard(req).await?.map(flip_first_byte)),
            _ => self.inner.read_shard(req).await,
        }
    }

    async fn seal_extent(&self, req: SealReq) -> Result<(), EpochError> {
        if self.fault == Fault::FailWrite {
            return Err(EpochError::DiskBroken);
        }
        self.inner.seal_extent(req).await
    }

    async fn delete_blob(&self, req: DeleteBlobReq) -> Result<(), EpochError> {
        self.inner.delete_blob(req).await
    }

    async fn list_blobs(
        &self,
        req: epoch_rpc::ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        self.inner.list_blobs(req).await
    }
}

/// Corrupts a shard body so its first stripe's bitrot frame fails to verify.
fn flip_first_byte(body: Bytes) -> Bytes {
    let mut v = body.to_vec();
    if let Some(first) = v.first_mut() {
        *first ^= 0xFF;
    }
    Bytes::from(v)
}

/// A [`ShardTransport`] that routes by [`NodeId`] to one in-process
/// [`LocalTransport`] per node.
struct RoutingTransport {
    nodes: HashMap<NodeId, LocalTransport>,
}

impl RoutingTransport {
    fn node(&self, node: NodeId) -> &LocalTransport {
        self.nodes.get(&node).expect("routing to a known node")
    }
}

#[async_trait]
impl ShardTransport for RoutingTransport {
    async fn create_extent(
        &self,
        node: NodeId,
        req: CreateExtentReq,
    ) -> Result<ExtentId, EpochError> {
        self.node(node).create_extent(node, req).await
    }

    async fn open_write(
        &self,
        node: NodeId,
        req: OpenReq,
    ) -> Result<Box<dyn WriteStream>, EpochError> {
        self.node(node).open_write(node, req).await
    }

    async fn read_shard(
        &self,
        node: NodeId,
        req: ReadShardReq,
    ) -> Result<Option<Bytes>, EpochError> {
        self.node(node).read_shard(node, req).await
    }

    async fn seal(&self, node: NodeId, req: SealReq) -> Result<(), EpochError> {
        self.node(node).seal(node, req).await
    }

    async fn delete_blob(&self, node: NodeId, req: DeleteBlobReq) -> Result<(), EpochError> {
        self.node(node).delete_blob(node, req).await
    }

    async fn list_blobs(
        &self,
        node: NodeId,
        req: epoch_rpc::ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        self.node(node).list_blobs(node, req).await
    }
}

/// A running test cluster: one engine per shard, the routing transport driving
/// them, and the code mode + placement the gateway writes against.
pub struct Cluster {
    /// The transport a [`crate::Writer`] / [`crate::get_object`] drives.
    pub transport: Arc<dyn ShardTransport>,
    /// The code mode the cluster is provisioned for.
    pub code: CodeMode,
    /// The shard placement (one shard per node, index-ordered).
    pub chunk: ChunkPlacement,
    engines: Vec<StorageEngine>,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    /// Joins every engine's writer threads. Call at test end.
    pub fn shutdown(&self) {
        for engine in &self.engines {
            engine.shutdown();
        }
    }
}

/// Formats a fresh temp disk and opens an engine on it.
fn fresh_engine() -> (TempDir, StorageEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    Disk::format(
        dir.path(),
        Superblock {
            disk_id: DiskId::new(1),
            cluster_id: CLUSTER,
            created_at: 0,
            flags: 0,
            extent_size: TEST_EXTENT_SIZE,
        },
    )
    .expect("format");
    let engine = StorageEngine::open(dir.path(), CLUSTER).expect("open");
    (dir, engine)
}

/// Builds a cluster for `code` with one engine per shard; `faults[i]` is applied
/// to shard `i`'s node. `faults.len()` must equal `code.total()`.
///
/// Each shard's writable extent is pre-provisioned directly on its engine (the
/// M3 stand-in for PD-driven chunk provisioning); the gateway under test only
/// OPENs these extents.
pub async fn cluster(code: CodeMode, faults: &[Fault]) -> Cluster {
    let total = code.total();
    assert_eq!(faults.len(), total, "one fault per shard");
    let chunk_id = ChunkId::new(1);

    let mut nodes = HashMap::new();
    let mut shards = Vec::with_capacity(total);
    let mut engines = Vec::with_capacity(total);
    let mut dirs = Vec::with_capacity(total);

    for (i, &fault) in faults.iter().enumerate() {
        let (dir, engine) = fresh_engine();
        let node = NodeId::new(i as u32);
        let shard = ShardId::new(chunk_id, i as u8, 0);

        // Provision the shard's extent on the raw engine so it exists even for a
        // FailWrite node (whose fault only intercepts the RPC verbs).
        engine.create_extent(shard).await.expect("provision extent");

        let handler: Arc<dyn ShardHandler> = if fault == Fault::None {
            Arc::new(EngineHandler::new(engine.clone()))
        } else {
            Arc::new(FaultyHandler {
                inner: EngineHandler::new(engine.clone()),
                fault,
            })
        };

        nodes.insert(node, LocalTransport::new(handler));
        shards.push((shard, node));
        engines.push(engine);
        dirs.push(dir);
    }

    Cluster {
        transport: Arc::new(RoutingTransport { nodes }),
        code,
        chunk: ChunkPlacement { chunk_id, shards },
        engines,
        _dirs: dirs,
    }
}

/// A deterministic pseudo-random object body of `len` bytes.
#[must_use]
pub fn object_of(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + 7) as u8).collect()
}
