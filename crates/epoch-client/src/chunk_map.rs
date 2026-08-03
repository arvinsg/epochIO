//! chunk→shards mapping cache (02 §2.1): resolves each chunk's shard slots to
//! data-plane endpoints once, then serves reads from memory; entries are
//! invalidated on epoch bumps and shard errors (`ShardNotFound` / seal-driven
//! placement changes), forcing a fresh fetch on the next read.
//!
//! Design: docs/design/02-datanode.md §2.1; docs/design/01-pd.md §4.4

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use epoch_proto::{ChunkId, ExtentId, NodeId};

use crate::error::ClientError;
use crate::pd::PdClient;
use crate::topology::Topology;

/// One shard slot resolved to a data-plane endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedShard {
    /// Shard index within the EC stripe (`0..N+M`).
    pub index: u8,
    /// Hosting node.
    pub node_id: NodeId,
    /// Node's data-plane address.
    pub addr: SocketAddr,
    /// The currently-bound on-disk extent.
    pub extent_id: ExtentId,
    /// Slot epoch (re-binding version, 01 §4.4).
    pub epoch: u32,
    /// Node's rack (topology affinity for read ordering, 04 §4).
    pub rack: String,
}

/// The resolved view of one chunk: code mode, status, and every shard slot.
#[derive(Debug, Clone)]
pub struct ChunkSlots {
    /// Erasure-code parameters (encoding shape for the gateway).
    pub code_mode: epoch_proto::CodeMode,
    /// Lifecycle status (writes target only `Writable`).
    pub writable: bool,
    /// `(shard index, endpoint)` in shard-index order.
    pub shards: Vec<ResolvedShard>,
}

/// A cheap-to-clone chunk→shards cache backed by PD lookups.
#[derive(Clone)]
pub struct ChunkMap {
    pd: PdClient,
    topology: Topology,
    entries: Arc<RwLock<HashMap<ChunkId, ChunkSlots>>>,
}

impl ChunkMap {
    /// Builds an empty cache over a PD client and a topology cache.
    #[must_use]
    pub fn new(pd: PdClient, topology: Topology) -> Self {
        Self {
            pd,
            topology,
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Resolves a chunk, fetching and caching on a miss. Unresolvable shards
    /// (disk not in topology yet) are fetched again next time rather than
    /// cached incomplete.
    ///
    /// # Errors
    /// [`ClientError::NotFound`] if the chunk is unknown; [`ClientError::NoLeader`]
    /// if PD cannot serve the read.
    pub async fn get(&self, chunk_id: ChunkId) -> Result<ChunkSlots, ClientError> {
        if let Some(slots) = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&chunk_id)
            .cloned()
        {
            return Ok(slots);
        }
        self.topology.refresh_if_stale().await;
        let view = self.pd.get_chunk(chunk_id).await?;
        let mut shards = Vec::with_capacity(view.shards.len());
        for (i, slot) in view.shards.iter().enumerate() {
            let index =
                u8::try_from(i).map_err(|_| ClientError::Internal("shard index > 255".into()))?;
            let Some(node) = self
                .topology
                .disk_node(epoch_proto::DiskId::new(slot.disk_id))
            else {
                return Err(ClientError::Internal(format!(
                    "chunk {} shard {} disk {} not in topology",
                    chunk_id.get(),
                    index,
                    slot.disk_id
                )));
            };
            let extent_bytes: [u8; 16] = slot
                .extent_id
                .as_slice()
                .try_into()
                .map_err(|_| ClientError::Internal("bad extent id length".into()))?;
            shards.push(ResolvedShard {
                index,
                node_id: node.node_id,
                addr: node.addr,
                extent_id: ExtentId::from_bytes(extent_bytes),
                epoch: slot.epoch,
                rack: node.rack.clone(),
            });
        }
        let code_mode = view
            .code_mode
            .ok_or_else(|| ClientError::Internal("chunk view missing code mode".into()))?;
        let slots = ChunkSlots {
            code_mode: epoch_proto::CodeMode {
                id: epoch_proto::CodeModeId::new(
                    u16::try_from(code_mode.id)
                        .map_err(|_| ClientError::Internal("code mode id exceeds u16".into()))?,
                ),
                data: u8::try_from(code_mode.data)
                    .map_err(|_| ClientError::Internal("code mode data > u8".into()))?,
                parity: u8::try_from(code_mode.parity)
                    .map_err(|_| ClientError::Internal("code mode parity > u8".into()))?,
                stripe_size: code_mode.stripe_size,
                blob_size: code_mode.blob_size,
            },
            writable: view.status == epoch_proto::grpc::pd::ChunkStatus::Writable as i32,
            shards,
        };
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(chunk_id, slots.clone());
        Ok(slots)
    }

    /// The topology cache this map resolves through (rack/addr reads).
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// Drops a cached entry (epoch bump / shard error / seal): the next read
    /// re-fetches from PD (02 §2.1 error-driven refresh).
    pub fn invalidate(&self, chunk_id: ChunkId) {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&chunk_id);
    }

    /// Reports a bad/missing shard to PD for a ShardRepair (heal-on-read, 04 §4 /
    /// 01 §6.3). Best-effort: `Ok(true)` if a new ticket was recorded, `Ok(false)`
    /// if one was already pending or the shard is unknown. The gateway calls this
    /// after (not before) returning the reconstructed bytes, so a repair report
    /// never blocks or fails a GET.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if PD cannot serve the write.
    pub async fn report_shard_repair(
        &self,
        chunk_id: ChunkId,
        index: u8,
    ) -> Result<bool, ClientError> {
        self.pd.report_shard_repair(chunk_id, index).await
    }
}
