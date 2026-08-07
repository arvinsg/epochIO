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

//! Cluster topology cache: the disk → `(node, address)` resolution the gateway
//! and PD-side drivers use to reach shards on the data plane (02 §2.1).
//!
//! The snapshot is rebuilt wholesale from `ListNodes` on refresh (periodic plus
//! error-driven); lookups are lock-free reads behind an [`arc_swap::ArcSwap`]
//! — never a network call on the hot path.
//!
//! Design: docs/design/02-datanode.md §2.1

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use epoch_proto::{DiskId, NodeId};

use crate::error::ClientError;
use crate::pd::PdClient;

/// Default topology refresh interval (matches the writable-set pull, 02 §2.1).
pub const DEFAULT_TOPOLOGY_REFRESH: Duration = Duration::from_secs(30);

/// One resolved node: its cluster id, data-plane address, and rack (topology
/// affinity for shard selection, 01 §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeTopo {
    /// Cluster node id.
    pub node_id: NodeId,
    /// Data-plane address (`host:port`).
    pub addr: std::net::SocketAddr,
    /// Rack (topology affinity domain).
    pub rack: String,
    /// Whether the node serves DATA and is `Live`.
    pub serving: bool,
}

/// An immutable topology snapshot: disk → owning node's resolution.
#[derive(Debug, Default)]
struct Snapshot {
    nodes: HashMap<NodeId, NodeTopo>,
    disks: HashMap<DiskId, NodeId>,
}

/// A cheap-to-clone topology cache with periodic + error-driven refresh.
#[derive(Clone)]
pub struct Topology {
    pd: PdClient,
    refresh_interval: Duration,
    inner: Arc<ArcSwapBox>,
    last_refresh: Arc<RwLock<Option<Instant>>>,
}

/// Tiny `ArcSwap` stand-in (no new dependency): an `RwLock` around an `Arc` so
/// readers pay only a read lock.
struct ArcSwapBox(RwLock<Arc<Snapshot>>);

impl ArcSwapBox {
    fn load(&self) -> Arc<Snapshot> {
        Arc::clone(
            &self
                .0
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn store(&self, snapshot: Snapshot) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(snapshot);
    }
}

impl Topology {
    /// Builds an empty cache; call [`refresh`](Self::refresh) to fill it.
    #[must_use]
    pub fn new(pd: PdClient, refresh_interval: Duration) -> Self {
        Self {
            pd,
            refresh_interval,
            inner: Arc::new(ArcSwapBox(RwLock::new(Arc::new(Snapshot::default())))),
            last_refresh: Arc::new(RwLock::new(None)),
        }
    }

    /// Resolves a disk to its owning node's topology, if both are known.
    #[must_use]
    pub fn disk_node(&self, disk_id: DiskId) -> Option<NodeTopo> {
        let snapshot = self.inner.load();
        let node_id = snapshot.disks.get(&disk_id)?;
        snapshot.nodes.get(node_id).cloned()
    }

    /// Resolves a node id to its topology (address/rack), if known.
    #[must_use]
    pub fn node(&self, node_id: NodeId) -> Option<NodeTopo> {
        self.inner.load().nodes.get(&node_id).cloned()
    }

    /// Every currently-serving node's `(id, data-plane address)` — the
    /// endpoint map a `RemoteTransport` is built from (02 §2.1).
    #[must_use]
    pub fn serving_endpoints(&self) -> std::collections::HashMap<NodeId, std::net::SocketAddr> {
        self.inner
            .load()
            .nodes
            .values()
            .filter(|node| node.serving)
            .map(|node| (node.node_id, node.addr))
            .collect()
    }

    /// Whether the cached snapshot is older than the refresh interval (or absent).
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.last_refresh
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none_or(|t| t.elapsed() >= self.refresh_interval)
    }

    /// Rebuilds the snapshot from PD (`ListNodes`), keeping only `Live` DATA
    /// nodes and their disks.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if PD cannot serve the read.
    pub async fn refresh(&self) -> Result<(), ClientError> {
        let (nodes, disks) = self.pd.list_nodes().await?;
        let mut snapshot = Snapshot::default();
        for node in nodes {
            let Ok(addr) = node.addr.parse::<std::net::SocketAddr>() else {
                tracing::warn!(node = node.node_id, addr = %node.addr, "skipping node with bad address");
                continue;
            };
            let serving = node.status == epoch_proto::grpc::pd::NodeStatus::Live as i32
                && node.roles & ROLE_DATA != 0;
            snapshot.nodes.insert(
                NodeId::new(node.node_id),
                NodeTopo {
                    node_id: NodeId::new(node.node_id),
                    addr,
                    rack: node.rack,
                    serving,
                },
            );
        }
        for disk in disks {
            if disk.status == epoch_proto::grpc::pd::DiskStatus::Normal as i32 {
                snapshot
                    .disks
                    .insert(DiskId::new(disk.disk_id), NodeId::new(disk.node_id));
            }
        }
        self.inner.store(snapshot);
        *self
            .last_refresh
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Instant::now());
        Ok(())
    }

    /// Refreshes if stale; errors are logged, not surfaced (stale reads are
    /// still better than failing the hot path).
    pub async fn refresh_if_stale(&self) {
        if self.is_stale()
            && let Err(err) = self.refresh().await
        {
            tracing::debug!(error = %err, "topology refresh failed; serving stale");
        }
    }
}

/// The `RoleSet::DATA` bit mirrored from PD (01 §3).
const ROLE_DATA: u32 = 1;
