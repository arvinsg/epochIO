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

//! The `data` role: open (or format) this node's disk, register with PD when
//! configured, heartbeat capacity, and serve the data plane until shutdown.
//!
//! Two provisioning modes:
//! - **PD-driven** (`pd` endpoints configured): the node registers itself and
//!   its disk with PD, heartbeats capacity (`free` / `used` /
//!   `writable_extents`), and lets PD's placement loop create chunks — which
//!   arrive as `CreateExtent` RPCs on the data plane (01 §4.1).
//! - **Static** (no `pd` endpoints, M3 fallback): the node pre-creates a
//!   writable extent for each shard the config map hosts. Provisioning is
//!   idempotent across restarts — a shard already bound to a writable extent is
//!   left untouched, since re-creating it would orphan the existing extent and
//!   lose its blobs.
//!
//! Design: docs/design/02-datanode.md §2; docs/design/06-code-layout.md §12;
//! docs/design/07-iteration-plan.md (M4).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use epoch_client::PdClient;
use epoch_proto::grpc::pd as pdpb;
use epoch_proto::{DiskId, EpochError, NodeId, ShardId};
use epoch_rpc::Server;
use epoch_store::{Disk, DiskError, EngineHandler, StorageEngine, Superblock, space_stats};
use tokio::task::JoinHandle;

use crate::config::{ClusterConfig, NodeSpec};
use crate::error::NodeError;

/// Heartbeat interval for the PD-driven mode (01 §7; PD's placement sweep
/// reacts within one sweep interval of a landed heartbeat).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The DataNode's `/metrics` port offset from its data port.
///
/// Not the shared +1000: that is the MetaNode's port on a co-located node
/// (`meta_addr` = data + 1000), so reusing it would clash.
const DATA_METRICS_PORT_OFFSET: u16 = 1100;

/// The `RoleSet::DATA` bit announced at PD registration (design 01 §3).
const ROLE_DATA: u8 = 1;

/// Runs the `data` role for `node_id` until `shutdown` resolves, then stops
/// accepting and joins the engine's writer threads.
///
/// # Errors
///
/// [`NodeError`] if the node id is not in the config, the disk cannot be
/// opened/formatted, PD registration fails, a shard cannot be provisioned, or
/// the listen socket cannot be bound.
pub async fn run(
    config: &ClusterConfig,
    node_id: NodeId,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), NodeError> {
    let spec = config
        .node(node_id)
        .ok_or(NodeError::UnknownNode(node_id.get()))?;
    let cluster_id = config.cluster_id()?;
    let addr = spec.socket_addr()?;

    // 1. Learn the persisted disk identity (if any) before registering — a disk
    //    is idempotent by (node, path) at PD, so the identity survives restarts.
    let persisted = persisted_disk_id(spec, cluster_id)?;

    // 2. PD-driven registration (optional; empty `pd` selects static mode).
    //    Registration tolerates PD not being up yet (a dev cluster starts
    //    every process at once) — bounded retries with backoff (01 §7).
    let pd = connect_pd(config)?;
    let (cluster_node_id, disk_id) = match &pd {
        Some(client) => {
            let (cluster_node_id, assigned) =
                register_with_retry(client, spec, &spec.addr, persisted).await?;
            (cluster_node_id, assigned)
        }
        None => (node_id, persisted.unwrap_or_else(|| DiskId::new(spec.id))),
    };

    // 3. Format if unregistered (with the final disk id), then open the engine.
    let engine = open_engine(
        spec,
        cluster_id,
        config.extent_size,
        disk_id,
        config.qos.to_config(),
    )?;

    // 4. Provision: in PD mode creation arrives as CreateExtent RPCs from PD's
    //    placement loop; static mode pre-creates from the config map (M3).
    if pd.is_none() {
        provision_shards(&engine, &config.shards_hosted_by(node_id)).await?;
    }

    // 5. Heartbeat loop (PD mode only): drives Starting→Live liveness and the
    //    placement watermark on PD.
    let heartbeat = pd
        .as_ref()
        .map(|client| spawn_heartbeat(client.clone(), engine.clone(), cluster_node_id, disk_id));

    // 5b. RepairDisk coordinator (PD mode only, M7 01 §6.1): poll PD for repair
    //     jobs assigned to this node and drive them (rebuild survivors → local
    //     Rebuilding extent → PD rebind). Reads peers via a topology-built
    //     transport. Data-affinity target = this node's own disk.
    let _repair = match &pd {
        Some(client) => Some(
            spawn_repair_coordinator_for(client.clone(), &engine, cluster_node_id, disk_id).await,
        ),
        None => None,
    };

    // 5c. GcRound self-scan daemon (PD mode only, M7 01 §6.3 / Q27): periodically
    //     reclaim orphaned blobs on this disk (unreferenced + past their token
    //     watermark). Each node scans its own disk; PD/MetaNode only serve the
    //     live-token table + reference keep-set.
    let _gc = pd.as_ref().map(|client| {
        crate::roles::gc::spawn_gc_daemon(
            client.clone(),
            epoch_client::MetaClient::new(client.clone()),
            engine.clone(),
            cluster_node_id,
            config.gc.interval(),
        )
    });

    // 5d. Maintenance daemon (02 §1.6/§1.9): reclaim retired extents, compact the
    //     dirtiest ones, scrub a rotating slice. This is the ONLY path that turns
    //     tombstoned bytes back into free space — DELETE and GcRound only mark
    //     tombstones — so it runs in blueprint mode too (PD is needed only to
    //     report scrub-detected corruption).
    let _maintenance = crate::roles::maintenance::spawn_maintenance(
        engine.clone(),
        config.maintenance.clone(),
        cluster_node_id,
        disk_id,
        pd.as_ref().map(|client| Arc::new(client.clone())),
    );

    // 6. Serve the data plane.
    let handler = Arc::new(EngineHandler::new(engine.clone()));
    let server = Server::bind(addr, handler).await?;
    let bound = server.local_addr()?;
    // Observability (08 §5.1): each role serves its own /metrics. The DataNode's
    // store counters are registered in *this* process, so without an endpoint here
    // they can never be scraped. Offset from the bound port so an OS-assigned
    // port (tests) still gets a unique endpoint.
    let _metrics = crate::metrics_server::spawn_metrics_server(
        crate::metrics_server::derived_addr(bound, DATA_METRICS_PORT_OFFSET),
    );
    tracing::info!(node = node_id.get(), %bound, "data node serving");

    server.serve(shutdown).await;

    tracing::info!(node = node_id.get(), "data node stopping; joining writers");
    if let Some(handle) = &heartbeat {
        handle.abort();
    }
    engine.shutdown();
    Ok(())
}

/// Returns the persisted superblock disk id, or `None` for an unformatted disk.
fn persisted_disk_id(spec: &NodeSpec, cluster_id: u128) -> Result<Option<DiskId>, NodeError> {
    match Disk::load(&spec.disk, cluster_id) {
        Ok(disk) => Ok(Some(disk.disk_id())),
        Err(DiskError::Unregistered(_)) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Connects the PD control-plane client when `pd` endpoints are configured.
fn connect_pd(config: &ClusterConfig) -> Result<Option<PdClient>, NodeError> {
    if config.pd.is_empty() {
        return Ok(None);
    }
    PdClient::connect(&config.pd)
        .map(Some)
        .map_err(|e| NodeError::Pd(format!("connect: {e}")))
}

/// Registration with bounded retry: a dev cluster starts every process at
/// once, so PD may not be listening (or leading) when this node comes up.
/// Retries every second for up to a minute before failing.
async fn register_with_retry(
    client: &PdClient,
    spec: &NodeSpec,
    addr: &str,
    persisted: Option<DiskId>,
) -> Result<(NodeId, DiskId), NodeError> {
    let (total, _free, _used) = space_stats(&spec.disk);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let result = async {
            let cluster_node_id = client
                .register_node(
                    addr.to_string(),
                    spec.az.clone(),
                    spec.rack.clone(),
                    ROLE_DATA,
                )
                .await
                .map_err(|e| NodeError::Pd(format!("register node: {e}")))?;
            let assigned = client
                .register_disk(
                    cluster_node_id,
                    spec.az.clone(),
                    spec.rack.clone(),
                    spec.disk.to_string_lossy().to_string(),
                    total,
                )
                .await
                .map_err(|e| NodeError::Pd(format!("register disk: {e}")))?;
            if let Some(persisted) = persisted
                && persisted != assigned
            {
                return Err(NodeError::DiskIdMismatch {
                    persisted: persisted.get(),
                    assigned: assigned.get(),
                });
            }
            Ok((cluster_node_id, assigned))
        }
        .await;
        match result {
            Ok(ids) => return Ok(ids),
            // A persisted/assigned identity conflict is a real error — retrying
            // cannot fix it.
            Err(err @ NodeError::DiskIdMismatch { .. }) => return Err(err),
            Err(err) if attempt >= 60 => return Err(err),
            Err(err) => {
                tracing::debug!(attempt, error = %err, "PD registration failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Opens the disk at `spec.disk`, auto-formatting it with `disk_id` if
/// unregistered (M3 convenience retained; PD mode passes the PD-assigned id).
fn open_engine(
    spec: &NodeSpec,
    cluster_id: u128,
    extent_size: u64,
    disk_id: DiskId,
    qos: epoch_store::QosConfig,
) -> Result<StorageEngine, NodeError> {
    match Disk::load(&spec.disk, cluster_id) {
        Ok(_) => {}
        Err(DiskError::Unregistered(_)) => {
            Disk::format(
                &spec.disk,
                Superblock {
                    disk_id,
                    cluster_id,
                    created_at: 0,
                    flags: 0,
                    extent_size,
                },
            )?;
            tracing::info!(disk = %spec.disk.display(), "formatted unregistered disk");
        }
        Err(err) => return Err(err.into()),
    }
    Ok(StorageEngine::open_with_qos(&spec.disk, cluster_id, qos)?)
}

/// Ensures each hosted shard has a writable extent, idempotently: only an
/// unbound shard is created, so a restart does not orphan a live extent.
async fn provision_shards(engine: &StorageEngine, shards: &[ShardId]) -> Result<(), NodeError> {
    for &shard in shards {
        match engine.writable_extent(shard).await {
            Ok(_) => {}
            Err(EpochError::ShardNotFound) => {
                engine.create_extent(shard).await?;
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

/// Builds a peer-reaching transport from the current topology and spawns the
/// RepairDisk coordinator daemon (M7 01 §6.1). The transport is a one-shot
/// topology snapshot (repair reads tolerate a stale peer — a missing read just
/// drops that survivor); a topology-refreshing transport is a follow-up.
async fn spawn_repair_coordinator_for(
    client: PdClient,
    engine: &StorageEngine,
    node_id: NodeId,
    disk_id: DiskId,
) -> crate::roles::repair::RepairCoordinatorHandle {
    let topology = epoch_client::Topology::new(client.clone(), HEARTBEAT_INTERVAL);
    let _ = topology.refresh().await;
    let transport: Arc<dyn epoch_rpc::ShardTransport> = Arc::new(epoch_rpc::RemoteTransport::new(
        topology.serving_endpoints(),
    ));
    crate::roles::repair::spawn_repair_coordinator(
        client,
        transport,
        engine.clone(),
        node_id,
        disk_id,
    )
}

/// Spawns the heartbeat loop: a full-replacement capacity report every
/// [`HEARTBEAT_INTERVAL`] (01 §7). Failures are logged, not fatal — the next
/// tick retries and PD's liveness sweep handles the gap.
fn spawn_heartbeat(
    client: PdClient,
    engine: StorageEngine,
    node_id: NodeId,
    disk_id: DiskId,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            ticker.tick().await;
            let (_total, free, used) = space_stats(engine.disk().root());
            let disks = vec![pdpb::DiskStats {
                disk_id: disk_id.get(),
                free,
                used,
                writable_extents: engine
                    .writable_extent_count()
                    .try_into()
                    .unwrap_or(u32::MAX),
                broken: engine.is_broken(),
            }];
            if let Err(err) = client.heartbeat(node_id, disks).await {
                tracing::warn!(node = node_id.get(), error = %err, "heartbeat failed");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::ChunkId;

    use crate::config::{ChunkSpec, CodeSpec, MetaSpec, SchedulerSpec, WriterSpec};

    const CLUSTER: u128 = 0x00c0_ffee;
    const EXTENT_SIZE: u64 = 8 * 1024 * 1024;

    fn node_spec(dir: &std::path::Path) -> NodeSpec {
        NodeSpec {
            id: 0,
            addr: "127.0.0.1:0".to_string(),
            meta_addr: None,
            disk: dir.to_path_buf(),
            az: "az1".to_string(),
            rack: "r1".to_string(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_engine_formats_then_provisions_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let spec = node_spec(dir.path());
        let engine = open_engine(
            &spec,
            CLUSTER,
            EXTENT_SIZE,
            DiskId::new(1),
            epoch_store::QosConfig::default(),
        )
        .unwrap();

        let shards = [
            ShardId::new(ChunkId::new(1), 0, 0),
            ShardId::new(ChunkId::new(1), 1, 0),
        ];
        provision_shards(&engine, &shards).await.unwrap();

        // Both shards now resolve to a writable extent.
        let mut before = Vec::new();
        for &s in &shards {
            before.push(engine.writable_extent(s).await.unwrap());
        }

        // Re-provisioning is a no-op: the bindings are unchanged (no orphan).
        provision_shards(&engine, &shards).await.unwrap();
        let mut after = Vec::new();
        for &s in &shards {
            after.push(engine.writable_extent(s).await.unwrap());
        }
        assert_eq!(before, after, "re-provision must not rebind shards");

        engine.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_binds_serves_and_shuts_down() {
        let dir = tempfile::tempdir().unwrap();
        let config = ClusterConfig {
            cluster_id: "00c0ffee".to_string(),
            extent_size: EXTENT_SIZE,
            pd: Vec::new(),
            pdnodes: Vec::new(),
            nodes: vec![node_spec(dir.path())],
            code: CodeSpec {
                data: 1,
                parity: 1,
                stripe_size: 1024,
                blob_size: 1024,
                write_quorum: None,
            },
            writer: WriterSpec { token: 1 },
            chunks: vec![ChunkSpec {
                id: 1,
                shards: vec![0, 0],
            }],
            meta: MetaSpec::default(),
            scheduler: SchedulerSpec::default(),
            qos: crate::config::QosSpec::default(),
            gc: crate::config::GcSpec::default(),
            maintenance: Default::default(),
        };
        // An already-resolved shutdown makes serve() return immediately after
        // binding, exercising the whole assemble → serve → teardown path.
        run(&config, NodeId::new(0), std::future::ready(()))
            .await
            .unwrap();
    }
}
