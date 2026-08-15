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

//! The `meta` role: run one MetaNode — register with PD (META role), recover
//! the multi-raft runtime, serve the MetaNode gRPC (partition admin + flat
//! namespace ops) and the batched raft transport on one port, and report
//! partition heartbeats (01 §5, 03 §8, M5a).
//!
//! Groups are not created here: PD drives `CreateRaftGroup` onto the chosen
//! nodes (01 §5 分区创建), and a restart re-opens whatever the registry
//! persisted (03 §8 kill -9 恢复). The role only wires the runtime together.
//!
//! draft/design/07-iteration-plan.md (M5)

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use epoch_client::PdClient;
use epoch_meta::partition::PartitionRegistry;
use epoch_meta::raft::GroupManager;
use epoch_meta::raft::network::MetaRaftTransportService;
use epoch_meta::ref_extractor::EpochRefExtractor;
use epoch_meta::service::MetaNodeService;
use epoch_meta::store::rocks::RocksEngine;
use epoch_meta::ticker;
use epoch_proto::NodeId;

use crate::config::{ClusterConfig, NodeSpec};
use crate::error::NodeError;

/// The `RoleSet::META` bit announced at PD registration (design 01 §3).
const ROLE_META: u8 = 4;

/// Runs the `meta` role for `node_id` until `shutdown` resolves, then stops
/// every group core and closes the engines.
///
/// # Errors
///
/// [`NodeError`] if the node id is not in the config, PD registration fails,
/// the stores cannot be opened, group recovery fails, or the listen socket
/// cannot be bound.
pub async fn run(
    config: &ClusterConfig,
    node_id: NodeId,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), NodeError> {
    let spec = config
        .node(node_id)
        .ok_or(NodeError::UnknownNode(node_id.get()))?;
    let addr = spec.meta_socket_addr()?;

    // 1. PD registration (bounded retry — a dev cluster starts everything at
    //    once). The cluster node id doubles as the raft node id of every
    //    partition group hosted here (03 §8 node identity).
    let pd = connect_pd(config)?;
    let cluster_node_id = match &pd {
        Some(client) => register_with_retry(client, spec, &addr.to_string()).await?,
        None => node_id,
    };
    let raft_node_id = u64::from(cluster_node_id.get());

    // 2. Open the multi-raft runtime over the shared engines and recover
    //    every persisted group (03 §8: data and delq resume from the
    //    persisted apply position).
    let engine = Arc::new(
        RocksEngine::open(&spec.disk.join("sm"))
            .map_err(|e| NodeError::Pd(format!("open meta engine: {e}")))?,
    );
    let manager = Arc::new(
        GroupManager::open(
            &spec.disk.join("raft-log"),
            raft_node_id,
            engine,
            Arc::new(EpochRefExtractor),
            openraft::Config {
                cluster_name: config.cluster_id.clone(),
                ..Default::default()
            },
        )
        .map_err(|e| NodeError::Pd(format!("open group manager: {e}")))?,
    );
    manager
        .recover()
        .await
        .map_err(|e| NodeError::Pd(format!("recover groups: {e}")))?;
    let registry = Arc::new(PartitionRegistry::default());

    // 3. gRPC: MetaNode service + batched raft transport on one port.
    let meta_service = MetaNodeService::new(
        cluster_node_id.get(),
        Arc::clone(&manager),
        Arc::clone(&registry),
    )
    .into_server();
    let raft_service = MetaRaftTransportService::new(Arc::clone(&manager)).into_server();

    // 4. Partition heartbeat loop (PD mode only): leader reports + the hosted
    //    set PD's CreateRaftGroup reconciliation consumes (01 §5).
    let mut background: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if let Some(pd_addr) = config.pd.first() {
        background.push(ticker::spawn_partition_heartbeat(
            Arc::clone(&manager),
            cluster_node_id.get(),
            pd_addr.clone(),
            ticker::DEFAULT_HEARTBEAT_INTERVAL,
        ));
        // Split reconcile (M7, 03 §2/§8): derive the child group of any
        // committed split. Without this a PD-proposed split narrows the parent
        // but never materializes the child. Idempotent + crash-safe, so a plain
        // periodic tick suffices.
        background.push(ticker::spawn_split_reconcile(
            Arc::clone(&manager),
            ticker::DEFAULT_HEARTBEAT_INTERVAL,
        ));
    }

    // 5. Delete queue drive (PD mode only, 03 §8): tombstones flow through
    //    the injected DataNode sink (03 §12.2 — the sink is assembled here,
    //    not inside epoch-meta).
    if let Some(client) = &pd {
        let topology = Arc::new(epoch_client::Topology::new(
            client.clone(),
            crate::sink::SINK_TOPOLOGY_REFRESH,
        ));
        let _ = topology.refresh().await;
        let transport = Arc::new(epoch_rpc::RemoteTransport::new(
            topology.serving_endpoints(),
        ));
        let sink = Arc::new(crate::sink::DataNodeDeleteSink::new(
            client.clone(),
            Arc::clone(&topology),
            transport,
        ));
        // Topology + transport refresh: on a cold start the serving set is
        // still converging, and membership changes later — both are picked up
        // here (the transport rebuild keeps its connection cache per node).
        {
            let sink = Arc::clone(&sink);
            background.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(crate::sink::SINK_TOPOLOGY_REFRESH);
                loop {
                    ticker.tick().await;
                    topology.refresh_if_stale().await;
                    sink.refresh_transport(topology.serving_endpoints());
                }
            }));
        }
        let deleter = Arc::new(epoch_meta::deleter::Deleter::with_safety_delay(
            sink,
            Duration::from_secs(config.meta.delete_safety_delay_secs),
        ));
        background.push(ticker::spawn_delete_sweep(
            Arc::clone(&manager),
            Arc::clone(&registry),
            deleter,
            Duration::from_secs(config.meta.delete_sweep_interval_secs),
        ));
        background.push(ticker::spawn_upload_ttl_sweep(
            Arc::clone(&manager),
            Arc::clone(&registry),
            epoch_meta::ns_flat::multipart::DEFAULT_UPLOAD_TTL,
            epoch_meta::ns_flat::multipart::DEFAULT_UPLOAD_TTL_SWEEP_INTERVAL,
        ));
        background.push(ticker::spawn_orphan_sentinel_sweep(
            Arc::clone(&manager),
            Arc::clone(&registry),
            epoch_meta::ns_hier::DEFAULT_ORPHAN_SENTINEL_TTL,
            epoch_meta::ns_hier::DEFAULT_ORPHAN_SWEEP_INTERVAL,
        ));
    }

    // Observability (08 §5.1): the MetaNode's apply/delq metrics live in this
    // process, so it serves its own endpoint.
    let _metrics =
        crate::metrics_server::spawn_metrics_server(crate::metrics_server::metrics_addr(addr));
    tracing::info!(node = node_id.get(), %addr, "meta node serving");
    tonic::transport::Server::builder()
        .add_service(meta_service)
        .add_service(raft_service)
        .serve_with_shutdown(addr, shutdown)
        .await
        .map_err(|e| NodeError::Pd(format!("grpc serve: {e}")))?;

    // Teardown: stop the background loops and every group core so the engine
    // locks release promptly for a same-dir restart.
    for handle in &background {
        handle.abort();
    }
    for group in manager.group_ids() {
        if let Some(raft) = manager.raft(group) {
            let _ = raft.shutdown().await;
        }
    }
    Ok(())
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

/// Registration with bounded retry (01 §7): a dev cluster starts every
/// process at once, so PD may not be listening (or leading) when this node
/// comes up. Retries every second for up to a minute before failing.
async fn register_with_retry(
    client: &PdClient,
    spec: &NodeSpec,
    meta_addr: &str,
) -> Result<NodeId, NodeError> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match client
            .register_node(
                meta_addr.to_string(),
                spec.az.clone(),
                spec.rack.clone(),
                ROLE_META,
            )
            .await
        {
            Ok(node_id) => return Ok(node_id),
            Err(err) if attempt >= 60 => {
                return Err(NodeError::Pd(format!("register node: {err}")));
            }
            Err(err) => {
                tracing::debug!(attempt, error = %err, "PD registration failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
