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

//! M4 acceptance: a dev cluster (PD + DataNodes, in-process) converges from
//! registration and heartbeats to PD-driven chunk creation, and the gateway
//! writes through it — including sealing a chunk and watching the write
//! rewrite on a fresh one (07 §M4).
//!
//! Timing model: heartbeat 5s + placement sweep 5s + liveness sweep 5s, so
//! convergence is on the order of tens of seconds; every wait below polls with
//! a generous deadline rather than assuming a fixed delay.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_client::{ChunkMap, PdClient, Topology, WritableSet, WriterSession};
use epoch_gateway::{CodeMode, Gateway};
use epoch_node::roles;
use epoch_node::{
    ChunkSpec, ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec,
    WriterSpec,
};
use epoch_proto::{ChunkId, CodeModeId, NodeId};
use epoch_rpc::RemoteTransport;
use tempfile::TempDir;
use tokio::task::JoinHandle;

const CLUSTER: &str = "00c0ffee";
const EXTENT_SIZE: u64 = 8 * 1024 * 1024;
const CODE_MODE_ID: u16 = 1;
const STRIPE: usize = 4096;
const BLOB: usize = 64 * 1024;

fn code_mode() -> CodeMode {
    CodeMode::new(2, 1, STRIPE, BLOB).expect("code mode")
}

fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// A running dev cluster: role tasks and the PD endpoints.
struct DevCluster {
    tasks: Vec<JoinHandle<()>>,
    pd_endpoints: Vec<String>,
    _dirs: Vec<TempDir>,
}

impl DevCluster {
    /// Starts `pd_count` PD replicas and `data_count` data nodes, all with
    /// auto-assigned loopback ports and temp dirs.
    async fn start(pd_count: u64, data_count: u32) -> Self {
        let _ =
            epoch_telemetry::logging::init(epoch_telemetry::logging::LogFormat::Pretty, "debug");
        let mut dirs = Vec::new();
        let mut pdnodes = Vec::new();
        let mut pd_endpoints = Vec::new();
        for id in 1..=pd_count {
            let addr = free_port().to_string();
            let dir = tempfile::tempdir().expect("pd dir");
            pd_endpoints.push(addr.clone());
            pdnodes.push(PdSpec {
                id,
                addr,
                dir: dir.path().to_path_buf(),
            });
            dirs.push(dir);
        }
        let mut nodes = Vec::new();
        for id in 1..=data_count {
            let addr = free_port().to_string();
            let dir = tempfile::tempdir().expect("data dir");
            nodes.push(NodeSpec {
                id,
                addr,
                meta_addr: None,
                disk: dir.path().to_path_buf(),
                az: "az1".to_string(),
                rack: "r1".to_string(),
            });
            dirs.push(dir);
        }

        let config = ClusterConfig {
            cluster_id: CLUSTER.to_string(),
            extent_size: EXTENT_SIZE,
            pd: pd_endpoints.clone(),
            pdnodes,
            nodes,
            code: CodeSpec {
                data: 2,
                parity: 1,
                stripe_size: STRIPE,
                blob_size: BLOB,
                write_quorum: None,
            },
            writer: WriterSpec { token: 1 },
            chunks: Vec::<ChunkSpec>::new(),
            meta: MetaSpec::default(),
            scheduler: SchedulerSpec::default(),
            qos: QosSpec::default(),
            gc: GcSpec::default(),
            maintenance: Default::default(),
        };

        let mut tasks = Vec::new();
        for spec in &config.pdnodes {
            let config = config.clone();
            let id = spec.id;
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::pd::run(&config, id, std::future::pending()).await {
                    tracing::error!(replica = id, error = %err, "pd role exited");
                }
            }));
        }
        for spec in &config.nodes {
            let config = config.clone();
            let id = NodeId::new(spec.id);
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::data::run(&config, id, std::future::pending()).await {
                    tracing::error!(node = id.get(), error = %err, "data role exited");
                }
            }));
        }

        let cluster = Self {
            tasks,
            pd_endpoints,
            _dirs: dirs,
        };
        // Let the replicas bind and the bootstrap initialize before clients arrive.
        tokio::time::sleep(Duration::from_millis(500)).await;
        cluster
    }

    fn pd_client(&self) -> PdClient {
        PdClient::connect(&self.pd_endpoints).expect("pd client")
    }

    /// A gateway against this cluster (fresh writer session, remote transport).
    async fn gateway(&self) -> Gateway<PdClient> {
        let pd = self.pd_client();
        let topology = Topology::new(pd.clone(), Duration::from_secs(1));
        topology.refresh().await.expect("topology refresh");
        let endpoints = topology.serving_endpoints();
        let chunk_map = ChunkMap::new(pd.clone(), topology);
        let writable = WritableSet::new(
            pd.clone(),
            chunk_map,
            CodeModeId::new(CODE_MODE_ID),
            Duration::from_secs(1),
        );
        let gw_node = pd
            .register_node("127.0.0.1:9", "az1", "r1", 2)
            .await
            .expect("register gateway node");
        let session = Arc::new(
            WriterSession::establish(pd, gw_node, 60_000, now_millis())
                .await
                .expect("writer session"),
        );
        // The heartbeat driver keeps the session's stale gate open (01 §4.3);
        // the handle rides with the gateway (aborted when dropped).
        let heartbeat = session.spawn_heartbeat(Duration::from_secs(5));
        std::mem::forget(heartbeat); // test-scoped: lives for the cluster's life
        let transport: Arc<dyn epoch_rpc::ShardTransport> =
            Arc::new(RemoteTransport::new(endpoints));
        Gateway::new(code_mode(), gw_node, writable, session, transport)
    }

    /// Waits until at least `min` chunks are published as writable.
    async fn wait_chunks(&self, min: usize) {
        let client = self.pd_client();
        wait_for("writable chunks", Duration::from_secs(45), || {
            let client = client.clone();
            async move {
                client
                    .get_writable_chunks(CodeModeId::new(CODE_MODE_ID))
                    .await
                    .map(|chunks| chunks.len() >= min)
                    .unwrap_or(false)
            }
        })
        .await;
    }

    fn stop(self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Acceptance A (07 §M4): a fresh dev cluster converges to PD-driven chunk
/// creation — nodes register, heartbeats land, and the placement loop fills
/// every disk's writable watermark, with shards spread across distinct nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_cluster_converges_to_pd_driven_chunk_creation() {
    let cluster = DevCluster::start(1, 3).await;
    cluster.wait_chunks(1).await;

    let client = cluster.pd_client();
    let chunks = client
        .get_writable_chunks(CodeModeId::new(CODE_MODE_ID))
        .await
        .expect("get writable chunks");
    assert!(!chunks.is_empty(), "no chunks were created");

    let chunk = client
        .get_chunk(ChunkId::new(chunks[0].chunk_id))
        .await
        .expect("get chunk");
    assert_eq!(chunk.shards.len(), 3, "EC2+1 chunk must have 3 shards");
    let disks: HashSet<u32> = chunk.shards.iter().map(|s| s.disk_id).collect();
    assert_eq!(
        disks.len(),
        chunk.shards.len(),
        "host-aware placement must spread shards across distinct disks"
    );

    cluster.stop();
}

/// Acceptance C (07 §M4 seal): a sealed chunk is evicted from the gateway's
/// writable set, and the next blob is written to a different chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_chunk_is_evicted_and_blob_rewrites_elsewhere() {
    let cluster = DevCluster::start(1, 3).await;
    cluster.wait_chunks(2).await;

    let gateway = cluster.gateway().await;
    let first = gateway
        .put_object(b"epochio-m4-seal-test")
        .await
        .expect("first put");
    let sealed_id = first.blobs[0].chunk.chunk_id;

    cluster
        .pd_client()
        .seal_chunk(sealed_id)
        .await
        .expect("seal chunk");

    // The next PUT must succeed by picking a different chunk (error-driven
    // eviction + rewrite, 04 §3.3).
    let second = gateway
        .put_object(b"epochio-m4-after-seal")
        .await
        .expect("put after seal");
    assert_ne!(
        second.blobs[0].chunk.chunk_id, sealed_id,
        "the rewritten blob must land on a different chunk"
    );

    cluster.stop();
}

/// Acceptance B (07 §M4 failover): with three PD replicas, killing the leader
/// fails the cluster over to a new leader and gateway writes keep flowing.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn gateway_writes_recover_after_pd_leader_kill() {
    let cluster = DevCluster::start(3, 3).await;
    cluster.wait_chunks(1).await;

    // Kill the bootstrap replica (id 1 — the likely first leader), then wait
    // for the survivors to elect a new one (election timeout is sub-second to
    // a few seconds; poll until a call succeeds again).
    cluster.tasks[0].abort();
    wait_for("new pd leader elected", Duration::from_secs(15), || {
        let client = cluster.pd_client();
        async move {
            client
                .get_writable_chunks(CodeModeId::new(CODE_MODE_ID))
                .await
                .is_ok()
        }
    })
    .await;

    let gateway = cluster.gateway().await;
    let layout = gateway
        .put_object(b"epochio-m4-failover")
        .await
        .expect("put after leader kill");

    // The object is readable end to end (layout replays through the new leader's
    // published topology as well).
    let back = gateway.get_object(&layout).await.expect("get back");
    assert_eq!(back.bytes, b"epochio-m4-failover");

    cluster.stop();
}

/// Bucket + config KV end to end over a live dev cluster: a created bucket is
/// listed with its policy, and a config entry round-trips through the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_and_config_round_trip_over_dev_cluster() {
    let cluster = DevCluster::start(1, 1).await;
    let client = cluster.pd_client();

    let bucket_id = client
        .create_bucket(
            "datasets",
            epoch_proto::grpc::pd::NsMode::Flat,
            None,
            CODE_MODE_ID,
            epoch_proto::grpc::pd::MetaEngine::Rocks,
        )
        .await
        .expect("create bucket");
    // Idempotent by name: a retried creation reuses the id.
    let again = client
        .create_bucket(
            "datasets",
            epoch_proto::grpc::pd::NsMode::Flat,
            None,
            CODE_MODE_ID,
            epoch_proto::grpc::pd::MetaEngine::Rocks,
        )
        .await
        .expect("idempotent create");
    assert_eq!(bucket_id, again);

    let buckets = client.list_buckets().await.expect("list buckets");
    assert!(
        buckets
            .iter()
            .any(|b| b.bucket_id == bucket_id.get() && b.name == "datasets"),
        "created bucket must be listed: {buckets:?}"
    );

    client
        .put_config("placement.writable_target", b"8".to_vec())
        .await
        .expect("put config");
    let value = client
        .get_config("placement.writable_target")
        .await
        .expect("get config");
    assert_eq!(value, Some(b"8".to_vec()));
    assert_eq!(
        client.get_config("missing.key").await.expect("get missing"),
        None
    );

    cluster.stop();
}
