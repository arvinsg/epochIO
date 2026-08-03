//! M7 partition-scheduler acceptance (01 §5): a dev cluster of one PD and
//! three MetaNodes drives an *automatic* partition split — no test proposes the
//! split; the PD scheduler observes the partition's reported size cross the
//! configured threshold and drives the whole flow (SuggestSplitPoint on the
//! MetaNode leader → PD route narrow + child insert → ApplySplit in the parent
//! group's log → split reconcile derives the child group). The route table
//! ending with two partitions where it began with one is proof the production
//! driver — not a test — did the work.
//!
//! Timing: partition heartbeat 5s (carries the size signal) + scheduler sweep +
//! split reconcile 5s, so convergence is tens of seconds; every wait polls with
//! a generous deadline.

use std::time::{Duration, Instant};

use epoch_node::roles;
use epoch_node::{
    ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec, WriterSpec,
};
use epoch_proto::NodeId;
use epoch_proto::grpc::meta::meta_node_client::MetaNodeClient;
use epoch_proto::grpc::meta::{PutObjectRequest, SliceRef};
use epoch_proto::grpc::pd::pd_control_client::PdControlClient;
use epoch_proto::grpc::pd::{
    CreatePartitionRequest, GetRouteRequest, ListNodesRequest, ListPartitionsRequest, NsMode,
};
use tempfile::TempDir;
use tokio::task::JoinHandle;

const CLUSTER: &str = "00c0ffee";
const BUCKET: u64 = 1;
/// A deliberately tiny split threshold so a handful of object writes crosses it.
const SPLIT_THRESHOLD: u64 = 64 * 1024;

fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// A running metadata dev cluster with the partition scheduler active.
struct SchedCluster {
    tasks: Vec<JoinHandle<()>>,
    pd_addr: String,
    _dirs: Vec<TempDir>,
}

impl SchedCluster {
    async fn start(meta_count: u32) -> Self {
        let mut dirs = Vec::new();
        let pd_addr = free_port().to_string();
        let pd_dir = tempfile::tempdir().expect("pd dir");
        let mut config = ClusterConfig {
            cluster_id: CLUSTER.to_string(),
            extent_size: 8 * 1024 * 1024,
            pd: vec![pd_addr.clone()],
            pdnodes: vec![PdSpec {
                id: 1,
                addr: pd_addr.clone(),
                dir: pd_dir.path().to_path_buf(),
            }],
            nodes: Vec::new(),
            code: CodeSpec {
                data: 2,
                parity: 1,
                stripe_size: 4096,
                blob_size: 64 * 1024,
            },
            writer: WriterSpec { token: 1 },
            chunks: Vec::new(),
            meta: MetaSpec::default(),
            scheduler: SchedulerSpec {
                // Fast sweep + tiny threshold so the test converges quickly.
                interval_secs: 2,
                split_threshold_bytes: SPLIT_THRESHOLD,
                ..SchedulerSpec::default()
            },
            qos: QosSpec::default(),
            gc: GcSpec::default(),
            maintenance: Default::default(),
        };
        dirs.push(pd_dir);

        for id in 1..=meta_count {
            let addr = free_port().to_string();
            let dir = tempfile::tempdir().expect("meta dir");
            config.nodes.push(NodeSpec {
                id,
                addr: addr.clone(),
                meta_addr: Some(addr),
                disk: dir.path().to_path_buf(),
                az: "az1".to_string(),
                rack: "r1".to_string(),
            });
            dirs.push(dir);
        }

        let mut tasks = Vec::new();
        let pd_config = config.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(err) = roles::pd::run(&pd_config, 1, std::future::pending()).await {
                tracing::error!(error = %err, "pd role exited");
            }
        }));
        for spec in &config.nodes {
            let meta_config = config.clone();
            let id = NodeId::new(spec.id);
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::meta::run(&meta_config, id, std::future::pending()).await {
                    tracing::error!(node = id.get(), error = %err, "meta role exited");
                }
            }));
        }

        let cluster = Self {
            tasks,
            pd_addr,
            _dirs: dirs,
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        cluster
    }

    fn pd_client(&self) -> PdControlClient<tonic::transport::Channel> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", self.pd_addr))
            .expect("endpoint")
            .connect_lazy();
        PdControlClient::new(channel)
    }

    fn meta_client(&self, addr: &str) -> MetaNodeClient<tonic::transport::Channel> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect_lazy();
        MetaNodeClient::new(channel)
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
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn scheduler_auto_splits_an_oversized_partition() {
    let cluster = SchedCluster::start(3).await;

    // MetaNodes become Live.
    wait_for("meta nodes live", Duration::from_secs(45), || {
        let mut pd = cluster.pd_client();
        async move {
            pd.list_nodes(ListNodesRequest {})
                .await
                .map(|resp| {
                    let nodes = &resp.get_ref().nodes;
                    nodes.len() == 3 && nodes.iter().all(|n| n.roles & 4 != 0 && n.status == 2)
                })
                .unwrap_or(false)
        }
    })
    .await;

    // One full-range flat partition to start.
    let mut pd = cluster.pd_client();
    pd.create_partition(CreatePartitionRequest {
        ns: NsMode::Flat as i32,
        start_bucket: 0,
        start_key: Vec::new(),
        start_unbounded: true,
        end_bucket: 0,
        end_key: Vec::new(),
        end_unbounded: true,
    })
    .await
    .expect("create partition");

    // Wait for its leader to converge in the route table.
    wait_for("partition leader", Duration::from_secs(45), || {
        let mut pd = cluster.pd_client();
        async move {
            pd.get_route(GetRouteRequest {
                bucket_id: BUCKET,
                ns: NsMode::Flat as i32,
                routing_key: b"a".to_vec(),
            })
            .await
            .map(|resp| {
                resp.into_inner()
                    .partition
                    .is_some_and(|v| !v.leader_addr.is_empty())
            })
            .unwrap_or(false)
        }
    })
    .await;
    let leader_addr = pd
        .get_route(GetRouteRequest {
            bucket_id: BUCKET,
            ns: NsMode::Flat as i32,
            routing_key: b"a".to_vec(),
        })
        .await
        .expect("route")
        .into_inner()
        .partition
        .expect("partition")
        .leader_addr;

    // Write several distinct objects, each large enough that a couple cross the
    // tiny split threshold. Distinct keys give the median picker a real interior
    // boundary to choose.
    let mut leader = cluster.meta_client(&leader_addr);
    for i in 0..8u32 {
        let put = leader
            .put_object(PutObjectRequest {
                bucket_id: BUCKET,
                key: format!("obj/{i:04}").into_bytes(),
                size: 32 * 1024,
                etag: vec![i as u8; 16],
                ts_millis: 1_700_000_000_000,
                inline_data: Vec::new(),
                slices: vec![SliceRef {
                    chunk_id: 1,
                    blob_ids: vec![u64::from(i) + 1],
                    blob_size: 32 * 1024,
                }],
                ..Default::default()
            })
            .await
            .expect("put")
            .into_inner();
        assert!(put.error.is_none(), "put {i} failed: {:?}", put.error);
    }

    // The scheduler observes total_bytes > threshold (via the 5s partition
    // heartbeat), asks the leader for a boundary, narrows the route + inserts a
    // child, and the split reconcile derives the child group. Assert the route
    // table grew from one flat partition to two.
    wait_for("partition auto-split", Duration::from_secs(60), || {
        let mut pd = cluster.pd_client();
        async move {
            pd.list_partitions(ListPartitionsRequest {})
                .await
                .map(|resp| {
                    resp.into_inner()
                        .partitions
                        .iter()
                        .filter(|p| p.ns == NsMode::Flat as i32)
                        .count()
                        >= 2
                })
                .unwrap_or(false)
        }
    })
    .await;

    // The two partitions must tile the key space contiguously: routes for keys
    // on either side of the boundary resolve, and to *different* partitions.
    let low = pd
        .get_route(GetRouteRequest {
            bucket_id: BUCKET,
            ns: NsMode::Flat as i32,
            routing_key: b"obj/0000".to_vec(),
        })
        .await
        .expect("route low")
        .into_inner()
        .partition
        .expect("low partition");
    let high = pd
        .get_route(GetRouteRequest {
            bucket_id: BUCKET,
            ns: NsMode::Flat as i32,
            routing_key: b"obj/9999".to_vec(),
        })
        .await
        .expect("route high")
        .into_inner()
        .partition
        .expect("high partition");
    assert_ne!(
        low.partition_id, high.partition_id,
        "the split boundary must separate the low and high keys into two partitions"
    );

    cluster.stop();
}
