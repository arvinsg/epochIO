//! M5a control + metadata path acceptance: a dev cluster of one PD and three
//! MetaNodes converges from registration to a PD-driven partition —
//! `CreatePartition` picks three META peers and pushes `CreateRaftGroup`, a
//! partition leader emerges, partition heartbeats converge the route table,
//! and flat object ops round-trip through the leader while a follower
//! redirect reports NOT_LEADER in-band (07 §M5a; 01 §5; 03 §5).
//!
//! Timing model: partition heartbeat 5s + PD liveness sweep 5s, so
//! convergence is on the order of tens of seconds; every wait below polls
//! with a generous deadline rather than assuming a fixed delay.

use std::time::{Duration, Instant};

use epoch_node::roles;
use epoch_node::{
    ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec, WriterSpec,
};
use epoch_proto::NodeId;
use epoch_proto::grpc::meta::meta_node_client::MetaNodeClient;
use epoch_proto::grpc::meta::{
    DeleteObjectRequest, HeadObjectRequest, ListObjectsRequest, PutObjectRequest, SliceRef,
};
use epoch_proto::grpc::pd::pd_control_client::PdControlClient;
use epoch_proto::grpc::pd::{CreatePartitionRequest, GetRouteRequest, ListNodesRequest, NsMode};
use tempfile::TempDir;
use tokio::task::JoinHandle;

const CLUSTER: &str = "00c0ffee";
const BUCKET: u64 = 1;

fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// A running metadata dev cluster: PD + MetaNode role tasks and endpoints.
struct MetaCluster {
    tasks: Vec<JoinHandle<()>>,
    pd_addr: String,
    meta_addrs: Vec<String>,
    _dirs: Vec<TempDir>,
}

impl MetaCluster {
    /// Starts one PD and `meta_count` MetaNodes, all on auto-assigned
    /// loopback ports with temp dirs.
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
            scheduler: SchedulerSpec::default(),
            qos: QosSpec::default(),
            gc: GcSpec::default(),
            maintenance: Default::default(),
        };
        dirs.push(pd_dir);

        let mut meta_addrs = Vec::new();
        for id in 1..=meta_count {
            let addr = free_port().to_string();
            let dir = tempfile::tempdir().expect("meta dir");
            meta_addrs.push(addr.clone());
            config.nodes.push(NodeSpec {
                id,
                addr: addr.clone(),
                // Meta-only node: bind the reserved port directly.
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
            meta_addrs,
            _dirs: dirs,
        };
        // Let the replicas bind and register before clients arrive.
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
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn partition_lifecycle_and_object_ops_round_trip() {
    let cluster = MetaCluster::start(3).await;

    // MetaNodes become Live (partition heartbeats drive their liveness).
    wait_for("meta nodes live", Duration::from_secs(45), || {
        let mut pd = cluster.pd_client();
        async move {
            pd.list_nodes(ListNodesRequest {})
                .await
                .map(|resp| {
                    let nodes = &resp.get_ref().nodes;
                    nodes.len() == 3
                        && nodes.iter().all(
                            |n| n.roles & 4 != 0 && n.status == 2, /* NODE_STATUS_LIVE */
                        )
                })
                .unwrap_or(false)
        }
    })
    .await;

    // PD creates a full-range flat partition and pushes CreateRaftGroup.
    let mut pd = cluster.pd_client();
    let partition = pd
        .create_partition(CreatePartitionRequest {
            ns: NsMode::Flat as i32,
            start_bucket: 0,
            start_key: Vec::new(),
            start_unbounded: true,
            end_bucket: 0,
            end_key: Vec::new(),
            end_unbounded: true,
        })
        .await
        .expect("create partition")
        .into_inner()
        .partition
        .expect("partition view");
    assert_eq!(partition.peers.len(), 3);

    // A partition leader emerges and the heartbeat converges the route table.
    wait_for("partition leader", Duration::from_secs(45), || {
        let mut pd = cluster.pd_client();
        let partition_id = partition.partition_id;
        async move {
            pd.get_route(GetRouteRequest {
                bucket_id: BUCKET,
                ns: NsMode::Flat as i32,
                routing_key: b"a".to_vec(),
            })
            .await
            .map(|resp| {
                resp.into_inner().partition.is_some_and(|view| {
                    view.partition_id == partition_id && !view.leader_addr.is_empty()
                })
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
        .expect("get route")
        .into_inner()
        .partition
        .expect("partition view")
        .leader_addr;

    // Object ops round-trip through the leader.
    let mut leader = cluster.meta_client(&leader_addr);
    let put = leader
        .put_object(PutObjectRequest {
            bucket_id: BUCKET,
            key: b"dataset/ckpt-1".to_vec(),
            size: 96 * 1024,
            etag: vec![7u8; 16],
            ts_millis: 1_700_000_000_000,
            inline_data: Vec::new(),
            slices: vec![SliceRef {
                chunk_id: 42,
                blob_ids: vec![11, 12, 13],
                blob_size: 32 << 20,
            }],
            ..Default::default()
        })
        .await
        .expect("put")
        .into_inner();
    assert!(put.error.is_none(), "put failed: {:?}", put.error);

    let head = leader
        .head_object(HeadObjectRequest {
            bucket_id: BUCKET,
            key: b"dataset/ckpt-1".to_vec(),
        })
        .await
        .expect("head")
        .into_inner();
    assert!(head.found, "head must find the object");
    let view = head.head.expect("head view");
    assert_eq!(view.size, 96 * 1024);
    assert_eq!(view.embedded_slices, 1);
    assert_eq!(view.seg_count, 0);

    let list = leader
        .list_objects(ListObjectsRequest {
            bucket_id: BUCKET,
            prefix: b"dataset/".to_vec(),
            start_after: Vec::new(),
            limit: 10,
        })
        .await
        .expect("list")
        .into_inner();
    assert_eq!(list.entries.len(), 1);
    assert_eq!(list.entries[0].key, b"dataset/ckpt-1".to_vec());

    // A follower redirect comes back in-band as NOT_LEADER.
    let follower_addr = cluster
        .meta_addrs
        .iter()
        .find(|addr| **addr != leader_addr)
        .expect("a follower exists");
    let mut follower = cluster.meta_client(follower_addr);
    let redirected = follower
        .put_object(PutObjectRequest {
            bucket_id: BUCKET,
            key: b"dataset/ckpt-2".to_vec(),
            size: 1,
            etag: vec![0u8; 16],
            ts_millis: 1_700_000_000_001,
            inline_data: b"x".to_vec(),
            slices: Vec::new(),
            ..Default::default()
        })
        .await
        .expect("put on follower")
        .into_inner();
    let error = redirected.error.expect("in-band route error");
    assert_eq!(
        error.kind,
        epoch_proto::grpc::meta::route_error::Kind::NotLeader as i32,
        "follower must report NOT_LEADER, got {error:?}"
    );

    // Delete flows, and the head is then gone.
    let deleted = leader
        .delete_object(DeleteObjectRequest {
            bucket_id: BUCKET,
            key: b"dataset/ckpt-1".to_vec(),
            ts_millis: 1_700_000_000_002,
        })
        .await
        .expect("delete")
        .into_inner();
    assert!(
        deleted.error.is_none(),
        "delete failed: {:?}",
        deleted.error
    );
    let head = leader
        .head_object(HeadObjectRequest {
            bucket_id: BUCKET,
            key: b"dataset/ckpt-1".to_vec(),
        })
        .await
        .expect("head after delete")
        .into_inner();
    assert!(!head.found, "deleted object must not be found");

    cluster.stop();
}
