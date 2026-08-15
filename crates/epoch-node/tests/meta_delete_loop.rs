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

//! M5a 删除闭环验收 (07 §M5a): PutObject → DeleteObject → MetaNode 的
//! 分区 deleter 在安全延迟（测试置 0）后直驱 `DeleteBlob` RPC → DataNode
//! tombstone 成功 → delq 攒批出队归零（03 §8 正确性: 持久队列 +
//! at-least-once + tombstone 幂等 = 不漏删）。
//!
//! Timing model: PD placement 收敛（heartbeat 5s + sweep 5s）是长极，全部
//! 等待用宽松轮询而非固定 sleep。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use epoch_node::roles;
use epoch_node::{
    ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec, WriterSpec,
};
use epoch_proto::grpc::meta::meta_node_client::MetaNodeClient;
use epoch_proto::grpc::meta::{
    DeleteObjectRequest, DelqBacklogRequest, PutObjectRequest, SliceRef,
};
use epoch_proto::grpc::pd::pd_control_client::PdControlClient;
use epoch_proto::grpc::pd::{
    CreateBucketRequest, CreatePartitionRequest, GetRouteRequest, GetWritableChunksRequest,
    ListNodesRequest, NsMode,
};
use epoch_proto::{BlobId, ChunkId, NodeId, ShardId, WriterToken};
use epoch_rpc::{EndReq, OpenReq, RemoteTransport, ShardTransport};
use tempfile::TempDir;
use tokio::task::JoinHandle;

const CLUSTER: &str = "00c0ffee";
const BLOB_SIZE: u32 = 64 * 1024;

fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// A running delete-loop cluster: PD + 3 DataNodes + 3 MetaNodes.
struct Cluster {
    tasks: Vec<JoinHandle<()>>,
    pd_addr: String,
    _dirs: Vec<TempDir>,
}

impl Cluster {
    async fn start() -> Self {
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
                blob_size: BLOB_SIZE as usize,
                write_quorum: None,
            },
            writer: WriterSpec { token: 1 },
            chunks: Vec::new(),
            meta: MetaSpec {
                delete_safety_delay_secs: 0,
                delete_sweep_interval_secs: 1,
            },
            scheduler: SchedulerSpec::default(),
            qos: QosSpec::default(),
            gc: GcSpec::default(),
            maintenance: Default::default(),
        };
        dirs.push(pd_dir);

        let mut data_specs = Vec::new();
        let mut meta_specs = Vec::new();
        for id in 1..=3u32 {
            let data_dir = tempfile::tempdir().expect("data dir");
            data_specs.push(NodeSpec {
                id,
                addr: free_port().to_string(),
                meta_addr: None,
                disk: data_dir.path().to_path_buf(),
                az: "az1".to_string(),
                rack: "r1".to_string(),
            });
            dirs.push(data_dir);
        }
        for id in 11..=13u32 {
            let addr = free_port().to_string();
            let dir = tempfile::tempdir().expect("meta dir");
            meta_specs.push(NodeSpec {
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
        config.nodes = [data_specs.clone(), meta_specs].concat();

        let mut tasks = Vec::new();
        let pd_config = config.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(err) = roles::pd::run(&pd_config, 1, std::future::pending()).await {
                tracing::error!(error = %err, "pd role exited");
            }
        }));
        for spec in &data_specs {
            let data_config = config.clone();
            let id = NodeId::new(spec.id);
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::data::run(&data_config, id, std::future::pending()).await {
                    tracing::error!(node = id.get(), error = %err, "data role exited");
                }
            }));
        }
        for spec in &config.nodes[3..] {
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
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Writes one small blob to the first shard of a writable chunk, returning
/// the `(chunk_id, blob_id)` the metadata will reference.
async fn write_test_blob(pd: &PdControlClient<tonic::transport::Channel>) -> (u32, BlobId) {
    let chunks = pd
        .clone()
        .get_writable_chunks(GetWritableChunksRequest { code_mode_id: 1 })
        .await
        .expect("writable chunks")
        .into_inner()
        .chunks;
    let chunk = chunks.first().expect("a writable chunk").clone();
    let shard = chunk.shards.first().expect("shard 0").clone();

    // Resolve the shard's node address via the topology listing.
    let listing = pd
        .clone()
        .list_nodes(ListNodesRequest {})
        .await
        .expect("list nodes")
        .into_inner();
    let node_id = listing
        .disks
        .iter()
        .find(|d| d.disk_id == shard.disk_id)
        .expect("disk known")
        .node_id;
    let addr = listing
        .nodes
        .iter()
        .find(|n| n.node_id == node_id)
        .expect("node known")
        .addr
        .parse()
        .expect("node addr");

    let transport = RemoteTransport::new(HashMap::from([(NodeId::new(node_id), addr)]));
    let blob_id = BlobId::new(WriterToken::new(1), 1);
    let shard_id =
        ShardId::try_new(ChunkId::new(chunk.chunk_id), 0, shard.epoch).expect("shard id");
    let body = vec![0x5Au8; 4096];
    let mut stream = transport
        .open_write(NodeId::new(node_id), OpenReq { blob_id, shard_id })
        .await
        .expect("open write");
    use bytes::Bytes;
    stream
        .send_frame(Bytes::from(body.clone()))
        .await
        .expect("send frame");
    stream
        .finish(EndReq {
            frame_count: 1,
            blob_crc: crc32c::crc32c(&body),
        })
        .await
        .expect("finish");
    (chunk.chunk_id, blob_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delete_loop_closes_end_to_end() {
    let _ = epoch_telemetry::logging::init(epoch_telemetry::logging::LogFormat::Pretty, "debug");
    let cluster = Cluster::start().await;

    // Placement converges: one writable EC 2+1 chunk across the DataNodes.
    wait_for("writable chunk", Duration::from_secs(60), || {
        let mut pd = cluster.pd_client();
        async move {
            pd.get_writable_chunks(GetWritableChunksRequest { code_mode_id: 1 })
                .await
                .map(|resp| !resp.into_inner().chunks.is_empty())
                .unwrap_or(false)
        }
    })
    .await;

    // A bucket and a full-range flat partition with a converged leader.
    let mut pd = cluster.pd_client();
    let bucket_id = pd
        .create_bucket(CreateBucketRequest {
            name: "delete-loop".to_string(),
            ns_mode: NsMode::Flat as i32,
            inline_threshold: 0,
            codemode_id: 1,
            engine: 1, // META_ENGINE_ROCKS
        })
        .await
        .expect("create bucket")
        .into_inner()
        .bucket_id;
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
    wait_for("partition leader", Duration::from_secs(45), || {
        let mut pd = cluster.pd_client();
        let partition_id = partition.partition_id;
        async move {
            pd.get_route(GetRouteRequest {
                bucket_id,
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
            bucket_id,
            ns: NsMode::Flat as i32,
            routing_key: b"a".to_vec(),
        })
        .await
        .expect("get route")
        .into_inner()
        .partition
        .expect("partition view")
        .leader_addr;
    let mut leader = cluster.meta_client(&leader_addr);

    // Write real data, then commit its metadata through the object path.
    let (chunk_id, blob_id) = write_test_blob(&pd).await;
    let put = leader
        .put_object(PutObjectRequest {
            bucket_id,
            key: b"ckpt/model.bin".to_vec(),
            size: 4096,
            etag: vec![3u8; 16],
            ts_millis: 1_700_000_000_000,
            inline_data: Vec::new(),
            slices: vec![SliceRef {
                chunk_id,
                blob_ids: vec![blob_id.as_u64()],
                blob_size: BLOB_SIZE,
            }],
            ..Default::default()
        })
        .await
        .expect("put object")
        .into_inner();
    assert!(put.error.is_none(), "put failed: {:?}", put.error);

    // Delete the object: the partition's deleter must now drive the
    // tombstone through the DataNode sink and dequeue the entry (safety
    // delay 0, sweep 1s in this cluster's meta spec).
    let deleted = leader
        .delete_object(DeleteObjectRequest {
            bucket_id,
            key: b"ckpt/model.bin".to_vec(),
            ts_millis: 1_700_000_000_001,
        })
        .await
        .expect("delete object")
        .into_inner();
    assert!(
        deleted.error.is_none(),
        "delete failed: {:?}",
        deleted.error
    );

    // The loop closes: the queue drains to zero — which only happens after
    // the DataNode sink has tombstoned every referenced blob (03 §8).
    wait_for("delq drained", Duration::from_secs(30), || {
        let mut client = cluster.meta_client(&leader_addr);
        let partition_id = partition.partition_id;
        async move {
            client
                .delq_backlog(DelqBacklogRequest { partition_id })
                .await
                .map(|resp| {
                    let inner = resp.into_inner();
                    inner.hosted && inner.entries == 0
                })
                .unwrap_or(false)
        }
    })
    .await;

    cluster.stop();
}
