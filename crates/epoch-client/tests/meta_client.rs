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

//! MetaClient end-to-end over real gRPC: route resolution via a fake PD
//! `GetRoute`, a Put/Head round-trip against a fake MetaNode, NotLeader
//! redirect to the reported leader, and PartitionMoved re-resolution.
//!
//! Both servers are hand-rolled from the generated traits (epoch-proto), bound
//! to OS-assigned loopback ports — epoch-client must not depend on epoch-pd or
//! epoch-meta (that would invert L2 → L3), so the tests synthesize just enough
//! of each contract to drive the client. Every unused trait method returns
//! `unimplemented` (written out because `#[tonic::async_trait]` cannot expand
//! methods produced by a nested macro).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use epoch_client::{MetaClient, PdClient};
use epoch_proto::grpc::{meta, pd};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

const BUCKET: u64 = 1;
const PARTITION: u64 = 7;

/// How the fake MetaNode responds to object ops, to drive the redirect loop.
#[derive(Clone, Copy, PartialEq)]
enum MetaMode {
    /// Serves the op (leader).
    Leader,
    /// Returns NOT_LEADER pointing at `leader_addr`.
    NotLeader,
    /// Returns PARTITION_MOVED (the client must re-resolve via PD).
    Moved,
}

struct FakeMeta {
    mode: MetaMode,
    leader_addr: String,
    puts: Arc<AtomicU32>,
}

impl FakeMeta {
    fn route_error(&self) -> Option<meta::RouteError> {
        match self.mode {
            MetaMode::Leader => None,
            MetaMode::NotLeader => Some(meta::RouteError {
                kind: meta::route_error::Kind::NotLeader as i32,
                leader_addr: self.leader_addr.clone(),
                partition_id: PARTITION,
            }),
            MetaMode::Moved => Some(meta::RouteError {
                kind: meta::route_error::Kind::PartitionMoved as i32,
                leader_addr: String::new(),
                partition_id: PARTITION,
            }),
        }
    }
}

#[tonic::async_trait]
impl meta::meta_node_server::MetaNode for FakeMeta {
    async fn put_object(
        &self,
        _r: Request<meta::PutObjectRequest>,
    ) -> Result<Response<meta::PutObjectResponse>, Status> {
        if self.mode == MetaMode::Leader {
            self.puts.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Response::new(meta::PutObjectResponse {
            error: self.route_error(),
        }))
    }

    async fn head_object(
        &self,
        r: Request<meta::HeadObjectRequest>,
    ) -> Result<Response<meta::HeadObjectResponse>, Status> {
        let found = self.mode == MetaMode::Leader && !r.into_inner().key.is_empty();
        Ok(Response::new(meta::HeadObjectResponse {
            error: self.route_error(),
            found,
            head: found.then(|| meta::ObjectHeadView {
                size: 3,
                etag: vec![7; 16],
                mtime: 0,
                inline: true,
                embedded_slices: 0,
                seg_count: 0,
                ..Default::default()
            }),
        }))
    }

    async fn create_raft_group(
        &self,
        _r: Request<meta::CreateRaftGroupRequest>,
    ) -> Result<Response<meta::CreateRaftGroupResponse>, Status> {
        Err(Status::unimplemented("create_raft_group"))
    }
    async fn suggest_split_point(
        &self,
        _r: Request<meta::SuggestSplitPointRequest>,
    ) -> Result<Response<meta::SuggestSplitPointResponse>, Status> {
        Err(Status::unimplemented("suggest_split_point"))
    }
    async fn apply_split(
        &self,
        _r: Request<meta::ApplySplitRequest>,
    ) -> Result<Response<meta::ApplySplitResponse>, Status> {
        Err(Status::unimplemented("apply_split"))
    }
    async fn prepare_migrate_target(
        &self,
        _r: Request<meta::PrepareMigrateTargetRequest>,
    ) -> Result<Response<meta::PrepareMigrateTargetResponse>, Status> {
        Err(Status::unimplemented("prepare_migrate_target"))
    }
    async fn migrate_group_member(
        &self,
        _r: Request<meta::MigrateGroupMemberRequest>,
    ) -> Result<Response<meta::MigrateGroupMemberResponse>, Status> {
        Err(Status::unimplemented("migrate_group_member"))
    }
    async fn get_object_meta(
        &self,
        _r: Request<meta::GetObjectMetaRequest>,
    ) -> Result<Response<meta::GetObjectMetaResponse>, Status> {
        Err(Status::unimplemented("get_object_meta"))
    }
    async fn delete_object(
        &self,
        _r: Request<meta::DeleteObjectRequest>,
    ) -> Result<Response<meta::DeleteObjectResponse>, Status> {
        Err(Status::unimplemented("delete_object"))
    }
    async fn list_objects(
        &self,
        _r: Request<meta::ListObjectsRequest>,
    ) -> Result<Response<meta::ListObjectsResponse>, Status> {
        Err(Status::unimplemented("list_objects"))
    }
    async fn create_multipart_upload(
        &self,
        _r: Request<meta::CreateMultipartUploadRequest>,
    ) -> Result<Response<meta::CreateMultipartUploadResponse>, Status> {
        Err(Status::unimplemented("create_multipart_upload"))
    }
    async fn put_part(
        &self,
        _r: Request<meta::PutPartRequest>,
    ) -> Result<Response<meta::PutPartResponse>, Status> {
        Err(Status::unimplemented("put_part"))
    }
    async fn complete_multipart(
        &self,
        _r: Request<meta::CompleteMultipartRequest>,
    ) -> Result<Response<meta::CompleteMultipartResponse>, Status> {
        Err(Status::unimplemented("complete_multipart"))
    }
    async fn abort_multipart_upload(
        &self,
        _r: Request<meta::AbortMultipartUploadRequest>,
    ) -> Result<Response<meta::AbortMultipartUploadResponse>, Status> {
        Err(Status::unimplemented("abort_multipart_upload"))
    }
    async fn delq_backlog(
        &self,
        _r: Request<meta::DelqBacklogRequest>,
    ) -> Result<Response<meta::DelqBacklogResponse>, Status> {
        Err(Status::unimplemented("delq_backlog"))
    }
    async fn hier_lookup(
        &self,
        _r: Request<meta::HierLookupRequest>,
    ) -> Result<Response<meta::HierLookupResponse>, Status> {
        Err(Status::unimplemented("hier_lookup"))
    }
    async fn hier_write(
        &self,
        _r: Request<meta::HierWriteRequest>,
    ) -> Result<Response<meta::HierWriteResponse>, Status> {
        Err(Status::unimplemented("hier_write"))
    }
    async fn hier_unlink(
        &self,
        _r: Request<meta::HierUnlinkRequest>,
    ) -> Result<Response<meta::HierUnlinkResponse>, Status> {
        Err(Status::unimplemented("hier_unlink"))
    }
    async fn hier_mkdir_sentinel(
        &self,
        _r: Request<meta::HierMkdirSentinelRequest>,
    ) -> Result<Response<meta::HierMkdirSentinelResponse>, Status> {
        Err(Status::unimplemented("hier_mkdir_sentinel"))
    }
    async fn hier_mkdir_link(
        &self,
        _r: Request<meta::HierMkdirLinkRequest>,
    ) -> Result<Response<meta::HierMkdirLinkResponse>, Status> {
        Err(Status::unimplemented("hier_mkdir_link"))
    }
    async fn hier_readdir(
        &self,
        _r: Request<meta::HierReaddirRequest>,
    ) -> Result<Response<meta::HierReaddirResponse>, Status> {
        Err(Status::unimplemented("hier_readdir"))
    }
    async fn export_references(
        &self,
        _r: Request<meta::ExportReferencesRequest>,
    ) -> Result<Response<meta::ExportReferencesResponse>, Status> {
        Err(Status::unimplemented("export_references"))
    }
}

struct FakePd {
    /// The MetaNode leader address GetRoute points at.
    meta_addr: String,
    /// GetRoute call count (to assert re-resolution on PartitionMoved).
    routes: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl pd::pd_control_server::PdControl for FakePd {
    async fn get_route(
        &self,
        _r: Request<pd::GetRouteRequest>,
    ) -> Result<Response<pd::GetRouteResponse>, Status> {
        self.routes.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(pd::GetRouteResponse {
            partition: Some(pd::MetaPartitionView {
                partition_id: PARTITION,
                ns: pd::NsMode::Flat as i32,
                start_bucket: 0,
                start_key: Vec::new(),
                start_unbounded: true,
                end_bucket: 0,
                end_key: Vec::new(),
                end_unbounded: true,
                peers: vec![1],
                leader_node_id: 1,
                leader_addr: self.meta_addr.clone(),
                epoch: 1,
            }),
        }))
    }

    async fn heartbeat(
        &self,
        _r: Request<pd::HeartbeatRequest>,
    ) -> Result<Response<pd::HeartbeatResponse>, Status> {
        Err(Status::unimplemented("heartbeat"))
    }
    async fn get_writable_chunks(
        &self,
        _r: Request<pd::GetWritableChunksRequest>,
    ) -> Result<Response<pd::GetWritableChunksResponse>, Status> {
        Err(Status::unimplemented("get_writable_chunks"))
    }
    async fn get_chunk(
        &self,
        _r: Request<pd::GetChunkRequest>,
    ) -> Result<Response<pd::GetChunkResponse>, Status> {
        Err(Status::unimplemented("get_chunk"))
    }
    async fn list_disk_shards(
        &self,
        _r: Request<pd::ListDiskShardsRequest>,
    ) -> Result<Response<pd::ListDiskShardsResponse>, Status> {
        Err(Status::unimplemented("list_disk_shards"))
    }
    async fn list_nodes(
        &self,
        _r: Request<pd::ListNodesRequest>,
    ) -> Result<Response<pd::ListNodesResponse>, Status> {
        Err(Status::unimplemented("list_nodes"))
    }
    async fn register_writer(
        &self,
        _r: Request<pd::RegisterWriterRequest>,
    ) -> Result<Response<pd::RegisterWriterResponse>, Status> {
        Err(Status::unimplemented("register_writer"))
    }
    async fn writer_heartbeat(
        &self,
        _r: Request<pd::WriterHeartbeatRequest>,
    ) -> Result<Response<pd::WriterHeartbeatResponse>, Status> {
        Err(Status::unimplemented("writer_heartbeat"))
    }
    async fn register_node(
        &self,
        _r: Request<pd::RegisterNodeRequest>,
    ) -> Result<Response<pd::RegisterNodeResponse>, Status> {
        Err(Status::unimplemented("register_node"))
    }
    async fn register_disk(
        &self,
        _r: Request<pd::RegisterDiskRequest>,
    ) -> Result<Response<pd::RegisterDiskResponse>, Status> {
        Err(Status::unimplemented("register_disk"))
    }
    async fn seal_chunk(
        &self,
        _r: Request<pd::SealChunkRequest>,
    ) -> Result<Response<pd::SealChunkResponse>, Status> {
        Err(Status::unimplemented("seal_chunk"))
    }
    async fn create_bucket(
        &self,
        _r: Request<pd::CreateBucketRequest>,
    ) -> Result<Response<pd::CreateBucketResponse>, Status> {
        Err(Status::unimplemented("create_bucket"))
    }
    async fn list_buckets(
        &self,
        _r: Request<pd::ListBucketsRequest>,
    ) -> Result<Response<pd::ListBucketsResponse>, Status> {
        Err(Status::unimplemented("list_buckets"))
    }
    async fn put_config(
        &self,
        _r: Request<pd::PutConfigRequest>,
    ) -> Result<Response<pd::PutConfigResponse>, Status> {
        Err(Status::unimplemented("put_config"))
    }
    async fn get_config(
        &self,
        _r: Request<pd::GetConfigRequest>,
    ) -> Result<Response<pd::GetConfigResponse>, Status> {
        Err(Status::unimplemented("get_config"))
    }
    async fn create_partition(
        &self,
        _r: Request<pd::CreatePartitionRequest>,
    ) -> Result<Response<pd::CreatePartitionResponse>, Status> {
        Err(Status::unimplemented("create_partition"))
    }
    async fn list_partitions(
        &self,
        _r: Request<pd::ListPartitionsRequest>,
    ) -> Result<Response<pd::ListPartitionsResponse>, Status> {
        Err(Status::unimplemented("list_partitions"))
    }
    async fn partition_heartbeat(
        &self,
        _r: Request<pd::PartitionHeartbeatRequest>,
    ) -> Result<Response<pd::PartitionHeartbeatResponse>, Status> {
        Err(Status::unimplemented("partition_heartbeat"))
    }
    async fn list_node_jobs(
        &self,
        _r: Request<pd::ListNodeJobsRequest>,
    ) -> Result<Response<pd::ListNodeJobsResponse>, Status> {
        Err(Status::unimplemented("list_node_jobs"))
    }
    async fn commit_job_progress(
        &self,
        _r: Request<pd::CommitJobProgressRequest>,
    ) -> Result<Response<pd::CommitJobProgressResponse>, Status> {
        Err(Status::unimplemented("commit_job_progress"))
    }
    async fn commit_shard_mapping(
        &self,
        _r: Request<pd::CommitShardMappingRequest>,
    ) -> Result<Response<pd::CommitShardMappingResponse>, Status> {
        Err(Status::unimplemented("commit_shard_mapping"))
    }
    async fn list_chunks(
        &self,
        _r: Request<pd::ListChunksRequest>,
    ) -> Result<Response<pd::ListChunksResponse>, Status> {
        Err(Status::unimplemented("list_chunks"))
    }
    async fn mark_disk_draining(
        &self,
        _r: Request<pd::MarkDiskDrainingRequest>,
    ) -> Result<Response<pd::MarkDiskDrainingResponse>, Status> {
        Err(Status::unimplemented("mark_disk_draining"))
    }
    async fn get_live_writers(
        &self,
        _r: Request<pd::GetLiveWritersRequest>,
    ) -> Result<Response<pd::GetLiveWritersResponse>, Status> {
        Err(Status::unimplemented("get_live_writers"))
    }
    async fn report_shard_repair(
        &self,
        _r: Request<pd::ReportShardRepairRequest>,
    ) -> Result<Response<pd::ReportShardRepairResponse>, Status> {
        Err(Status::unimplemented("report_shard_repair"))
    }
    async fn list_node_shard_repairs(
        &self,
        _r: Request<pd::ListNodeShardRepairsRequest>,
    ) -> Result<Response<pd::ListNodeShardRepairsResponse>, Status> {
        Err(Status::unimplemented("list_node_shard_repairs"))
    }
    async fn commit_shard_repair(
        &self,
        _r: Request<pd::CommitShardRepairRequest>,
    ) -> Result<Response<pd::CommitShardRepairResponse>, Status> {
        Err(Status::unimplemented("commit_shard_repair"))
    }
    async fn put_credential(
        &self,
        _r: Request<pd::PutCredentialRequest>,
    ) -> Result<Response<pd::PutCredentialResponse>, Status> {
        Err(Status::unimplemented("put_credential"))
    }
    async fn list_credentials(
        &self,
        _r: Request<pd::ListCredentialsRequest>,
    ) -> Result<Response<pd::ListCredentialsResponse>, Status> {
        Err(Status::unimplemented("list_credentials"))
    }
}

async fn serve_meta(meta: FakeMeta) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(meta::meta_node_server::MetaNodeServer::new(meta))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

async fn serve_pd(pd: FakePd) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(pd::pd_control_server::PdControlServer::new(pd))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_and_head_route_through_pd_to_the_leader() {
    let puts = Arc::new(AtomicU32::new(0));
    let (meta_addr, _mh) = serve_meta(FakeMeta {
        mode: MetaMode::Leader,
        leader_addr: String::new(),
        puts: puts.clone(),
    })
    .await;
    let routes = Arc::new(AtomicU32::new(0));
    let (pd_addr, _ph) = serve_pd(FakePd {
        meta_addr,
        routes: routes.clone(),
    })
    .await;

    let client = MetaClient::new(PdClient::connect(&[pd_addr]).expect("pd client"));

    client
        .put_object(epoch_client::PutObject {
            bucket: BUCKET,
            key: b"obj",
            size: 3,
            etag: [7; 16],
            inline_data: b"abc".to_vec(),
            slices: Vec::new(),
            ts_millis: 100,
            http: Default::default(),
        })
        .await
        .expect("put");
    assert_eq!(puts.load(Ordering::SeqCst), 1);

    let head = client.head_object(BUCKET, b"obj").await.expect("head");
    assert_eq!(head.expect("present").size, 3);

    // The route was resolved once and cached: the second op does not re-hit PD.
    assert_eq!(
        routes.load(Ordering::SeqCst),
        1,
        "route cached after first resolve"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn not_leader_redirects_to_the_reported_leader() {
    let puts = Arc::new(AtomicU32::new(0));
    let (leader_addr, _lh) = serve_meta(FakeMeta {
        mode: MetaMode::Leader,
        leader_addr: String::new(),
        puts: puts.clone(),
    })
    .await;
    let (follower_addr, _fh) = serve_meta(FakeMeta {
        mode: MetaMode::NotLeader,
        leader_addr: leader_addr.clone(),
        puts: Arc::new(AtomicU32::new(0)),
    })
    .await;
    // PD points the route at the follower; the client must redirect to leader.
    let (pd_addr, _ph) = serve_pd(FakePd {
        meta_addr: follower_addr,
        routes: Arc::new(AtomicU32::new(0)),
    })
    .await;

    let client = MetaClient::new(PdClient::connect(&[pd_addr]).expect("pd"));
    client
        .put_object(epoch_client::PutObject {
            bucket: BUCKET,
            key: b"k",
            size: 3,
            etag: [7; 16],
            inline_data: b"abc".to_vec(),
            slices: Vec::new(),
            ts_millis: 100,
            http: Default::default(),
        })
        .await
        .expect("put after redirect");
    assert_eq!(puts.load(Ordering::SeqCst), 1, "write landed on the leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_moved_re_resolves_via_pd() {
    let (meta_addr, _mh) = serve_meta(FakeMeta {
        mode: MetaMode::Moved,
        leader_addr: String::new(),
        puts: Arc::new(AtomicU32::new(0)),
    })
    .await;
    let routes = Arc::new(AtomicU32::new(0));
    let (pd_addr, _ph) = serve_pd(FakePd {
        meta_addr,
        routes: routes.clone(),
    })
    .await;

    let client = MetaClient::new(PdClient::connect(&[pd_addr]).expect("pd"));
    let err = client
        .head_object(BUCKET, b"k")
        .await
        .expect_err("moved must exhaust");
    assert!(
        matches!(err, epoch_client::ClientError::NoLeader { .. }),
        "got {err:?}"
    );
    // Each attempt re-resolved via PD (route evicted on every Moved).
    assert!(
        routes.load(Ordering::SeqCst) >= 2,
        "re-resolved on PartitionMoved"
    );
}
