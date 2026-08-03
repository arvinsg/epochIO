//! PD client end-to-end over real gRPC: leader discovery, redirect past a
//! non-leader replica, `NoLeader` exhaustion, terminal `NOT_FOUND`, and the
//! read projections.
//!
//! The server is a hand-rolled `PdControl` (the generated server trait from
//! epoch-proto) bound to an OS-assigned loopback port — epoch-client must not
//! depend on epoch-pd (that would invert the L2 → L3 layering), so the tests
//! synthesize just enough of the contract to drive the client.

use epoch_client::{ClientError, PdClient};
use epoch_proto::grpc::pd;
use epoch_proto::{ChunkId, CodeModeId, NodeId, WriterToken};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

const KNOWN_NODE: u32 = 1;
const UNKNOWN_NODE: u32 = 999;
const ISSUED_TOKEN: u32 = 42;
const KNOWN_CHUNK: u32 = 7;
const CODE_MODE_ID: u16 = 1;

/// Whether the fake replica acts as the raft leader or rejects every call the
/// way a follower does (`FAILED_PRECONDITION`).
#[derive(Clone, Copy)]
enum Mode {
    Leader,
    NotLeader,
}

struct FakePd {
    mode: Mode,
}

impl FakePd {
    /// Mirrors the real server's leader gate: a follower answers
    /// `FAILED_PRECONDITION` so the client redirects.
    fn leader_only(&self) -> Result<(), Status> {
        match self.mode {
            Mode::Leader => Ok(()),
            Mode::NotLeader => Err(Status::failed_precondition("pd node is not the leader")),
        }
    }
}

fn sample_chunk() -> pd::ChunkView {
    pd::ChunkView {
        chunk_id: KNOWN_CHUNK,
        code_mode: Some(pd::CodeMode {
            id: u32::from(CODE_MODE_ID),
            data: 4,
            parity: 2,
            stripe_size: 1 << 20,
            blob_size: 32 << 20,
        }),
        status: pd::ChunkStatus::Writable as i32,
        shards: Vec::new(),
    }
}

#[tonic::async_trait]
impl pd::pd_control_server::PdControl for FakePd {
    async fn heartbeat(
        &self,
        _request: Request<pd::HeartbeatRequest>,
    ) -> Result<Response<pd::HeartbeatResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::HeartbeatResponse {}))
    }

    async fn get_writable_chunks(
        &self,
        request: Request<pd::GetWritableChunksRequest>,
    ) -> Result<Response<pd::GetWritableChunksResponse>, Status> {
        self.leader_only()?;
        let chunks = if request.into_inner().code_mode_id == u32::from(CODE_MODE_ID) {
            vec![sample_chunk()]
        } else {
            Vec::new()
        };
        Ok(Response::new(pd::GetWritableChunksResponse { chunks }))
    }

    async fn get_chunk(
        &self,
        request: Request<pd::GetChunkRequest>,
    ) -> Result<Response<pd::GetChunkResponse>, Status> {
        self.leader_only()?;
        if request.into_inner().chunk_id == KNOWN_CHUNK {
            Ok(Response::new(pd::GetChunkResponse {
                chunk: Some(sample_chunk()),
            }))
        } else {
            Err(Status::not_found("chunk not found"))
        }
    }

    async fn list_disk_shards(
        &self,
        _request: Request<pd::ListDiskShardsRequest>,
    ) -> Result<Response<pd::ListDiskShardsResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::ListDiskShardsResponse {
            shards: Vec::new(),
        }))
    }

    async fn register_writer(
        &self,
        request: Request<pd::RegisterWriterRequest>,
    ) -> Result<Response<pd::RegisterWriterResponse>, Status> {
        self.leader_only()?;
        if request.into_inner().node_id == KNOWN_NODE {
            Ok(Response::new(pd::RegisterWriterResponse {
                writer_token: ISSUED_TOKEN,
            }))
        } else {
            Err(Status::not_found("gateway node is not registered"))
        }
    }

    async fn writer_heartbeat(
        &self,
        request: Request<pd::WriterHeartbeatRequest>,
    ) -> Result<Response<pd::WriterHeartbeatResponse>, Status> {
        self.leader_only()?;
        let req = request.into_inner();
        // Live only for the known node's issued token (mirrors PD: a Dead or
        // unknown token answers live=false, 01 §4.3).
        let live = req.node_id == KNOWN_NODE && req.writer_token == ISSUED_TOKEN;
        Ok(Response::new(pd::WriterHeartbeatResponse { live }))
    }

    async fn list_nodes(
        &self,
        _request: Request<pd::ListNodesRequest>,
    ) -> Result<Response<pd::ListNodesResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::ListNodesResponse {
            nodes: vec![pd::NodeInfo {
                node_id: KNOWN_NODE,
                addr: "127.0.0.1:1".to_string(),
                az: "az1".to_string(),
                rack: "r1".to_string(),
                roles: 1,
                status: pd::NodeStatus::Live as i32,
            }],
            disks: Vec::new(),
        }))
    }

    async fn register_node(
        &self,
        request: Request<pd::RegisterNodeRequest>,
    ) -> Result<Response<pd::RegisterNodeResponse>, Status> {
        self.leader_only()?;
        let _ = request.into_inner();
        Ok(Response::new(pd::RegisterNodeResponse {
            node_id: KNOWN_NODE,
        }))
    }

    async fn register_disk(
        &self,
        request: Request<pd::RegisterDiskRequest>,
    ) -> Result<Response<pd::RegisterDiskResponse>, Status> {
        self.leader_only()?;
        if request.into_inner().node_id == KNOWN_NODE {
            Ok(Response::new(pd::RegisterDiskResponse { disk_id: 1 }))
        } else {
            Err(Status::not_found("owning node is not registered"))
        }
    }

    async fn seal_chunk(
        &self,
        request: Request<pd::SealChunkRequest>,
    ) -> Result<Response<pd::SealChunkResponse>, Status> {
        self.leader_only()?;
        if request.into_inner().chunk_id == KNOWN_CHUNK {
            Ok(Response::new(pd::SealChunkResponse {}))
        } else {
            Err(Status::not_found("chunk not found"))
        }
    }

    async fn create_bucket(
        &self,
        request: Request<pd::CreateBucketRequest>,
    ) -> Result<Response<pd::CreateBucketResponse>, Status> {
        self.leader_only()?;
        let _ = request.into_inner();
        Ok(Response::new(pd::CreateBucketResponse { bucket_id: 1 }))
    }

    async fn list_buckets(
        &self,
        _request: Request<pd::ListBucketsRequest>,
    ) -> Result<Response<pd::ListBucketsResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::ListBucketsResponse {
            buckets: Vec::new(),
        }))
    }

    async fn put_config(
        &self,
        _request: Request<pd::PutConfigRequest>,
    ) -> Result<Response<pd::PutConfigResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::PutConfigResponse {}))
    }

    async fn get_config(
        &self,
        _request: Request<pd::GetConfigRequest>,
    ) -> Result<Response<pd::GetConfigResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::GetConfigResponse {
            found: false,
            value: Vec::new(),
        }))
    }

    async fn get_route(
        &self,
        _request: Request<pd::GetRouteRequest>,
    ) -> Result<Response<pd::GetRouteResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::GetRouteResponse { partition: None }))
    }

    async fn create_partition(
        &self,
        _request: Request<pd::CreatePartitionRequest>,
    ) -> Result<Response<pd::CreatePartitionResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::CreatePartitionResponse {
            partition: None,
        }))
    }

    async fn list_partitions(
        &self,
        _request: Request<pd::ListPartitionsRequest>,
    ) -> Result<Response<pd::ListPartitionsResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::ListPartitionsResponse {
            partitions: Vec::new(),
        }))
    }

    async fn partition_heartbeat(
        &self,
        _request: Request<pd::PartitionHeartbeatRequest>,
    ) -> Result<Response<pd::PartitionHeartbeatResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::PartitionHeartbeatResponse {}))
    }
    async fn list_node_jobs(
        &self,
        _request: Request<pd::ListNodeJobsRequest>,
    ) -> Result<Response<pd::ListNodeJobsResponse>, Status> {
        Err(Status::unimplemented("list_node_jobs"))
    }
    async fn commit_job_progress(
        &self,
        _request: Request<pd::CommitJobProgressRequest>,
    ) -> Result<Response<pd::CommitJobProgressResponse>, Status> {
        Err(Status::unimplemented("commit_job_progress"))
    }
    async fn commit_shard_mapping(
        &self,
        _request: Request<pd::CommitShardMappingRequest>,
    ) -> Result<Response<pd::CommitShardMappingResponse>, Status> {
        Err(Status::unimplemented("commit_shard_mapping"))
    }
    async fn list_chunks(
        &self,
        _request: Request<pd::ListChunksRequest>,
    ) -> Result<Response<pd::ListChunksResponse>, Status> {
        Err(Status::unimplemented("list_chunks"))
    }
    async fn mark_disk_draining(
        &self,
        _request: Request<pd::MarkDiskDrainingRequest>,
    ) -> Result<Response<pd::MarkDiskDrainingResponse>, Status> {
        Err(Status::unimplemented("mark_disk_draining"))
    }
    async fn get_live_writers(
        &self,
        _request: Request<pd::GetLiveWritersRequest>,
    ) -> Result<Response<pd::GetLiveWritersResponse>, Status> {
        Err(Status::unimplemented("get_live_writers"))
    }
    async fn report_shard_repair(
        &self,
        _request: Request<pd::ReportShardRepairRequest>,
    ) -> Result<Response<pd::ReportShardRepairResponse>, Status> {
        Err(Status::unimplemented("report_shard_repair"))
    }
    async fn list_node_shard_repairs(
        &self,
        _request: Request<pd::ListNodeShardRepairsRequest>,
    ) -> Result<Response<pd::ListNodeShardRepairsResponse>, Status> {
        Err(Status::unimplemented("list_node_shard_repairs"))
    }
    async fn commit_shard_repair(
        &self,
        _request: Request<pd::CommitShardRepairRequest>,
    ) -> Result<Response<pd::CommitShardRepairResponse>, Status> {
        Err(Status::unimplemented("commit_shard_repair"))
    }
    async fn put_credential(
        &self,
        _request: Request<pd::PutCredentialRequest>,
    ) -> Result<Response<pd::PutCredentialResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::PutCredentialResponse {}))
    }
    async fn list_credentials(
        &self,
        _request: Request<pd::ListCredentialsRequest>,
    ) -> Result<Response<pd::ListCredentialsResponse>, Status> {
        self.leader_only()?;
        Ok(Response::new(pd::ListCredentialsResponse {
            credentials: Vec::new(),
        }))
    }
}

/// Binds a fake replica on an OS-assigned loopback port (bound before serving,
/// so client connections are never refused) and returns its address + task.
async fn spawn(mode: Mode) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr").to_string();
    let service = pd::pd_control_server::PdControlServer::new(FakePd { mode });
    let handle = tokio::spawn(async move {
        let incoming = TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    });
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_writer_returns_the_issued_token() {
    let (addr, server) = spawn(Mode::Leader).await;
    let client = PdClient::connect(&[addr]).expect("connect");

    let token = client
        .register_writer(NodeId::new(KNOWN_NODE))
        .await
        .expect("register");
    assert_eq!(token, WriterToken::new(ISSUED_TOKEN));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirects_past_a_non_leader_to_the_leader() {
    let (follower, s1) = spawn(Mode::NotLeader).await;
    let (leader, s2) = spawn(Mode::Leader).await;
    let client = PdClient::connect(&[follower, leader]).expect("connect");

    // The first call must skip the follower (FAILED_PRECONDITION) and land on
    // the leader.
    let token = client
        .register_writer(NodeId::new(KNOWN_NODE))
        .await
        .expect("register via leader");
    assert_eq!(token, WriterToken::new(ISSUED_TOKEN));

    // The leader hint now points at the second endpoint; a follow-up read still
    // succeeds against the cached leader.
    let chunk = client
        .get_chunk(ChunkId::new(KNOWN_CHUNK))
        .await
        .expect("get chunk via cached leader");
    assert_eq!(chunk.chunk_id, KNOWN_CHUNK);

    s1.abort();
    s2.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_leader_when_every_replica_rejects() {
    let (a, s1) = spawn(Mode::NotLeader).await;
    let (b, s2) = spawn(Mode::NotLeader).await;
    let client = PdClient::connect(&[a, b]).expect("connect");

    let err = client
        .register_writer(NodeId::new(KNOWN_NODE))
        .await
        .expect_err("no leader");
    assert!(matches!(err, ClientError::NoLeader { attempts: 2, .. }));

    s1.abort();
    s2.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_node_is_a_terminal_not_found() {
    let (addr, server) = spawn(Mode::Leader).await;
    let client = PdClient::connect(&[addr]).expect("connect");

    let err = client
        .register_writer(NodeId::new(UNKNOWN_NODE))
        .await
        .expect_err("unknown node");
    assert!(matches!(err, ClientError::NotFound(_)));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_writable_chunks_and_reports_a_missing_chunk() {
    let (addr, server) = spawn(Mode::Leader).await;
    let client = PdClient::connect(&[addr]).expect("connect");

    let chunks = client
        .get_writable_chunks(CodeModeId::new(CODE_MODE_ID))
        .await
        .expect("writable chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].chunk_id, KNOWN_CHUNK);

    let missing = client
        .get_chunk(ChunkId::new(4242))
        .await
        .expect_err("missing chunk");
    assert!(matches!(missing, ClientError::NotFound(_)));

    server.abort();
}
