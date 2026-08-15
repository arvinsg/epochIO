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

//! Cross-partition LIST merging (03 §5 跨分区归并). A bucket split into two
//! partitions must list *all* its objects in key order — not just the partition
//! the scan started in. Drives two fake MetaNode partitions (each holding part
//! of the key space) behind a PD whose `GetRoute` splits the range at `m`, and
//! asserts the merged listing walks both.

use epoch_client::{MetaClient, PdClient};
use epoch_proto::grpc::meta::{self, meta_node_server::MetaNode};
use epoch_proto::grpc::pd::{self, pd_control_server::PdControl};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

const BUCKET: u64 = 1;
// The range boundary: partition 1 owns keys < SPLIT, partition 2 owns >= SPLIT.
const SPLIT: &[u8] = b"m";

/// A MetaNode partition serving a fixed set of keys from an in-memory table.
struct FakePartition {
    /// The keys this partition holds, ascending.
    keys: Vec<Vec<u8>>,
    /// This partition's exclusive end key (empty = unbounded), used to mimic the
    /// shared-CF scan leaking past the boundary (the client must truncate).
    leaks: bool,
}

#[tonic::async_trait]
impl MetaNode for FakePartition {
    async fn list_objects(
        &self,
        r: Request<meta::ListObjectsRequest>,
    ) -> Result<Response<meta::ListObjectsResponse>, Status> {
        let req = r.into_inner();
        let start_after = if req.start_after.is_empty() {
            req.prefix.clone()
        } else {
            req.start_after.clone()
        };
        // A shared-CF scan is prefix-bounded, not partition-bounded: return every
        // held key above start_after (and, when `leaks`, also the *next*
        // partition's keys, which a correct merger must cut off).
        let mut entries: Vec<meta::ObjectEntry> = self
            .keys
            .iter()
            .filter(|k| k.as_slice() > start_after.as_slice())
            .take(req.limit.max(1) as usize)
            .map(|k| meta::ObjectEntry {
                key: k.clone(),
                head: None,
            })
            .collect();
        let _ = self.leaks;
        Ok(Response::new(meta::ListObjectsResponse {
            error: None,
            entries: std::mem::take(&mut entries),
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
    async fn put_object(
        &self,
        _r: Request<meta::PutObjectRequest>,
    ) -> Result<Response<meta::PutObjectResponse>, Status> {
        Err(Status::unimplemented("put_object"))
    }
    async fn head_object(
        &self,
        _r: Request<meta::HeadObjectRequest>,
    ) -> Result<Response<meta::HeadObjectResponse>, Status> {
        Err(Status::unimplemented("head_object"))
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
    async fn hier_rename(
        &self,
        _r: Request<meta::HierRenameRequest>,
    ) -> Result<Response<meta::HierRenameResponse>, Status> {
        Err(Status::unimplemented("hier_rename"))
    }
    async fn hier_rmdir_unlink(
        &self,
        _r: Request<meta::HierRmdirUnlinkRequest>,
    ) -> Result<Response<meta::HierRmdirUnlinkResponse>, Status> {
        Err(Status::unimplemented("hier_rmdir_unlink"))
    }
    async fn hier_rmdir_sentinel(
        &self,
        _r: Request<meta::HierRmdirSentinelRequest>,
    ) -> Result<Response<meta::HierRmdirSentinelResponse>, Status> {
        Err(Status::unimplemented("hier_rmdir_sentinel"))
    }
    async fn export_references(
        &self,
        _r: Request<meta::ExportReferencesRequest>,
    ) -> Result<Response<meta::ExportReferencesResponse>, Status> {
        Err(Status::unimplemented("export_references"))
    }
    async fn delete_range(
        &self,
        _r: Request<meta::DeleteRangeRequest>,
    ) -> Result<Response<meta::DeleteRangeResponse>, Status> {
        Err(Status::unimplemented("delete_range"))
    }
}

/// A PD whose `GetRoute` splits the flat namespace at `SPLIT`.
struct FakePdSplit {
    left_addr: String,
    right_addr: String,
}

#[tonic::async_trait]
impl PdControl for FakePdSplit {
    async fn get_route(
        &self,
        r: Request<pd::GetRouteRequest>,
    ) -> Result<Response<pd::GetRouteResponse>, Status> {
        let req = r.into_inner();
        let left = req.routing_key.as_slice() < SPLIT;
        let view = if left {
            pd::MetaPartitionView {
                partition_id: 1,
                ns: pd::NsMode::Flat as i32,
                start_bucket: 0,
                start_key: Vec::new(),
                start_unbounded: true,
                end_bucket: BUCKET,
                end_key: SPLIT.to_vec(),
                end_unbounded: false,
                peers: vec![1],
                leader_node_id: 1,
                leader_addr: self.left_addr.clone(),
                epoch: 1,
            }
        } else {
            pd::MetaPartitionView {
                partition_id: 2,
                ns: pd::NsMode::Flat as i32,
                start_bucket: BUCKET,
                start_key: SPLIT.to_vec(),
                start_unbounded: false,
                end_bucket: 0,
                end_key: Vec::new(),
                end_unbounded: true,
                peers: vec![2],
                leader_node_id: 2,
                leader_addr: self.right_addr.clone(),
                epoch: 1,
            }
        };
        Ok(Response::new(pd::GetRouteResponse {
            partition: Some(view),
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
    async fn delete_bucket(
        &self,
        _r: Request<pd::DeleteBucketRequest>,
    ) -> Result<Response<pd::DeleteBucketResponse>, Status> {
        Err(Status::unimplemented("delete_bucket"))
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
}

async fn serve_partition(p: FakePartition) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(meta::meta_node_server::MetaNodeServer::new(p))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

async fn serve_pd_split(pd_: FakePdSplit) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(pd::pd_control_server::PdControlServer::new(pd_))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

fn keys(list: &[&str]) -> Vec<Vec<u8>> {
    list.iter().map(|k| k.as_bytes().to_vec()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_list_walks_both_partitions_in_key_order() {
    // Partition 1 holds keys below SPLIT, partition 2 the rest.
    let (left_addr, _l) = serve_partition(FakePartition {
        keys: keys(&["a", "c", "k"]),
        leaks: false,
    })
    .await;
    let (right_addr, _r) = serve_partition(FakePartition {
        keys: keys(&["n", "q", "z"]),
        leaks: false,
    })
    .await;
    let (pd_addr, _p) = serve_pd_split(FakePdSplit {
        left_addr,
        right_addr,
    })
    .await;

    let client = MetaClient::new(PdClient::connect(&[pd_addr]).expect("pd client"));

    let (rows, next) = client
        .list_objects_merged(BUCKET, b"", b"", 100)
        .await
        .expect("merged list");
    let got: Vec<String> = rows
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect();
    assert_eq!(
        got,
        vec!["a", "c", "k", "n", "q", "z"],
        "the merged listing must walk both partitions in key order"
    );
    assert!(next.is_none(), "a complete listing reports no resume token");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_list_resumes_across_the_partition_boundary() {
    let (left_addr, _l) = serve_partition(FakePartition {
        keys: keys(&["a", "c", "k"]),
        leaks: false,
    })
    .await;
    let (right_addr, _r) = serve_partition(FakePartition {
        keys: keys(&["n", "q", "z"]),
        leaks: false,
    })
    .await;
    let (pd_addr, _p) = serve_pd_split(FakePdSplit {
        left_addr,
        right_addr,
    })
    .await;
    let client = MetaClient::new(PdClient::connect(&[pd_addr]).expect("pd client"));

    // First page of 4: spans the boundary (3 from partition 1, 1 from 2).
    let (page1, token1) = client
        .list_objects_merged(BUCKET, b"", b"", 4)
        .await
        .expect("page 1");
    let got1: Vec<String> = page1
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect();
    assert_eq!(got1, vec!["a", "c", "k", "n"]);
    let token1 = token1.expect("a truncated listing must carry a resume token");

    // Second page resumes after the token and completes the listing.
    let (page2, token2) = client
        .list_objects_merged(BUCKET, b"", &token1, 4)
        .await
        .expect("page 2");
    let got2: Vec<String> = page2
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect();
    assert_eq!(got2, vec!["q", "z"]);
    assert!(token2.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_list_truncates_a_shared_cf_scan_at_the_partition_end() {
    // Partition 1's scan leaks partition 2's keys (shared CF, not partition-
    // bounded): the merger must truncate at the end key, or keys appear twice.
    let (left_addr, _l) = serve_partition(FakePartition {
        keys: keys(&["a", "c", "n", "q"]), // holds + leaks into partition 2's space
        leaks: true,
    })
    .await;
    let (right_addr, _r) = serve_partition(FakePartition {
        keys: keys(&["n", "q"]),
        leaks: false,
    })
    .await;
    let (pd_addr, _p) = serve_pd_split(FakePdSplit {
        left_addr,
        right_addr,
    })
    .await;
    let client = MetaClient::new(PdClient::connect(&[pd_addr]).expect("pd client"));

    let (rows, _next) = client
        .list_objects_merged(BUCKET, b"", b"", 100)
        .await
        .expect("merged list");
    let got: Vec<String> = rows
        .iter()
        .map(|e| String::from_utf8_lossy(&e.key).into_owned())
        .collect();
    assert_eq!(
        got,
        vec!["a", "c", "n", "q"],
        "each key must appear exactly once — the boundary truncation must cut the leak"
    );
}
