//! PD control-plane gRPC service: the server side of the `PdControl` contract
//! generated into `epoch_proto::grpc::pd` (design 01 §7).
//!
//! [`PdControlService`] wraps the shared [`Journal`] and answers the
//! Gateway/DataNode-facing calls:
//!
//! - `Heartbeat` records volatile per-disk statistics into the leader-only
//!   tracker (Q18); it is rejected on a follower so the client retargets.
//! - `GetWritableChunks` / `GetChunk` / `ListDiskShards` are reads served with
//!   **leader ReadIndex** semantics via [`Raft::ensure_linearizable`]: a
//!   non-leader (or a leader that cannot confirm leadership) returns
//!   `FAILED_PRECONDITION` so the client redirects. Extent operations
//!   (CreateExtent / SealExtent) are not here — they travel on the data-plane
//!   binary RPC.
//!
//! Multi-replica leadership and client redirect land with the raft gRPC network
//! in a later phase; here a single-node group is always its own leader once it
//! has initialized.
//!
//! Design: docs/design/01-pd.md §7
//!
//! [`Raft::ensure_linearizable`]: openraft::Raft::ensure_linearizable

use std::sync::Arc;

use epoch_proto::grpc::pd;
use epoch_proto::{ChunkId, CodeMode, CodeModeId, DiskId, NodeId};
use tonic::{Request, Response, Status};

use crate::chunk::model::{Chunk, ChunkStatus, ShardSlot};
use crate::chunk::writable_set::{DiskHealth, WritableThreshold, is_publishable};
use crate::cluster::{Clock, DiskHeartbeat, DiskStatus, HeartbeatReport, RejectReason, RoleSet};
use crate::error::PdError;
use crate::journal::{ApplyResult, Journal};

/// The PD control-plane gRPC service, backed by the shared journal.
///
/// Cheap to construct; holds an [`Arc`] to the same [`Journal`] the raft node,
/// liveness ticker, and (later) placement ticker share, plus the [`Clock`] used
/// to stamp heartbeat arrival on the leader and the [`WritableThreshold`] that
/// filters the published writable set.
pub struct PdControlService {
    journal: Arc<Journal>,
    clock: Arc<dyn Clock>,
    writable_threshold: WritableThreshold,
}

impl PdControlService {
    /// Builds the service over a shared journal and clock, with the default
    /// [`WritableThreshold`].
    #[must_use]
    pub fn new(journal: Arc<Journal>, clock: Arc<dyn Clock>) -> Self {
        Self {
            journal,
            clock,
            writable_threshold: WritableThreshold::default(),
        }
    }

    /// Overrides the writable-set free-space threshold (01 §4.2).
    #[must_use]
    pub fn with_writable_threshold(mut self, threshold: WritableThreshold) -> Self {
        self.writable_threshold = threshold;
        self
    }

    /// Wraps the service in the generated tonic server, ready to hand to
    /// `tonic::transport::Server::add_service`.
    #[must_use]
    pub fn into_server(self) -> pd::pd_control_server::PdControlServer<Self> {
        pd::pd_control_server::PdControlServer::new(self)
    }

    /// Confirms this node can serve a linearizable read (leader ReadIndex),
    /// mapping any failure to a redirect-worthy `FAILED_PRECONDITION` status.
    async fn ensure_read_leader(&self) -> Result<(), Status> {
        self.journal
            .raft()
            .ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(not_leader)
    }

    /// Whether this node is the current raft leader (cheap metrics read, no
    /// quorum round-trip). Used to gate the heartbeat intake.
    fn is_leader(&self) -> bool {
        self.journal.raft().metrics().borrow().state.is_leader()
    }

    /// Picks `count` live META-role nodes for a new partition's voter set,
    /// spreading across AZs first (01 §5: 选 3 个 MetaNode, AZ 感知).
    fn pick_meta_peers(&self, count: usize) -> Result<Vec<NodeId>, Status> {
        let nodes = self.journal.state().nodes().list();
        let mut metas: Vec<_> = nodes
            .iter()
            .filter(|n| {
                n.roles.contains(RoleSet::META) && n.status == crate::cluster::NodeStatus::Live
            })
            .collect();
        if metas.len() < count {
            return Err(Status::failed_precondition(format!(
                "need at least {count} live META nodes, have {}",
                metas.len()
            )));
        }
        metas.sort_by_key(|n| (n.az.clone(), n.node_id.get()));
        let mut picked: Vec<NodeId> = Vec::with_capacity(count);
        let mut seen_az = std::collections::BTreeSet::new();
        for node in &metas {
            if picked.len() == count {
                break;
            }
            if seen_az.insert(&node.az) {
                picked.push(node.node_id);
            }
        }
        for node in &metas {
            if picked.len() == count {
                break;
            }
            if !picked.contains(&node.node_id) {
                picked.push(node.node_id);
            }
        }
        Ok(picked)
    }

    /// Pushes one `CreateRaftGroup` to a peer MetaNode (best-effort, with
    /// retry inside a spawned task; partition heartbeats re-drive it until
    /// the group reports hosted, 01 §5).
    fn push_create_raft_group(&self, partition: &crate::meta_mgr::MetaPartition, peer: NodeId) {
        let partition_id = partition.partition_id;
        let Some(node) = self.journal.state().nodes().get(peer) else {
            tracing::warn!(
                partition = partition_id,
                node = peer.get(),
                "create_raft_group push: peer unknown"
            );
            return;
        };
        let request = create_raft_group_request(partition, self.journal.state());
        let addr = node.addr.clone();
        tokio::spawn(async move {
            for attempt in 1..=3u32 {
                let result = async {
                    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                        .map_err(|e| e.to_string())?
                        .timeout(std::time::Duration::from_secs(5))
                        .connect_lazy();
                    let mut client =
                        epoch_proto::grpc::meta::meta_node_client::MetaNodeClient::new(channel);
                    client
                        .create_raft_group(request.clone())
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<(), String>(())
                }
                .await;
                match result {
                    Ok(()) => return,
                    Err(err) => {
                        tracing::debug!(
                            partition = partition_id,
                            node = peer.get(),
                            attempt,
                            error = %err,
                            "create_raft_group push failed; retrying"
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
            tracing::warn!(
                partition = partition_id,
                node = peer.get(),
                "create_raft_group push exhausted retries; heartbeat reconciliation will re-drive it"
            );
        });
    }

    /// Projects a route record plus its leader report (leader memory) to the
    /// wire `MetaPartitionView`.
    fn partition_to_proto(
        &self,
        partition: &crate::meta_mgr::MetaPartition,
    ) -> pd::MetaPartitionView {
        let report = self
            .journal
            .state()
            .partitions()
            .leader(partition.partition_id);
        let (leader_node_id, leader_addr) = match report {
            Some(report) => {
                let addr = self
                    .journal
                    .state()
                    .nodes()
                    .get(report.leader)
                    .map(|n| n.addr)
                    .unwrap_or_default();
                (report.leader.get(), addr)
            }
            None => (0, String::new()),
        };
        pd::MetaPartitionView {
            partition_id: partition.partition_id,
            ns: ns_mode_to_proto(partition.ns) as i32,
            start_bucket: partition.start.bucket,
            start_key: partition.start.routing_key.clone(),
            start_unbounded: partition.start.unbounded,
            end_bucket: partition.end.bucket,
            end_key: partition.end.routing_key.clone(),
            end_unbounded: partition.end.unbounded,
            peers: partition.peers.iter().map(|n| n.get()).collect(),
            leader_node_id,
            leader_addr,
            epoch: partition.epoch,
        }
    }
}

#[tonic::async_trait]
impl pd::pd_control_server::PdControl for PdControlService {
    async fn heartbeat(
        &self,
        request: Request<pd::HeartbeatRequest>,
    ) -> Result<Response<pd::HeartbeatResponse>, Status> {
        // Heartbeats target the leader; its tracker is the only one that drives
        // the liveness sweep (Q18). A follower rejects so the client retargets.
        if !self.is_leader() {
            return Err(not_leader_now());
        }
        let report = decode_heartbeat(request.into_inner());
        self.journal
            .record_heartbeat(&report, self.clock.now_millis());
        Ok(Response::new(pd::HeartbeatResponse {}))
    }

    async fn get_writable_chunks(
        &self,
        request: Request<pd::GetWritableChunksRequest>,
    ) -> Result<Response<pd::GetWritableChunksResponse>, Status> {
        self.ensure_read_leader().await?;
        let code_mode_id = decode_code_mode_id(request.into_inner().code_mode_id)?;
        let state = self.journal.state();
        let disks = state.disks();
        // A shard's disk is publishable when it is Normal and its known free is
        // above the threshold; an un-reported disk is kept optimistically (01 §4.2).
        let health = |disk_id: DiskId| match disks.get(disk_id) {
            Some(disk) if disk.status == DiskStatus::Normal => DiskHealth::Usable {
                free: self.journal.disk_free(disk.node_id, disk_id),
            },
            Some(_) => DiskHealth::Failed,
            None => DiskHealth::Unknown,
        };
        let chunks = state
            .chunks()
            .writable_chunks(code_mode_id)
            .into_iter()
            .filter(|chunk| is_publishable(chunk, self.writable_threshold, &health))
            .map(|chunk| chunk_to_proto(&chunk))
            .collect();
        Ok(Response::new(pd::GetWritableChunksResponse { chunks }))
    }

    async fn get_chunk(
        &self,
        request: Request<pd::GetChunkRequest>,
    ) -> Result<Response<pd::GetChunkResponse>, Status> {
        self.ensure_read_leader().await?;
        let chunk_id = ChunkId::new(request.into_inner().chunk_id);
        let chunk = self
            .journal
            .state()
            .chunks()
            .get(chunk_id)
            .ok_or_else(|| Status::not_found(format!("chunk {} not found", chunk_id.get())))?;
        Ok(Response::new(pd::GetChunkResponse {
            chunk: Some(chunk_to_proto(&chunk)),
        }))
    }

    async fn list_disk_shards(
        &self,
        request: Request<pd::ListDiskShardsRequest>,
    ) -> Result<Response<pd::ListDiskShardsResponse>, Status> {
        self.ensure_read_leader().await?;
        let disk_id = DiskId::new(request.into_inner().disk_id);
        let shards = self
            .journal
            .state()
            .chunks()
            .shard_slots_on_disk(disk_id)
            .iter()
            .map(shard_slot_to_proto)
            .collect();
        Ok(Response::new(pd::ListDiskShardsResponse { shards }))
    }

    async fn list_chunks(
        &self,
        request: Request<pd::ListChunksRequest>,
    ) -> Result<Response<pd::ListChunksResponse>, Status> {
        self.ensure_read_leader().await?;
        let req = request.into_inner();
        let limit = usize::try_from(req.limit)
            .map_err(|_| Status::invalid_argument("limit exceeds usize"))?;
        let chunks = self
            .journal
            .state()
            .chunks()
            .list_chunks(ChunkId::new(req.after), limit)
            .iter()
            .map(chunk_to_proto)
            .collect();
        Ok(Response::new(pd::ListChunksResponse { chunks }))
    }

    async fn mark_disk_draining(
        &self,
        request: Request<pd::MarkDiskDrainingRequest>,
    ) -> Result<Response<pd::MarkDiskDrainingResponse>, Status> {
        let disk_id = DiskId::new(request.into_inner().disk_id);
        match self.journal.mark_disk_draining(disk_id).await {
            Ok(draining) => Ok(Response::new(pd::MarkDiskDrainingResponse { draining })),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("mark_disk_draining failed: {e}"))),
        }
    }

    async fn get_live_writers(
        &self,
        _request: Request<pd::GetLiveWritersRequest>,
    ) -> Result<Response<pd::GetLiveWritersResponse>, Status> {
        // Leader-only: the commit watermarks are volatile leader memory (Q18).
        if !self.is_leader() {
            return Err(not_leader_now());
        }
        let writers = self
            .journal
            .live_writers_with_watermark()
            .into_iter()
            .map(|(token, node, watermark)| pd::LiveWriter {
                writer_token: token.get(),
                node_id: node.get(),
                commit_watermark: watermark,
            })
            .collect();
        Ok(Response::new(pd::GetLiveWritersResponse { writers }))
    }

    async fn register_writer(
        &self,
        request: Request<pd::RegisterWriterRequest>,
    ) -> Result<Response<pd::RegisterWriterResponse>, Status> {
        // A write (raft propose): only the leader can serve it.
        let node_id = NodeId::new(request.into_inner().node_id);
        match self.journal.register_writer(node_id).await {
            Ok(ApplyResult::WriterRegistered { token }) => {
                Ok(Response::new(pd::RegisterWriterResponse {
                    writer_token: token.get(),
                }))
            }
            Ok(ApplyResult::Rejected(RejectReason::NodeNotFound)) => {
                Err(Status::not_found("gateway node is not registered"))
            }
            Ok(other) => Err(Status::internal(format!(
                "unexpected register_writer result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("register_writer failed: {e}"))),
        }
    }

    async fn writer_heartbeat(
        &self,
        request: Request<pd::WriterHeartbeatRequest>,
    ) -> Result<Response<pd::WriterHeartbeatResponse>, Status> {
        // Leader-only like node heartbeats: only the leader's tracker feeds the
        // liveness sweeps (Q18 — last-seen never enters raft).
        if !self.is_leader() {
            return Err(not_leader_now());
        }
        let req = request.into_inner();
        let node_id = NodeId::new(req.node_id);
        let token = epoch_proto::WriterToken::new(req.writer_token);

        // A writer heartbeat also proves the node is alive: refresh the node's
        // last-seen so the writer sweep (keyed on node staleness, 01 §4.3)
        // sees the session as fresh even between capacity heartbeats.
        self.journal
            .record_writer_seen(node_id, self.clock.now_millis());
        // Record the GC commit watermark (Q27, leader memory) so a GcRound knows
        // which of this token's blobs are past their terminal outcome.
        self.journal
            .record_writer_watermark(token, req.commit_watermark);

        // INVARIANT(design 01 §4.3): a Dead token never revives — a heartbeat
        // for a Dead or unknown token answers live=false and the gateway must
        // rotate to a fresh token.
        let live = self
            .journal
            .state()
            .writers()
            .get(token)
            .is_some_and(|record| {
                record.status == crate::writer::WriterStatus::Live && record.node_id == node_id
            });
        Ok(Response::new(pd::WriterHeartbeatResponse { live }))
    }

    async fn register_node(
        &self,
        request: Request<pd::RegisterNodeRequest>,
    ) -> Result<Response<pd::RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let roles = crate::cluster::RoleSet::from_bits(
            u8::try_from(req.roles)
                .map_err(|_| Status::invalid_argument("roles exceeds the 8-bit role bitset"))?,
        );
        match self
            .journal
            .register_node(req.addr, req.az, req.rack, roles)
            .await
        {
            Ok(node_id) => Ok(Response::new(pd::RegisterNodeResponse {
                node_id: node_id.get(),
            })),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("register_node failed: {e}"))),
        }
    }

    async fn register_disk(
        &self,
        request: Request<pd::RegisterDiskRequest>,
    ) -> Result<Response<pd::RegisterDiskResponse>, Status> {
        let req = request.into_inner();
        match self
            .journal
            .register_disk(
                NodeId::new(req.node_id),
                req.az,
                req.rack,
                req.path,
                req.total,
            )
            .await
        {
            Ok(ApplyResult::DiskRegistered { disk_id }) => {
                Ok(Response::new(pd::RegisterDiskResponse {
                    disk_id: disk_id.get(),
                }))
            }
            Ok(ApplyResult::Rejected(RejectReason::NodeNotFound)) => {
                Err(Status::not_found("owning node is not registered"))
            }
            Ok(other) => Err(Status::internal(format!(
                "unexpected register_disk result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("register_disk failed: {e}"))),
        }
    }

    async fn seal_chunk(
        &self,
        request: Request<pd::SealChunkRequest>,
    ) -> Result<Response<pd::SealChunkResponse>, Status> {
        let chunk_id = ChunkId::new(request.into_inner().chunk_id);
        match self.journal.seal_chunk(chunk_id).await {
            Ok(ApplyResult::Applied) => Ok(Response::new(pd::SealChunkResponse {})),
            Ok(ApplyResult::Rejected(reason)) => Err(Status::not_found(format!(
                "chunk {} cannot be sealed: {reason:?}",
                chunk_id.get()
            ))),
            Ok(other) => Err(Status::internal(format!(
                "unexpected seal_chunk result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("seal_chunk failed: {e}"))),
        }
    }

    async fn list_nodes(
        &self,
        _request: Request<pd::ListNodesRequest>,
    ) -> Result<Response<pd::ListNodesResponse>, Status> {
        self.ensure_read_leader().await?;
        let state = self.journal.state();
        let nodes = state
            .nodes()
            .list()
            .iter()
            .map(|node| pd::NodeInfo {
                node_id: node.node_id.get(),
                addr: node.addr.clone(),
                az: node.az.clone(),
                rack: node.rack.clone(),
                roles: u32::from(node.roles.bits()),
                status: node_status_to_proto(node.status) as i32,
            })
            .collect();
        let disks = state
            .disks()
            .list()
            .iter()
            .map(|disk| pd::DiskInfo {
                disk_id: disk.disk_id.get(),
                node_id: disk.node_id.get(),
                path: disk.path.clone(),
                status: disk_status_to_proto(disk.status) as i32,
            })
            .collect();
        Ok(Response::new(pd::ListNodesResponse { nodes, disks }))
    }

    async fn create_bucket(
        &self,
        request: Request<pd::CreateBucketRequest>,
    ) -> Result<Response<pd::CreateBucketResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("bucket name must not be empty"));
        }
        let ns_mode = ns_mode_from_proto(req.ns_mode())?;
        let engine = meta_engine_from_proto(req.engine())?;
        let codemode_id = u16::try_from(req.codemode_id)
            .map_err(|_| Status::invalid_argument("codemode_id exceeds u16"))?;
        let inline_threshold = (req.inline_threshold > 0).then_some(req.inline_threshold);
        match self
            .journal
            .create_bucket(
                req.name,
                ns_mode,
                inline_threshold,
                codemode_id,
                engine,
                self.clock.now_millis() as i64,
            )
            .await
        {
            Ok(ApplyResult::BucketCreated { bucket_id }) => {
                Ok(Response::new(pd::CreateBucketResponse {
                    bucket_id: bucket_id.get(),
                }))
            }
            Ok(other) => Err(Status::internal(format!(
                "unexpected create_bucket result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("create_bucket failed: {e}"))),
        }
    }

    async fn list_buckets(
        &self,
        _request: Request<pd::ListBucketsRequest>,
    ) -> Result<Response<pd::ListBucketsResponse>, Status> {
        self.ensure_read_leader().await?;
        let buckets = self
            .journal
            .state()
            .buckets()
            .list()
            .iter()
            .map(bucket_to_proto)
            .collect();
        Ok(Response::new(pd::ListBucketsResponse { buckets }))
    }

    async fn put_config(
        &self,
        request: Request<pd::PutConfigRequest>,
    ) -> Result<Response<pd::PutConfigResponse>, Status> {
        let req = request.into_inner();
        match self.journal.put_config(req.key, req.value).await {
            Ok(ApplyResult::Applied) => Ok(Response::new(pd::PutConfigResponse {})),
            Ok(other) => Err(Status::internal(format!(
                "unexpected put_config result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("put_config failed: {e}"))),
        }
    }

    async fn get_config(
        &self,
        request: Request<pd::GetConfigRequest>,
    ) -> Result<Response<pd::GetConfigResponse>, Status> {
        self.ensure_read_leader().await?;
        let value = self
            .journal
            .state()
            .configs()
            .get(&request.into_inner().key);
        Ok(Response::new(pd::GetConfigResponse {
            found: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    async fn put_credential(
        &self,
        request: Request<pd::PutCredentialRequest>,
    ) -> Result<Response<pd::PutCredentialResponse>, Status> {
        let req = request.into_inner();
        if req.access_key.is_empty() {
            return Err(Status::invalid_argument("access_key must not be empty"));
        }
        // `all_buckets` means root (no allow-list); otherwise the explicit list.
        let allowed_buckets = (!req.all_buckets).then_some(req.allowed_buckets);
        match self
            .journal
            .put_credential(
                req.access_key,
                req.secret_key,
                allowed_buckets,
                console_role_from_proto(req.role),
            )
            .await
        {
            Ok(ApplyResult::Applied) => Ok(Response::new(pd::PutCredentialResponse {})),
            Ok(other) => Err(Status::internal(format!(
                "unexpected put_credential result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("put_credential failed: {e}"))),
        }
    }

    async fn list_credentials(
        &self,
        _request: Request<pd::ListCredentialsRequest>,
    ) -> Result<Response<pd::ListCredentialsResponse>, Status> {
        self.ensure_read_leader().await?;
        let credentials = self
            .journal
            .state()
            .credentials()
            .list()
            .into_iter()
            .map(|(access_key, cred)| pd::CredentialInfo {
                access_key,
                secret_key: cred.secret_key,
                all_buckets: cred.allowed_buckets.is_none(),
                allowed_buckets: cred.allowed_buckets.unwrap_or_default(),
                role: console_role_to_proto(cred.role) as i32,
            })
            .collect();
        Ok(Response::new(pd::ListCredentialsResponse { credentials }))
    }

    async fn get_route(
        &self,
        request: Request<pd::GetRouteRequest>,
    ) -> Result<Response<pd::GetRouteResponse>, Status> {
        self.ensure_read_leader().await?;
        let req = request.into_inner();
        let ns = ns_mode_from_proto(req.ns())?;
        let partition =
            self.journal
                .state()
                .partitions()
                .route(ns, req.bucket_id, &req.routing_key);
        Ok(Response::new(pd::GetRouteResponse {
            partition: partition.map(|p| self.partition_to_proto(&p)),
        }))
    }

    async fn create_partition(
        &self,
        request: Request<pd::CreatePartitionRequest>,
    ) -> Result<Response<pd::CreatePartitionResponse>, Status> {
        let req = request.into_inner();
        let ns = ns_mode_from_proto(req.ns())?;
        let start = crate::meta_mgr::PartitionBound {
            bucket: req.start_bucket,
            routing_key: req.start_key,
            unbounded: req.start_unbounded,
        };
        let end = crate::meta_mgr::PartitionBound {
            bucket: req.end_bucket,
            routing_key: req.end_key,
            unbounded: req.end_unbounded,
        };
        validate_partition_bounds(ns, &start, &end)?;
        {
            let existing = self.journal.state().partitions().list();
            if let Some(overlap) = find_overlap(&existing, ns, &start, &end) {
                return Err(Status::failed_precondition(format!(
                    "range overlaps partition {overlap}"
                )));
            }
        }
        let peers = self.pick_meta_peers(3)?;
        match self.journal.create_partition(ns, start, end, peers).await {
            Ok(ApplyResult::PartitionCreated { partition_id }) => {
                let Some(partition) = self.journal.state().partitions().get(partition_id) else {
                    return Err(Status::internal("partition missing after commit"));
                };
                // Push CreateRaftGroup to the chosen peers (best-effort; the
                // partition heartbeat reconciles any push that fails here,
                // 01 §5).
                for peer in &partition.peers {
                    self.push_create_raft_group(&partition, *peer);
                }
                Ok(Response::new(pd::CreatePartitionResponse {
                    partition: Some(self.partition_to_proto(&partition)),
                }))
            }
            Ok(ApplyResult::Rejected(reason)) => Err(Status::failed_precondition(format!(
                "create_partition rejected: {reason:?}"
            ))),
            Ok(other) => Err(Status::internal(format!(
                "unexpected create_partition result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("create_partition failed: {e}"))),
        }
    }

    async fn list_partitions(
        &self,
        _request: Request<pd::ListPartitionsRequest>,
    ) -> Result<Response<pd::ListPartitionsResponse>, Status> {
        self.ensure_read_leader().await?;
        let partitions = self
            .journal
            .state()
            .partitions()
            .list()
            .iter()
            .map(|p| self.partition_to_proto(p))
            .collect();
        Ok(Response::new(pd::ListPartitionsResponse { partitions }))
    }

    async fn partition_heartbeat(
        &self,
        request: Request<pd::PartitionHeartbeatRequest>,
    ) -> Result<Response<pd::PartitionHeartbeatResponse>, Status> {
        // Same leader-only discipline as the data-node heartbeat (Q18).
        if !self.is_leader() {
            return Err(not_leader_now());
        }
        let req = request.into_inner();
        let reporter = NodeId::new(req.node_id);
        let now = self.clock.now_millis();
        // A partition heartbeat is also proof of node liveness: MetaNodes
        // never call the data-node Heartbeat RPC, so this is what drives
        // their Starting→Live transition for peer selection (01 §5).
        self.journal.record_heartbeat(
            &HeartbeatReport {
                node_id: reporter,
                disks: Vec::new(),
            },
            now,
        );
        let partitions = self.journal.state().partitions().clone();
        for stat in req.stats {
            // Only a self-leadership claim updates the leader record; a
            // follower's hearsay view is ignored, so the record tracks the
            // actual leader's own report.
            if stat.leader_node_id == req.node_id {
                partitions.record_leader(
                    stat.partition_id,
                    crate::meta_mgr::LeaderReport {
                        leader: reporter,
                        applied_index: stat.applied_index,
                        inline_bytes: stat.inline_bytes,
                        inline_count: stat.inline_count,
                        total_bytes: stat.total_bytes,
                        last_seen_millis: now,
                    },
                );
            }
        }
        // Reconcile: the reporter must host every partition whose voter set
        // contains it; re-push the missing ones (a CreateRaftGroup that was
        // lost at creation time, 01 §5).
        let hosted: std::collections::BTreeSet<u64> = req.hosted_partitions.into_iter().collect();
        for partition in partitions.partitions_on(reporter) {
            if !hosted.contains(&partition.partition_id) {
                self.push_create_raft_group(&partition, reporter);
            }
        }
        Ok(Response::new(pd::PartitionHeartbeatResponse {}))
    }

    async fn list_node_jobs(
        &self,
        request: Request<pd::ListNodeJobsRequest>,
    ) -> Result<Response<pd::ListNodeJobsResponse>, Status> {
        self.ensure_read_leader().await?;
        let node = NodeId::new(request.into_inner().node_id);
        let jobs = self
            .journal
            .state()
            .jobs()
            .jobs_for_coordinator(node)
            .into_iter()
            .map(|job| {
                let (kind, disk_id) = job_kind_to_proto(&job.kind);
                pd::AssignedJob {
                    job_id: u64::from(job.id),
                    kind: kind as i32,
                    disk_id,
                    progress_watermark: job.progress_watermark,
                }
            })
            .collect();
        Ok(Response::new(pd::ListNodeJobsResponse { jobs }))
    }

    async fn commit_job_progress(
        &self,
        request: Request<pd::CommitJobProgressRequest>,
    ) -> Result<Response<pd::CommitJobProgressResponse>, Status> {
        let req = request.into_inner();
        let job_id = u32::try_from(req.job_id)
            .map_err(|_| Status::invalid_argument("job_id exceeds u32"))?;
        let result = if req.done {
            self.journal.complete_job(job_id).await
        } else {
            // A checkpoint renews the coordinator's lease (01 §6.4). The fresh
            // expiry is computed here, before proposal, so apply stays
            // deterministic (AGENTS §8).
            let lease_expiry = self
                .clock
                .now_millis()
                .saturating_add(crate::job::JOB_LEASE_MILLIS);
            self.journal
                .advance_job_watermark(job_id, req.watermark, lease_expiry)
                .await
        };
        match result {
            Ok(_) => Ok(Response::new(pd::CommitJobProgressResponse {})),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("commit_job_progress failed: {e}"))),
        }
    }

    async fn commit_shard_mapping(
        &self,
        request: Request<pd::CommitShardMappingRequest>,
    ) -> Result<Response<pd::CommitShardMappingResponse>, Status> {
        let req = request.into_inner();
        let job_id = u32::try_from(req.job_id)
            .map_err(|_| Status::invalid_argument("job_id exceeds u32"))?;
        let index = u8::try_from(req.index)
            .map_err(|_| Status::invalid_argument("shard index exceeds u8"))?;
        let cmd = crate::chunk::CommitShardMapping {
            job_id,
            chunk_id: ChunkId::new(req.chunk_id),
            index,
            expected_epoch: req.expected_epoch,
            new_disk: DiskId::new(req.new_disk_id),
            new_create_ts: req.new_create_ts,
            committer: NodeId::new(req.committer_node_id),
        };
        match self.journal.commit_shard_mapping(cmd).await {
            Ok(ApplyResult::ShardRebound { new_epoch }) => {
                Ok(Response::new(pd::CommitShardMappingResponse {
                    new_epoch,
                    rebound: true,
                }))
            }
            Ok(ApplyResult::Rejected(_)) => Ok(Response::new(pd::CommitShardMappingResponse {
                new_epoch: 0,
                rebound: false,
            })),
            Ok(other) => Err(Status::internal(format!(
                "unexpected commit_shard_mapping result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!(
                "commit_shard_mapping failed: {e}"
            ))),
        }
    }

    async fn report_shard_repair(
        &self,
        request: Request<pd::ReportShardRepairRequest>,
    ) -> Result<Response<pd::ReportShardRepairResponse>, Status> {
        let req = request.into_inner();
        let index = u8::try_from(req.index)
            .map_err(|_| Status::invalid_argument("shard index exceeds u8"))?;
        let cmd = crate::shard_repair::ReportShardRepair {
            chunk_id: ChunkId::new(req.chunk_id),
            index,
        };
        match self.journal.report_shard_repair(cmd).await {
            Ok(ApplyResult::ShardRepairReported { created }) => {
                Ok(Response::new(pd::ReportShardRepairResponse { created }))
            }
            // A stale/unknown shard is a benign no-op (best-effort, 01 §6.4 Q5).
            Ok(ApplyResult::Rejected(_)) => Ok(Response::new(pd::ReportShardRepairResponse {
                created: false,
            })),
            Ok(other) => Err(Status::internal(format!(
                "unexpected report_shard_repair result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("report_shard_repair failed: {e}"))),
        }
    }

    async fn list_node_shard_repairs(
        &self,
        request: Request<pd::ListNodeShardRepairsRequest>,
    ) -> Result<Response<pd::ListNodeShardRepairsResponse>, Status> {
        self.ensure_read_leader().await?;
        let node = NodeId::new(request.into_inner().node_id);
        let repairs = self
            .journal
            .state()
            .shard_repairs()
            .tickets_for_node(node)
            .into_iter()
            .map(|t| pd::ShardRepairAssignment {
                chunk_id: t.chunk_id.get(),
                index: u32::from(t.index),
                expected_epoch: t.expected_epoch,
            })
            .collect();
        Ok(Response::new(pd::ListNodeShardRepairsResponse { repairs }))
    }

    async fn commit_shard_repair(
        &self,
        request: Request<pd::CommitShardRepairRequest>,
    ) -> Result<Response<pd::CommitShardRepairResponse>, Status> {
        let req = request.into_inner();
        let index = u8::try_from(req.index)
            .map_err(|_| Status::invalid_argument("shard index exceeds u8"))?;
        let cmd = crate::shard_repair::CommitShardRepair {
            chunk_id: ChunkId::new(req.chunk_id),
            index,
            expected_epoch: req.expected_epoch,
            new_disk: DiskId::new(req.new_disk_id),
            new_create_ts: req.new_create_ts,
            committer: NodeId::new(req.committer_node_id),
        };
        match self.journal.commit_shard_repair(cmd).await {
            Ok(ApplyResult::ShardRebound { new_epoch }) => {
                Ok(Response::new(pd::CommitShardRepairResponse {
                    new_epoch,
                    rebound: true,
                }))
            }
            Ok(ApplyResult::Rejected(_)) => Ok(Response::new(pd::CommitShardRepairResponse {
                new_epoch: 0,
                rebound: false,
            })),
            Ok(other) => Err(Status::internal(format!(
                "unexpected commit_shard_repair result: {other:?}"
            ))),
            Err(PdError::NotLeader) => Err(not_leader_now()),
            Err(e) => Err(Status::internal(format!("commit_shard_repair failed: {e}"))),
        }
    }
}

/// Maps the domain `JobKind` to its wire enum + disk id (0 for kinds with no
/// disk). Keeps the proto enum and the domain enum in sync at one site.
fn job_kind_to_proto(kind: &crate::job::types::JobKind) -> (pd::JobKind, u32) {
    use crate::job::types::JobKind;
    match kind {
        JobKind::RepairDisk { disk_id } => (pd::JobKind::RepairDisk, disk_id.get()),
        JobKind::DropDisk { disk_id } => (pd::JobKind::DropDisk, disk_id.get()),
        JobKind::Balance { disk_id } => (pd::JobKind::Balance, disk_id.get()),
        JobKind::InspectRound => (pd::JobKind::InspectRound, 0),
        JobKind::GcRound => (pd::JobKind::GcRound, 0),
    }
}

/// The status returned when a non-leader is asked to serve a leader-only call:
/// `FAILED_PRECONDITION` signals the client to rediscover the leader and retry.
fn not_leader(err: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!("pd node is not the leader: {err}"))
}

/// The heartbeat-path variant of [`not_leader`] (no underlying raft error).
fn not_leader_now() -> Status {
    Status::failed_precondition("pd node is not the leader")
}

/// Narrows a wire `code_mode_id` (u32) to the [`CodeModeId`] u16 registry key,
/// rejecting an out-of-range value at the boundary.
fn decode_code_mode_id(raw: u32) -> Result<CodeModeId, Status> {
    u16::try_from(raw)
        .map(CodeModeId::new)
        .map_err(|_| Status::invalid_argument("code_mode_id exceeds the 16-bit registry key"))
}

/// Decodes a heartbeat wire message into the in-process [`HeartbeatReport`].
fn decode_heartbeat(req: pd::HeartbeatRequest) -> HeartbeatReport {
    HeartbeatReport {
        node_id: NodeId::new(req.node_id),
        disks: req
            .disks
            .into_iter()
            .map(|disk| DiskHeartbeat {
                disk_id: DiskId::new(disk.disk_id),
                free: disk.free,
                used: disk.used,
                writable_extents: disk.writable_extents,
                broken: disk.broken,
            })
            .collect(),
    }
}

/// Projects a committed chunk to its wire `ChunkView`.
fn chunk_to_proto(chunk: &Chunk) -> pd::ChunkView {
    pd::ChunkView {
        chunk_id: chunk.chunk_id.get(),
        code_mode: Some(code_mode_to_proto(&chunk.code_mode)),
        status: chunk_status_to_proto(chunk.status) as i32,
        shards: chunk.shards.iter().map(shard_slot_to_proto).collect(),
    }
}

/// Projects erasure-code parameters to their wire form (the L0 `u8`/`u16` fields
/// widen losslessly to proto's `u32`).
fn code_mode_to_proto(mode: &CodeMode) -> pd::CodeMode {
    pd::CodeMode {
        id: u32::from(mode.id.get()),
        data: u32::from(mode.data),
        parity: u32::from(mode.parity),
        stripe_size: mode.stripe_size,
        blob_size: mode.blob_size,
    }
}

/// Projects a shard slot to its wire `ShardView` (extent id as its 16 raw bytes).
fn shard_slot_to_proto(slot: &ShardSlot) -> pd::ShardView {
    pd::ShardView {
        shard_prefix: slot.shard_prefix,
        epoch: slot.epoch,
        disk_id: slot.disk_id.get(),
        extent_id: slot.extent_id.as_bytes().to_vec(),
    }
}

/// Maps the domain chunk status to its wire enum.
fn chunk_status_to_proto(status: ChunkStatus) -> pd::ChunkStatus {
    match status {
        ChunkStatus::Writable => pd::ChunkStatus::Writable,
        ChunkStatus::Full => pd::ChunkStatus::Full,
        ChunkStatus::Sealed => pd::ChunkStatus::Sealed,
        ChunkStatus::Migrating => pd::ChunkStatus::Migrating,
        ChunkStatus::Broken => pd::ChunkStatus::Broken,
    }
}

/// Maps the domain node status to its wire enum.
fn node_status_to_proto(status: crate::cluster::NodeStatus) -> pd::NodeStatus {
    match status {
        crate::cluster::NodeStatus::Starting => pd::NodeStatus::Starting,
        crate::cluster::NodeStatus::Live => pd::NodeStatus::Live,
        crate::cluster::NodeStatus::Offline => pd::NodeStatus::Offline,
        crate::cluster::NodeStatus::Lost => pd::NodeStatus::Lost,
        crate::cluster::NodeStatus::Decommissioned => pd::NodeStatus::Decommissioned,
    }
}

/// Maps the domain disk status to its wire enum.
fn disk_status_to_proto(status: DiskStatus) -> pd::DiskStatus {
    match status {
        DiskStatus::Normal => pd::DiskStatus::Normal,
        DiskStatus::Broken => pd::DiskStatus::Broken,
        DiskStatus::Repairing => pd::DiskStatus::Repairing,
        DiskStatus::Repaired => pd::DiskStatus::Repaired,
        DiskStatus::Draining => pd::DiskStatus::Draining,
        DiskStatus::Dropped => pd::DiskStatus::Dropped,
    }
}

/// Maps the wire console role to the domain enum (`UNSPECIFIED` → `Readonly`,
/// least privilege — 08 §4.1).
fn console_role_from_proto(role: i32) -> crate::credential::ConsoleRole {
    use crate::credential::ConsoleRole;
    match pd::ConsoleRole::try_from(role) {
        Ok(pd::ConsoleRole::Admin) => ConsoleRole::Admin,
        _ => ConsoleRole::Readonly,
    }
}

/// Maps the domain console role to its wire enum.
fn console_role_to_proto(role: crate::credential::ConsoleRole) -> pd::ConsoleRole {
    use crate::credential::ConsoleRole;
    match role {
        ConsoleRole::Readonly => pd::ConsoleRole::Readonly,
        ConsoleRole::Admin => pd::ConsoleRole::Admin,
    }
}

/// Projects a bucket record to its wire `BucketInfo`.
fn bucket_to_proto(bucket: &crate::bucket::BucketMeta) -> pd::BucketInfo {
    pd::BucketInfo {
        bucket_id: bucket.bucket_id.get(),
        name: bucket.name.clone(),
        ns_mode: ns_mode_to_proto(bucket.ns_mode) as i32,
        inline_threshold: bucket.inline_threshold.unwrap_or(0),
        codemode_id: u32::from(bucket.codemode_id),
        engine: meta_engine_to_proto(bucket.engine) as i32,
        created_at: bucket.created_at,
    }
}

/// Maps the domain namespace mode to its wire enum.
fn ns_mode_to_proto(mode: crate::bucket::NsMode) -> pd::NsMode {
    match mode {
        crate::bucket::NsMode::Flat => pd::NsMode::Flat,
        crate::bucket::NsMode::Hier => pd::NsMode::Hier,
    }
}

/// Maps the domain metadata engine to its wire enum.
fn meta_engine_to_proto(engine: crate::bucket::MetaEngine) -> pd::MetaEngine {
    match engine {
        crate::bucket::MetaEngine::Rocks => pd::MetaEngine::Rocks,
        crate::bucket::MetaEngine::Mem => pd::MetaEngine::Mem,
    }
}

/// Narrows a wire namespace mode to the domain enum, rejecting UNSPECIFIED.
fn ns_mode_from_proto(mode: pd::NsMode) -> Result<crate::bucket::NsMode, Status> {
    match mode {
        pd::NsMode::Flat => Ok(crate::bucket::NsMode::Flat),
        pd::NsMode::Hier => Ok(crate::bucket::NsMode::Hier),
        pd::NsMode::Unspecified => Err(Status::invalid_argument("ns_mode is required")),
    }
}

/// Validates a new partition's bounds against the namespace-bit convention
/// (HIER_BUCKET_BIT, 03 §2/§4.1 归属不变量): a flat partition must end at or
/// below the bit, a hierarchical one must start at or above it — otherwise
/// its bucket span could contain the other namespace's keys in the shared
/// CFs, and range export would no longer be exact.
fn validate_partition_bounds(
    ns: crate::bucket::NsMode,
    start: &crate::meta_mgr::PartitionBound,
    end: &crate::meta_mgr::PartitionBound,
) -> Result<(), Status> {
    use epoch_proto::consts::HIER_BUCKET_BIT;
    match ns {
        crate::bucket::NsMode::Flat if !end.unbounded && end.bucket > HIER_BUCKET_BIT => {
            Err(Status::invalid_argument(
                "flat partition end must not cross the hierarchical namespace bit",
            ))
        }
        crate::bucket::NsMode::Hier if !start.unbounded && start.bucket < HIER_BUCKET_BIT => {
            Err(Status::invalid_argument(
                "hierarchical partition start must be at or above the namespace bit",
            ))
        }
        _ => Ok(()),
    }
}

/// Finds an existing partition of `ns` whose interval overlaps `[start, end)`
/// (both are half-open intervals in `(bucket, routing_key)` coordinates).
fn find_overlap(
    existing: &[crate::meta_mgr::MetaPartition],
    ns: crate::bucket::NsMode,
    start: &crate::meta_mgr::PartitionBound,
    end: &crate::meta_mgr::PartitionBound,
) -> Option<u64> {
    let start_coord = (!start.unbounded).then_some((start.bucket, start.routing_key.as_slice()));
    let end_coord = (!end.unbounded).then_some((end.bucket, end.routing_key.as_slice()));
    existing
        .iter()
        .filter(|p| p.ns == ns)
        .find(|p| {
            let p_start =
                (!p.start.unbounded).then_some((p.start.bucket, p.start.routing_key.as_slice()));
            let p_end = (!p.end.unbounded).then_some((p.end.bucket, p.end.routing_key.as_slice()));
            // [s, e) ∩ [ps, pe) ≠ ∅  ⟺  s < pe  &&  ps < e
            let s_before_pe = p_end.is_none_or(|pe| start_coord.is_none_or(|s| s < pe));
            let ps_before_e = end_coord.is_none_or(|e| p_start.is_none_or(|ps| ps < e));
            s_before_pe && ps_before_e
        })
        .map(|p| p.partition_id)
}

/// Builds the wire `CreateRaftGroupRequest` for a partition: the same request
/// goes to every peer; the first peer bootstraps membership (01 §5).
fn create_raft_group_request(
    partition: &crate::meta_mgr::MetaPartition,
    state: &crate::state::PdState,
) -> epoch_proto::grpc::meta::CreateRaftGroupRequest {
    epoch_proto::grpc::meta::CreateRaftGroupRequest {
        partition_id: partition.partition_id,
        ns: ns_mode_to_proto(partition.ns) as i32,
        start_bucket: partition.start.bucket,
        start_key: partition.start.routing_key.clone(),
        start_unbounded: partition.start.unbounded,
        end_bucket: partition.end.bucket,
        end_key: partition.end.routing_key.clone(),
        end_unbounded: partition.end.unbounded,
        peers: partition
            .peers
            .iter()
            .map(|id| epoch_proto::grpc::meta::RaftPeer {
                node_id: id.get(),
                addr: state
                    .nodes()
                    .get(*id)
                    .map(|n| n.addr.clone())
                    .unwrap_or_default(),
            })
            .collect(),
        bootstrap_node_id: partition.peers.first().map_or(0, |n| n.get()),
    }
}

/// Narrows a wire metadata engine to the domain enum, rejecting UNSPECIFIED.
fn meta_engine_from_proto(mode: pd::MetaEngine) -> Result<crate::bucket::MetaEngine, Status> {
    match mode {
        pd::MetaEngine::Rocks => Ok(crate::bucket::MetaEngine::Rocks),
        pd::MetaEngine::Mem => Ok(crate::bucket::MetaEngine::Mem),
        pd::MetaEngine::Unspecified => Err(Status::invalid_argument("engine is required")),
    }
}
