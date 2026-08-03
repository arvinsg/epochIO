//! The MetaNode gRPC service: PD-facing partition admin plus gateway-facing
//! flat-namespace metadata ops (06 §9 service.rs — gRPC 服务装配 +
//! 请求→分区路由 + NotLeader/PartitionMoved 回错).
//!
//! Every op routes by `(bucket_id, key)` through the local
//! [`PartitionRegistry`]: the key's partition must be hosted here
//! ([`RouteError::KIND_PARTITION_MOVED`] otherwise — the client refreshes its
//! route via PD `GetRoute`), and this replica must be its leader
//! ([`RouteError::KIND_NOT_LEADER`] with the hint otherwise). Writes propose
//! one raft entry (03 §5: 全部单分区单 propose); reads run through
//! `ensure_linearizable` (leader ReadIndex, 03 §5: 读路径 0 轮次).
//!
//! Design: docs/design/03-metanode.md §5; docs/design/01-pd.md §5

use std::collections::BTreeMap;
use std::sync::Arc;

use epoch_proto::grpc::meta::meta_node_server::{MetaNode, MetaNodeServer};
use epoch_proto::grpc::meta::{
    self, AbortMultipartUploadRequest, AbortMultipartUploadResponse, ApplySplitRequest,
    ApplySplitResponse, CompleteMultipartRequest, CompleteMultipartResponse,
    CreateMultipartUploadRequest, CreateMultipartUploadResponse, CreateRaftGroupRequest,
    CreateRaftGroupResponse, DeleteObjectRequest, DeleteObjectResponse, DelqBacklogRequest,
    DelqBacklogResponse, GetObjectMetaRequest, GetObjectMetaResponse, HeadObjectRequest,
    HeadObjectResponse, ListObjectsRequest, ListObjectsResponse, MigrateGroupMemberRequest,
    MigrateGroupMemberResponse, ObjectEntry, ObjectHeadView, PrepareMigrateTargetRequest,
    PrepareMigrateTargetResponse, PutObjectRequest, PutObjectResponse, PutPartRequest,
    PutPartResponse, RouteError, SuggestSplitPointRequest, SuggestSplitPointResponse,
};
use epoch_proto::grpc::pd::NsMode;
use epoch_proto::{BlobId, BucketId, ChunkId};
use openraft::BasicNode;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::MetaError;
use crate::ns_flat::{self, ContentHead, FlatOp, ObjectHead, StorageClass};
use crate::ns_hier;
use crate::partition::{Namespace, PartitionInfo, PartitionRange, PartitionRegistry};
use crate::raft::{GroupManager, MetaEntry, MetaRaft};
use crate::ref_extractor::Slice;
use crate::store::keys::hier_routing_key;

/// The MetaNode gRPC service (one instance per node; cheap to construct).
pub struct MetaNodeService {
    node_id: u32,
    manager: Arc<GroupManager>,
    registry: Arc<PartitionRegistry>,
}

impl MetaNodeService {
    /// Builds the service over the node's multi-group runtime and its
    /// partition registry.
    #[must_use]
    pub fn new(node_id: u32, manager: Arc<GroupManager>, registry: Arc<PartitionRegistry>) -> Self {
        Self {
            node_id,
            manager,
            registry,
        }
    }

    /// Wraps the service in the generated tonic server, ready to hand to
    /// `tonic::transport::Server::add_service` (mounted next to the raft
    /// transport on the same listener).
    #[must_use]
    pub fn into_server(self) -> MetaNodeServer<Self> {
        MetaNodeServer::new(self)
    }

    /// Routes `(bucket, key)` to the locally-hosted partition raft, or to the
    /// in-band redirect error (06 §9).
    fn route(&self, bucket: BucketId, key: &[u8]) -> Result<(u64, MetaRaft), RouteError> {
        let Some(group) = self.registry.route(bucket, key) else {
            return Err(route_error(meta::route_error::Kind::PartitionMoved, 0, ""));
        };
        let Some(raft) = self.manager.raft(group) else {
            return Err(route_error(
                meta::route_error::Kind::PartitionMoved,
                group,
                "",
            ));
        };
        Ok((group, raft))
    }

    /// Verifies this replica leads the group; on failure the error carries
    /// the last known leader hint.
    fn check_leader(&self, group: u64, raft: &MetaRaft) -> Result<(), RouteError> {
        let metrics = raft.metrics().borrow().clone();
        if metrics.state.is_leader() {
            return Ok(());
        }
        Err(self.not_leader(group, metrics.current_leader))
    }

    /// Builds the NOT_LEADER error with the best leader hint available.
    fn not_leader(&self, group: u64, leader: Option<u64>) -> RouteError {
        let leader_addr = leader
            .and_then(|id| self.registry.peer_addr(group, id))
            .unwrap_or_default();
        route_error(meta::route_error::Kind::NotLeader, group, &leader_addr)
    }

    /// Chooses a split boundary for `range`: the routing coordinate of the
    /// median primary-index key currently stored in the partition (03 §2 —
    /// the boundary is an object/directory-entry key so a key family stays
    /// together). Returns `None` when the range is empty or holds a single
    /// distinct coordinate (nothing to split off — the parent would keep an
    /// empty child). Bounded scan: at most `SPLIT_SCAN_LIMIT` keys.
    fn pick_split_point(&self, range: &PartitionRange) -> Result<Option<(u64, Vec<u8>)>, Status> {
        use crate::store::keys::routing_key;

        let (cf, start, end) = range.primary_scan_bounds();
        let keys = self
            .manager
            .store()
            .scan(cf, &start, &end, SPLIT_SCAN_LIMIT)
            .map_err(|e| Status::internal(format!("split scan: {e}")))?;
        if keys.len() < 2 {
            return Ok(None); // 0 or 1 key: no interior boundary
        }
        // Walk outward from the midpoint to the first key whose coordinate is
        // strictly inside the range (not equal to `start`), so both children
        // are non-empty. Keys are ascending, so scanning forward from the
        // midpoint finds the smallest valid boundary.
        let mid = keys.len() / 2;
        for (key, _) in keys.iter().skip(mid) {
            let Some((bucket, routing)) = routing_key(key) else {
                continue;
            };
            if range.split((bucket, routing.clone())).is_some() {
                return Ok(Some((bucket.get(), routing)));
            }
        }
        Ok(None)
    }
}

/// The per-sweep cap on the split-point median scan (bounded work on the
/// leader; a partition is split well before it holds this many keys).
const SPLIT_SCAN_LIMIT: usize = 4096;

/// Builds one in-band routing error.
fn route_error(kind: meta::route_error::Kind, partition_id: u64, leader_addr: &str) -> RouteError {
    RouteError {
        kind: kind as i32,
        leader_addr: leader_addr.to_string(),
        partition_id,
    }
}

/// Classifies a raft `client_write` failure: a genuine `ForwardToLeader` is an
/// in-band [`RouteError`] redirect the client retries against the new leader
/// (`Ok`); every other failure (storage / apply / timeout / fatal) is a real
/// fault surfaced as `Status::internal` (`Err`) — never masked as a redirect,
/// which would send the client looping against the same node while the true
/// cause vanishes from the response.
fn write_error(
    service: &MetaNodeService,
    group: u64,
    err: openraft::error::RaftError<u64, openraft::error::ClientWriteError<u64, BasicNode>>,
) -> Result<RouteError, Status> {
    use openraft::error::{ClientWriteError, RaftError};
    if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &err {
        let leader_addr = forward
            .leader_node
            .as_ref()
            .map(|n| n.addr.clone())
            .or_else(|| {
                forward
                    .leader_id
                    .and_then(|id| service.registry.peer_addr(group, id))
            })
            .unwrap_or_default();
        return Ok(route_error(
            meta::route_error::Kind::NotLeader,
            group,
            &leader_addr,
        ));
    }
    Err(Status::internal(format!("partition {group} write: {err}")))
}

/// Decodes the namespace wire enum.
fn decode_ns(ns: i32) -> Result<Namespace, Status> {
    match NsMode::try_from(ns) {
        Ok(NsMode::Flat) => Ok(Namespace::Flat),
        Ok(NsMode::Hier) => Ok(Namespace::Hier),
        _ => Err(Status::invalid_argument("ns must be FLAT or HIER")),
    }
}

/// Builds a bound: `(bucket, routing_key)` or `None` for unbounded.
fn decode_bound(bucket: u64, routing_key: Vec<u8>, unbounded: bool) -> Option<(BucketId, Vec<u8>)> {
    (!unbounded).then(|| (BucketId::new(bucket), routing_key))
}

/// Decodes one wire slice reference.
fn decode_slice(slice: meta::SliceRef) -> Slice {
    Slice {
        chunk_id: ChunkId::new(slice.chunk_id),
        blob_ids: slice
            .blob_ids
            .iter()
            .map(|&b| BlobId::from_raw(b))
            .collect(),
        blob_size: slice.blob_size,
    }
}

/// Encodes one slice to its wire reference (the GetObjectMeta read path).
fn slice_to_proto(slice: &Slice) -> meta::SliceRef {
    meta::SliceRef {
        chunk_id: slice.chunk_id.get(),
        blob_ids: slice.blob_ids.iter().map(|b| b.as_u64()).collect(),
        blob_size: slice.blob_size,
    }
}

/// Projects a stored head to its wire view.
fn head_to_proto(head: &ObjectHead) -> ObjectHeadView {
    let (inline, embedded) = match &head.content {
        ContentHead::Inline(_) => (true, 0),
        ContentHead::Slices(slices) => (false, slices.len() as u32),
    };
    ObjectHeadView {
        size: head.size,
        etag: head.etag.to_vec(),
        mtime: head.mtime,
        inline,
        embedded_slices: embedded,
        seg_count: head.seg_count,
        // Empty string = unset on the wire (proto3 has no optional string here);
        // the gateway maps empty back to "no header".
        content_type: head.http.content_type.clone().unwrap_or_default(),
        content_encoding: head.http.content_encoding.clone().unwrap_or_default(),
        cache_control: head.http.cache_control.clone().unwrap_or_default(),
        user_metadata: head.http.user.clone().into_iter().collect(),
    }
}

/// Decodes the wire HTTP metadata into the stored form, mapping empty strings
/// back to "unset" (proto3 cannot distinguish empty from absent for `string`).
fn decode_http_meta(
    content_type: String,
    content_encoding: String,
    cache_control: String,
    user: std::collections::HashMap<String, String>,
) -> crate::ns_common::HttpMeta {
    let non_empty = |s: String| if s.is_empty() { None } else { Some(s) };
    crate::ns_common::HttpMeta {
        content_type: non_empty(content_type),
        content_encoding: non_empty(content_encoding),
        cache_control: non_empty(cache_control),
        user: user.into_iter().collect(),
    }
}

/// Server-side cap on one list page (03 §5: LIST 分页).
const LIST_MAX_LIMIT: usize = 1000;

#[tonic::async_trait]
impl MetaNode for MetaNodeService {
    async fn create_raft_group(
        &self,
        request: Request<CreateRaftGroupRequest>,
    ) -> Result<Response<CreateRaftGroupResponse>, Status> {
        let req = request.into_inner();
        let ns = decode_ns(req.ns)?;
        let range = PartitionRange {
            ns,
            start: decode_bound(req.start_bucket, req.start_key, req.start_unbounded),
            end: decode_bound(req.end_bucket, req.end_key, req.end_unbounded),
        };
        let peers: Vec<(u64, String)> = req
            .peers
            .iter()
            .map(|p| (u64::from(p.node_id), p.addr.clone()))
            .collect();

        // Idempotent per node (01 §5: every peer receives the same push; PD
        // retries and heartbeat reconciliation re-push).
        if self.manager.raft(req.partition_id).is_none() {
            match self
                .manager
                .create_group(req.partition_id, range.clone())
                .await
            {
                Ok(_) => {}
                Err(MetaError::GroupExists(_)) => {}
                Err(e) => return Err(Status::internal(format!("create group: {e}"))),
            }
        }
        self.registry.register(
            req.partition_id,
            PartitionInfo {
                range,
                peers: peers.clone(),
            },
        );

        // The designated bootstrap node forms the cluster membership exactly
        // once; a restart replays the persisted membership instead (03 §8).
        if req.bootstrap_node_id == self.node_id {
            let raft = self
                .manager
                .raft(req.partition_id)
                .ok_or_else(|| Status::internal("group missing after create"))?;
            let initialized = raft
                .is_initialized()
                .await
                .map_err(|e| Status::internal(format!("check initialized: {e}")))?;
            if !initialized {
                let members: BTreeMap<u64, BasicNode> = peers
                    .into_iter()
                    .map(|(id, addr)| (id, BasicNode::new(addr)))
                    .collect();
                raft.initialize(members)
                    .await
                    .map_err(|e| Status::internal(format!("initialize: {e}")))?;
                info!(
                    partition = req.partition_id,
                    "bootstrapped partition raft group"
                );
            }
        }
        Ok(Response::new(CreateRaftGroupResponse {}))
    }

    async fn suggest_split_point(
        &self,
        request: Request<SuggestSplitPointRequest>,
    ) -> Result<Response<SuggestSplitPointResponse>, Status> {
        let req = request.into_inner();
        let Some(raft) = self.manager.raft(req.partition_id) else {
            // Group not hosted here — the scheduler refreshes its route view.
            return Ok(Response::new(SuggestSplitPointResponse {
                error: Some(route_error(
                    meta::route_error::Kind::PartitionMoved,
                    req.partition_id,
                    "",
                )),
                splittable: false,
                at_bucket: 0,
                at_routing_key: Vec::new(),
            }));
        };
        if let Err(error) = self.check_leader(req.partition_id, &raft) {
            return Ok(Response::new(SuggestSplitPointResponse {
                error: Some(error),
                splittable: false,
                at_bucket: 0,
                at_routing_key: Vec::new(),
            }));
        }
        let Some(info) = self.registry.info(req.partition_id) else {
            return Err(Status::internal("partition info missing for hosted group"));
        };
        // Pick a median object/directory-entry key strictly inside the range
        // (03 §2). The MetaNode holds the data, so it — not PD — chooses.
        let at = self.pick_split_point(&info.range)?;
        match at {
            Some((bucket, routing_key)) => Ok(Response::new(SuggestSplitPointResponse {
                error: None,
                splittable: true,
                at_bucket: bucket,
                at_routing_key: routing_key,
            })),
            None => Ok(Response::new(SuggestSplitPointResponse {
                error: None,
                splittable: false,
                at_bucket: 0,
                at_routing_key: Vec::new(),
            })),
        }
    }

    async fn apply_split(
        &self,
        request: Request<ApplySplitRequest>,
    ) -> Result<Response<ApplySplitResponse>, Status> {
        use crate::raft::SplitOp;
        let req = request.into_inner();
        let Some(raft) = self.manager.raft(req.partition_id) else {
            return Ok(Response::new(ApplySplitResponse {
                error: Some(route_error(
                    meta::route_error::Kind::PartitionMoved,
                    req.partition_id,
                    "",
                )),
            }));
        };
        let child_peers: Vec<(u64, String)> = req
            .child_peers
            .iter()
            .map(|p| (u64::from(p.node_id), p.addr.clone()))
            .collect();
        // Propose the deterministic in-log split into the parent group (03 §2/§8):
        // all replicas split at the same log position; a replay whose boundary is
        // already outside the narrowed live range is a no-op.
        let op = MetaEntry::Split(SplitOp {
            child_group: req.child_group,
            at_bucket: req.at_bucket,
            at_routing_key: req.at_routing_key,
            child_peers,
            bootstrap_node: u64::from(req.bootstrap_node_id),
            child_ino_tag: req.child_ino_tag,
        });
        match raft.client_write(op).await {
            Ok(_) => Ok(Response::new(ApplySplitResponse { error: None })),
            Err(err) => {
                let error = write_error(self, req.partition_id, err)?;
                Ok(Response::new(ApplySplitResponse { error: Some(error) }))
            }
        }
    }

    async fn prepare_migrate_target(
        &self,
        request: Request<PrepareMigrateTargetRequest>,
    ) -> Result<Response<PrepareMigrateTargetResponse>, Status> {
        let req = request.into_inner();
        let ns = decode_ns(req.ns)?;
        let range = PartitionRange {
            ns,
            start: decode_bound(req.start_bucket, req.start_key, req.start_unbounded),
            end: decode_bound(req.end_bucket, req.end_key, req.end_unbounded),
        };
        let peers: Vec<(u64, String)> = req
            .peers
            .iter()
            .map(|p| (u64::from(p.node_id), p.addr.clone()))
            .collect();
        // Pre-create the (un-initialized) group so the source leader can add it
        // as a learner (03 §2 迁移). Idempotent per node.
        self.manager
            .ensure_group_present(req.partition_id, range.clone())
            .await
            .map_err(|e| Status::internal(format!("ensure group present: {e}")))?;
        self.registry
            .register(req.partition_id, PartitionInfo { range, peers });
        Ok(Response::new(PrepareMigrateTargetResponse {}))
    }

    async fn migrate_group_member(
        &self,
        request: Request<MigrateGroupMemberRequest>,
    ) -> Result<Response<MigrateGroupMemberResponse>, Status> {
        let req = request.into_inner();
        let Some(raft) = self.manager.raft(req.partition_id) else {
            return Ok(Response::new(MigrateGroupMemberResponse {
                error: Some(route_error(
                    meta::route_error::Kind::PartitionMoved,
                    req.partition_id,
                    "",
                )),
            }));
        };
        if let Err(error) = self.check_leader(req.partition_id, &raft) {
            return Ok(Response::new(MigrateGroupMemberResponse {
                error: Some(error),
            }));
        }
        let to_addr = req
            .voters
            .iter()
            .find(|p| p.node_id == req.to_node_id)
            .map(|p| p.addr.clone())
            .unwrap_or_default();
        if to_addr.is_empty() {
            return Err(Status::invalid_argument(
                "migrate target address missing from voter set",
            ));
        }
        let current: std::collections::BTreeSet<u64> = req
            .voters
            .iter()
            .map(|p| u64::from(p.node_id))
            .chain(std::iter::once(u64::from(req.from_node_id)))
            .filter(|id| *id != u64::from(req.to_node_id))
            .collect();
        crate::raft::migrate::migrate_replica(
            &raft,
            u64::from(req.from_node_id),
            u64::from(req.to_node_id),
            to_addr,
            &current,
        )
        .await
        .map_err(|e| Status::internal(format!("migrate replica: {e}")))?;
        Ok(Response::new(MigrateGroupMemberResponse { error: None }))
    }

    async fn put_object(
        &self,
        request: Request<PutObjectRequest>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let (group, raft) = match self.route(bucket, &req.key) {
            Ok(ok) => ok,
            Err(error) => return Ok(Response::new(PutObjectResponse { error: Some(error) })),
        };
        if let Err(error) = self.check_leader(group, &raft) {
            return Ok(Response::new(PutObjectResponse { error: Some(error) }));
        }
        // Inline admission (03 §4.3 护栏): threshold + the partition ratio
        // guard. A rejected inline PUT downgrades to EC at the gateway
        // (resource_exhausted; the S3 mapping lands in M6). Bucket-level
        // threshold overrides ride the bucket-policy plumbing (99 登记).
        if req.slices.is_empty() {
            let guard = self
                .manager
                .guard(group)
                .ok_or_else(|| Status::internal("partition guard missing"))?;
            let threshold =
                crate::guard::resolve_threshold(crate::guard::DEFAULT_INLINE_THRESHOLD, None);
            if let Err(reject) = guard.check_inline(req.inline_data.len() as u64, threshold) {
                return Err(Status::resource_exhausted(format!(
                    "inline rejected ({reject}); retry with EC slices"
                )));
            }
        }
        let content = if req.slices.is_empty() {
            ContentHead::Inline(req.inline_data)
        } else {
            ContentHead::Slices(req.slices.into_iter().map(decode_slice).collect())
        };
        let head = ObjectHead {
            size: req.size,
            etag: <[u8; 16]>::try_from(req.etag.as_slice())
                .map_err(|_| Status::invalid_argument("etag must be 16 bytes"))?,
            mtime: req.ts_millis,
            storage: StorageClass::Standard,
            content,
            seg_count: 0,
            http: decode_http_meta(
                req.content_type,
                req.content_encoding,
                req.cache_control,
                req.user_metadata,
            ),
        };
        let entry = MetaEntry::Flat(FlatOp::Put {
            bucket,
            key: req.key,
            head,
            ts_millis: req.ts_millis,
        });
        match raft.client_write(entry).await {
            Ok(_) => Ok(Response::new(PutObjectResponse { error: None })),
            Err(err) => Ok(Response::new(PutObjectResponse {
                error: Some(write_error(self, group, err)?),
            })),
        }
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let (group, raft) = match self.route(bucket, &req.key) {
            Ok(ok) => ok,
            Err(error) => {
                return Ok(Response::new(HeadObjectResponse {
                    error: Some(error),
                    found: false,
                    head: None,
                }));
            }
        };
        // Leader ReadIndex (03 §5): confirms leadership against a quorum and
        // waits for the apply position to cover it.
        if let Err(error) = self.linearize(group, &raft).await {
            return Ok(Response::new(HeadObjectResponse {
                error: Some(error),
                found: false,
                head: None,
            }));
        }
        let head = ns_flat::get_head(self.manager.store().as_ref(), bucket, &req.key)
            .map_err(|e| Status::internal(format!("read head: {e}")))?;
        Ok(Response::new(HeadObjectResponse {
            error: None,
            found: head.is_some(),
            head: head.as_ref().map(head_to_proto),
        }))
    }

    async fn get_object_meta(
        &self,
        request: Request<GetObjectMetaRequest>,
    ) -> Result<Response<GetObjectMetaResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let miss = |error: Option<RouteError>| {
            Ok(Response::new(GetObjectMetaResponse {
                error,
                found: false,
                head: None,
                inline_data: Vec::new(),
                slices: Vec::new(),
            }))
        };
        let (group, raft) = match self.route(bucket, &req.key) {
            Ok(ok) => ok,
            Err(error) => return miss(Some(error)),
        };
        if let Err(error) = self.linearize(group, &raft).await {
            return miss(Some(error));
        }
        let store = self.manager.store();
        let Some(head) = ns_flat::get_head(store.as_ref(), bucket, &req.key)
            .map_err(|e| Status::internal(format!("read head: {e}")))?
        else {
            return miss(None);
        };
        // Inline object: return the bytes; no slices to assemble.
        if let ContentHead::Inline(bytes) = &head.content {
            return Ok(Response::new(GetObjectMetaResponse {
                error: None,
                found: true,
                head: Some(head_to_proto(&head)),
                inline_data: bytes.clone(),
                slices: Vec::new(),
            }));
        }
        // EC object: head-embedded slices, then every overflow segment in order
        // (03 §4.2 — the full list a GET reconstructs from).
        let mut slices: Vec<meta::SliceRef> = match &head.content {
            ContentHead::Slices(embedded) => embedded.iter().map(slice_to_proto).collect(),
            ContentHead::Inline(_) => unreachable!("inline handled above"),
        };
        for seg_no in 0..head.seg_count {
            let segment = ns_flat::get_segment(store.as_ref(), bucket, &req.key, seg_no)
                .map_err(|e| Status::internal(format!("read segment {seg_no}: {e}")))?
                .ok_or_else(|| {
                    Status::internal(format!(
                        "object head claims segment {seg_no} but it is absent"
                    ))
                })?;
            slices.extend(segment.slices.iter().map(slice_to_proto));
        }
        Ok(Response::new(GetObjectMetaResponse {
            error: None,
            found: true,
            head: Some(head_to_proto(&head)),
            inline_data: Vec::new(),
            slices,
        }))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let (group, raft) = match self.route(bucket, &req.key) {
            Ok(ok) => ok,
            Err(error) => return Ok(Response::new(DeleteObjectResponse { error: Some(error) })),
        };
        if let Err(error) = self.check_leader(group, &raft) {
            return Ok(Response::new(DeleteObjectResponse { error: Some(error) }));
        }
        let entry = MetaEntry::Flat(FlatOp::Delete {
            bucket,
            key: req.key,
            ts_millis: req.ts_millis,
        });
        match raft.client_write(entry).await {
            Ok(_) => Ok(Response::new(DeleteObjectResponse { error: None })),
            Err(err) => Ok(Response::new(DeleteObjectResponse {
                error: Some(write_error(self, group, err)?),
            })),
        }
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        // List routes by the scan start (start_after wins over prefix when
        // both are set — it is strictly past the prefix, 03 §5 跨分区拼接 is a
        // gateway concern for M6).
        let route_key = if req.start_after.is_empty() {
            req.prefix.clone()
        } else {
            req.start_after.clone()
        };
        let (group, raft) = match self.route(bucket, &route_key) {
            Ok(ok) => ok,
            Err(error) => {
                return Ok(Response::new(ListObjectsResponse {
                    error: Some(error),
                    entries: Vec::new(),
                }));
            }
        };
        if let Err(error) = self.linearize(group, &raft).await {
            return Ok(Response::new(ListObjectsResponse {
                error: Some(error),
                entries: Vec::new(),
            }));
        }
        let limit = (req.limit as usize).clamp(1, LIST_MAX_LIMIT);
        let start_after = (!req.start_after.is_empty()).then_some(req.start_after.as_slice());
        let page = ns_flat::list(
            self.manager.store().as_ref(),
            bucket,
            &req.prefix,
            start_after,
            limit,
        )
        .map_err(|e| Status::internal(format!("list: {e}")))?;
        let entries = page
            .iter()
            .map(|(key, head)| ObjectEntry {
                key: key.clone(),
                head: Some(head_to_proto(head)),
            })
            .collect();
        Ok(Response::new(ListObjectsResponse {
            error: None,
            entries,
        }))
    }

    async fn create_multipart_upload(
        &self,
        request: Request<CreateMultipartUploadRequest>,
    ) -> Result<Response<CreateMultipartUploadResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let upload_id = decode_upload_id(&req.upload_id)?;
        let op = FlatOp::CreateMultipart {
            bucket,
            key: req.key,
            upload_id,
            ts_millis: req.ts_millis,
        };
        match self.propose(bucket, op).await? {
            Ok(_) => Ok(Response::new(CreateMultipartUploadResponse { error: None })),
            Err(error) => Ok(Response::new(CreateMultipartUploadResponse {
                error: Some(error),
            })),
        }
    }

    async fn put_part(
        &self,
        request: Request<PutPartRequest>,
    ) -> Result<Response<PutPartResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let upload_id = decode_upload_id(&req.upload_id)?;
        let part = crate::ns_flat::multipart::PartMeta {
            size: req.size,
            etag: <[u8; 16]>::try_from(req.etag.as_slice())
                .map_err(|_| Status::invalid_argument("etag must be 16 bytes"))?,
            slices: req.slices.into_iter().map(decode_slice).collect(),
        };
        let op = FlatOp::PutPart {
            bucket,
            key: req.key,
            upload_id,
            part_no: req.part_no,
            part,
            ts_millis: req.ts_millis,
        };
        match self.propose(bucket, op).await? {
            Ok(ns_flat::MetaResponse::None) => Ok(Response::new(PutPartResponse {
                error: None,
                rejected: String::new(),
            })),
            Ok(ns_flat::MetaResponse::Rejected(rejected)) => Ok(Response::new(PutPartResponse {
                error: None,
                rejected,
            })),
            Ok(other) => Err(Status::internal(format!(
                "unexpected put_part response: {other:?}"
            ))),
            Err(error) => Ok(Response::new(PutPartResponse {
                error: Some(error),
                rejected: String::new(),
            })),
        }
    }

    async fn complete_multipart(
        &self,
        request: Request<CompleteMultipartRequest>,
    ) -> Result<Response<CompleteMultipartResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let upload_id = decode_upload_id(&req.upload_id)?;
        let mut parts = Vec::with_capacity(req.parts.len());
        for part in req.parts {
            parts.push(crate::ns_flat::multipart::PartRef {
                part_no: part.part_no,
                etag: <[u8; 16]>::try_from(part.etag.as_slice())
                    .map_err(|_| Status::invalid_argument("part etag must be 16 bytes"))?,
            });
        }
        let op = FlatOp::CompleteMultipart {
            bucket,
            key: req.key,
            upload_id,
            parts,
            ts_millis: req.ts_millis,
        };
        match self.propose(bucket, op).await? {
            Ok(ns_flat::MetaResponse::None) => Ok(Response::new(CompleteMultipartResponse {
                error: None,
                rejected: String::new(),
            })),
            Ok(ns_flat::MetaResponse::Rejected(rejected)) => {
                Ok(Response::new(CompleteMultipartResponse {
                    error: None,
                    rejected,
                }))
            }
            Ok(other) => Err(Status::internal(format!(
                "unexpected complete_multipart response: {other:?}"
            ))),
            Err(error) => Ok(Response::new(CompleteMultipartResponse {
                error: Some(error),
                rejected: String::new(),
            })),
        }
    }

    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartUploadRequest>,
    ) -> Result<Response<AbortMultipartUploadResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let upload_id = decode_upload_id(&req.upload_id)?;
        let op = FlatOp::AbortMultipart {
            bucket,
            key: req.key,
            upload_id,
            ts_millis: req.ts_millis,
        };
        match self.propose(bucket, op).await? {
            Ok(_) => Ok(Response::new(AbortMultipartUploadResponse { error: None })),
            Err(error) => Ok(Response::new(AbortMultipartUploadResponse {
                error: Some(error),
            })),
        }
    }

    async fn delq_backlog(
        &self,
        request: Request<DelqBacklogRequest>,
    ) -> Result<Response<DelqBacklogResponse>, Status> {
        let req = request.into_inner();
        // The delq depth is a live gauge on the partition guard, maintained
        // incrementally by apply (08 §5.1) — an O(1) read, no queue scan. A
        // group not hosted here has no guard.
        match self.manager.guard(req.partition_id) {
            Some(guard) => Ok(Response::new(DelqBacklogResponse {
                hosted: true,
                entries: guard.delq_depth(),
            })),
            None => Ok(Response::new(DelqBacklogResponse {
                hosted: false,
                entries: 0,
            })),
        }
    }

    async fn hier_lookup(
        &self,
        request: Request<meta::HierLookupRequest>,
    ) -> Result<Response<meta::HierLookupResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let routing = hier_routing_key(req.parent_ino, &req.name);
        let (group, raft) = match self.route(bucket, &routing) {
            Ok(ok) => ok,
            Err(error) => return Ok(Response::new(hier_lookup_err(error))),
        };
        if let Err(error) = self.linearize(group, &raft).await {
            return Ok(Response::new(hier_lookup_err(error)));
        }
        let record = ns_hier::lookup(
            self.manager.store().as_ref(),
            bucket,
            req.parent_ino,
            &req.name,
        )
        .map_err(|e| Status::internal(format!("hier lookup: {e}")))?;
        let (kind, file, dir_ino, inline_data, slices) = match record {
            Some(ns_hier::FsRecord::File(f)) => {
                let view = file_to_head_view(&f);
                let (inline_data, slices) = match &f.content {
                    ContentHead::Inline(bytes) => (bytes.clone(), Vec::new()),
                    ContentHead::Slices(sl) => {
                        (Vec::new(), sl.iter().map(slice_to_proto).collect())
                    }
                };
                (
                    meta::HierEntryKind::HierEntryFile,
                    Some(view),
                    0,
                    inline_data,
                    slices,
                )
            }
            Some(ns_hier::FsRecord::Dir(d)) => (
                meta::HierEntryKind::HierEntryDir,
                None,
                d.ino,
                Vec::new(),
                Vec::new(),
            ),
            // A sentinel is a directory too (lookup of name="" — getattr).
            Some(ns_hier::FsRecord::Sentinel(s)) => (
                meta::HierEntryKind::HierEntryDir,
                None,
                s.ino,
                Vec::new(),
                Vec::new(),
            ),
            None => (
                meta::HierEntryKind::HierEntryUnspecified,
                None,
                0,
                Vec::new(),
                Vec::new(),
            ),
        };
        Ok(Response::new(meta::HierLookupResponse {
            error: None,
            kind: kind as i32,
            file,
            dir_ino,
            inline_data,
            slices,
        }))
    }

    async fn hier_write(
        &self,
        request: Request<meta::HierWriteRequest>,
    ) -> Result<Response<meta::HierWriteResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let etag = <[u8; 16]>::try_from(req.etag.as_slice())
            .map_err(|_| Status::invalid_argument("etag must be 16 bytes"))?;
        let content = if req.slices.is_empty() {
            ContentHead::Inline(req.inline_data)
        } else {
            ContentHead::Slices(req.slices.into_iter().map(decode_slice).collect())
        };
        let op = ns_hier::HierOp::Write {
            bucket,
            parent_ino: req.parent_ino,
            name: req.name.clone(),
            size: req.size,
            etag,
            content,
            http: decode_http_meta(
                req.content_type,
                req.content_encoding,
                req.cache_control,
                req.user_metadata,
            ),
            ts_millis: req.ts_millis,
        };
        match self
            .propose_hier(bucket, req.parent_ino, &req.name, op)
            .await?
        {
            Ok(resp) => Ok(Response::new(meta::HierWriteResponse {
                error: None,
                rejected: rejected_reason(&resp),
            })),
            Err(error) => Ok(Response::new(meta::HierWriteResponse {
                error: Some(error),
                rejected: String::new(),
            })),
        }
    }

    async fn hier_unlink(
        &self,
        request: Request<meta::HierUnlinkRequest>,
    ) -> Result<Response<meta::HierUnlinkResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let op = ns_hier::HierOp::Unlink {
            bucket,
            parent_ino: req.parent_ino,
            name: req.name.clone(),
            ts_millis: req.ts_millis,
        };
        match self
            .propose_hier(bucket, req.parent_ino, &req.name, op)
            .await?
        {
            Ok(resp) => Ok(Response::new(meta::HierUnlinkResponse {
                error: None,
                rejected: rejected_reason(&resp),
            })),
            Err(error) => Ok(Response::new(meta::HierUnlinkResponse {
                error: Some(error),
                rejected: String::new(),
            })),
        }
    }

    async fn hier_mkdir_sentinel(
        &self,
        request: Request<meta::HierMkdirSentinelRequest>,
    ) -> Result<Response<meta::HierMkdirSentinelResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        // The sentinel is minted in the child's own partition — route by the
        // *parent* coordinate for M6 (single-partition namespaces); the split
        // invariant (03 §6.5) keeps a child's ino in its minting partition, so
        // a multi-partition hier bucket routes the sentinel via its parent.
        let op = ns_hier::HierOp::MkdirSentinel {
            bucket,
            parent_ino: req.parent_ino,
            name: req.name.clone(),
            ts_millis: req.ts_millis,
        };
        match self
            .propose_hier(bucket, req.parent_ino, &req.name, op)
            .await?
        {
            Ok(crate::ns_common::MetaResponse::MintedIno(child_ino)) => {
                Ok(Response::new(meta::HierMkdirSentinelResponse {
                    error: None,
                    child_ino,
                }))
            }
            Ok(other) => Err(Status::internal(format!(
                "mkdir sentinel expected a minted ino, got {other:?}"
            ))),
            Err(error) => Ok(Response::new(meta::HierMkdirSentinelResponse {
                error: Some(error),
                child_ino: 0,
            })),
        }
    }

    async fn hier_mkdir_link(
        &self,
        request: Request<meta::HierMkdirLinkRequest>,
    ) -> Result<Response<meta::HierMkdirLinkResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        let op = ns_hier::HierOp::MkdirLink {
            bucket,
            parent_ino: req.parent_ino,
            name: req.name.clone(),
            child_ino: req.child_ino,
            ts_millis: req.ts_millis,
        };
        match self
            .propose_hier(bucket, req.parent_ino, &req.name, op)
            .await?
        {
            Ok(resp) => Ok(Response::new(meta::HierMkdirLinkResponse {
                error: None,
                rejected: rejected_reason(&resp),
            })),
            Err(error) => Ok(Response::new(meta::HierMkdirLinkResponse {
                error: Some(error),
                rejected: String::new(),
            })),
        }
    }

    async fn hier_readdir(
        &self,
        request: Request<meta::HierReaddirRequest>,
    ) -> Result<Response<meta::HierReaddirResponse>, Status> {
        let req = request.into_inner();
        let bucket = BucketId::new(req.bucket_id);
        // A directory's entries live in its own ino range; route by the
        // sentinel coordinate `(dir_ino, "")`.
        let routing = hier_routing_key(req.dir_ino, b"");
        let (group, raft) = match self.route(bucket, &routing) {
            Ok(ok) => ok,
            Err(error) => {
                return Ok(Response::new(meta::HierReaddirResponse {
                    error: Some(error),
                    entries: Vec::new(),
                }));
            }
        };
        if let Err(error) = self.linearize(group, &raft).await {
            return Ok(Response::new(meta::HierReaddirResponse {
                error: Some(error),
                entries: Vec::new(),
            }));
        }
        let start_after = (!req.start_after.is_empty()).then_some(req.start_after.as_slice());
        let limit = (req.limit as usize).clamp(1, LIST_MAX_LIMIT);
        let page = ns_hier::readdir(
            self.manager.store().as_ref(),
            bucket,
            req.dir_ino,
            start_after,
            limit,
        )
        .map_err(|e| Status::internal(format!("readdir: {e}")))?;
        let entries = page
            .into_iter()
            .map(|(name, record)| match record {
                ns_hier::FsRecord::File(f) => meta::HierDirEntry {
                    name,
                    kind: meta::HierEntryKind::HierEntryFile as i32,
                    file: Some(file_to_head_view(&f)),
                    dir_ino: 0,
                },
                ns_hier::FsRecord::Dir(d) => meta::HierDirEntry {
                    name,
                    kind: meta::HierEntryKind::HierEntryDir as i32,
                    file: None,
                    dir_ino: d.ino,
                },
                ns_hier::FsRecord::Sentinel(s) => meta::HierDirEntry {
                    name,
                    kind: meta::HierEntryKind::HierEntryDir as i32,
                    file: None,
                    dir_ino: s.ino,
                },
            })
            .collect();
        Ok(Response::new(meta::HierReaddirResponse {
            error: None,
            entries,
        }))
    }

    async fn export_references(
        &self,
        request: Request<meta::ExportReferencesRequest>,
    ) -> Result<Response<meta::ExportReferencesResponse>, Status> {
        let req = request.into_inner();
        // Must host the partition and be its leader (a linearizable read of the
        // whole primary index — a follower's view could miss a just-committed
        // reference and wrongly expose a live blob to GC).
        let Some(info) = self.registry.info(req.partition_id) else {
            return Ok(Response::new(meta::ExportReferencesResponse {
                hosted: false,
                blob_ids: Vec::new(),
            }));
        };
        let Some(raft) = self.manager.raft(req.partition_id) else {
            return Ok(Response::new(meta::ExportReferencesResponse {
                hosted: false,
                blob_ids: Vec::new(),
            }));
        };
        if let Err(err) = self.linearize(req.partition_id, &raft).await {
            return Err(Status::failed_precondition(format!(
                "export_references not leader: {err:?}"
            )));
        }
        let refs = crate::gc_export::export_references(
            self.manager.store().as_ref(),
            &crate::ref_extractor::EpochRefExtractor,
            &info.range,
        )
        .map_err(|e| Status::internal(format!("export_references scan: {e}")))?;
        Ok(Response::new(meta::ExportReferencesResponse {
            hosted: true,
            blob_ids: refs.into_iter().collect(),
        }))
    }
}
fn decode_upload_id(bytes: &[u8]) -> Result<u128, Status> {
    <[u8; 16]>::try_from(bytes)
        .map(u128::from_be_bytes)
        .map_err(|_| Status::invalid_argument("upload_id must be 16 bytes"))
}

/// The routing key of a flat op (its object key — every flat op clusters
/// under it, multipart sessions included, 03 §4.1).
fn op_key(op: &FlatOp) -> &[u8] {
    match op {
        FlatOp::Put { key, .. }
        | FlatOp::Delete { key, .. }
        | FlatOp::CreateMultipart { key, .. }
        | FlatOp::PutPart { key, .. }
        | FlatOp::CompleteMultipart { key, .. }
        | FlatOp::AbortMultipart { key, .. } => key,
    }
}

impl MetaNodeService {
    /// Leader ReadIndex for reads: `ensure_linearizable` heartbeats a quorum
    /// and waits for the apply position (03 §5: 0 轮次读 — no raft log
    /// append, just the lease check).
    async fn linearize(&self, group: u64, raft: &MetaRaft) -> Result<(), RouteError> {
        raft.ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|_| self.not_leader(group, raft.metrics().borrow().current_leader))
    }

    /// Routes a flat op to its partition leader and proposes it (03 §5: 全部
    /// 单分区单 propose). The outer `Result` separates real faults
    /// (`Status::internal`, bubbled by the caller) from the in-band outcome:
    /// `Ok(MetaResponse)` when applied, `Err(RouteError)` for a redirect the
    /// client should follow.
    async fn propose(
        &self,
        bucket: BucketId,
        op: FlatOp,
    ) -> Result<Result<ns_flat::MetaResponse, RouteError>, Status> {
        let (group, raft) = match self.route(bucket, op_key(&op)) {
            Ok(routed) => routed,
            Err(redirect) => return Ok(Err(redirect)),
        };
        if let Err(redirect) = self.check_leader(group, &raft) {
            return Ok(Err(redirect));
        }
        match raft.client_write(MetaEntry::Flat(op)).await {
            Ok(response) => Ok(Ok(response.data)),
            Err(err) => write_error(self, group, err).map(Err),
        }
    }

    /// Routes a hier op by its `(parent_ino, name)` coordinate and proposes it
    /// (03 §6). Same outer/inner `Result` split as [`propose`](Self::propose).
    async fn propose_hier(
        &self,
        bucket: BucketId,
        parent_ino: u64,
        name: &[u8],
        op: ns_hier::HierOp,
    ) -> Result<Result<crate::ns_common::MetaResponse, RouteError>, Status> {
        let routing = hier_routing_key(parent_ino, name);
        let (group, raft) = match self.route(bucket, &routing) {
            Ok(routed) => routed,
            Err(redirect) => return Ok(Err(redirect)),
        };
        if let Err(redirect) = self.check_leader(group, &raft) {
            return Ok(Err(redirect));
        }
        match raft.client_write(MetaEntry::Hier(op)).await {
            Ok(response) => Ok(Ok(response.data)),
            Err(err) => write_error(self, group, err).map(Err),
        }
    }
}

/// The `rejected` string of a hier apply response (empty when applied).
fn rejected_reason(resp: &crate::ns_common::MetaResponse) -> String {
    match resp {
        crate::ns_common::MetaResponse::Rejected(msg) => msg.clone(),
        _ => String::new(),
    }
}

/// A HierLookup response carrying an in-band route error.
fn hier_lookup_err(error: RouteError) -> meta::HierLookupResponse {
    meta::HierLookupResponse {
        error: Some(error),
        kind: meta::HierEntryKind::HierEntryUnspecified as i32,
        file: None,
        dir_ino: 0,
        inline_data: Vec::new(),
        slices: Vec::new(),
    }
}

/// Projects a hier file record to the wire head view (mirrors `head_to_proto`
/// for flat objects).
fn file_to_head_view(file: &ns_hier::FileRecord) -> ObjectHeadView {
    let (inline, embedded) = match &file.content {
        ContentHead::Inline(_) => (true, 0),
        ContentHead::Slices(slices) => (false, slices.len() as u32),
    };
    ObjectHeadView {
        size: file.size,
        etag: file.etag.to_vec(),
        mtime: file.mtime,
        inline,
        embedded_slices: embedded,
        seg_count: file.seg_count,
        content_type: file.http.content_type.clone().unwrap_or_default(),
        content_encoding: file.http.content_encoding.clone().unwrap_or_default(),
        cache_control: file.http.cache_control.clone().unwrap_or_default(),
        user_metadata: file.http.user.clone().into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::GroupManager;

    use openraft::error::{ClientWriteError, Fatal, ForwardToLeader, RaftError};

    /// A service with no hosted groups — enough to exercise `write_error`'s
    /// classification (it only consults the registry for the leader hint).
    fn service() -> (tempfile::TempDir, MetaNodeService) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(
            crate::store::rocks::RocksEngine::open(&dir.path().join("sm")).expect("engine"),
        );
        let manager = Arc::new(
            GroupManager::open(
                &dir.path().join("raft-log"),
                1,
                engine,
                Arc::new(crate::ref_extractor::EpochRefExtractor),
                openraft::Config {
                    cluster_name: "write-error-test".to_string(),
                    ..Default::default()
                },
            )
            .expect("manager"),
        );
        let registry = Arc::new(PartitionRegistry::default());
        (dir, MetaNodeService::new(1, manager, registry))
    }

    type WriteErr = RaftError<u64, ClientWriteError<u64, BasicNode>>;

    #[test]
    fn forward_to_leader_maps_to_in_band_not_leader_redirect() {
        let (_dir, svc) = service();
        let err: WriteErr =
            RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
                leader_id: Some(2),
                leader_node: Some(BasicNode::new("meta-2:7100")),
            }));
        let mapped = write_error(&svc, 9, err).expect("redirect is not a fault");
        assert_eq!(mapped.kind, meta::route_error::Kind::NotLeader as i32);
        assert_eq!(mapped.partition_id, 9);
        // The redirect carries the leader hint from the error (not empty).
        assert_eq!(mapped.leader_addr, "meta-2:7100");
    }

    #[test]
    fn fatal_faults_surface_as_internal_not_a_redirect() {
        let (_dir, svc) = service();
        // A genuine engine/apply fault (not ForwardToLeader) must NOT be masked
        // as a NOT_LEADER redirect — it surfaces as gRPC internal so the client
        // stops looping against the same node and the cause stays visible.
        let err: WriteErr = RaftError::Fatal(Fatal::Panicked);
        let status = write_error(&svc, 9, err).expect_err("a fatal fault is not a redirect");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(
            status.message().contains("partition 9 write"),
            "internal status keeps the failing partition + cause: {}",
            status.message()
        );
    }
}
