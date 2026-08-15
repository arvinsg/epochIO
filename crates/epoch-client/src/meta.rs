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

//! MetaNode client: the gateway's metadata read/write path (design 01 §5,
//! 03 §5). Routes each op by `(bucket, routing_key)` to the partition that
//! owns it — resolved via PD [`GetRoute`](PdClient::get_route), cached with
//! local interval containment, and invalidated by the in-band
//! [`RouteError`](epoch_proto::grpc::meta::RouteError) a MetaNode returns when
//! it is not the leader or no longer hosts the key's partition.
//!
//! ## Routing & redirect
//!
//! - **Route resolution**: a cached [`MetaPartitionView`] whose interval
//!   contains the coordinate answers directly; otherwise PD `GetRoute` refills
//!   the cache. The MetaNode replica dialed is the partition's reported leader.
//! - **NotLeader**: the reply carries the new `leader_addr`; the client updates
//!   the cached leader and retries there.
//! - **PartitionMoved / NoPartition**: the cached route is stale; the client
//!   drops it, re-resolves via PD, and retries. A bounded retry budget guards
//!   against a routing flap.
//!
//! Connections to MetaNodes are lazy per address (like [`PdClient`]'s replica
//! channels), so construction does no I/O.
//!
//! Design: docs/design/01-pd.md §5; docs/design/03-metanode.md §5

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use epoch_proto::grpc::meta;
use epoch_proto::grpc::pd::{MetaPartitionView, NsMode};
use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;
use crate::pd::PdClient;

/// The generated MetaNode client bound to a transport channel.
type Stub = meta::meta_node_client::MetaNodeClient<Channel>;

/// Attempts before giving up on a routing flap (each is a re-resolve + retry).
const MAX_ROUTE_ATTEMPTS: usize = 4;

/// A leader-following MetaNode client with a PD-backed route cache.
///
/// Cheap to clone (shares the PD client, the route cache, and the channel
/// pool). Every op resolves its partition, dials that partition's leader, and
/// follows `RouteError` redirects.
#[derive(Clone)]
pub struct MetaClient {
    pd: PdClient,
    /// Resolved routes, keyed by namespace + bucket. Each entry is a partition
    /// view whose interval is checked locally before a re-resolve.
    routes: Arc<Mutex<Vec<CachedRoute>>>,
    /// Lazy MetaNode channels keyed by `host:port`.
    channels: Arc<Mutex<HashMap<String, Stub>>>,
}

/// A cached partition route (its interval + current leader address).
#[derive(Clone)]
struct CachedRoute {
    view: MetaPartitionView,
}

impl CachedRoute {
    /// Whether this route's interval covers `(ns, bucket, routing_key)`
    /// (start-inclusive, end-exclusive; the PD-side containment, mirrored so a
    /// hot key needs no PD round-trip, 03 §2).
    fn contains(&self, ns: NsMode, bucket: u64, routing_key: &[u8]) -> bool {
        let v = &self.view;
        if v.ns != ns as i32 {
            return false;
        }
        let coord = (bucket, routing_key);
        let after_start = v.start_unbounded || coord >= (v.start_bucket, v.start_key.as_slice());
        let before_end = v.end_unbounded || coord < (v.end_bucket, v.end_key.as_slice());
        after_start && before_end
    }
}

impl MetaClient {
    /// Builds a client over a PD client (for route resolution). MetaNode
    /// channels are dialed lazily as routes resolve.
    #[must_use]
    pub fn new(pd: PdClient) -> Self {
        Self {
            pd,
            routes: Arc::new(Mutex::new(Vec::new())),
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn routes(&self) -> std::sync::MutexGuard<'_, Vec<CachedRoute>> {
        self.routes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A cached route covering the coordinate, if any.
    fn cached(&self, ns: NsMode, bucket: u64, routing_key: &[u8]) -> Option<CachedRoute> {
        self.routes()
            .iter()
            .find(|r| r.contains(ns, bucket, routing_key))
            .cloned()
    }

    /// Inserts (or replaces) a resolved route, evicting any it overlaps so the
    /// cache never holds two routes covering one coordinate (a split/migrate
    /// changed the table).
    fn cache_route(&self, view: MetaPartitionView) {
        let mut routes = self.routes();
        routes.retain(|r| r.view.partition_id != view.partition_id);
        routes.push(CachedRoute { view });
    }

    /// Drops the cached route for a partition id (PartitionMoved / stale).
    fn evict(&self, partition_id: u64) {
        self.routes()
            .retain(|r| r.view.partition_id != partition_id);
    }

    /// Updates the cached leader address of a partition (NotLeader redirect).
    fn update_leader(&self, partition_id: u64, leader_addr: &str) {
        if leader_addr.is_empty() {
            return;
        }
        for r in self.routes().iter_mut() {
            if r.view.partition_id == partition_id {
                r.view.leader_addr = leader_addr.to_string();
            }
        }
    }

    /// Resolves the partition covering the coordinate, using the cache when it
    /// holds a covering route, else PD `GetRoute`.
    async fn resolve(
        &self,
        ns: NsMode,
        bucket: u64,
        routing_key: &[u8],
    ) -> Result<CachedRoute, ClientError> {
        if let Some(route) = self.cached(ns, bucket, routing_key) {
            return Ok(route);
        }
        let view = self
            .pd
            .get_route(ns, bucket, routing_key.to_vec())
            .await?
            .ok_or_else(|| {
                ClientError::NotFound(format!(
                    "no partition covers bucket {bucket} key {routing_key:?}"
                ))
            })?;
        self.cache_route(view.clone());
        Ok(CachedRoute { view })
    }

    /// A lazily-dialed MetaNode stub for `addr` (`host:port`).
    fn stub(&self, addr: &str) -> Result<Stub, ClientError> {
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(stub) = channels.get(addr) {
            return Ok(stub.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{addr}")).map_err(|source| {
            ClientError::Endpoint {
                endpoint: addr.to_string(),
                source,
            }
        })?;
        let stub = Stub::new(endpoint.connect_lazy());
        channels.insert(addr.to_string(), stub.clone());
        Ok(stub)
    }
}

/// Classifies an in-band [`RouteError`] into the redirect action to take.
fn classify(error: Option<meta::RouteError>) -> Option<RouteAction> {
    let error = error?;
    match meta::route_error::Kind::try_from(error.kind)
        .unwrap_or(meta::route_error::Kind::Unspecified)
    {
        meta::route_error::Kind::NotLeader => Some(RouteAction::NotLeader {
            leader_addr: error.leader_addr,
        }),
        meta::route_error::Kind::PartitionMoved | meta::route_error::Kind::NoPartition => {
            Some(RouteAction::Moved)
        }
        meta::route_error::Kind::Unspecified => None,
    }
}

enum RouteAction {
    NotLeader { leader_addr: String },
    Moved,
}

impl MetaClient {
    /// Runs a routed MetaNode op with redirect handling (01 §5). `run` receives
    /// a stub dialed at the partition leader and returns the RPC response;
    /// `route_error` extracts the response's in-band [`RouteError`] (if any).
    /// On NotLeader the leader hint is updated and retried; on
    /// PartitionMoved/NoPartition the route is evicted, re-resolved, and
    /// retried, up to [`MAX_ROUTE_ATTEMPTS`].
    async fn route_op<T, Run, Fut, Err>(
        &self,
        ns: NsMode,
        bucket: u64,
        routing_key: &[u8],
        run: Run,
        route_error: Err,
    ) -> Result<T, ClientError>
    where
        Run: Fn(Stub) -> Fut,
        Fut: std::future::Future<Output = Result<T, tonic::Status>>,
        Err: Fn(&T) -> Option<meta::RouteError>,
    {
        let mut last = String::new();
        for _ in 0..MAX_ROUTE_ATTEMPTS {
            let route = self.resolve(ns, bucket, routing_key).await?;
            let partition_id = route.view.partition_id;
            let addr = &route.view.leader_addr;
            if addr.is_empty() {
                // No known leader yet (PD has no leader report); re-resolve
                // after evicting, then let the loop retry.
                self.evict(partition_id);
                last = format!("partition {partition_id} has no known leader");
                continue;
            }
            let stub = self.stub(addr)?;
            let response = run(stub).await.map_err(|status| {
                // A transport/leader failure: drop the route so the next
                // attempt re-resolves against a possibly-new leader.
                self.evict(partition_id);
                ClientError::from_status(&status)
            })?;
            match classify(route_error(&response)) {
                None => return Ok(response),
                Some(RouteAction::NotLeader { leader_addr }) => {
                    self.update_leader(partition_id, &leader_addr);
                    if leader_addr.is_empty() {
                        self.evict(partition_id);
                    }
                    last = format!("partition {partition_id} not-leader");
                }
                Some(RouteAction::Moved) => {
                    self.evict(partition_id);
                    last = format!("partition {partition_id} moved");
                }
            }
        }
        Err(ClientError::NoLeader {
            attempts: MAX_ROUTE_ATTEMPTS,
            last_error: last,
        })
    }
}

/// The HTTP metadata an object carries (S3 headers replayed on GET/HEAD).
///
/// `None`/empty means unset, so the gateway can omit the header rather than
/// sending an empty one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpMeta {
    /// `Content-Type`.
    pub content_type: Option<String>,
    /// `Content-Encoding`.
    pub content_encoding: Option<String>,
    /// `Cache-Control`.
    pub cache_control: Option<String>,
    /// User metadata (`x-amz-meta-*`), lowercased name → value.
    pub user: std::collections::HashMap<String, String>,
}

impl HttpMeta {
    /// Decodes the metadata a head view carries, mapping the wire's empty
    /// strings back to "unset" (proto3 cannot distinguish empty from absent for
    /// `string`, and an empty `Content-Type` header is not the same as none).
    #[must_use]
    pub fn from_view(view: &meta::ObjectHeadView) -> Self {
        let non_empty = |s: &String| (!s.is_empty()).then(|| s.clone());
        Self {
            content_type: non_empty(&view.content_type),
            content_encoding: non_empty(&view.content_encoding),
            cache_control: non_empty(&view.cache_control),
            user: view.user_metadata.clone(),
        }
    }
}

/// One object-metadata commit (03 §5 PutObject).
///
/// A request struct rather than a long parameter list: the call already carried
/// seven arguments behind a `too_many_arguments` allow, and the HTTP metadata
/// would have made eleven (AGENTS §11 — a growing parameter list means the state
/// wants grouping).
#[derive(Debug, Clone)]
pub struct PutObject<'a> {
    /// Owning bucket id.
    pub bucket: u64,
    /// Object key (also the routing key).
    pub key: &'a [u8],
    /// Object size in bytes.
    pub size: u64,
    /// S3 ETag digest.
    pub etag: [u8; 16],
    /// Inline bytes (empty when the object is EC-stored).
    pub inline_data: Vec<u8>,
    /// The EC slice list (empty for an inline object).
    pub slices: Vec<meta::SliceRef>,
    /// Proposer wall-clock millis, replicated with the entry so apply never
    /// reads a clock (03 §8).
    pub ts_millis: i64,
    /// HTTP metadata to persist with the head.
    pub http: HttpMeta,
}

/// One hier file write (03 §6.2 create/write).
///
/// A request struct for the same reason as [`PutObject`]: the call already took
/// eight arguments before the HTTP metadata (AGENTS §11).
#[derive(Debug, Clone)]
pub struct HierWrite<'a> {
    /// Owning bucket id.
    pub bucket: u64,
    /// Parent directory inode.
    pub parent_ino: u64,
    /// File name within the parent.
    pub name: &'a [u8],
    /// File size in bytes.
    pub size: u64,
    /// S3 ETag digest.
    pub etag: [u8; 16],
    /// Inline bytes (hier bodies are inline in v1).
    pub inline_data: Vec<u8>,
    /// The EC slice list (empty while hier bodies stay inline).
    pub slices: Vec<meta::SliceRef>,
    /// Proposer wall-clock millis (replicated; apply reads no clock).
    pub ts_millis: i64,
    /// HTTP metadata to persist with the record.
    pub http: HttpMeta,
}

/// Flat-namespace object operations (design 03 §5). Each routes by the object
/// key; hierarchical operations (03 §6) land with the s3compat layer (M6-4).
impl MetaClient {
    /// Commits an object's metadata after the gateway has written its data —
    /// either inline bytes or the EC slice list (03 §5 PutObject).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`]; a rejected write surfaces as
    /// the MetaNode's status.
    pub async fn put_object(&self, put: PutObject<'_>) -> Result<(), ClientError> {
        let bucket = put.bucket;
        let key = put.key.to_vec();
        let req = meta::PutObjectRequest {
            bucket_id: bucket,
            key: key.clone(),
            size: put.size,
            etag: put.etag.to_vec(),
            ts_millis: put.ts_millis,
            inline_data: put.inline_data,
            slices: put.slices,
            content_type: put.http.content_type.unwrap_or_default(),
            content_encoding: put.http.content_encoding.unwrap_or_default(),
            cache_control: put.http.cache_control.unwrap_or_default(),
            user_metadata: put.http.user,
        };
        self.route_op(
            NsMode::Flat,
            bucket,
            &key,
            |mut stub| {
                let req = req.clone();
                async move { stub.put_object(req).await.map(tonic::Response::into_inner) }
            },
            |resp| resp.error.clone(),
        )
        .await
        .map(|_| ())
    }

    /// Reads an object's lightweight head (size/etag/inline flag), or `None`
    /// if absent (03 §5 HeadObject).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn head_object(
        &self,
        bucket: u64,
        key: &[u8],
    ) -> Result<Option<meta::ObjectHeadView>, ClientError> {
        let req = meta::HeadObjectRequest {
            bucket_id: bucket,
            key: key.to_vec(),
        };
        let resp = self
            .route_op(
                NsMode::Flat,
                bucket,
                key,
                |mut stub| {
                    let req = req.clone();
                    async move { stub.head_object(req).await.map(tonic::Response::into_inner) }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok(resp.found.then(|| resp.head.unwrap_or_default()))
    }

    /// Reads an object's full slice list for a GET (M6 read path): the head +
    /// every overflow segment, or `None` if absent. Inline objects return
    /// their bytes in the response's `inline_data`.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn get_object_meta(
        &self,
        bucket: u64,
        key: &[u8],
    ) -> Result<Option<meta::GetObjectMetaResponse>, ClientError> {
        let req = meta::GetObjectMetaRequest {
            bucket_id: bucket,
            key: key.to_vec(),
        };
        let resp = self
            .route_op(
                NsMode::Flat,
                bucket,
                key,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.get_object_meta(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok(resp.found.then_some(resp))
    }

    /// Deletes an object (idempotent); the MetaNode captures its slices into
    /// the delete queue (03 §5).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn delete_object(
        &self,
        bucket: u64,
        key: &[u8],
        ts_millis: i64,
    ) -> Result<(), ClientError> {
        let req = meta::DeleteObjectRequest {
            bucket_id: bucket,
            key: key.to_vec(),
            ts_millis,
        };
        self.route_op(
            NsMode::Flat,
            bucket,
            key,
            |mut stub| {
                let req = req.clone();
                async move {
                    stub.delete_object(req)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
            |resp| resp.error.clone(),
        )
        .await
        .map(|_| ())
    }

    /// Lists a page of objects under `prefix`, strictly after `start_after`
    /// (empty = from the prefix start), up to `limit` (03 §5 ListObjectsV2).
    /// Routes by the scan start (the greater of prefix / start_after).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn list_objects(
        &self,
        bucket: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> Result<Vec<meta::ObjectEntry>, ClientError> {
        let route_key = if start_after.is_empty() {
            prefix
        } else {
            start_after
        };
        let req = meta::ListObjectsRequest {
            bucket_id: bucket,
            prefix: prefix.to_vec(),
            start_after: start_after.to_vec(),
            limit,
        };
        let resp = self
            .route_op(
                NsMode::Flat,
                bucket,
                route_key,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.list_objects(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok(resp.entries)
    }

    /// Lists a bucket's objects **across partitions** in key order (03 §5 跨分区
    /// 归并). Walks one partition at a time, merging its pages until `limit` rows
    /// accumulate or the key space is exhausted, then advances to the next
    /// partition. This is what makes a multi-partition bucket's LIST complete —
    /// without it a split bucket silently lists only the partition the scan
    /// started in.
    ///
    /// Returns the merged, key-ordered page plus the resume token for the next
    /// call (`None` = the listing is complete).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn list_objects_merged(
        &self,
        bucket: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> Result<(Vec<meta::ObjectEntry>, Option<Vec<u8>>), ClientError> {
        let mut rows: Vec<meta::ObjectEntry> = Vec::new();
        let mut cursor = start_after.to_vec();
        // Bound the partitions visited so a misconfigured ring can never loop.
        for _ in 0..(MAX_ROUTE_ATTEMPTS.max(1) * 256) {
            let route = self.resolve(NsMode::Flat, bucket, &cursor).await?;
            // The partition's exclusive end key within this bucket (None = +∞).
            let part_end: Option<Vec<u8>> = (!route.view.end_unbounded
                && route.view.end_bucket == bucket)
                .then(|| route.view.end_key.clone());

            // Page within this partition until its end key or the budget.
            loop {
                let remaining = limit.saturating_sub(rows.len() as u32).max(1);
                let entries = self
                    .list_objects(bucket, prefix, &cursor, remaining)
                    .await?;
                if entries.is_empty() {
                    break;
                }
                // The scan is prefix-bounded but not partition-bounded (the CF is
                // shared), so truncate at this partition's end key — a page near
                // the boundary must not leak the next partition's keys.
                let mut hit_end = false;
                for e in entries {
                    if let Some(end) = &part_end
                        && e.key.as_slice() >= end.as_slice()
                    {
                        hit_end = true;
                        break;
                    }
                    cursor = e.key.clone();
                    rows.push(e);
                }
                if hit_end || rows.len() as u32 >= limit {
                    break;
                }
                // A short page means the partition has no more keys under prefix.
                if (rows.len() as u32) < limit {
                    break;
                }
            }

            // Budget met → there may be more; resume after the last row.
            if rows.len() as u32 >= limit {
                return Ok((rows, Some(cursor)));
            }
            // Advance past this partition's end into the next one, or stop when
            // this partition runs to +∞ in the bucket.
            match part_end {
                Some(end) if !end.is_empty() => {
                    cursor = end;
                }
                _ => break,
            }
        }
        Ok((rows, None))
    }

    /// Same-dir rename (03 §6.2, the checkpoint-publish primitive): atomically
    /// renames `from` → `to` under `parent_ino`. Both names must route to the
    /// same partition; a cross-split attempt returns `Some("EXDEV")` (v1
    /// semantics). Returns a `rejected` reason for a type mismatch, `None` when
    /// applied.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_rename(
        &self,
        bucket: u64,
        parent_ino: u64,
        from: &[u8],
        to: &[u8],
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        let routing = hier_routing_key(parent_ino, from);
        let req = meta::HierRenameRequest {
            bucket_id: bucket,
            parent_ino,
            from: from.to_vec(),
            to: to.to_vec(),
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move { stub.hier_rename(req).await.map(tonic::Response::into_inner) }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// rmdir step 1 (03 §6.2): unlinks the child directory from its parent.
    /// Returns a `rejected` reason when the directory is non-empty or missing,
    /// `None` when applied.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_rmdir_unlink(
        &self,
        bucket: u64,
        parent_ino: u64,
        name: &[u8],
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        let routing = hier_routing_key(parent_ino, name);
        let req = meta::HierRmdirUnlinkRequest {
            bucket_id: bucket,
            parent_ino,
            name: name.to_vec(),
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.hier_rmdir_unlink(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// rmdir step 2 / commit point (03 §6.2): reclaims the empty directory's
    /// sentinel. Returns a `rejected` reason if the directory is still non-empty
    /// or the sentinel is missing, `None` when applied.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_rmdir_sentinel(
        &self,
        bucket: u64,
        ino: u64,
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        // Route by the sentinel's own coordinate (its ino lives in its minting
        // partition; the empty-name routing key targets the sentinel record).
        let routing = hier_routing_key(ino, b"");
        let req = meta::HierRmdirSentinelRequest {
            bucket_id: bucket,
            ino,
            ts_millis,
        };
        // HierRmdirSentinelRequest is all-scalar (Copy); no clone needed.
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| async move {
                    stub.hier_rmdir_sentinel(req)
                        .await
                        .map(tonic::Response::into_inner)
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }
}

/// Multipart-upload operations (design 03 §5). Every op routes by the object
/// key (sessions cluster under it), exactly like [`put_object`](MetaClient::put_object).
impl MetaClient {
    /// Opens a multipart session (idempotent per `upload_id`).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn create_multipart(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        ts_millis: i64,
    ) -> Result<(), ClientError> {
        let req = meta::CreateMultipartUploadRequest {
            bucket_id: bucket,
            key: key.to_vec(),
            upload_id: upload_id.to_be_bytes().to_vec(),
            ts_millis,
        };
        self.route_op(
            NsMode::Flat,
            bucket,
            key,
            |mut stub| {
                let req = req.clone();
                async move {
                    stub.create_multipart_upload(req)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
            |resp| resp.error.clone(),
        )
        .await
        .map(|_| ())
    }

    /// Writes one part's metadata (the gateway has written its data). Returns
    /// the deterministic `rejected` reason if the MetaNode refused it (unknown
    /// upload id), else `None`.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    #[allow(clippy::too_many_arguments)]
    pub async fn put_part(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        part_no: u32,
        size: u64,
        etag: [u8; 16],
        slices: Vec<meta::SliceRef>,
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        let req = meta::PutPartRequest {
            bucket_id: bucket,
            key: key.to_vec(),
            upload_id: upload_id.to_be_bytes().to_vec(),
            part_no,
            size,
            etag: etag.to_vec(),
            slices,
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Flat,
                bucket,
                key,
                |mut stub| {
                    let req = req.clone();
                    async move { stub.put_part(req).await.map(tonic::Response::into_inner) }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// Completes a multipart upload, assembling the listed parts. Returns the
    /// deterministic `rejected` reason (unknown upload / missing part / etag
    /// mismatch / empty list) if refused, else `None`.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn complete_multipart(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        parts: Vec<meta::PartRef>,
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        let req = meta::CompleteMultipartRequest {
            bucket_id: bucket,
            key: key.to_vec(),
            upload_id: upload_id.to_be_bytes().to_vec(),
            parts,
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Flat,
                bucket,
                key,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.complete_multipart(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// Aborts a multipart upload (idempotent; tombstones its parts' data).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn abort_multipart(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        ts_millis: i64,
    ) -> Result<(), ClientError> {
        let req = meta::AbortMultipartUploadRequest {
            bucket_id: bucket,
            key: key.to_vec(),
            upload_id: upload_id.to_be_bytes().to_vec(),
            ts_millis,
        };
        self.route_op(
            NsMode::Flat,
            bucket,
            key,
            |mut stub| {
                let req = req.clone();
                async move {
                    stub.abort_multipart_upload(req)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
            |resp| resp.error.clone(),
        )
        .await
        .map(|_| ())
    }

    /// Exports a partition's GC reference set (01 §6.3 / Q27) from the MetaNode
    /// at `addr` (its leader). Returns the referenced blob ids (raw u64), or
    /// `None` if that node does not host/lead the partition. Targets a specific
    /// node by address (a GcRound coordinator queries each partition leader),
    /// not key-routed like the object ops.
    ///
    /// # Errors
    /// [`ClientError`] on a transport/endpoint failure.
    pub async fn export_references(
        &self,
        addr: &str,
        partition_id: u64,
    ) -> Result<Option<Vec<u64>>, ClientError> {
        let mut stub = self.stub(addr)?;
        let resp = stub
            .export_references(meta::ExportReferencesRequest { partition_id })
            .await
            .map_err(|status| ClientError::Rpc {
                code: format!("{:?}", status.code()),
                message: status.message().to_string(),
            })?
            .into_inner();
        Ok(resp.hosted.then_some(resp.blob_ids))
    }
    /// Purges one bucket's records from a partition (99-Q15 DeleteRange).
    /// `None` when the addressed node does not lead the partition (the caller
    /// re-resolves the leader and retries).
    ///
    /// # Errors
    /// [`ClientError::Rpc`] on a transport or server failure.
    pub async fn delete_range(
        &self,
        addr: &str,
        partition_id: u64,
        bucket_id: u64,
    ) -> Result<Option<u64>, ClientError> {
        let mut stub = self.stub(addr)?;
        let resp = stub
            .delete_range(meta::DeleteRangeRequest {
                partition_id,
                bucket_id,
            })
            .await
            .map_err(|status| ClientError::Rpc {
                code: format!("{:?}", status.code()),
                message: status.message().to_string(),
            })?
            .into_inner();
        Ok(resp.hosted.then_some(resp.purged))
    }
}
fn hier_routing_key(parent_ino: u64, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key.extend_from_slice(name);
    key
}

/// Hierarchical-namespace operations (design 03 §6). Each routes by the
/// `(parent_ino, name)` coordinate; the gateway's s3compat layer orchestrates
/// path walk + the two-step mkdir over these primitives.
impl MetaClient {
    /// Resolves one path component `(parent_ino, name)` — path walk / getattr.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_lookup(
        &self,
        bucket: u64,
        parent_ino: u64,
        name: &[u8],
    ) -> Result<meta::HierLookupResponse, ClientError> {
        let routing = hier_routing_key(parent_ino, name);
        let req = meta::HierLookupRequest {
            bucket_id: bucket,
            parent_ino,
            name: name.to_vec(),
        };
        self.route_op(
            NsMode::Hier,
            bucket,
            &routing,
            |mut stub| {
                let req = req.clone();
                async move { stub.hier_lookup(req).await.map(tonic::Response::into_inner) }
            },
            |resp| resp.error.clone(),
        )
        .await
    }

    /// Creates or overwrites a file at `(parent_ino, name)`; returns the
    /// deterministic `rejected` reason (dir/file conflict) if refused.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    #[allow(clippy::too_many_arguments)]
    pub async fn hier_write(&self, write: HierWrite<'_>) -> Result<Option<String>, ClientError> {
        let HierWrite {
            bucket,
            parent_ino,
            name,
            size,
            etag,
            inline_data,
            slices,
            ts_millis,
            http,
        } = write;
        let routing = hier_routing_key(parent_ino, name);
        let req = meta::HierWriteRequest {
            bucket_id: bucket,
            parent_ino,
            name: name.to_vec(),
            size,
            etag: etag.to_vec(),
            ts_millis,
            inline_data,
            slices,
            content_type: http.content_type.unwrap_or_default(),
            content_encoding: http.content_encoding.unwrap_or_default(),
            cache_control: http.cache_control.unwrap_or_default(),
            user_metadata: http.user,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move { stub.hier_write(req).await.map(tonic::Response::into_inner) }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// Removes a file at `(parent_ino, name)` (idempotent).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_unlink(
        &self,
        bucket: u64,
        parent_ino: u64,
        name: &[u8],
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        let routing = hier_routing_key(parent_ino, name);
        let req = meta::HierUnlinkRequest {
            bucket_id: bucket,
            parent_ino,
            name: name.to_vec(),
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move { stub.hier_unlink(req).await.map(tonic::Response::into_inner) }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// mkdir step 1 (03 §6.2): mints the child directory sentinel; returns the
    /// minted child inode for step 2.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_mkdir_sentinel(
        &self,
        bucket: u64,
        parent_ino: u64,
        name: &[u8],
        ts_millis: i64,
    ) -> Result<u64, ClientError> {
        let routing = hier_routing_key(parent_ino, name);
        let req = meta::HierMkdirSentinelRequest {
            bucket_id: bucket,
            parent_ino,
            name: name.to_vec(),
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.hier_mkdir_sentinel(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok(resp.child_ino)
    }

    /// mkdir step 2 / commit point (03 §6.2): links the child dir into its
    /// parent (idempotent); returns a `rejected` reason on dir/file conflict.
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_mkdir_link(
        &self,
        bucket: u64,
        parent_ino: u64,
        name: &[u8],
        child_ino: u64,
        ts_millis: i64,
    ) -> Result<Option<String>, ClientError> {
        let routing = hier_routing_key(parent_ino, name);
        let req = meta::HierMkdirLinkRequest {
            bucket_id: bucket,
            parent_ino,
            name: name.to_vec(),
            child_ino,
            ts_millis,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.hier_mkdir_link(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok((!resp.rejected.is_empty()).then_some(resp.rejected))
    }

    /// A page of a directory's entries (03 §6.1), ordered by name, after
    /// `start_after` (empty = from the first child).
    ///
    /// # Errors
    /// Route/leader failures as [`ClientError`].
    pub async fn hier_readdir(
        &self,
        bucket: u64,
        dir_ino: u64,
        start_after: &[u8],
        limit: u32,
    ) -> Result<Vec<meta::HierDirEntry>, ClientError> {
        // A directory's entries live in its own ino range; route by its
        // sentinel coordinate `(dir_ino, "")`.
        let routing = hier_routing_key(dir_ino, b"");
        let req = meta::HierReaddirRequest {
            bucket_id: bucket,
            dir_ino,
            start_after: start_after.to_vec(),
            limit,
        };
        let resp = self
            .route_op(
                NsMode::Hier,
                bucket,
                &routing,
                |mut stub| {
                    let req = req.clone();
                    async move {
                        stub.hier_readdir(req)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
                |resp| resp.error.clone(),
            )
            .await?;
        Ok(resp.entries)
    }
}
