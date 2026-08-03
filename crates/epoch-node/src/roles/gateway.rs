//! The `gateway` role (M6): run one S3 gateway — register with PD (gateway
//! role), assemble the EC write/read path + MetaNode metadata client + bucket
//! and credential caches, and serve the S3 HTTP API (`s3s` over hyper) with
//! SigV4 auth until shutdown.
//!
//! The assembly mirrors the M4 test harness (`tests/dev_cluster.rs`): PD client
//! → topology → chunk map → writable set → writer session (with its heartbeat
//! driver) → object service; plus the M6 metadata client, caches, and the S3
//! head. The gateway is stateless beyond its writer-token session, so a restart
//! re-registers and re-establishes.
//!
//! Design: docs/design/00-overview.md §3/§5; docs/design/07-iteration-plan.md M6

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use epoch_client::{
    BucketCache, ChunkMap, CredentialCache, MetaClient, PdClient, Topology, WritableSet,
    WriterSession,
};
use epoch_gateway::{Admission, CodeMode, CredentialAuth, Gateway, ObjectService, S3Backend};
use epoch_proto::NodeId;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

use crate::config::ClusterConfig;
use crate::error::NodeError;

/// The `RoleSet` bit announced at PD registration for a gateway node (a
/// synthetic access-layer identity; it hosts no disks). Design 01 §3 roles are
/// a bitset — the gateway bit is 2 (matches the test harness).
const ROLE_GATEWAY: u8 = 2;

/// The writer-session stale window and heartbeat cadence (01 §4.3).
const WRITER_STALE_MS: u64 = 60_000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The cache refresh cadence for topology / writable set (01 §7 periodic pull).
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Runs the `gateway` role, serving S3 on `s3_addr` until `shutdown` resolves.
///
/// # Errors
///
/// [`NodeError`] if PD is not configured/reachable, registration fails, the
/// caches cannot prime, or the S3 socket cannot be bound.
pub async fn run(
    config: &ClusterConfig,
    s3_addr: SocketAddr,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), NodeError> {
    let pd = PdClient::connect(&config.pd).map_err(|e| NodeError::Pd(format!("connect: {e}")))?;

    // Register the gateway as a node so it can hold a writer token (01 §4.3).
    let gw_node = register_gateway(&pd, s3_addr).await?;

    // EC write/read path (mirrors the M4 harness assembly). The transport is a
    // static snapshot of the serving endpoints (RemoteTransport is built from a
    // fixed map, 02 §2.1), so wait until at least a full stripe's worth of nodes
    // (data + parity) are serving before snapshotting — a gateway that boots
    // ahead of the data nodes would otherwise capture an empty map and fail
    // every EC write. A topology-backed transport that tracks joins/leaves at
    // runtime is a follow-up (M7).
    let topology = Topology::new(pd.clone(), REFRESH_INTERVAL);
    let code = config.code_mode().map_err(NodeError::Config)?;
    let need = usize::from(code.data) + usize::from(code.parity);
    let endpoints = wait_for_serving_endpoints(&topology, need).await?;
    let code_mode_id = code.id;
    let chunk_map = ChunkMap::new(pd.clone(), topology);
    let writable = WritableSet::new(
        pd.clone(),
        chunk_map.clone(),
        code_mode_id,
        REFRESH_INTERVAL,
    );
    let session = Arc::new(
        WriterSession::establish(pd.clone(), gw_node, WRITER_STALE_MS, now_millis())
            .await
            .map_err(|e| NodeError::Pd(format!("writer session: {e}")))?,
    );
    // The heartbeat driver keeps the session's stale gate open (01 §4.3); it
    // aborts on drop, so hold it for the serving lifetime.
    let _heartbeat = session.spawn_heartbeat(HEARTBEAT_INTERVAL);
    let transport: Arc<dyn epoch_rpc::ShardTransport> =
        Arc::new(epoch_rpc::RemoteTransport::new(endpoints));
    let gateway = Arc::new(Gateway::new(
        gateway_code_mode(&code)?,
        gw_node,
        writable,
        session,
        transport,
    ));

    // M6 metadata + caches + the S3 head.
    let meta = MetaClient::new(pd.clone());
    let buckets = BucketCache::new(pd.clone());
    let credentials = CredentialCache::new(pd.clone());
    let objects = ObjectService::new(
        gateway,
        meta,
        chunk_map,
        Admission::with_default_pool(),
        gateway_code_mode(&code)?,
    );
    // The credential cache serves two roles from one PD-backed source: SigV4
    // secret lookup (`CredentialAuth`) and bucket-level authorization inside the
    // backend (01 §6). One cache, so a revoked key stops signing *and* stops
    // authorizing at the same moment.
    let backend = S3Backend::new(objects, buckets, credentials.clone());
    let mut builder = S3ServiceBuilder::new(backend);
    builder.set_auth(CredentialAuth::new(credentials));
    let s3 = builder.build();

    // Serve S3 over hyper until shutdown.
    let listener = TcpListener::bind(s3_addr).await.map_err(NodeError::Io)?;
    // Observability (08 §5.1): request/latency metrics are the gateway's own.
    let _metrics =
        crate::metrics_server::spawn_metrics_server(crate::metrics_server::metrics_addr(s3_addr));
    tracing::info!(node = gw_node.get(), %s3_addr, "gateway serving S3");
    serve_until(listener, s3, shutdown).await;
    Ok(())
}

/// Accepts connections and serves the s3s service on each until `shutdown`.
/// `S3Service` is itself a `hyper::service::Service`, so it is served directly.
async fn serve_until(
    listener: TcpListener,
    s3: s3s::service::S3Service,
    shutdown: impl Future<Output = ()> + Send,
) {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accept = listener.accept() => {
                let Ok((stream, _peer)) = accept else { continue };
                let s3 = s3.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    if let Err(e) = ConnBuilder::new(TokioExecutor::new())
                        .serve_connection(io, s3)
                        .await
                    {
                        tracing::debug!(error = %e, "s3 connection ended");
                    }
                });
            }
        }
    }
}

/// Registers the gateway node with PD (bounded retry — a dev cluster starts
/// everything at once), returning its assigned cluster node id.
async fn register_gateway(pd: &PdClient, s3_addr: SocketAddr) -> Result<NodeId, NodeError> {
    let addr = s3_addr.to_string();
    let deadline = SystemTime::now() + Duration::from_secs(60);
    loop {
        match pd
            .register_node(addr.clone(), "az1", "r1", ROLE_GATEWAY)
            .await
        {
            Ok(id) => return Ok(id),
            Err(e) if SystemTime::now() < deadline => {
                tracing::debug!(error = %e, "gateway registration retry");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(NodeError::Pd(format!("register gateway: {e}"))),
        }
    }
}

/// Refreshes the topology until at least `need` nodes are serving, returning
/// the serving endpoint map to snapshot into the transport. Bounded so a
/// never-converging cluster fails startup rather than hanging.
async fn wait_for_serving_endpoints(
    topology: &Topology,
    need: usize,
) -> Result<std::collections::HashMap<NodeId, SocketAddr>, NodeError> {
    let deadline = SystemTime::now() + Duration::from_secs(60);
    loop {
        topology
            .refresh()
            .await
            .map_err(|e| NodeError::Pd(format!("topology: {e}")))?;
        let endpoints = topology.serving_endpoints();
        if endpoints.len() >= need {
            return Ok(endpoints);
        }
        if SystemTime::now() >= deadline {
            return Err(NodeError::Gateway(format!(
                "only {} of {need} nodes serving after 60s",
                endpoints.len()
            )));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Builds the gateway's `CodeMode` from the PD-published code parameters.
fn gateway_code_mode(code: &epoch_proto::CodeMode) -> Result<CodeMode, NodeError> {
    CodeMode::new(
        code.data as usize,
        code.parity as usize,
        code.stripe_size as usize,
        code.blob_size as usize,
    )
    .map_err(|e| NodeError::Gateway(format!("code mode: {e}")))
}

/// Local wall-clock milliseconds for the writer session's liveness gate.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
