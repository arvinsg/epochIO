//! Batched multi-group raft transport: every openraft peer RPC of every
//! partition group between one node pair is multiplexed onto a single gRPC
//! method (03 §8 心跳合并: 多 group batch 到单连接).
//!
//! Client side: openraft hands each group a per-target [`MetaNetwork`] (via
//! [`MetaNetworkFactory`]); instead of dialing per group, all of them enqueue
//! envelopes into the shared [`BatcherHub`], which keeps **one flush task per
//! target node** — coalescing envelopes for a short window (few ms) or a size
//! cap into one `MetaRaftTransport/Batch` RPC, then routing each reply back to
//! the waiting caller. Server side: [`MetaRaftTransportService`] fans each
//! envelope out to the addressed local group and aligns the replies
//! positionally.
//!
//! Failure model mirrors the PD transport: transport failures surface as
//! [`Unreachable`] (openraft retries), remote `RaftError`s as
//! [`RPCError::RemoteError`], local (de)serialization failures as
//! [`RPCError::Network`]. A failed batch fails only its own envelopes;
//! heartbeats of the next window retry normally.
//!
//! Design: docs/design/03-metanode.md §8; docs/design/06-code-layout.md §9
//! (raft/network.rs)

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use epoch_proto::grpc::raft::meta_raft_envelope::Kind;
use epoch_proto::grpc::raft::meta_raft_transport_client::MetaRaftTransportClient;
use epoch_proto::grpc::raft::meta_raft_transport_server::{
    MetaRaftTransport, MetaRaftTransportServer,
};
use epoch_proto::grpc::raft::{
    MetaRaftBatchReply, MetaRaftBatchRequest, MetaRaftEnvelope, meta_raft_batch_reply,
};
use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{debug, instrument};

use crate::raft::GroupManager;
use crate::raft::{MetaRaft, MetaTypeConfig, NodeId};

/// Default coalescing window: how long a flush task waits for more envelopes
/// after the first one of a batch (03 §8: 心跳合并窗口). Small enough that an
/// extra one-way latency of this order is irrelevant to raft (heartbeat
/// interval is two orders of magnitude larger).
const BATCH_WINDOW: Duration = Duration::from_millis(5);

/// Default cap on envelopes per batch: a full flush ships immediately.
const BATCH_MAX: usize = 128;

/// Per-target queue depth. Full queues apply backpressure into raft's own
/// retry pacing rather than growing memory unboundedly.
const QUEUE_DEPTH: usize = 1024;

/// Per-RPC timeout on the client endpoint: a wedged handler or a half-open
/// connection must never stall the per-target flush loop for good (tonic has
/// no default RPC timeout).
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Server-side bound on one raft call: a call into a stale core must fail the
/// envelope, not hang the handler (and through it the sender's flush task).
const RAFT_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Message-size headroom for batched payloads: snapshot chunks (~3 MiB each,
/// openraft default) dominate; a batch must never hit the 4 MiB tonic default.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// A failure to deliver one envelope through the hub.
#[derive(Debug, thiserror::Error)]
enum HubError {
    /// The flush task or the caller half of the channel is gone.
    #[error("raft batch channel closed")]
    Closed,
    /// The remote side (or the transport to it) failed this envelope.
    #[error("{0}")]
    Remote(String),
}

/// One envelope waiting for its reply.
struct OutItem {
    envelope: MetaRaftEnvelope,
    reply: oneshot::Sender<Result<Vec<u8>, HubError>>,
}

/// The live flush-task handle of one target node.
struct TargetEntry {
    addr: String,
    sender: mpsc::Sender<OutItem>,
}

/// The client-side batching hub shared by every group on this node.
///
/// Holds one flush task per target node (lazily spawned on first use), so the
/// whole node keeps exactly one outbound RPC stream per peer regardless of how
/// many partition groups replicate to it — no per-group connection storm
/// (03 §8).
pub struct BatcherHub {
    targets: Mutex<BTreeMap<NodeId, TargetEntry>>,
    window: Duration,
    max_batch: usize,
}

impl Default for BatcherHub {
    fn default() -> Self {
        Self::new()
    }
}

impl BatcherHub {
    /// A hub with the production batching parameters.
    #[must_use]
    pub fn new() -> Self {
        Self {
            targets: Mutex::new(BTreeMap::new()),
            window: BATCH_WINDOW,
            max_batch: BATCH_MAX,
        }
    }

    /// A hub with explicit batching parameters (tests shrink the window).
    #[cfg(test)]
    #[must_use]
    pub fn with_params(window: Duration, max_batch: usize) -> Self {
        Self {
            targets: Mutex::new(BTreeMap::new()),
            window,
            max_batch,
        }
    }

    /// Sends one envelope to `target` and awaits its reply payload.
    ///
    /// # Errors
    ///
    /// Returns [`HubError::Closed`] if the flush task is gone (the caller maps
    /// this to an unreachable-style retry and a fresh task spawns on the next
    /// send), or [`HubError::Remote`] if the batch or this envelope failed
    /// remotely.
    async fn send(
        &self,
        target: NodeId,
        addr: &str,
        kind: Kind,
        group: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, HubError> {
        let envelope = MetaRaftEnvelope {
            group_id: group,
            kind: kind as i32,
            payload,
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let sender = self.sender_for(target, addr);
        sender
            .send(OutItem {
                envelope,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HubError::Closed)?;
        reply_rx.await.map_err(|_| HubError::Closed)?
    }

    /// The queue of `target`, spawning its flush task on first use or after an
    /// address change (a re-registered node gets a fresh connection).
    fn sender_for(&self, target: NodeId, addr: &str) -> mpsc::Sender<OutItem> {
        let mut targets = self.targets.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = targets.get(&target)
            && entry.addr == addr
        {
            return entry.sender.clone();
        }
        let (tx, rx) = mpsc::channel(QUEUE_DEPTH);
        tokio::spawn(flush_loop(
            target,
            addr.to_string(),
            rx,
            self.window,
            self.max_batch,
        ));
        targets.insert(
            target,
            TargetEntry {
                addr: addr.to_string(),
                sender: tx.clone(),
            },
        );
        tx
    }
}

/// The per-target coalescing loop: collect envelopes for `window` (or until
/// `max_batch`), ship one `Batch` RPC, fan the replies back out.
async fn flush_loop(
    target: NodeId,
    addr: String,
    mut rx: mpsc::Receiver<OutItem>,
    window: Duration,
    max_batch: usize,
) {
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .map(|endpoint| endpoint.timeout(RPC_TIMEOUT).connect_lazy())
        .ok();
    while let Some(first) = rx.recv().await {
        let mut envelopes = vec![first.envelope];
        let mut replies = vec![first.reply];
        let deadline = Instant::now() + window;
        while envelopes.len() < max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(item)) => {
                    envelopes.push(item.envelope);
                    replies.push(item.reply);
                }
                // Window elapsed or queue closed: ship what we have.
                Ok(None) | Err(_) => break,
            }
        }
        let outcomes = dispatch(target, channel.as_ref(), envelopes).await;
        for (reply, outcome) in replies.into_iter().zip(outcomes) {
            let _ = reply.send(outcome);
        }
    }
}

/// Ships one batch to `target` and unfolds the reply per envelope.
async fn dispatch(
    target: NodeId,
    channel: Option<&Channel>,
    envelopes: Vec<MetaRaftEnvelope>,
) -> Vec<Result<Vec<u8>, HubError>> {
    let n = envelopes.len();
    let Some(channel) = channel else {
        debug!(target, "no channel to raft peer (invalid address)");
        return (0..n)
            .map(|_| {
                Err(HubError::Remote(format!(
                    "no channel to raft peer {target}"
                )))
            })
            .collect();
    };
    let mut client =
        MetaRaftTransportClient::new(channel.clone()).max_decoding_message_size(MAX_MESSAGE_SIZE);
    match client.batch(MetaRaftBatchRequest { envelopes }).await {
        Ok(response) => {
            let results = response.into_inner().results;
            if results.len() != n {
                return (0..n)
                    .map(|_| {
                        Err(HubError::Remote(format!(
                            "batch reply from {target} has {} results for {n} envelopes",
                            results.len()
                        )))
                    })
                    .collect();
            }
            results
                .into_iter()
                .map(|result| {
                    if result.error.is_empty() {
                        Ok(result.payload)
                    } else {
                        Err(HubError::Remote(result.error))
                    }
                })
                .collect()
        }
        Err(status) => {
            debug!(target, error = %status, "raft batch RPC failed");
            (0..n)
                .map(|_| Err(HubError::Remote(status.to_string())))
                .collect()
        }
    }
}

/// Factory building per-group, per-target [`MetaNetwork`] clients over the
/// shared hub.
pub struct MetaNetworkFactory {
    group: u64,
    hub: Arc<BatcherHub>,
}

impl MetaNetworkFactory {
    /// Binds the factory of `group` to the node-wide `hub`.
    #[must_use]
    pub fn new(group: u64, hub: Arc<BatcherHub>) -> Self {
        Self { group, hub }
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for MetaNetworkFactory {
    type Network = MetaNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        MetaNetwork {
            group: self.group,
            target,
            addr: node.addr.clone(),
            hub: Arc::clone(&self.hub),
        }
    }
}

/// Per-group, per-target raft peer client. Carries no connection of its own —
/// every call goes through the node-wide [`BatcherHub`] (03 §8).
pub struct MetaNetwork {
    group: u64,
    target: NodeId,
    addr: String,
    hub: Arc<BatcherHub>,
}

impl MetaNetwork {
    /// Encodes `rpc`, ships it through the hub as `kind`, decodes the reply.
    async fn round_trip<Req, Resp, E>(
        &self,
        kind: Kind,
        rpc: &Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, E>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error + serde::de::DeserializeOwned,
    {
        let payload = serde_json::to_vec(rpc).map_err(to_network)?;
        let bytes = self
            .hub
            .send(self.target, &self.addr, kind, self.group, payload)
            .await
            .map_err(|e| match e {
                HubError::Closed => RPCError::Unreachable(Unreachable::new(&e)),
                HubError::Remote(msg) => {
                    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(msg)))
                }
            })?;
        let result: Result<Resp, E> = serde_json::from_slice(&bytes).map_err(to_network)?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

impl RaftNetwork<MetaTypeConfig> for MetaNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.round_trip(Kind::AppendEntries, &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.round_trip(Kind::Vote, &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.round_trip(Kind::InstallSnapshot, &rpc).await
    }
}

/// Maps a local (de)serialization failure to [`RPCError::Network`].
fn to_network<N, E>(err: serde_json::Error) -> RPCError<NodeId, N, E>
where
    N: openraft::Node,
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&err))
}

/// Coalescing evidence counters of the transport server — the primary
/// observability that client-side batching works (envelopes ≫ batches), used
/// by tests and later by metrics (AGENTS §9.3).
#[derive(Debug, Default)]
pub struct TransportCounters {
    batches: AtomicUsize,
    envelopes: AtomicUsize,
}

impl TransportCounters {
    /// How many `Batch` RPCs the node has received.
    #[must_use]
    pub fn batches(&self) -> usize {
        self.batches.load(Ordering::Relaxed)
    }

    /// How many individual envelopes those batches carried.
    #[must_use]
    pub fn envelopes(&self) -> usize {
        self.envelopes.load(Ordering::Relaxed)
    }
}

/// The server side of the batched transport: fans each envelope out to the
/// addressed local group.
///
/// The group registry is held **weakly**: the node assembly owns the
/// [`GroupManager`], and a long-lived gRPC connection (or a handler stuck on
/// a wedged peer) must never pin it — with a strong reference, lingering
/// handlers would keep the manager and its engines alive past teardown,
/// blocking a same-dir restart. Once the manager is gone, envelopes fail with
/// a per-envelope "unknown group" error, which openraft treats as a
/// retryable unreachable-style failure.
pub struct MetaRaftTransportService {
    groups: std::sync::Weak<GroupManager>,
    counters: Arc<TransportCounters>,
}

impl MetaRaftTransportService {
    /// Builds the service over the node's group registry.
    #[must_use]
    pub fn new(groups: Arc<GroupManager>) -> Self {
        Self {
            groups: Arc::downgrade(&groups),
            counters: Arc::new(TransportCounters::default()),
        }
    }

    /// The shared counters handle (kept across `into_server` for tests /
    /// metrics export).
    #[must_use]
    pub fn counters(&self) -> Arc<TransportCounters> {
        Arc::clone(&self.counters)
    }

    /// Wraps the service in the generated tonic server (snapshot-chunk
    /// headroom applied), ready for `tonic::transport::Server::add_service`.
    #[must_use]
    pub fn into_server(self) -> MetaRaftTransportServer<Self> {
        MetaRaftTransportServer::new(self).max_decoding_message_size(MAX_MESSAGE_SIZE)
    }
}

/// Decodes a wire payload into a raft request.
fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, String> {
    serde_json::from_slice(payload).map_err(|e| format!("malformed raft payload: {e}"))
}

/// Serde-encodes a raft handler `Result` into the reply payload.
fn encode_reply<T: Serialize>(result: &T) -> Result<Vec<u8>, String> {
    serde_json::to_vec(result).map_err(|e| format!("encode raft reply: {e}"))
}

/// Calls the addressed raft API and encodes the reply.
async fn call_raft(raft: &MetaRaft, kind: Kind, payload: &[u8]) -> Result<Vec<u8>, String> {
    match kind {
        Kind::AppendEntries => {
            let rpc: AppendEntriesRequest<MetaTypeConfig> = decode(payload)?;
            encode_reply(&raft.append_entries(rpc).await)
        }
        Kind::Vote => {
            let rpc: VoteRequest<NodeId> = decode(payload)?;
            encode_reply(&raft.vote(rpc).await)
        }
        Kind::InstallSnapshot => {
            let rpc: InstallSnapshotRequest<MetaTypeConfig> = decode(payload)?;
            encode_reply(&raft.install_snapshot(rpc).await)
        }
        _ => Err(format!("unknown raft envelope kind {kind:?}")),
    }
}

/// Dispatches one envelope to its group and captures the reply (or the
/// per-envelope error string — never a whole-batch failure).
async fn dispatch_envelope(
    raft: &MetaRaft,
    envelope: MetaRaftEnvelope,
) -> meta_raft_batch_reply::Result {
    // INVARIANT: never call into a shut-down core. While any raft handle
    // clone exists the API channel stays open, so a call would await a dead
    // core forever — wedging this handler and, through the hanging reply,
    // the sender's flush task for this node (03 §8 failure model).
    if raft.metrics().borrow().state == openraft::ServerState::Shutdown {
        return meta_raft_batch_reply::Result {
            payload: Vec::new(),
            error: format!("group {} is shut down", envelope.group_id),
        };
    }
    let kind = Kind::try_from(envelope.kind).unwrap_or(Kind::Unspecified);
    let result =
        match tokio::time::timeout(RAFT_CALL_TIMEOUT, call_raft(raft, kind, &envelope.payload))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(format!(
                "raft call timed out after {RAFT_CALL_TIMEOUT:?} (group {})",
                envelope.group_id
            )),
        };
    match result {
        Ok(payload) => meta_raft_batch_reply::Result {
            payload,
            error: String::new(),
        },
        Err(error) => meta_raft_batch_reply::Result {
            payload: Vec::new(),
            error,
        },
    }
}

#[tonic::async_trait]
impl MetaRaftTransport for MetaRaftTransportService {
    #[instrument(skip(self, request), fields(envelopes = request.get_ref().envelopes.len()))]
    async fn batch(
        &self,
        request: Request<MetaRaftBatchRequest>,
    ) -> Result<Response<MetaRaftBatchReply>, Status> {
        let envelopes = request.into_inner().envelopes;
        self.counters.batches.fetch_add(1, Ordering::Relaxed);
        self.counters
            .envelopes
            .fetch_add(envelopes.len(), Ordering::Relaxed);

        let mut reply = MetaRaftBatchReply {
            results: Vec::with_capacity(envelopes.len()),
        };
        let groups = self.groups.upgrade();
        for envelope in envelopes {
            let group = envelope.group_id;
            let raft = groups.as_ref().and_then(|groups| groups.raft(group));
            match raft {
                Some(raft) => reply.results.push(dispatch_envelope(&raft, envelope).await),
                None => reply.results.push(meta_raft_batch_reply::Result {
                    payload: Vec::new(),
                    error: format!("unknown group {group}"),
                }),
            }
        }
        Ok(Response::new(reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_error_display_is_stable() {
        assert_eq!(HubError::Closed.to_string(), "raft batch channel closed");
        assert_eq!(HubError::Remote("x".to_string()).to_string(), "x");
    }
}
