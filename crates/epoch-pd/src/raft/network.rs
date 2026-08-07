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

//! Raft peer network for the PD group: openraft's [`RaftNetwork`] implemented
//! over the `raft.proto` gRPC transport (design 01 §2, Q2/Q22).
//!
//! Each peer request is serde-encoded into a [`RaftEnvelope`] and sent to the
//! target replica's address (carried by openraft in the [`BasicNode`]); the
//! reply envelope holds the serde-encoded `Result<Response, RaftError>`, which
//! is decoded and, on a remote `RaftError`, surfaced as [`RPCError::RemoteError`]
//! so openraft's retry / backoff logic applies. Transport failures (no channel,
//! a gRPC status) map to [`RPCError::Unreachable`]; local (de)serialization
//! failures map to [`RPCError::Network`].
//!
//! A single-node group never sends peer RPCs, so the network is simply never
//! invoked there.
//!
//! Design: docs/design/01-pd.md §2; docs/design/06-code-layout.md §8

use epoch_proto::grpc::raft::RaftEnvelope;
use epoch_proto::grpc::raft::pd_raft_peer_client::PdRaftPeerClient;
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
use tonic::transport::{Channel, Endpoint};

use crate::raft::{NodeId, PdTypeConfig};

/// The openraft peer RPC a round-trip carries (selects the gRPC method).
#[derive(Clone, Copy)]
enum PeerCall {
    AppendEntries,
    Vote,
    InstallSnapshot,
}

/// Factory that builds a per-target [`PdRaftNetwork`] client from the peer's
/// advertised address.
pub struct PdRaftNetworkFactory;

impl RaftNetworkFactory<PdTypeConfig> for PdRaftNetworkFactory {
    type Network = PdRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        // Lazy connect: the channel dials on first use, so `new_client` never
        // blocks and a temporarily-down peer just yields `Unreachable` later.
        let channel = Endpoint::from_shared(format!("http://{}", node.addr))
            .map(|endpoint| endpoint.connect_lazy())
            .ok();
        PdRaftNetwork { target, channel }
    }
}

/// Per-target raft peer client over the `raft.proto` gRPC transport.
pub struct PdRaftNetwork {
    target: NodeId,
    channel: Option<Channel>,
}

impl PdRaftNetwork {
    /// Sends one framed envelope to the target and returns the reply payload.
    ///
    /// Any transport failure (missing channel or gRPC status) is reported as
    /// [`Unreachable`] so openraft retries against the current leader.
    async fn round_trip(&self, call: PeerCall, payload: Vec<u8>) -> Result<Vec<u8>, Unreachable> {
        let Some(channel) = self.channel.clone() else {
            return Err(unreachable(&std::io::Error::other(format!(
                "PD raft peer {} has no channel (invalid address)",
                self.target
            ))));
        };
        let mut client = PdRaftPeerClient::new(channel);
        let request = RaftEnvelope { payload };
        let reply = match call {
            PeerCall::AppendEntries => client.append_entries(request).await,
            PeerCall::Vote => client.vote(request).await,
            PeerCall::InstallSnapshot => client.install_snapshot(request).await,
        };
        reply
            .map(|response| response.into_inner().payload)
            .map_err(|status| unreachable(&status))
    }
}

impl RaftNetwork<PdTypeConfig> for PdRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<PdTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload = encode(&rpc).map_err(to_network)?;
        let bytes = self
            .round_trip(PeerCall::AppendEntries, payload)
            .await
            .map_err(RPCError::Unreachable)?;
        let result: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            decode(&bytes).map_err(to_network)?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let payload = encode(&rpc).map_err(to_network)?;
        let bytes = self
            .round_trip(PeerCall::Vote, payload)
            .await
            .map_err(RPCError::Unreachable)?;
        let result: Result<VoteResponse<NodeId>, RaftError<NodeId>> =
            decode(&bytes).map_err(to_network)?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<PdTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let payload = encode(&rpc).map_err(to_network)?;
        let bytes = self
            .round_trip(PeerCall::InstallSnapshot, payload)
            .await
            .map_err(RPCError::Unreachable)?;
        let result: Result<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, InstallSnapshotError>,
        > = decode(&bytes).map_err(to_network)?;
        result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

/// Serde-encodes a raft payload for the wire.
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

/// Serde-decodes a raft payload from the wire.
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Builds an [`Unreachable`] transport error from the underlying cause.
fn unreachable(cause: &(impl std::error::Error + 'static)) -> Unreachable {
    Unreachable::new(cause)
}

/// Maps a local (de)serialization failure to [`RPCError::Network`] for any of
/// the three method error shapes.
fn to_network<N, E>(err: serde_json::Error) -> RPCError<NodeId, N, E>
where
    N: openraft::Node,
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&err))
}
