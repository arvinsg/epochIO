//! Raft peer gRPC server for the PD group: the receiving side of the
//! `raft.proto` transport (design 01 §2).
//!
//! [`PdRaftPeerService`] decodes each [`RaftEnvelope`] into an openraft peer
//! request, dispatches it to the local raft node, and replies with the
//! serde-encoded `Result<Response, RaftError>` — the exact reply the sending
//! [`PdRaftNetwork`](crate::raft::network) decodes. Process assembly (binding a
//! socket, mounting this alongside the control-plane service) lands with the PD
//! node role in a later phase; this crate provides only the service.
//!
//! Design: docs/design/01-pd.md §2; docs/design/06-code-layout.md §8

use epoch_proto::grpc::raft::RaftEnvelope;
use epoch_proto::grpc::raft::pd_raft_peer_server::{PdRaftPeer, PdRaftPeerServer};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::{Request, Response, Status};

use crate::raft::{PdRaft, PdTypeConfig};

/// The raft peer transport server, backed by the local raft node.
pub struct PdRaftPeerService {
    raft: PdRaft,
}

impl PdRaftPeerService {
    /// Builds the service over a clone of the local raft handle.
    #[must_use]
    pub fn new(raft: PdRaft) -> Self {
        Self { raft }
    }

    /// Wraps the service in the generated tonic server, ready to hand to
    /// `tonic::transport::Server::add_service`.
    #[must_use]
    pub fn into_server(self) -> PdRaftPeerServer<Self> {
        PdRaftPeerServer::new(self)
    }
}

#[tonic::async_trait]
impl PdRaftPeer for PdRaftPeerService {
    async fn append_entries(
        &self,
        request: Request<RaftEnvelope>,
    ) -> Result<Response<RaftEnvelope>, Status> {
        let rpc: AppendEntriesRequest<PdTypeConfig> = decode(&request.into_inner().payload)?;
        let result = self.raft.append_entries(rpc).await;
        Ok(Response::new(reply(&result)?))
    }

    async fn vote(&self, request: Request<RaftEnvelope>) -> Result<Response<RaftEnvelope>, Status> {
        let rpc: VoteRequest<crate::raft::NodeId> = decode(&request.into_inner().payload)?;
        let result = self.raft.vote(rpc).await;
        Ok(Response::new(reply(&result)?))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftEnvelope>,
    ) -> Result<Response<RaftEnvelope>, Status> {
        let rpc: InstallSnapshotRequest<PdTypeConfig> = decode(&request.into_inner().payload)?;
        let result = self.raft.install_snapshot(rpc).await;
        Ok(Response::new(reply(&result)?))
    }
}

/// Decodes a wire payload into a raft request, mapping a malformed body to
/// `INVALID_ARGUMENT`.
fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(payload)
        .map_err(|e| Status::invalid_argument(format!("malformed raft payload: {e}")))
}

/// Serde-encodes a raft handler `Result` into a reply envelope, mapping an
/// encode failure to `INTERNAL`.
fn reply<T: Serialize>(result: &T) -> Result<RaftEnvelope, Status> {
    serde_json::to_vec(result)
        .map(|payload| RaftEnvelope { payload })
        .map_err(|e| Status::internal(format!("encode raft reply: {e}")))
}
