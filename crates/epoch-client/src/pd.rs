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

//! PD control-plane gRPC client: the gateway/DataNode side of the `PdControl`
//! contract (design 01 §7).
//!
//! A [`PdClient`] holds one lazy channel per configured PD replica and finds
//! the leader by trying them in turn. Because the current server maps every
//! not-leader condition to `FAILED_PRECONDITION` without a leader hint, redirect
//! is a round-robin over the endpoints starting at the last-known-good one;
//! `UNAVAILABLE` (an unreachable replica) is treated the same way. Any other
//! status is terminal and mapped to a semantic [`ClientError`]. A successful
//! call caches the serving endpoint as the leader hint for the next call.
//!
//! The read projections currently return the generated `pd::ChunkView`; the
//! gateway-facing domain view and its cache land with the read/write path in a
//! later phase.
//!
//! Design: docs/design/01-pd.md §7

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use epoch_proto::grpc::pd;
use epoch_proto::{ChunkId, CodeModeId, DiskId, NodeId, WriterToken};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Response, Status};

use crate::error::ClientError;
use crate::writer_token::TokenSource;

/// The generated PD control client bound to a transport channel.
type Stub = pd::pd_control_client::PdControlClient<Channel>;

/// A leader-following client for the PD control plane.
///
/// Cheap to clone (shares the channels and the leader hint); every call
/// discovers the leader via [`ClientError`]-mapped redirect.
#[derive(Clone)]
pub struct PdClient {
    clients: Arc<[Stub]>,
    /// Index of the last endpoint that served a call — where the next call
    /// starts its search. A hint only; correctness never depends on it.
    leader_hint: Arc<AtomicUsize>,
}

impl PdClient {
    /// Builds a client over the PD replica endpoints (`host:port` each).
    ///
    /// Channels connect lazily, so this does not perform I/O and does not fail
    /// on an unreachable replica; it only rejects a syntactically invalid
    /// address. Calls made before any replica is reachable/leader return
    /// [`ClientError::NoLeader`].
    ///
    /// # Errors
    /// Returns [`ClientError::Endpoint`] if an address cannot form a URI.
    pub fn connect(endpoints: &[String]) -> Result<Self, ClientError> {
        let mut clients = Vec::with_capacity(endpoints.len());
        for addr in endpoints {
            let endpoint = Endpoint::from_shared(format!("http://{addr}")).map_err(|source| {
                ClientError::Endpoint {
                    endpoint: addr.clone(),
                    source,
                }
            })?;
            clients.push(Stub::new(endpoint.connect_lazy()));
        }
        Ok(Self {
            clients: clients.into(),
            leader_hint: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Registers `node_id` and returns a fresh, never-reused writer token
    /// (design 01 §4.3).
    ///
    /// # Errors
    /// [`ClientError::NotFound`] if the gateway node is not registered with PD;
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn register_writer(&self, node_id: NodeId) -> Result<WriterToken, ClientError> {
        let request = pd::RegisterWriterRequest {
            node_id: node_id.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .register_writer(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(WriterToken::new(response.writer_token))
    }

    /// Sends a writer-session heartbeat for `token` (01 §4.3): refreshes the
    /// leader's last-seen for the owning node and returns whether the token is
    /// still `Live`. `false` means the token is Dead (never revives) or unknown
    /// — the session must rotate to a fresh token before minting again.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the call as leader,
    /// or the mapped [`ClientError`] from the transport.
    pub async fn writer_heartbeat(
        &self,
        node_id: NodeId,
        token: WriterToken,
        commit_watermark: i64,
    ) -> Result<bool, ClientError> {
        let request = pd::WriterHeartbeatRequest {
            node_id: node_id.get(),
            writer_token: token.get(),
            commit_watermark,
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .writer_heartbeat(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.live)
    }

    /// Fetches the currently publishable writable chunks for an EC mode.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn get_writable_chunks(
        &self,
        code_mode_id: CodeModeId,
    ) -> Result<Vec<pd::ChunkView>, ClientError> {
        let request = pd::GetWritableChunksRequest {
            code_mode_id: u32::from(code_mode_id.get()),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .get_writable_chunks(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.chunks)
    }

    /// Fetches the cluster topology (nodes + disks) for disk→node→addr
    /// resolution on the data plane (02 §2.1).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn list_nodes(&self) -> Result<(Vec<pd::NodeInfo>, Vec<pd::DiskInfo>), ClientError> {
        let request = pd::ListNodesRequest {};
        let response = self
            .call_on_leader(|mut client| async move {
                client.list_nodes(request).await.map(Response::into_inner)
            })
            .await?;
        Ok((response.nodes, response.disks))
    }

    /// Lists the full partition route table (01 §5). A GcRound coordinator uses
    /// each partition's `leader_addr` to fetch its reference keep-set.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn list_partitions(&self) -> Result<Vec<pd::MetaPartitionView>, ClientError> {
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .list_partitions(pd::ListPartitionsRequest {})
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.partitions)
    }

    /// Registers this node with PD (idempotent by address), returning its
    /// assigned cluster id. `roles` is the [`RoleSet`] bitset of hosted roles.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn register_node(
        &self,
        addr: impl Into<String>,
        az: impl Into<String>,
        rack: impl Into<String>,
        roles: u8,
    ) -> Result<NodeId, ClientError> {
        let request = pd::RegisterNodeRequest {
            addr: addr.into(),
            az: az.into(),
            rack: rack.into(),
            roles: u32::from(roles),
        };
        let response = self
            .call_on_leader(|mut client| {
                let request = request.clone();
                async move {
                    client
                        .register_node(request)
                        .await
                        .map(Response::into_inner)
                }
            })
            .await?;
        Ok(NodeId::new(response.node_id))
    }

    /// Registers a disk with PD (idempotent by `(node_id, path)`), returning
    /// its assigned disk id.
    ///
    /// # Errors
    /// [`ClientError::NotFound`] if the owning node is not registered;
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn register_disk(
        &self,
        node_id: NodeId,
        az: impl Into<String>,
        rack: impl Into<String>,
        path: impl Into<String>,
        total: u64,
    ) -> Result<epoch_proto::DiskId, ClientError> {
        let request = pd::RegisterDiskRequest {
            node_id: node_id.get(),
            az: az.into(),
            rack: rack.into(),
            path: path.into(),
            total,
        };
        let response = self
            .call_on_leader(|mut client| {
                let request = request.clone();
                async move {
                    client
                        .register_disk(request)
                        .await
                        .map(Response::into_inner)
                }
            })
            .await?;
        Ok(epoch_proto::DiskId::new(response.disk_id))
    }

    /// Sends one heartbeat: full replacement of the node's per-disk statistics
    /// on the leader (01 §7). Followers reject so the client retargets.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn heartbeat(
        &self,
        node_id: NodeId,
        disks: Vec<pd::DiskStats>,
    ) -> Result<(), ClientError> {
        let request = pd::HeartbeatRequest {
            node_id: node_id.get(),
            disks,
        };
        self.call_on_leader(|mut client| {
            let request = request.clone();
            async move { client.heartbeat(request).await.map(Response::into_inner) }
        })
        .await?;
        Ok(())
    }

    /// Seals a committed chunk (the migration/decommission fence, 01 §4.2):
    /// the chunk transitions to `Sealed` and every shard's extent is sealed
    /// over the data plane.
    ///
    /// # Errors
    /// [`ClientError::NotFound`] if the chunk is unknown or not sealable;
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn seal_chunk(&self, chunk_id: ChunkId) -> Result<(), ClientError> {
        let request = pd::SealChunkRequest {
            chunk_id: chunk_id.get(),
        };
        self.call_on_leader(|mut client| async move {
            client.seal_chunk(request).await.map(Response::into_inner)
        })
        .await?;
        Ok(())
    }

    /// Creates a bucket (idempotent by name), returning its assigned id.
    ///
    /// # Errors
    /// [`ClientError::InvalidArgument`] if the name is empty or the enums are
    /// unspecified; [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn create_bucket(
        &self,
        name: impl Into<String>,
        ns_mode: pd::NsMode,
        inline_threshold: Option<u64>,
        codemode_id: u16,
        engine: pd::MetaEngine,
    ) -> Result<epoch_proto::BucketId, ClientError> {
        let request = pd::CreateBucketRequest {
            name: name.into(),
            ns_mode: ns_mode as i32,
            inline_threshold: inline_threshold.unwrap_or(0),
            codemode_id: u32::from(codemode_id),
            engine: engine as i32,
        };
        let response = self
            .call_on_leader(|mut client| {
                let request = request.clone();
                async move {
                    client
                        .create_bucket(request)
                        .await
                        .map(Response::into_inner)
                }
            })
            .await?;
        Ok(epoch_proto::BucketId::new(response.bucket_id))
    }

    /// Lists every registered bucket (identity + policy).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn list_buckets(&self) -> Result<Vec<pd::BucketInfo>, ClientError> {
        let request = pd::ListBucketsRequest {};
        let response = self
            .call_on_leader(|mut client| async move {
                client.list_buckets(request).await.map(Response::into_inner)
            })
            .await?;
        Ok(response.buckets)
    }

    /// The credential table (01 §6): the gateway pulls it to verify SigV4
    /// locally (secrets stay in-domain). Cached with periodic refresh by the
    /// caller.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn list_credentials(&self) -> Result<Vec<pd::CredentialInfo>, ClientError> {
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .list_credentials(pd::ListCredentialsRequest {})
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.credentials)
    }

    /// Stores (creates or replaces) an access-key credential (01 §6 认证).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn put_credential(
        &self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        allowed_buckets: Option<Vec<String>>,
        role: pd::ConsoleRole,
    ) -> Result<(), ClientError> {
        let request = pd::PutCredentialRequest {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            all_buckets: allowed_buckets.is_none(),
            allowed_buckets: allowed_buckets.unwrap_or_default(),
            role: role as i32,
        };
        self.call_on_leader(|mut client| {
            let request = request.clone();
            async move {
                client
                    .put_credential(request)
                    .await
                    .map(Response::into_inner)
            }
        })
        .await?;
        Ok(())
    }

    /// Resolves the partition covering `(bucket, routing_key)` in namespace
    /// `ns` — the gateway's metadata routing lookup (01 §5). Returns `None`
    /// when no partition covers the coordinate (the caller waits for bootstrap
    /// or a split, or issues a CreatePartition).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn get_route(
        &self,
        ns: pd::NsMode,
        bucket_id: u64,
        routing_key: Vec<u8>,
    ) -> Result<Option<pd::MetaPartitionView>, ClientError> {
        let request = pd::GetRouteRequest {
            bucket_id,
            ns: ns as i32,
            routing_key,
        };
        let response = self
            .call_on_leader(|mut client| {
                let request = request.clone();
                async move { client.get_route(request).await.map(Response::into_inner) }
            })
            .await?;
        Ok(response.partition)
    }

    /// Writes a cluster-level config KV entry (01 §1 配置中心).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn put_config(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
    ) -> Result<(), ClientError> {
        let request = pd::PutConfigRequest {
            key: key.into(),
            value,
        };
        self.call_on_leader(|mut client| {
            let request = request.clone();
            async move { client.put_config(request).await.map(Response::into_inner) }
        })
        .await?;
        Ok(())
    }

    /// Reads one config value, or `None` if absent.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn get_config(&self, key: impl Into<String>) -> Result<Option<Vec<u8>>, ClientError> {
        let request = pd::GetConfigRequest { key: key.into() };
        let response = self
            .call_on_leader(|mut client| {
                let request = request.clone();
                async move { client.get_config(request).await.map(Response::into_inner) }
            })
            .await?;
        Ok(response.found.then_some(response.value))
    }

    /// Looks up a single chunk by id.
    ///
    /// # Errors
    /// [`ClientError::NotFound`] if the chunk is unknown; [`ClientError::NoLeader`]
    /// if no replica could serve the read as leader.
    pub async fn get_chunk(&self, chunk_id: ChunkId) -> Result<pd::ChunkView, ClientError> {
        let request = pd::GetChunkRequest {
            chunk_id: chunk_id.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client.get_chunk(request).await.map(Response::into_inner)
            })
            .await?;
        response
            .chunk
            .ok_or_else(|| ClientError::NotFound(format!("chunk {} not found", chunk_id.get())))
    }

    /// Lists the shard slots currently bound to `disk_id` (01 §7): the repair
    /// coordinator's enumeration of a broken disk's shards.
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn list_disk_shards(
        &self,
        disk_id: DiskId,
    ) -> Result<Vec<pd::ShardView>, ClientError> {
        let request = pd::ListDiskShardsRequest {
            disk_id: disk_id.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .list_disk_shards(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.shards)
    }

    /// A page of committed chunks with id > `after`, ascending, capped at
    /// `limit` (01 §6.3 InspectRound enumeration). An empty page ends the round.
    ///
    /// # Errors
    ///
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn list_chunks(
        &self,
        after: ChunkId,
        limit: u32,
    ) -> Result<Vec<pd::ChunkView>, ClientError> {
        let request = pd::ListChunksRequest {
            after: after.get(),
            limit,
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client.list_chunks(request).await.map(Response::into_inner)
            })
            .await?;
        Ok(response.chunks)
    }

    /// Marks a disk for decommission (01 §6.3 DropDisk): PD transitions it to
    /// `Draining` and drains its shards. Returns whether it transitioned (false =
    /// already past `Normal`). The operator-facing admin call.
    ///
    /// # Errors
    ///
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn mark_disk_draining(&self, disk_id: DiskId) -> Result<bool, ClientError> {
        let request = pd::MarkDiskDrainingRequest {
            disk_id: disk_id.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .mark_disk_draining(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.draining)
    }

    /// Fetches the live-token table (01 §6.3 / Q27): each `Live` writer token,
    /// its node, and its commit watermark `W(t)`. A GcRound coordinator uses it
    /// to classify local blobs (dead token / below watermark).
    ///
    /// # Errors
    ///
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn get_live_writers(&self) -> Result<Vec<pd::LiveWriter>, ClientError> {
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .get_live_writers(pd::GetLiveWritersRequest {})
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.writers)
    }

    /// Polls PD for the Jobs assigned to `node` as coordinator (01 §6.1
    /// pull-based delivery).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve the read as leader.
    pub async fn list_node_jobs(&self, node: NodeId) -> Result<Vec<pd::AssignedJob>, ClientError> {
        let request = pd::ListNodeJobsRequest {
            node_id: node.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .list_node_jobs(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.jobs)
    }

    /// Commits a Job progress checkpoint (or completion) to PD (01 §6.1).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn commit_job_progress(
        &self,
        job_id: u64,
        watermark: u64,
        done: bool,
    ) -> Result<(), ClientError> {
        let request = pd::CommitJobProgressRequest {
            job_id,
            watermark,
            done,
        };
        self.call_on_leader(|mut client| async move {
            client
                .commit_job_progress(request)
                .await
                .map(Response::into_inner)
        })
        .await?;
        Ok(())
    }

    /// Commits a shard-mapping rebind to PD (01 §6.4). Returns
    /// `(new_epoch, rebound)`: `rebound=false` means PD rejected it (not the
    /// coordinator, stale epoch, unknown slot).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_shard_mapping(
        &self,
        job_id: u64,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        new_disk: DiskId,
        new_create_ts: i64,
        committer: NodeId,
    ) -> Result<(u32, bool), ClientError> {
        let request = pd::CommitShardMappingRequest {
            job_id,
            chunk_id: chunk_id.get(),
            index: u32::from(index),
            expected_epoch,
            new_disk_id: new_disk.get(),
            new_create_ts,
            committer_node_id: committer.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .commit_shard_mapping(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok((response.new_epoch, response.rebound))
    }

    /// Reports a bad/missing shard to PD (01 §6.3 / §6.4 Q5 fast path): the
    /// heal-on-read / scrub report. Best-effort; returns whether a new repair
    /// ticket was created (false = one already pending or the shard is unknown).
    ///
    /// # Errors
    ///
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn report_shard_repair(
        &self,
        chunk_id: ChunkId,
        index: u8,
    ) -> Result<bool, ClientError> {
        let request = pd::ReportShardRepairRequest {
            chunk_id: chunk_id.get(),
            index: u32::from(index),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .report_shard_repair(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.created)
    }

    /// Lists the single-shard repairs PD dispatched to `node` (01 §6.3): the
    /// node-pull delivery, mirroring [`list_node_jobs`](Self::list_node_jobs).
    ///
    /// # Errors
    ///
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    pub async fn list_node_shard_repairs(
        &self,
        node: NodeId,
    ) -> Result<Vec<pd::ShardRepairAssignment>, ClientError> {
        let request = pd::ListNodeShardRepairsRequest {
            node_id: node.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .list_node_shard_repairs(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok(response.repairs)
    }

    /// Commits a completed single-shard repair (01 §6.3): rebind the slot to the
    /// rebuilt extent (epoch+1), authorized by the ShardRepair ticket. Returns
    /// `(new_epoch, rebound)`; `rebound == false` means rejected (no ticket,
    /// wrong node, stale epoch).
    ///
    /// # Errors
    ///
    /// [`ClientError::NoLeader`] if no replica could serve as leader.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_shard_repair(
        &self,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        new_disk: DiskId,
        new_create_ts: i64,
        committer: NodeId,
    ) -> Result<(u32, bool), ClientError> {
        let request = pd::CommitShardRepairRequest {
            chunk_id: chunk_id.get(),
            index: u32::from(index),
            expected_epoch,
            new_disk_id: new_disk.get(),
            new_create_ts,
            committer_node_id: committer.get(),
        };
        let response = self
            .call_on_leader(|mut client| async move {
                client
                    .commit_shard_repair(request)
                    .await
                    .map(Response::into_inner)
            })
            .await?;
        Ok((response.new_epoch, response.rebound))
    }
    async fn call_on_leader<T, F, Fut>(&self, op: F) -> Result<T, ClientError>
    where
        F: Fn(Stub) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let count = self.clients.len();
        let start = self.leader_hint.load(Ordering::Relaxed);
        let mut last_error = String::from("no PD endpoints configured");
        for offset in 0..count {
            let index = (start + offset) % count;
            match op(self.clients[index].clone()).await {
                Ok(value) => {
                    self.leader_hint.store(index, Ordering::Relaxed);
                    return Ok(value);
                }
                Err(status) if is_retriable(status.code()) => {
                    tracing::debug!(
                        endpoint = index,
                        code = ?status.code(),
                        "PD replica not leader or unreachable; redirecting"
                    );
                    last_error = format!("{:?}: {}", status.code(), status.message());
                }
                Err(status) => return Err(ClientError::from_status(&status)),
            }
        }
        Err(ClientError::NoLeader {
            attempts: count,
            last_error,
        })
    }
}

#[async_trait::async_trait]
impl TokenSource for PdClient {
    async fn register_writer(&self, node_id: NodeId) -> Result<WriterToken, ClientError> {
        PdClient::register_writer(self, node_id).await
    }

    async fn writer_heartbeat(
        &self,
        node_id: NodeId,
        token: WriterToken,
        commit_watermark: i64,
    ) -> Result<bool, ClientError> {
        PdClient::writer_heartbeat(self, node_id, token, commit_watermark).await
    }
}

/// Whether a gRPC status should trigger a redirect to the next replica rather
/// than being surfaced as terminal: `FAILED_PRECONDITION` is the server's
/// not-leader signal and `UNAVAILABLE` marks an unreachable replica.
fn is_retriable(code: Code) -> bool {
    matches!(code, Code::FailedPrecondition | Code::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_not_leader_and_unreachable_are_retriable() {
        assert!(is_retriable(Code::FailedPrecondition));
        assert!(is_retriable(Code::Unavailable));
        for terminal in [
            Code::NotFound,
            Code::InvalidArgument,
            Code::Internal,
            Code::Ok,
            Code::PermissionDenied,
        ] {
            assert!(!is_retriable(terminal), "{terminal:?} must be terminal");
        }
    }

    #[test]
    fn connect_rejects_a_malformed_endpoint() {
        let result = PdClient::connect(&["ht tp://bad host".to_string()]);
        assert!(matches!(result, Err(ClientError::Endpoint { .. })));
    }
}
