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

//! The production [`MetaAdmin`] (M7 分区调度器): the PD scheduler's bridge to
//! MetaNode gRPC. The scheduler decides *what* to do (split/migrate); this
//! turns each decision into the matching admin RPC against the right MetaNode,
//! resolving node addresses from PD's committed node table and the partition's
//! leader report.
//!
//! Address resolution: a partition's admin operations go to its raft *leader*
//! (the only replica that can propose the in-log split or drive the membership
//! change). The leader is read from PD's leader-report memory (Q18); its
//! data-plane node address comes from the committed node table. A missing
//! leader report (just after a failover) means "retry next sweep" — the same
//! best-effort contract as `CreateRaftGroup` push.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use epoch_pd::meta_mgr::{MetaPartition, PartitionBound};
use epoch_pd::meta_sched::MetaAdmin;
use epoch_pd::{Journal, PdError};
use epoch_proto::NodeId;
use epoch_proto::grpc::meta::meta_node_client::MetaNodeClient;
use epoch_proto::grpc::meta::{
    ApplySplitRequest, MigrateGroupMemberRequest, PrepareMigrateTargetRequest, RaftPeer,
    SuggestSplitPointRequest,
};
use epoch_proto::grpc::pd::NsMode;

/// Per-call gRPC timeout for admin pushes (a scheduler sweep must not block on
/// an unreachable MetaNode; the op is retried next sweep).
const ADMIN_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolves MetaNode addresses from PD state and drives the partition admin RPCs.
pub struct PdMetaAdmin {
    journal: Arc<Journal>,
}

impl PdMetaAdmin {
    /// Builds the admin bridge over the PD journal (its committed node table +
    /// leader-report memory are the address source).
    #[must_use]
    pub fn new(journal: Arc<Journal>) -> Self {
        Self { journal }
    }

    /// The data-plane address of a node id, from the committed node table.
    fn node_addr(&self, node: NodeId) -> Option<String> {
        self.journal.state().nodes().get(node).map(|n| n.addr)
    }

    /// The address of a partition's current leader (from PD leader memory), or
    /// `None` when no leader has reported yet (retry next sweep).
    fn leader_addr(&self, partition: &MetaPartition) -> Option<String> {
        let report = self
            .journal
            .state()
            .partitions()
            .leader(partition.partition_id)?;
        self.node_addr(report.leader)
    }

    /// The `(node_id, addr)` wire peers of a partition, skipping any node whose
    /// address is not currently known.
    fn peers(&self, partition: &MetaPartition) -> Vec<RaftPeer> {
        partition
            .peers
            .iter()
            .filter_map(|&id| {
                self.node_addr(id).map(|addr| RaftPeer {
                    node_id: id.get(),
                    addr,
                })
            })
            .collect()
    }

    /// Dials a MetaNode at `addr` with the per-call timeout.
    fn connect(addr: &str) -> Result<MetaNodeClient<tonic::transport::Channel>, PdError> {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| PdError::Raft(format!("bad meta addr {addr}: {e}")))?
            .timeout(ADMIN_RPC_TIMEOUT)
            .connect_lazy();
        Ok(MetaNodeClient::new(channel))
    }
}

#[async_trait]
impl MetaAdmin for PdMetaAdmin {
    async fn suggest_split_point(
        &self,
        parent: &MetaPartition,
    ) -> Result<Option<PartitionBound>, PdError> {
        let Some(addr) = self.leader_addr(parent) else {
            return Ok(None); // no leader yet; retry next sweep
        };
        let mut client = Self::connect(&addr)?;
        let resp = client
            .suggest_split_point(SuggestSplitPointRequest {
                partition_id: parent.partition_id,
            })
            .await
            .map_err(|e| PdError::Raft(format!("suggest_split_point: {e}")))?
            .into_inner();
        // A route error (not leader / moved) just means "retry next sweep".
        if resp.error.is_some() || !resp.splittable {
            return Ok(None);
        }
        Ok(Some(PartitionBound::at(
            resp.at_bucket,
            resp.at_routing_key,
        )))
    }

    async fn apply_split(
        &self,
        parent: &MetaPartition,
        child_group: u64,
        at: &PartitionBound,
        child_ino_tag: u32,
    ) -> Result<(), PdError> {
        let Some(addr) = self.leader_addr(parent) else {
            return Err(PdError::Raft("split target leader unknown".to_string()));
        };
        // The child inherits the parent's replica set; the bootstrap node is the
        // parent leader (it proposes and forms the child membership, 03 §8).
        let bootstrap_node_id = self
            .journal
            .state()
            .partitions()
            .leader(parent.partition_id)
            .map(|r| r.leader.get())
            .unwrap_or(0);
        let mut client = Self::connect(&addr)?;
        let resp = client
            .apply_split(ApplySplitRequest {
                partition_id: parent.partition_id,
                child_group,
                at_bucket: at.bucket,
                at_routing_key: at.routing_key.clone(),
                child_peers: self.peers(parent),
                bootstrap_node_id,
                child_ino_tag,
            })
            .await
            .map_err(|e| PdError::Raft(format!("apply_split: {e}")))?
            .into_inner();
        if let Some(err) = resp.error {
            // Not-leader/moved: the next sweep re-plans against fresh state.
            return Err(PdError::Raft(format!(
                "apply_split rejected: partition {} kind {}",
                err.partition_id, err.kind
            )));
        }
        Ok(())
    }

    async fn migrate_group_member(
        &self,
        partition: &MetaPartition,
        from: NodeId,
        to: NodeId,
    ) -> Result<(), PdError> {
        let ns = partition_ns(partition);
        let peers = self.peers(partition);
        let Some(to_addr) = self.node_addr(to) else {
            return Err(PdError::Raft(format!(
                "migrate target node {} address unknown",
                to.get()
            )));
        };

        // 1. Pre-create the target group so it can receive replication (03 §2).
        let mut target = Self::connect(&to_addr)?;
        target
            .prepare_migrate_target(PrepareMigrateTargetRequest {
                partition_id: partition.partition_id,
                ns: ns as i32,
                start_bucket: partition.start.bucket,
                start_key: partition.start.routing_key.clone(),
                start_unbounded: partition.start.unbounded,
                end_bucket: partition.end.bucket,
                end_key: partition.end.routing_key.clone(),
                end_unbounded: partition.end.unbounded,
                peers: peers.clone(),
            })
            .await
            .map_err(|e| PdError::Raft(format!("prepare_migrate_target: {e}")))?;

        // 2. Drive the source leader's membership swap.
        let Some(leader_addr) = self.leader_addr(partition) else {
            return Err(PdError::Raft("migrate source leader unknown".to_string()));
        };
        let mut leader = Self::connect(&leader_addr)?;
        let resp = leader
            .migrate_group_member(MigrateGroupMemberRequest {
                partition_id: partition.partition_id,
                from_node_id: from.get(),
                to_node_id: to.get(),
                to_addr,
                voters: peers,
            })
            .await
            .map_err(|e| PdError::Raft(format!("migrate_group_member: {e}")))?
            .into_inner();
        if let Some(err) = resp.error {
            return Err(PdError::Raft(format!(
                "migrate_group_member rejected: partition {} kind {}",
                err.partition_id, err.kind
            )));
        }
        Ok(())
    }
}

/// The wire namespace of a partition (the route record's `ns`).
fn partition_ns(partition: &MetaPartition) -> NsMode {
    match partition.ns {
        epoch_pd::NsMode::Flat => NsMode::Flat,
        epoch_pd::NsMode::Hier => NsMode::Hier,
    }
}
