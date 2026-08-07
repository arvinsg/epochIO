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

//! End-to-end ShardRepair over the PD raft (01 §6.3 / §6.4): report a bad shard
//! → PD resolves the target node + epoch and records a ticket → the target node
//! pulls it → completion-receipt rebind (in place, epoch+1) authorized by the
//! ticket, not a Job → ticket cleared.
//!
//! The rebind mechanism and ticket registry are unit-tested in-crate; this drives
//! the full report → pull → commit chain through the real journal API with
//! committed chunk + disk state, exercising the apply-time target resolution and
//! ticket authorization the unit tests cannot reach.

use std::time::Duration;

use epoch_pd::shard_repair::{CommitShardRepair, ReportShardRepair};
use epoch_pd::{
    AntiAffinity, ApplyResult, DiskCandidate, Journal, RoleSet, SlotPlan, plan_placement,
};
use epoch_proto::{ChunkId, CodeMode, CodeModeId, DiskId, NodeId, ShardId};
use openraft::ServerState;

const RAFT_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7231";
const TIMEOUT: Duration = Duration::from_secs(10);
const CREATE_TS: i64 = 1_700_000_000_000;

fn code_mode() -> CodeMode {
    CodeMode {
        id: CodeModeId::new(1),
        data: 2,
        parity: 1,
        stripe_size: 1 << 20,
        blob_size: 32 << 20,
    }
}

async fn become_leader(journal: &Journal) {
    journal
        .raft()
        .wait(Some(TIMEOUT))
        .state(ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");
}

/// Registers three single-disk nodes; returns `(disk_id, node_id)` per shard slot.
async fn register_topology(journal: &Journal) -> Vec<(DiskId, NodeId)> {
    let mut out = Vec::new();
    for i in 0..3 {
        let addr = format!("10.0.1.{i}:9000");
        let rack = format!("r{i}");
        let node_id = journal
            .register_node(addr, "az1", rack.clone(), RoleSet::DATA)
            .await
            .expect("register node");
        let path = format!("/data/{i}");
        let disk_id = match journal
            .register_disk(node_id, "az1", rack, path, 1 << 40)
            .await
            .expect("register disk")
        {
            ApplyResult::DiskRegistered { disk_id } => disk_id,
            other => panic!("unexpected register_disk result: {other:?}"),
        };
        out.push((disk_id, node_id));
    }
    out
}

/// Registers topology and commits one `Writable` chunk; returns its id and the
/// per-slot `(disk, node)` placement in shard-index order.
async fn commit_chunk(journal: &Journal) -> (ChunkId, Vec<(DiskId, NodeId)>) {
    let topo = register_topology(journal).await;
    let candidates: Vec<DiskCandidate> = topo
        .iter()
        .enumerate()
        .map(|(i, &(disk_id, node_id))| DiskCandidate {
            disk_id,
            node_id,
            rack: format!("r{i}"),
            writable_extents: 0,
        })
        .collect();
    let chosen = plan_placement(
        &candidates,
        code_mode().shards_total(),
        AntiAffinity::HostAware,
    )
    .expect("placement");
    let slots: Vec<SlotPlan> = chosen
        .iter()
        .enumerate()
        .map(|(index, &disk_id)| SlotPlan {
            index: u8::try_from(index).expect("small index"),
            disk_id,
        })
        .collect();
    let chunk_id = match journal
        .create_chunk_staging(code_mode(), slots, CREATE_TS, 0)
        .await
        .expect("stage")
    {
        ApplyResult::ChunkStaged { chunk_id } => chunk_id,
        other => panic!("unexpected stage result: {other:?}"),
    };
    assert_eq!(
        journal.commit_chunk(chunk_id).await.expect("commit"),
        ApplyResult::Applied
    );
    // Map each committed slot's disk back to its owning node (shard-index order).
    let chunk = journal.state().chunks().get(chunk_id).expect("committed");
    let placement = chunk
        .shards
        .iter()
        .map(|slot| {
            let node = topo
                .iter()
                .find(|(d, _)| *d == slot.disk_id)
                .map(|(_, n)| *n)
                .expect("slot disk in topology");
            (slot.disk_id, node)
        })
        .collect();
    (chunk_id, placement)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn report_dispatch_and_commit_rebinds_in_place() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, ADDR)
        .await
        .expect("open");
    become_leader(&journal).await;

    let (chunk_id, placement) = commit_chunk(&journal).await;
    let target_index = 1u8;
    let (target_disk, target_node) = placement[usize::from(target_index)];

    // --- Report a bad shard: PD resolves target node + epoch, records a ticket. ---
    match journal
        .report_shard_repair(ReportShardRepair {
            chunk_id,
            index: target_index,
        })
        .await
        .expect("report")
    {
        ApplyResult::ShardRepairReported { created } => assert!(created, "new ticket"),
        other => panic!("unexpected report result: {other:?}"),
    }
    // A re-report of the same shard is idempotent (no second ticket).
    match journal
        .report_shard_repair(ReportShardRepair {
            chunk_id,
            index: target_index,
        })
        .await
        .expect("re-report")
    {
        ApplyResult::ShardRepairReported { created } => {
            assert!(!created, "duplicate report creates no new ticket")
        }
        other => panic!("unexpected re-report result: {other:?}"),
    }

    // The ticket surfaces on the target node's pull, with the resolved epoch.
    let repairs = journal
        .state()
        .shard_repairs()
        .tickets_for_node(target_node);
    assert_eq!(repairs.len(), 1, "one ticket targets the shard's node");
    assert_eq!(repairs[0].chunk_id, chunk_id);
    assert_eq!(repairs[0].index, target_index);
    assert_eq!(repairs[0].expected_epoch, 0, "resolved from committed slot");
    // No other node was targeted.
    let other_node = placement[0].1;
    if other_node != target_node {
        assert!(
            journal
                .state()
                .shard_repairs()
                .tickets_for_node(other_node)
                .is_empty()
        );
    }

    // --- A wrong-node / stale-epoch commit is rejected (ticket stays). ---
    let wrong_node = NodeId::new(target_node.get() + 100);
    let (_e, rebound) = commit(&journal, chunk_id, target_index, 0, target_disk, wrong_node).await;
    assert!(!rebound, "a non-target committer cannot rebind");
    let (_e, rebound) = commit(
        &journal,
        chunk_id,
        target_index,
        5,
        target_disk,
        target_node,
    )
    .await;
    assert!(!rebound, "a stale-epoch commit is rejected");
    assert_eq!(
        journal
            .state()
            .shard_repairs()
            .tickets_for_node(target_node)
            .len(),
        1,
        "ticket survives rejected commits"
    );

    // --- The target commits the repair in place (same disk): epoch+1, ticket cleared. ---
    let (new_epoch, rebound) = commit(
        &journal,
        chunk_id,
        target_index,
        0,
        target_disk,
        target_node,
    )
    .await;
    assert!(rebound, "target-authorized rebind applies");
    assert_eq!(
        new_epoch, 1,
        "in-place rebind bumps the slot epoch (01 §6.4 inv 5)"
    );

    let chunk = journal.state().chunks().get(chunk_id).expect("chunk");
    let slot = chunk
        .shards
        .iter()
        .find(|s| s.index() == target_index)
        .expect("slot");
    assert_eq!(slot.epoch, 1, "slot epoch bumped");
    assert_eq!(slot.disk_id, target_disk, "in-place: same disk");
    assert_eq!(
        slot.extent_id,
        epoch_proto::ExtentId::new(ShardId::new(chunk_id, target_index, 1), CREATE_TS + 1),
        "extent derives from the rebuilt shard id + create_ts"
    );
    // The ticket is cleared once the rebind lands.
    assert!(
        journal
            .state()
            .shard_repairs()
            .tickets_for_node(target_node)
            .is_empty(),
        "completed ticket cleared"
    );

    // A replayed commit after clearing is a benign no-op (no ticket ⇒ rejected).
    let (_e, rebound) = commit(
        &journal,
        chunk_id,
        target_index,
        1,
        target_disk,
        target_node,
    )
    .await;
    assert!(!rebound, "replayed commit without a ticket does not rebind");

    journal.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn report_of_unknown_shard_is_a_benign_noop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7232")
        .await
        .expect("open");
    become_leader(&journal).await;
    let _ = commit_chunk(&journal).await;

    // A report for a chunk that does not exist records no ticket (stale view).
    match journal
        .report_shard_repair(ReportShardRepair {
            chunk_id: ChunkId::new(9999),
            index: 0,
        })
        .await
        .expect("report")
    {
        ApplyResult::Rejected(_) => {}
        other => panic!("unexpected result for unknown chunk: {other:?}"),
    }
    journal.shutdown().await.expect("shutdown");
}

/// INVARIANT(design 01 §4.1): a repair rebind must not collocate two shards of
/// one chunk on a single node. The ShardRepair coordinator targets its *own* disk
/// for data affinity, so without this gate repeated repairs would pile shards of
/// one EC stripe onto one host — reads keep working, so the lost redundancy stays
/// invisible until that host fails and takes two shards with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebind_onto_a_node_already_holding_a_shard_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7233")
        .await
        .expect("open");
    become_leader(&journal).await;

    let (chunk_id, placement) = commit_chunk(&journal).await;
    let target_index = 1u8;
    let (own_disk, target_node) = placement[usize::from(target_index)];
    // A disk on a node that already hosts a *different* slot of this chunk.
    let (occupied_disk, occupied_node) = placement[0];
    assert_ne!(occupied_node, target_node, "distinct nodes after placement");

    journal
        .report_shard_repair(ReportShardRepair {
            chunk_id,
            index: target_index,
        })
        .await
        .expect("report");

    // Committing onto the occupied node is rejected — the ticket survives so the
    // repair can retry with a legal target.
    let (_e, rebound) = commit(
        &journal,
        chunk_id,
        target_index,
        0,
        occupied_disk,
        target_node,
    )
    .await;
    assert!(
        !rebound,
        "a rebind that collocates two shards of one chunk must be rejected"
    );
    let slot_disk = |journal: &Journal| {
        journal
            .state()
            .chunks()
            .get(chunk_id)
            .expect("chunk")
            .shards
            .iter()
            .find(|s| s.index() == target_index)
            .expect("slot")
            .disk_id
    };
    assert_eq!(
        slot_disk(&journal),
        own_disk,
        "the rejected rebind left the slot untouched"
    );
    assert_eq!(
        journal
            .state()
            .shard_repairs()
            .tickets_for_node(target_node)
            .len(),
        1,
        "ticket survives so the repair can retry on a legal disk"
    );

    // The in-place rebuild (its own disk) is still allowed: the moving slot is
    // excluded from the collision set.
    let (new_epoch, rebound) =
        commit(&journal, chunk_id, target_index, 0, own_disk, target_node).await;
    assert!(rebound, "in-place repair is unaffected by the gate");
    assert_eq!(new_epoch, 1);

    journal.shutdown().await.expect("shutdown");
}

/// Proposes a `CommitShardRepair` and returns `(new_epoch, rebound)`.
async fn commit(
    journal: &Journal,
    chunk_id: ChunkId,
    index: u8,
    expected_epoch: u32,
    new_disk: DiskId,
    committer: NodeId,
) -> (u32, bool) {
    match journal
        .commit_shard_repair(CommitShardRepair {
            chunk_id,
            index,
            expected_epoch,
            new_disk,
            new_create_ts: CREATE_TS + 1,
            committer,
        })
        .await
        .expect("commit_shard_repair")
    {
        ApplyResult::ShardRebound { new_epoch } => (new_epoch, true),
        ApplyResult::Rejected(_) => (0, false),
        other => panic!("unexpected commit result: {other:?}"),
    }
}
