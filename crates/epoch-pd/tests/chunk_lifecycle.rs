//! End-to-end chunk creation over the PD raft: watermark placement → two-phase
//! staging → crash before commit → recovery scan → epoch re-bump → commit.
//!
//! Exercises the journal typed chunk API, the deterministic apply path, and
//! recovery of a half-created chunk across a restart (design 01 §4.1). Placement
//! itself is unit-tested in the chunk module; here it feeds the real staging
//! flow.

use std::time::Duration;

use epoch_pd::{
    AntiAffinity, ApplyResult, ChunkStatus, DiskCandidate, Journal, RoleSet, SlotPlan,
    plan_placement,
};
use epoch_proto::{ChunkId, CodeMode, CodeModeId, DiskId, NodeId};
use openraft::ServerState;

const RAFT_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7201";
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

/// Registers three single-disk nodes and returns their disk ids.
async fn register_topology(journal: &Journal) -> Vec<DiskId> {
    let mut disks = Vec::new();
    for i in 0..3 {
        let addr = format!("10.0.0.{i}:9000");
        let rack = format!("r{i}");
        let node_id = journal
            .register_node(addr.clone(), "az1", rack.clone(), RoleSet::DATA)
            .await
            .expect("register node");
        let path = format!("/data/{i}");
        match journal
            .register_disk(node_id, "az1", rack, path, 1 << 40)
            .await
            .expect("register disk")
        {
            ApplyResult::DiskRegistered { disk_id } => disks.push(disk_id),
            other => panic!("unexpected register_disk result: {other:?}"),
        }
    }
    disks
}

/// Builds placement candidates for the registered disks (equal watermark; the
/// heartbeat wiring lands in a later phase).
fn candidates(disks: &[DiskId]) -> Vec<DiskCandidate> {
    disks
        .iter()
        .enumerate()
        .map(|(i, &disk_id)| DiskCandidate {
            disk_id,
            node_id: NodeId::new(u32::try_from(i + 1).expect("small")),
            rack: format!("r{i}"),
            writable_extents: 0,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_creation_recovers_staging_across_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- First start: register topology, plan placement, stage a chunk. ---
    let (chunk_id, planned_disks) = {
        let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, ADDR)
            .await
            .expect("open");
        become_leader(&journal).await;

        let disks = register_topology(&journal).await;
        let chosen = plan_placement(
            &candidates(&disks),
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

        // Staged but not committed; crash here (shut down before commit).
        assert!(journal.state().chunks().get(chunk_id).is_none());
        assert_eq!(journal.state().chunks().pending_staging().len(), 1);
        journal.shutdown().await.expect("shutdown");
        (chunk_id, chosen)
    };

    // --- Restart: recover the staging plan, re-bump epoch, commit. ---
    {
        let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, ADDR)
            .await
            .expect("reopen");
        become_leader(&journal).await;

        // The half-created plan survived the restart.
        let pending = journal.state().chunks().pending_staging();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].chunk_id, chunk_id);
        assert_eq!(pending[0].shards[0].epoch, 0);

        // Recovery: bump epoch (+3) then commit.
        assert_eq!(
            journal.rebump_staging(chunk_id).await.expect("rebump"),
            ApplyResult::Applied
        );
        assert_eq!(
            journal.commit_chunk(chunk_id).await.expect("commit"),
            ApplyResult::Applied
        );

        let chunk = journal.state().chunks().get(chunk_id).expect("committed");
        assert_eq!(chunk.status, ChunkStatus::Writable);
        assert_eq!(chunk.shards.len(), 3);
        assert!(journal.state().chunks().pending_staging().is_empty());
        for (slot, &disk_id) in chunk.shards.iter().zip(&planned_disks) {
            assert_eq!(slot.epoch, 3, "epoch jumped by 3 on recovery");
            assert_eq!(slot.disk_id, disk_id, "shard stays on its planned disk");
        }
        journal.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_rejects_unusable_disks_and_bad_plans() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7202")
        .await
        .expect("open");
    become_leader(&journal).await;
    let disks = register_topology(&journal).await;

    // A slot referencing an unregistered disk is rejected.
    let bad_disk_slots = vec![
        SlotPlan {
            index: 0,
            disk_id: disks[0],
        },
        SlotPlan {
            index: 1,
            disk_id: disks[1],
        },
        SlotPlan {
            index: 2,
            disk_id: DiskId::new(9999),
        },
    ];
    assert_eq!(
        journal
            .create_chunk_staging(code_mode(), bad_disk_slots, CREATE_TS, 0)
            .await
            .expect("stage"),
        ApplyResult::Rejected(epoch_pd::RejectReason::DiskUnavailable)
    );

    // A plan with the wrong slot count is rejected.
    let short_plan = vec![
        SlotPlan {
            index: 0,
            disk_id: disks[0],
        },
        SlotPlan {
            index: 1,
            disk_id: disks[1],
        },
    ];
    assert_eq!(
        journal
            .create_chunk_staging(code_mode(), short_plan, CREATE_TS, 0)
            .await
            .expect("stage"),
        ApplyResult::Rejected(epoch_pd::RejectReason::InvalidChunkPlan)
    );

    // No chunk was staged by either rejection.
    assert!(journal.state().chunks().pending_staging().is_empty());
    assert_eq!(journal.state().chunks().get(ChunkId::new(1)), None);
    journal.shutdown().await.expect("shutdown");
}
