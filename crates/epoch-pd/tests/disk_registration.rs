//! End-to-end disk membership over the single-node PD raft group.
//!
//! Covers registration (id allocation + `(node, path)` idempotency + owning-node
//! validation), read-back through the shared state, a status transition (valid +
//! rejected), and recovery of the disk set, statuses, and id counter across a
//! snapshot + restart (design 01 §1; 02 §1.8).

use std::time::Duration;

use epoch_pd::{ApplyResult, DiskStatus, Journal, RejectReason, RoleSet};
use epoch_proto::{DiskId, NodeId};
use openraft::ServerState;

const PD_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7201";
const TIMEOUT: Duration = Duration::from_secs(10);
const TOTAL: u64 = 1 << 40;

async fn become_leader(journal: &Journal) {
    journal
        .raft()
        .wait(Some(TIMEOUT))
        .state(ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");
}

fn disk_id_of(result: ApplyResult) -> DiskId {
    match result {
        ApplyResult::DiskRegistered { disk_id } => disk_id,
        other => panic!("expected DiskRegistered, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_disk_transition_and_recover() {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- First start: register a node, a disk on it, transition, snapshot. ---
    let disk_id: DiskId = {
        let journal = Journal::open_single_node(dir.path(), PD_NODE_ID, ADDR)
            .await
            .expect("open");
        become_leader(&journal).await;

        let node = journal
            .register_node("10.0.0.1:9000", "az-a", "rack-1", RoleSet::DATA)
            .await
            .expect("register node");

        let disk_id = disk_id_of(
            journal
                .register_disk(node, "az-a", "rack-1", "/data/0", TOTAL)
                .await
                .expect("register disk"),
        );
        assert_eq!(disk_id.get(), 1);

        // Registration is idempotent by (node, path).
        let again = disk_id_of(
            journal
                .register_disk(node, "az-a", "rack-1", "/data/0", TOTAL)
                .await
                .expect("re-register disk"),
        );
        assert_eq!(again, disk_id);
        assert_eq!(journal.state().disks().len(), 1);

        // A disk for an unknown node is rejected (cross-manager invariant).
        assert!(matches!(
            journal
                .register_disk(NodeId::new(999), "az-a", "rack-9", "/data/9", TOTAL)
                .await
                .expect("register disk on unknown node"),
            ApplyResult::Rejected(RejectReason::NodeNotFound)
        ));

        // Read back through the shared state.
        let disk = journal.state().disks().get(disk_id).expect("disk present");
        assert_eq!(disk.node_id, node);
        assert_eq!(disk.status, DiskStatus::Normal);
        assert_eq!(disk.total, TOTAL);
        assert_eq!(disk.path, "/data/0");

        // Valid transition applies; an illegal one is rejected without effect.
        assert_eq!(
            journal
                .update_disk_status(disk_id, DiskStatus::Broken)
                .await
                .expect("update to broken"),
            ApplyResult::Applied
        );
        assert_eq!(
            journal.state().disks().get(disk_id).expect("disk").status,
            DiskStatus::Broken
        );
        assert!(matches!(
            journal
                .update_disk_status(disk_id, DiskStatus::Normal)
                .await
                .expect("update to normal"),
            ApplyResult::Rejected(_)
        ));

        journal
            .raft()
            .trigger()
            .snapshot()
            .await
            .expect("trigger snapshot");
        journal
            .raft()
            .wait(Some(TIMEOUT))
            .metrics(|m| m.snapshot.is_some(), "snapshot built")
            .await
            .expect("snapshot built");
        journal.shutdown().await.expect("shutdown");
        disk_id
    };

    // --- Restart: disk set, status, and id counter survive. ---
    {
        let journal = Journal::open_single_node(dir.path(), PD_NODE_ID, ADDR)
            .await
            .expect("reopen");
        become_leader(&journal).await;

        assert_eq!(journal.state().disks().len(), 1);
        assert_eq!(
            journal.state().disks().get(disk_id).expect("disk").status,
            DiskStatus::Broken
        );

        // The counter survived: the next disk continues the sequence.
        let node = journal
            .register_node("10.0.0.1:9000", "az-a", "rack-1", RoleSet::DATA)
            .await
            .expect("node reclaim");
        let second = disk_id_of(
            journal
                .register_disk(node, "az-a", "rack-1", "/data/1", TOTAL)
                .await
                .expect("register second disk"),
        );
        assert_eq!(second.get(), 2);
        journal.shutdown().await.expect("shutdown");
    }
}
