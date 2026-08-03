//! End-to-end single-node PD raft: bootstrap → propose → snapshot → restart.
//!
//! Covers the raft assembly ([`Journal::open_single_node`]), the journal propose
//! path, snapshot triggering, and recovery of applied state from the persisted
//! log + state machine across a restart (design 01 §2).

use std::time::Duration;

use epoch_pd::{ApplyResult, Journal, PdEntry};
use openraft::ServerState;

const NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7001";
const TIMEOUT: Duration = Duration::from_secs(10);

fn applied_index(journal: &Journal) -> u64 {
    journal
        .raft()
        .metrics()
        .borrow()
        .last_applied
        .map(|log_id| log_id.index)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_propose_snapshot_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- First start: elect self, propose, build a snapshot. ---
    let applied_before = {
        let journal = Journal::open_single_node(dir.path(), NODE_ID, ADDR)
            .await
            .expect("open");
        journal
            .raft()
            .wait(Some(TIMEOUT))
            .state(ServerState::Leader, "single node becomes leader")
            .await
            .expect("become leader");

        assert_eq!(
            journal.propose(PdEntry::Noop).await.expect("propose"),
            ApplyResult::Applied
        );

        let applied = applied_index(&journal);
        assert!(
            applied >= 1,
            "applied index should advance past bootstrap entries"
        );

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
        applied
    };

    // --- Restart on the same directory: recover, re-elect, stay writable. ---
    {
        let journal = Journal::open_single_node(dir.path(), NODE_ID, ADDR)
            .await
            .expect("reopen");
        journal
            .raft()
            .wait(Some(TIMEOUT))
            .state(ServerState::Leader, "recovered node becomes leader")
            .await
            .expect("re-become leader");

        assert!(
            applied_index(&journal) >= applied_before,
            "applied state must survive restart (log + state machine are durable)"
        );

        assert_eq!(
            journal
                .propose(PdEntry::Noop)
                .await
                .expect("propose after restart"),
            ApplyResult::Applied
        );
        journal.shutdown().await.expect("shutdown");
    }
}
