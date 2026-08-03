//! End-to-end node membership over the single-node PD raft group.
//!
//! Covers registration (id allocation + address idempotency), read-back through
//! the shared state, a status transition (valid + rejected), and recovery of the
//! node set, statuses, and id counter across a snapshot + restart (design 01 §1).

use std::time::Duration;

use epoch_pd::{ApplyResult, Journal, NodeStatus, RoleSet};
use epoch_proto::NodeId;
use openraft::ServerState;

const PD_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7101";
const TIMEOUT: Duration = Duration::from_secs(10);

async fn become_leader(journal: &Journal) {
    journal
        .raft()
        .wait(Some(TIMEOUT))
        .state(ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_read_transition_and_recover() {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- First start: register nodes, transition one, snapshot. ---
    let (first, second): (NodeId, NodeId) = {
        let journal = Journal::open_single_node(dir.path(), PD_NODE_ID, ADDR)
            .await
            .expect("open");
        become_leader(&journal).await;

        let first = journal
            .register_node("10.0.0.1:9000", "az-a", "rack-1", RoleSet::DATA)
            .await
            .expect("register first");
        let second = journal
            .register_node(
                "10.0.0.2:9000",
                "az-a",
                "rack-2",
                RoleSet::DATA.union(RoleSet::GATEWAY),
            )
            .await
            .expect("register second");
        assert_eq!(first.get(), 1);
        assert_eq!(second.get(), 2);

        // Registration is idempotent by address.
        let again = journal
            .register_node("10.0.0.1:9000", "az-a", "rack-1", RoleSet::DATA)
            .await
            .expect("re-register");
        assert_eq!(again, first);
        assert_eq!(journal.state().nodes().len(), 2);

        // Read back through the shared state.
        let node = journal.state().nodes().get(first).expect("node present");
        assert_eq!(node.addr, "10.0.0.1:9000");
        assert_eq!(node.status, NodeStatus::Starting);
        assert!(node.roles.contains(RoleSet::DATA));

        // Valid transition applies; an illegal one is rejected without effect.
        assert_eq!(
            journal
                .update_node_status(first, NodeStatus::Live)
                .await
                .expect("update to live"),
            ApplyResult::Applied
        );
        assert_eq!(
            journal.state().nodes().get(first).expect("node").status,
            NodeStatus::Live
        );
        assert!(matches!(
            journal
                .update_node_status(first, NodeStatus::Lost)
                .await
                .expect("update to lost"),
            ApplyResult::Rejected(_)
        ));
        assert_eq!(
            journal.state().nodes().get(first).expect("node").status,
            NodeStatus::Live
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
        (first, second)
    };

    // --- Restart: node set, statuses, and id counter survive. ---
    {
        let journal = Journal::open_single_node(dir.path(), PD_NODE_ID, ADDR)
            .await
            .expect("reopen");
        become_leader(&journal).await;

        assert_eq!(journal.state().nodes().len(), 2);
        assert_eq!(
            journal.state().nodes().get(first).expect("first").status,
            NodeStatus::Live
        );
        assert_eq!(
            journal.state().nodes().get(second).expect("second").addr,
            "10.0.0.2:9000"
        );

        // The counter survived: the next registration continues the sequence.
        let third = journal
            .register_node("10.0.0.3:9000", "az-b", "rack-3", RoleSet::META)
            .await
            .expect("register third");
        assert_eq!(third.get(), 3);
        journal.shutdown().await.expect("shutdown");
    }
}
