//! Writer-token registry over the PD raft: issuance (never reused), session
//! liveness driven by gateway heartbeat staleness, and the one-way Live → Dead
//! retirement (design 01 §4.3). Exercises the journal writer API and the
//! leader-only liveness sweep end to end, plus recovery across a restart.

use std::time::Duration;

use epoch_pd::{ApplyResult, HeartbeatReport, Journal, RejectReason, RoleSet, WriterStatus};
use epoch_proto::{NodeId, WriterToken};
use openraft::ServerState;

const RAFT_NODE_ID: u64 = 1;
const TIMEOUT: Duration = Duration::from_secs(10);
const DEAD_AFTER: u64 = 90_000;

async fn become_leader(journal: &Journal) {
    journal
        .raft()
        .wait(Some(TIMEOUT))
        .state(ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");
}

/// Registers a gateway node and returns its id.
async fn register_gateway(journal: &Journal, addr: &str) -> NodeId {
    journal
        .register_node(addr, "az1", "r1", RoleSet::GATEWAY)
        .await
        .expect("register gateway")
}

fn heartbeat(node_id: NodeId) -> HeartbeatReport {
    HeartbeatReport {
        node_id,
        disks: Vec::new(),
    }
}

async fn register_writer(journal: &Journal, node_id: NodeId) -> WriterToken {
    match journal
        .register_writer(node_id)
        .await
        .expect("register writer")
    {
        ApplyResult::WriterRegistered { token } => token,
        other => panic!("unexpected register_writer result: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokens_are_issued_uniquely_and_require_a_known_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7401")
        .await
        .expect("open");
    become_leader(&journal).await;
    let gateway = register_gateway(&journal, "10.0.0.1:9000").await;

    // Distinct, never-reused tokens even for the same gateway (token rotation).
    let first = register_writer(&journal, gateway).await;
    let second = register_writer(&journal, gateway).await;
    assert_ne!(first, second);
    assert_eq!(journal.state().writers().len(), 2);

    // A token for an unknown node is rejected.
    assert_eq!(
        journal
            .register_writer(NodeId::new(999))
            .await
            .expect("propose"),
        ApplyResult::Rejected(RejectReason::NodeNotFound)
    );

    journal.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_session_is_retired_and_never_revives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7402")
        .await
        .expect("open");
    become_leader(&journal).await;

    // A heartbeating gateway holds a live token; a second gateway never
    // heartbeats (its token must not be force-retired).
    let seen = register_gateway(&journal, "10.0.0.1:9000").await;
    let unseen = register_gateway(&journal, "10.0.0.2:9000").await;
    journal.record_heartbeat(&heartbeat(seen), 1_000);
    let live_token = register_writer(&journal, seen).await;
    let unseen_token = register_writer(&journal, unseen).await;

    // A fresh heartbeat keeps the session: nothing retired.
    assert_eq!(
        journal
            .sweep_dead_writers(1_000, DEAD_AFTER)
            .await
            .expect("sweep"),
        0
    );
    assert_eq!(
        journal
            .state()
            .writers()
            .get(live_token)
            .expect("present")
            .status,
        WriterStatus::Live
    );

    // The heartbeat lapses past the grace: the seen token is retired, the
    // never-seen token is left alone.
    let now = 1_000 + DEAD_AFTER + 1;
    assert_eq!(
        journal
            .sweep_dead_writers(now, DEAD_AFTER)
            .await
            .expect("sweep"),
        1
    );
    assert_eq!(
        journal
            .state()
            .writers()
            .get(live_token)
            .expect("present")
            .status,
        WriterStatus::Dead
    );
    assert_eq!(
        journal
            .state()
            .writers()
            .get(unseen_token)
            .expect("present")
            .status,
        WriterStatus::Live
    );

    // A Dead token never revives: a later heartbeat + sweep does not resurrect it,
    // and an explicit retire is an idempotent no-op.
    journal.record_heartbeat(&heartbeat(seen), now);
    assert_eq!(
        journal
            .sweep_dead_writers(now, DEAD_AFTER)
            .await
            .expect("sweep"),
        0
    );
    assert_eq!(
        journal
            .mark_writer_dead(live_token)
            .await
            .expect("mark dead"),
        ApplyResult::Applied
    );
    assert_eq!(
        journal
            .state()
            .writers()
            .get(live_token)
            .expect("present")
            .status,
        WriterStatus::Dead
    );

    journal.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issued_tokens_survive_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let token = {
        let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7403")
            .await
            .expect("open");
        become_leader(&journal).await;
        let gateway = register_gateway(&journal, "10.0.0.1:9000").await;
        let token = register_writer(&journal, gateway).await;
        journal.shutdown().await.expect("shutdown");
        token
    };

    // Reopen: the issued token and the allocation counter survive.
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7403")
        .await
        .expect("reopen");
    become_leader(&journal).await;
    assert_eq!(
        journal
            .state()
            .writers()
            .get(token)
            .expect("present")
            .status,
        WriterStatus::Live
    );
    // The gateway node also survived, so a fresh token continues the sequence.
    let gateway = journal
        .state()
        .nodes()
        .get(NodeId::new(1))
        .expect("gateway");
    let next = register_writer(&journal, gateway.node_id).await;
    assert_ne!(next, token);

    journal.shutdown().await.expect("shutdown");
}
