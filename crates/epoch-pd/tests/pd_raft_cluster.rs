//! Multi-replica PD raft over the `raft.proto` gRPC transport: three real
//! servers elect a leader, replicate a journal write, and — after the leader is
//! killed — fail over to a new leader that keeps accepting writes (07 §M4
//! acceptance: "kill PD leader → failover → writes recover").
//!
//! Servers bind OS-assigned loopback ports; the raft network dials peers by the
//! address carried in the cluster membership.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_pd::{Journal, PdRaftPeerService, RoleSet};
use epoch_proto::NodeId;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

const TIMEOUT: Duration = Duration::from_secs(15);

/// One running PD replica: its raft node (via the journal) and the serving task.
struct Member {
    id: u64,
    journal: Arc<Journal>,
    server: Option<JoinHandle<()>>,
}

/// Serves the raft peer transport for `journal` on `listener` until aborted.
fn serve(journal: &Arc<Journal>, listener: TcpListener) -> JoinHandle<()> {
    let service = PdRaftPeerService::new(journal.raft().clone()).into_server();
    tokio::spawn(async move {
        let incoming = TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await;
    })
}

/// The id of the first member currently reporting itself leader, skipping
/// `exclude` (the just-killed replica).
fn current_leader(members: &[Member], exclude: Option<u64>) -> Option<u64> {
    members.iter().find_map(|m| {
        if exclude == Some(m.id) {
            return None;
        }
        m.journal
            .raft()
            .metrics()
            .borrow()
            .state
            .is_leader()
            .then_some(m.id)
    })
}

/// Polls until a leader emerges (skipping `exclude`), or panics on timeout.
async fn wait_for_leader(members: &[Member], exclude: Option<u64>) -> u64 {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Some(id) = current_leader(members, exclude) {
            return id;
        }
        assert!(Instant::now() < deadline, "no leader elected in time");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Borrows the member with `id`.
fn member(members: &[Member], id: u64) -> &Member {
    members.iter().find(|m| m.id == id).expect("member present")
}

/// Polls until `journal` has applied the registration of `node_id`, or panics.
async fn wait_replicated(journal: &Journal, node_id: NodeId) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if journal.state().nodes().contains(node_id) {
            return;
        }
        assert!(Instant::now() < deadline, "entry not replicated in time");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_replicates_and_fails_over() {
    let dirs: Vec<_> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();

    // Open three members without initializing membership.
    let mut members = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let id = u64::try_from(i + 1).expect("small id");
        let journal = Arc::new(
            Journal::open_member(dir.path(), id)
                .await
                .expect("open member"),
        );
        members.push(Member {
            id,
            journal,
            server: None,
        });
    }

    // Bind a loopback port per member and start serving the raft transport.
    let mut addrs = BTreeMap::new();
    for m in &mut members {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        addrs.insert(m.id, listener.local_addr().expect("addr").to_string());
        m.server = Some(serve(&m.journal, listener));
    }

    // Bootstrap the cluster from member 1; a leader emerges.
    member(&members, 1)
        .journal
        .initialize_cluster(addrs.clone())
        .await
        .expect("initialize cluster");
    let leader_id = wait_for_leader(&members, None).await;

    // A write on the leader replicates to a follower.
    let first = member(&members, leader_id)
        .journal
        .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
        .await
        .expect("register on leader");
    let follower = members
        .iter()
        .find(|m| m.id != leader_id)
        .expect("a follower exists");
    wait_replicated(&follower.journal, first).await;

    // Kill the leader: stop its raft node and abort its server task.
    {
        let dead = member(&members, leader_id);
        dead.journal.shutdown().await.expect("shutdown leader");
        if let Some(server) = &dead.server {
            server.abort();
        }
    }

    // The surviving quorum elects a new leader that keeps accepting writes.
    let new_leader_id = wait_for_leader(&members, Some(leader_id)).await;
    assert_ne!(
        new_leader_id, leader_id,
        "a different replica must take over"
    );
    let second = member(&members, new_leader_id)
        .journal
        .register_node("10.0.0.2:9000", "az2", "r2", RoleSet::DATA)
        .await
        .expect("register on new leader");
    let survivor = members
        .iter()
        .find(|m| m.id != leader_id && m.id != new_leader_id)
        .expect("other survivor exists");
    wait_replicated(&survivor.journal, second).await;

    // Tear down the survivors.
    for m in &members {
        if m.id != leader_id {
            let _ = m.journal.shutdown().await;
        }
        if let Some(server) = &m.server {
            server.abort();
        }
    }
}
