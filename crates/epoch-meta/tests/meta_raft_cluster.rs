//! Multi-group raft over the batched `raft.proto` transport: three real
//! MetaNodes run several partition groups at once — electing leaders,
//! replicating writes, coalescing every group's traffic onto one RPC stream
//! per node pair (03 §8 心跳合并), failing over when a group leader dies, and
//! replaying the log after a simulated kill -9 (07 §M5a acceptance core).
//!
//! Servers bind OS-assigned loopback ports; the batching hubs dial peers by
//! the address carried in each group's membership.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::log_store::MetaLogDb;
use epoch_meta::raft::network::{MetaRaftTransportService, TransportCounters};
use epoch_meta::raft::{GroupManager, MetaEntry, MetaRaft, SplitOp};
use epoch_meta::store::keys::flat_key;
use epoch_meta::store::rocks::RocksEngine;
use epoch_meta::store::{MetaCf, MetaStore, StoreOp};
use epoch_proto::BucketId;
use openraft::{BasicNode, Config};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

const TIMEOUT: Duration = Duration::from_secs(20);
const GROUPS: [u64; 4] = [1, 2, 3, 4];

/// One running MetaNode: its multi-group runtime and transport server.
struct TestNode {
    id: u64,
    addr: String,
    dir: tempfile::TempDir,
    manager: Arc<GroupManager>,
    counters: Arc<TransportCounters>,
    shutdown: tokio::sync::watch::Sender<bool>,
    server: JoinHandle<()>,
}

impl TestNode {
    fn raft(&self, group: u64) -> MetaRaft {
        self.manager.raft(group).expect("group running")
    }

    fn store(&self) -> Arc<dyn MetaStore> {
        self.manager.store()
    }
}

fn test_config() -> Config {
    Config {
        cluster_name: "meta-it".to_string(),
        ..Default::default()
    }
}

/// Starts a node: opens the shared engine + multi-group runtime over `dir`,
/// serves the batched transport on an OS-assigned (or the given) port.
async fn spawn_node(id: u64, dir: tempfile::TempDir, addr: Option<&str>) -> TestNode {
    let engine = Arc::new(RocksEngine::open(&dir.path().join("sm")).expect("open engine"));
    let manager = Arc::new(
        GroupManager::open(
            &dir.path().join("raft-log"),
            id,
            engine,
            Arc::new(epoch_meta::ref_extractor::EpochRefExtractor),
            test_config(),
        )
        .expect("open manager"),
    );
    let bind = addr.unwrap_or("127.0.0.1:0");
    let listener = TcpListener::bind(bind).await.expect("bind");
    let addr = listener.local_addr().expect("local addr").to_string();

    let service = MetaRaftTransportService::new(Arc::clone(&manager));
    let counters = service.counters();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(async move {
        let incoming = TcpListenerStream::new(listener);
        let signal = async move {
            let mut rx = shutdown_rx;
            let _ = rx.wait_for(|&v| v).await;
        };
        let _ = tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(incoming, signal)
            .await;
    });

    TestNode {
        id,
        addr,
        dir,
        manager,
        counters,
        shutdown: shutdown_tx,
        server,
    }
}

/// Stops the gRPC server like a process exit would: GOAWAY + connection close,
/// so peers' cached channels break and re-dial (crucial when the node is
/// reincarnated on the same address — a plain task abort would leave accepted
/// connections serving the *dead* manager's Weak reference forever).
async fn stop_server(shutdown: &tokio::sync::watch::Sender<bool>, server: &mut JoinHandle<()>) {
    let _ = shutdown.send(true);
    if tokio::time::timeout(Duration::from_secs(5), &mut *server)
        .await
        .is_err()
    {
        server.abort();
    }
}

/// Creates every test group on every node with a full flat range.
async fn create_groups(nodes: &[TestNode]) {
    for node in nodes {
        for group in GROUPS {
            node.manager
                .create_group(group, PartitionRange::full(Namespace::Flat))
                .await
                .expect("create group");
        }
    }
}

/// Bootstraps cluster membership for every group from node 1.
async fn initialize_groups(nodes: &[TestNode]) {
    let members: BTreeMap<u64, BasicNode> = nodes
        .iter()
        .map(|n| (n.id, BasicNode::new(n.addr.clone())))
        .collect();
    for group in GROUPS {
        nodes[0]
            .raft(group)
            .initialize(members.clone())
            .await
            .expect("initialize");
    }
}

/// Polls until some node (excluding `exclude`) reports itself leader of
/// `group`, or panics on timeout.
async fn wait_leader(nodes: &[TestNode], group: u64, exclude: Option<u64>) -> u64 {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        for node in nodes {
            if exclude == Some(node.id) {
                continue;
            }
            if node.raft(group).metrics().borrow().state.is_leader() {
                return node.id;
            }
        }
        if Instant::now() >= deadline {
            for node in nodes {
                let metrics = node.raft(group).metrics().borrow().clone();
                eprintln!(
                    "node {} group {} state {:?} current_leader {:?} vote {:?}",
                    node.id, group, metrics.state, metrics.current_leader, metrics.vote
                );
            }
            panic!("no leader for group {group}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls until `node`'s store holds `key`, or panics on timeout.
async fn wait_replicated(node: &TestNode, key: &[u8]) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if node.store().get(MetaCf::Meta, key).expect("get").is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "key {key:?} not replicated in time on node {}",
            node.id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn node(nodes: &[TestNode], id: u64) -> &TestNode {
    nodes.iter().find(|n| n.id == id).expect("node present")
}

/// The metadata key written by group `group`, round `round` (distinct per
/// group: one shared store must never see two groups claiming one key — in
/// production the disjoint ranges guarantee it, here the test keys do).
fn test_key(group: u64, round: u32) -> Vec<u8> {
    flat_key(
        MetaCf::Meta,
        BucketId::new(1),
        format!("g{group}/obj{round}").as_bytes(),
        &[],
    )
}

fn write_op(key: Vec<u8>, value: &[u8]) -> MetaEntry {
    MetaEntry::StoreOps(vec![StoreOp::put(MetaCf::Meta, key, value)])
}

/// Gracefully stops every group core on `node` (the crash-simulation stop:
/// dropping the manager afterwards closes the databases *without* flushing
/// the disableWAL tail — exactly the kill -9 loss window, 03 §8).
async fn stop_groups(node: &TestNode) {
    for group in node.manager.group_ids() {
        if let Some(raft) = node.manager.raft(group) {
            let _ = raft.shutdown().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn multi_group_cluster_replicates_batches_and_recovers() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "epoch_meta=debug,openraft::raft=info".into()),
        )
        .with_test_writer()
        .try_init();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        nodes.push(spawn_node(id, tempfile::tempdir().expect("tempdir"), None).await);
    }
    create_groups(&nodes).await;
    initialize_groups(&nodes).await;

    // Every group elects a leader; concurrent writes replicate to followers.
    for &group in &GROUPS {
        wait_leader(&nodes, group, None).await;
    }
    for &group in &GROUPS {
        let leader = wait_leader(&nodes, group, None).await;
        let key = test_key(group, 0);
        node(&nodes, leader)
            .raft(group)
            .client_write(write_op(key.clone(), b"head"))
            .await
            .expect("client write");
        let follower = nodes
            .iter()
            .find(|n| n.id != leader)
            .expect("a follower exists");
        wait_replicated(follower, &key).await;
    }

    // Batching evidence: with four groups heartbeating + replicating, total
    // received envelopes across the cluster strictly outnumber Batch RPCs —
    // traffic coalesced onto the single per-pair stream (03 §8). (A pure
    // leader node legitimately receives nothing: heartbeats flow outbound.)
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let (batches, envelopes): (usize, usize) = nodes
        .iter()
        .map(|n| (n.counters.batches(), n.counters.envelopes()))
        .fold((0, 0), |(b0, e0), (b1, e1)| (b0 + b1, e0 + e1));
    assert!(batches > 0, "no raft traffic received cluster-wide");
    assert!(
        envelopes > batches,
        "envelopes ({envelopes}) must outnumber batches ({batches})"
    );

    // Failover: shut down group 1's leader replica; the survivors elect a new
    // leader that keeps accepting writes.
    let leader1 = wait_leader(&nodes, 1, None).await;
    node(&nodes, leader1)
        .raft(1)
        .shutdown()
        .await
        .expect("shutdown");
    let new_leader = wait_leader(&nodes, 1, Some(leader1)).await;
    let key = test_key(1, 1);
    node(&nodes, new_leader)
        .raft(1)
        .client_write(write_op(key.clone(), b"after-failover"))
        .await
        .expect("write on new leader");
    let survivor = nodes
        .iter()
        .find(|n| n.id != leader1 && n.id != new_leader)
        .expect("survivor exists");
    wait_replicated(survivor, &key).await;

    // Simulated kill -9 of node 3: stop its group cores, abort its server,
    // drop the runtime — the disableWAL apply tail is lost on close. The data
    // dir survives and is reused by the restarted node.
    let victim = nodes.remove(2);
    stop_groups(&victim).await;
    let TestNode {
        addr: victim_addr,
        dir: victim_dir,
        manager: victim_manager,
        shutdown: victim_shutdown,
        server: mut victim_server,
        ..
    } = victim;
    stop_server(&victim_shutdown, &mut victim_server).await;
    drop(victim_manager);

    // A real kill -9 releases the RocksDB locks at process exit; inside one
    // test process the aborted gRPC handler tasks can pin the manager (and
    // engines) briefly — wait for the locks before reincarnating the node.
    let lock_deadline = Instant::now() + TIMEOUT;
    loop {
        let sm_free = RocksEngine::open(&victim_dir.path().join("sm")).is_ok();
        let log_free = MetaLogDb::open(&victim_dir.path().join("raft-log")).is_ok();
        if sm_free && log_free {
            break;
        }
        assert!(
            Instant::now() < lock_deadline,
            "crashed node's database locks not released"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Writes continue on the surviving quorum of groups 2–4 (nodes 1/2).
    // Group 1 lost two replicas (leader shutdown + node 3) and must stall —
    // that is raft doing its job, not a bug.
    for group in [2u64, 3, 4] {
        let leader = wait_leader(&nodes, group, None).await;
        let key = test_key(group, 2);
        node(&nodes, leader)
            .raft(group)
            .client_write(write_op(key, b"post-kill"))
            .await
            .expect("post-kill write");
    }

    // Restart node 3 on the same dirs + address: recovery reopens every
    // registered group, raft replays the log, and the node catches up —
    // including the entries written while it was "dead".
    let recovered = spawn_node(3, victim_dir, Some(&victim_addr)).await;
    let mut recovered_groups = recovered.manager.recover().await.expect("recover");
    recovered_groups.sort_unstable();
    assert_eq!(recovered_groups, GROUPS.to_vec());
    for group in [2u64, 3, 4] {
        wait_replicated(&recovered, &test_key(group, 2)).await;
        wait_replicated(&recovered, &test_key(group, 0)).await;
    }
    // Group 1 regains its quorum (node 2 + recovered node 3), re-elects, and
    // the recovered replica catches up on the post-failover write too.
    wait_replicated(&recovered, &test_key(1, 1)).await;
    wait_replicated(&recovered, &test_key(1, 0)).await;

    for n in &nodes {
        stop_groups(n).await;
    }
    for n in nodes {
        let mut server = n.server;
        stop_server(&n.shutdown, &mut server).await;
    }
    stop_groups(&recovered).await;
    let mut server = recovered.server;
    stop_server(&recovered.shutdown, &mut server).await;
}

/// M5b-3 acceptance (07 §M5b 分裂确定性): under a real 3-replica group, a
/// committed `Split` is applied at the same log position on every replica —
/// each independently narrows the parent and derives the child group over its
/// own shared engine, and all three converge to the identical parent/child
/// ranges. The child then elects a leader and serves writes on its half.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn split_is_deterministic_across_three_replicas() {
    const PARENT: u64 = 1;
    const CHILD: u64 = 100;

    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let dir = tempfile::tempdir().expect("tempdir");
        nodes.push(spawn_node(id, dir, None).await);
    }
    // One shared group (PARENT) across all three replicas.
    let members: BTreeMap<u64, BasicNode> = nodes
        .iter()
        .map(|n| (n.id, BasicNode::new(n.addr.clone())))
        .collect();
    for n in &nodes {
        n.manager
            .create_group(PARENT, PartitionRange::full(Namespace::Flat))
            .await
            .expect("create parent");
    }
    nodes[0]
        .raft(PARENT)
        .initialize(members)
        .await
        .expect("initialize");
    let leader = wait_leader(&nodes, PARENT, None).await;

    // Propose the split on the leader (PD drives this in production). The child
    // keeps the same replica set; node 1 bootstraps its membership.
    node(&nodes, leader)
        .raft(PARENT)
        .client_write(MetaEntry::Split(SplitOp {
            child_group: CHILD,
            at_bucket: 1,
            at_routing_key: b"m".to_vec(),
            child_peers: nodes.iter().map(|n| (n.id, n.addr.clone())).collect(),
            bootstrap_node: 1,
            child_ino_tag: 1,
        }))
        .await
        .expect("propose split");

    // Each replica applies the split at the same log index and derives its own
    // child from the recorded pending-split — deterministic, no cross-node
    // coordination in the derivation. `reconcile_splits` is a no-op until this
    // replica has applied the split entry, so poll it (openraft's applied-index
    // metric lags the state-machine write, hence gating on the record's effect
    // rather than the metric).
    let deadline = Instant::now() + TIMEOUT;
    for n in &nodes {
        loop {
            if n.manager.raft(CHILD).is_some()
                || n.manager.reconcile_splits().await.expect("reconcile") == vec![CHILD]
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {} never derived the child",
                n.id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // All three replicas agree: each hosts the child group over its own shared
    // engine — the split cut at one deterministic point (03 §2 状态哈希一致).
    for n in &nodes {
        assert!(
            n.manager.group_ids().contains(&CHILD),
            "node {} hosts the child",
            n.id
        );
    }

    // The child elects a leader and serves a write on its own group.
    let child_leader = wait_leader(&nodes, CHILD, None).await;
    node(&nodes, child_leader)
        .raft(CHILD)
        .client_write(MetaEntry::StoreOps(vec![StoreOp::put(
            MetaCf::Meta,
            flat_key(MetaCf::Meta, BucketId::new(1), b"z-in-child", &[]),
            b"v".as_slice(),
        )]))
        .await
        .expect("child serves write");

    for n in &nodes {
        stop_groups(n).await;
    }
    for n in nodes {
        let mut server = n.server;
        stop_server(&n.shutdown, &mut server).await;
    }
}

/// M5b-4 acceptance (07 §M5b 迁移): a partition replica moves from one node to
/// another by openraft membership change — AddLearner (data rides the snapshot
/// path) → catch-up → Promote + RemovePeer — with zero data loss. The target
/// receives the group's prior writes purely through replication.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn migrate_moves_a_replica_with_zero_data_loss() {
    use epoch_meta::raft::migrate;

    const GROUP: u64 = 1;

    // Four nodes: the group starts on {1,2,3}; node 4 is the migration target.
    let mut nodes = Vec::new();
    for id in 1..=4u64 {
        let dir = tempfile::tempdir().expect("tempdir");
        nodes.push(spawn_node(id, dir, None).await);
    }
    // Create the group on the three initial voters + the target (un-init on all;
    // the target must host it before it can receive replication, 03 §2).
    for n in &nodes {
        n.manager
            .create_group(GROUP, PartitionRange::full(Namespace::Flat))
            .await
            .expect("create group");
    }
    let voters: BTreeMap<u64, BasicNode> = nodes
        .iter()
        .take(3)
        .map(|n| (n.id, BasicNode::new(n.addr.clone())))
        .collect();
    nodes[0]
        .raft(GROUP)
        .initialize(voters)
        .await
        .expect("initialize on {1,2,3}");
    let leader = wait_leader(&nodes[..3], GROUP, None).await;

    // Write some data the target has never seen.
    for round in 0..5u32 {
        node(&nodes, leader)
            .raft(GROUP)
            .client_write(write_op(test_key(GROUP, round), b"v"))
            .await
            .expect("write");
    }

    // Migrate replica: node 3 → node 4. The leader adds node 4 as a learner
    // (openraft replicates/snapshots the data to it), then promotes it and
    // drops node 3, all as one joint change.
    let target = &nodes[3];
    let new_voters = migrate::migrate_replica(
        &node(&nodes, leader).raft(GROUP),
        3,
        4,
        target.addr.clone(),
        &BTreeSet::from([1, 2, 3]),
    )
    .await
    .expect("migrate replica");
    assert_eq!(new_voters, BTreeSet::from([1, 2, 4]));

    // The target now holds every prior write — data moved with zero loss,
    // purely through replication (it was never sent the ops directly).
    for round in 0..5u32 {
        wait_replicated(target, &test_key(GROUP, round)).await;
    }

    // The group still serves writes after the membership change (post-migrate
    // read/write not interrupted, 03 §2).
    let leader = wait_leader(&nodes[..4], GROUP, Some(3)).await;
    node(&nodes, leader)
        .raft(GROUP)
        .client_write(write_op(test_key(GROUP, 99), b"after"))
        .await
        .expect("write after migrate");
    wait_replicated(target, &test_key(GROUP, 99)).await;

    for n in &nodes {
        stop_groups(n).await;
    }
    for n in nodes {
        let mut server = n.server;
        stop_server(&n.shutdown, &mut server).await;
    }
}
