//! Orphan-sentinel sweep (03 §6.2 注: 孤儿哨兵回收). A mkdir that commits step 1
//! (the child sentinel) but never step 2 (the parent `DirEntry`) leaves an
//! orphan sentinel; the per-partition leader sweep reclaims it once aged, while
//! a properly-linked directory is never touched. Drives a single-node group so
//! the sweep's leader gate and propose path run for real.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_meta::ns_hier::{self, FsRecord, HierOp};
use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::{GroupManager, MetaRaft};
use epoch_meta::ref_extractor::EpochRefExtractor;
use epoch_meta::store::rocks::RocksEngine;
use epoch_proto::BucketId;
use openraft::{BasicNode, Config};

const TIMEOUT: Duration = Duration::from_secs(20);

fn bucket() -> BucketId {
    BucketId::new(epoch_proto::consts::HIER_BUCKET_BIT | 1)
}

async fn single_node_group() -> (tempfile::TempDir, Arc<GroupManager>, MetaRaft) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(RocksEngine::open(&dir.path().join("sm")).expect("engine"));
    let manager = Arc::new(
        GroupManager::open(
            &dir.path().join("raft-log"),
            1,
            engine,
            Arc::new(EpochRefExtractor),
            Config {
                cluster_name: "orphan-sweep".to_string(),
                ..Default::default()
            },
        )
        .expect("open"),
    );
    let raft = manager
        .create_group(1, PartitionRange::full(Namespace::Hier))
        .await
        .expect("create");
    raft.initialize(BTreeMap::from([(1u64, BasicNode::new("a:1"))]))
        .await
        .expect("initialize");
    let deadline = Instant::now() + TIMEOUT;
    while !raft.metrics().borrow().state.is_leader() {
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (dir, manager, raft)
}

/// mkdir step 1 only (via `MkdirSentinel`); returns the minted child ino.
async fn mkdir_step1(raft: &MetaRaft, parent_ino: u64, name: &[u8], ts: i64) -> u64 {
    let resp = raft
        .client_write(epoch_meta::raft::MetaEntry::Hier(HierOp::MkdirSentinel {
            bucket: bucket(),
            parent_ino,
            name: name.to_vec(),
            ts_millis: ts,
        }))
        .await
        .expect("mkdir step1");
    match resp.data {
        epoch_meta::ns_common::MetaResponse::MintedIno(ino) => ino,
        other => panic!("unexpected step1 response: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_sentinel_is_reclaimed_linked_one_is_kept() {
    let (_dir, manager, raft) = single_node_group().await;
    let range = PartitionRange::full(Namespace::Hier);
    let store = manager.store();

    // Directory A: only step 1 committed → orphan sentinel.
    let orphan_ino = mkdir_step1(&raft, 1, b"crashed", 1_000).await;
    // Directory B: both steps committed → properly linked, must survive.
    let linked_ino = mkdir_step1(&raft, 1, b"good", 1_000).await;
    raft.client_write(epoch_meta::raft::MetaEntry::Hier(HierOp::MkdirLink {
        bucket: bucket(),
        parent_ino: 1,
        name: b"good".to_vec(),
        child_ino: linked_ino,
        ts_millis: 1_001,
    }))
    .await
    .expect("mkdir step2 for good");

    // Both sentinels exist before the sweep.
    assert!(matches!(
        ns_hier::lookup(store.as_ref(), bucket(), orphan_ino, b"").expect("lookup"),
        Some(FsRecord::Sentinel(_))
    ));

    // Sweep with ttl=0 (every sentinel is "aged"): the orphan is reclaimed,
    // the linked directory is kept.
    let reclaimed = ns_hier::sweep_orphan_sentinels(&store, &raft, &range, Duration::ZERO, 2_000)
        .await
        .expect("sweep");
    assert_eq!(reclaimed, 1, "exactly the orphan sentinel is reclaimed");

    assert!(
        ns_hier::lookup(store.as_ref(), bucket(), orphan_ino, b"")
            .expect("lookup orphan")
            .is_none(),
        "orphan sentinel must be gone"
    );
    assert!(
        matches!(
            ns_hier::lookup(store.as_ref(), bucket(), linked_ino, b"").expect("lookup linked"),
            Some(FsRecord::Sentinel(_))
        ),
        "linked directory's sentinel must survive"
    );

    // Idempotent: a second sweep finds nothing.
    let again = ns_hier::sweep_orphan_sentinels(&store, &raft, &range, Duration::ZERO, 3_000)
        .await
        .expect("sweep again");
    assert_eq!(again, 0);

    raft.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_orphan_within_ttl_is_not_reclaimed() {
    let (_dir, manager, raft) = single_node_group().await;
    let range = PartitionRange::full(Namespace::Hier);
    let store = manager.store();

    let orphan_ino = mkdir_step1(&raft, 1, b"recent", 10_000).await;
    // now = created_ts + 1h, ttl = 24h → still inside the intervention window.
    let reclaimed = ns_hier::sweep_orphan_sentinels(
        &store,
        &raft,
        &range,
        Duration::from_secs(24 * 60 * 60),
        10_000 + 60 * 60 * 1000,
    )
    .await
    .expect("sweep");
    assert_eq!(reclaimed, 0, "a fresh orphan is within the safety window");
    assert!(
        ns_hier::lookup(store.as_ref(), bucket(), orphan_ino, b"")
            .expect("lookup")
            .is_some(),
        "fresh orphan sentinel must survive"
    );
    raft.shutdown().await.expect("shutdown");
}
