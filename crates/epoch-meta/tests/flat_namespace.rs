//! Flat-namespace ops through a real raft group, headlined by the reference
//! reconciliation check of 07 §M5a acceptance: under concurrent overwrites of
//! one key, every slice ever written must be referenced exactly once at the
//! end — either by the live head/segments or by exactly one delq entry
//! (INVARIANT 03 §5: 旧 slices 由 apply 捕获, mechanically verified).
//!
//! A single-node group suffices: the invariant lives in the serial apply
//! path, and replication is covered by `meta_raft_cluster.rs`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_meta::ns_flat::{self, ContentHead, FlatOp, ObjectHead, StorageClass};
use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::{GroupManager, MetaEntry};
use epoch_meta::ref_extractor::{EpochRefExtractor, PendingDelete, Slice};
use epoch_meta::store::keys::{MetaCf, flat_key};
use epoch_meta::store::rocks::RocksEngine;
use epoch_proto::{BlobId, BucketId, ChunkId};
use openraft::{BasicNode, Config};

const TIMEOUT: Duration = Duration::from_secs(20);
const BUCKET: u64 = 1;
const WRITERS: u64 = 2;
const ROUNDS: u32 = 500;
const SLICES_PER_PUT: u64 = 2;

/// Every blob id a writer ever writes is derived from (writer, round, slot),
/// so the full written set is computable up front.
fn blob_id(writer: u64, round: u32, slot: u64) -> u64 {
    writer * 1_000_000 + u64::from(round) * SLICES_PER_PUT + slot
}

fn put_entry(key: &[u8], writer: u64, round: u32) -> MetaEntry {
    let slices: Vec<Slice> = (0..SLICES_PER_PUT)
        .map(|slot| Slice {
            chunk_id: ChunkId::new(round),
            blob_ids: vec![BlobId::from_raw(blob_id(writer, round, slot))],
            blob_size: 32 << 20,
        })
        .collect();
    MetaEntry::Flat(FlatOp::Put {
        bucket: BucketId::new(BUCKET),
        key: key.to_vec(),
        head: ObjectHead {
            size: 1,
            etag: [0; 16],
            mtime: 0,
            storage: StorageClass::Standard,
            content: ContentHead::Slices(slices),
            seg_count: 0,
            http: Default::default(),
        },
        ts_millis: 0,
    })
}

/// Drives one writer's puts through the shared raft handle.
async fn write_rounds(raft: epoch_meta::raft::MetaRaft, key: &[u8], writer: u64) {
    for round in 0..ROUNDS {
        raft.client_write(put_entry(key, writer, round))
            .await
            .expect("client write");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_overwrites_never_leak_references() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(RocksEngine::open(&dir.path().join("sm")).expect("open engine"));
    let manager = Arc::new(
        GroupManager::open(
            &dir.path().join("raft-log"),
            1,
            engine,
            Arc::new(EpochRefExtractor),
            Config {
                cluster_name: "flat-it".to_string(),
                ..Default::default()
            },
        )
        .expect("open manager"),
    );
    let raft = manager
        .create_group(1, PartitionRange::full(Namespace::Flat))
        .await
        .expect("create group");
    raft.initialize(std::collections::BTreeMap::from([(
        1u64,
        BasicNode::new("127.0.0.1:1"),
    )]))
    .await
    .expect("initialize");

    // Wait for leadership before writing.
    let deadline = Instant::now() + TIMEOUT;
    while !raft.metrics().borrow().state.is_leader() {
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let key = b"hot".as_slice();
    let writers: Vec<_> = (1..=WRITERS)
        .map(|writer| {
            let raft = raft.clone();
            tokio::spawn(async move { write_rounds(raft, key, writer).await })
        })
        .collect();
    for writer in writers {
        writer.await.expect("writer task");
    }

    // Reconcile: live references (head + segments) …
    let store = manager.store();
    let bucket = BucketId::new(BUCKET);
    let head = ns_flat::get_head(store.as_ref(), bucket, key)
        .expect("get head")
        .expect("object must exist");
    let live: HashSet<u64> = head
        .into_slices()
        .iter()
        .flat_map(|s| s.blob_ids.iter().map(|b| b.as_u64()))
        .collect();
    // … plus delq-captured references …
    let delq_start = flat_key(MetaCf::Delq, bucket, key, &[]);
    let delq_end = flat_key(MetaCf::Delq, bucket, key, &[0xFF; 12]);
    let mut captured: HashSet<u64> = HashSet::new();
    let mut cursor = delq_start.clone();
    loop {
        let page = store
            .scan(MetaCf::Delq, &cursor, &delq_end, 1024)
            .expect("scan delq");
        if page.is_empty() {
            break;
        }
        cursor = page
            .last()
            .map(|(k, _)| {
                let mut next = k.clone();
                next.push(0);
                next
            })
            .expect("non-empty page");
        for (_, value) in page {
            let pending: PendingDelete = serde_json::from_slice(&value).expect("decode delq");
            for slice in pending.slices {
                for blob in slice.blob_ids {
                    assert!(
                        captured.insert(blob.as_u64()),
                        "blob {} captured twice — double tombstone would follow",
                        blob.as_u64()
                    );
                }
            }
        }
    }

    // … must partition the full written set: no leak, no double-reference.
    let written: HashSet<u64> = (1..=WRITERS)
        .flat_map(|w| {
            (0..ROUNDS).flat_map(move |r| (0..SLICES_PER_PUT).map(move |s| blob_id(w, r, s)))
        })
        .collect();
    let leaked: Vec<u64> = written
        .difference(&captured)
        .filter(|b| !live.contains(b))
        .copied()
        .collect();
    assert!(
        leaked.is_empty(),
        "{} blobs leaked (never captured, not live)",
        leaked.len()
    );
    let double: Vec<u64> = live.intersection(&captured).copied().collect();
    assert!(
        double.is_empty(),
        "{} blobs both live and captured",
        double.len()
    );
    assert_eq!(
        live.len() + captured.len(),
        written.len(),
        "live + captured must equal everything ever written"
    );
    assert_eq!(
        live.len() as u64,
        SLICES_PER_PUT,
        "exactly the final head's slices stay live"
    );

    // The delq counter persisted one sequence per put (03 §8 单调计数器).
    let counter = store
        .get(MetaCf::Applied, &[0, 0, 0, 0, 0, 0, 0, 1, b'd'])
        .expect("get counter")
        .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
        .map(u64::from_be_bytes)
        .expect("counter persisted");
    assert_eq!(counter, WRITERS * u64::from(ROUNDS));
}
