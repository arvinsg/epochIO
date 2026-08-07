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

//! MemEngine durability (03 §7 cubefs 配方: raft log 即 WAL + 重放恢复). The
//! MemEngine holds no on-disk state, so a restart must rebuild the partition
//! purely by replaying the persistent raft log into a fresh engine. This test
//! writes through a single-node group backed by a MemEngine, drops everything,
//! reopens with a *new* empty MemEngine over the *same* raft-log directory, and
//! verifies recovery replays the log so the state returns — the MemEngine
//! analogue of the kill -9 acceptance (07 §M5b).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_meta::ns_flat::{ContentHead, FlatOp, ObjectHead, StorageClass};
use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::{GroupManager, MetaEntry};
use epoch_meta::ref_extractor::{EpochRefExtractor, Slice};
use epoch_meta::store::MemEngine;
use epoch_meta::{ns_flat, store::MetaStore};
use epoch_proto::{BlobId, BucketId, ChunkId};
use openraft::{BasicNode, Config};

const TIMEOUT: Duration = Duration::from_secs(20);

fn config() -> Config {
    Config {
        cluster_name: "mem-recovery".to_string(),
        ..Default::default()
    }
}

fn put(key: &[u8]) -> MetaEntry {
    MetaEntry::Flat(FlatOp::Put {
        bucket: BucketId::new(1),
        key: key.to_vec(),
        head: ObjectHead {
            size: 10,
            etag: [1; 16],
            mtime: 0,
            storage: StorageClass::Standard,
            content: ContentHead::Slices(vec![Slice {
                chunk_id: ChunkId::new(1),
                blob_ids: vec![BlobId::from_raw(42)],
                blob_size: 32 << 20,
            }]),
            seg_count: 0,
            http: Default::default(),
        },
        ts_millis: 1_000,
    })
}

async fn await_leader(raft: &epoch_meta::raft::MetaRaft) {
    let deadline = Instant::now() + TIMEOUT;
    while !raft.metrics().borrow().state.is_leader() {
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mem_engine_state_recovers_by_log_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log_dir = dir.path().join("raft-log");
    let bucket = BucketId::new(1);

    // Phase 1: write three objects through a MemEngine-backed group.
    {
        let engine: Arc<dyn MetaStore> = Arc::new(MemEngine::new());
        let manager =
            GroupManager::open(&log_dir, 1, engine, Arc::new(EpochRefExtractor), config())
                .expect("open");
        let raft = manager
            .create_group(1, PartitionRange::full(Namespace::Flat))
            .await
            .expect("create");
        raft.initialize(BTreeMap::from([(1u64, BasicNode::new("mem:1"))]))
            .await
            .expect("initialize");
        await_leader(&raft).await;
        for key in [b"a".as_slice(), b"b", b"c"] {
            raft.client_write(put(key)).await.expect("write");
        }
        // Confirm the writes are visible in this engine.
        assert!(
            ns_flat::get_head(manager.store().as_ref(), bucket, b"c")
                .expect("get")
                .is_some()
        );
        raft.shutdown().await.expect("shutdown");
    }

    // Phase 2: reopen with a BRAND-NEW empty MemEngine over the same raft log.
    // The engine starts empty; recovery must replay the persisted log into it.
    let fresh_engine: Arc<dyn MetaStore> = Arc::new(MemEngine::new());
    // Sanity: the fresh engine really is empty before recovery.
    assert!(
        ns_flat::get_head(fresh_engine.as_ref(), bucket, b"a")
            .expect("get")
            .is_none(),
        "fresh MemEngine must start empty"
    );
    let manager = GroupManager::open(
        &log_dir,
        1,
        Arc::clone(&fresh_engine),
        Arc::new(EpochRefExtractor),
        config(),
    )
    .expect("reopen");
    let recovered = manager.recover().await.expect("recover");
    assert_eq!(recovered, vec![1], "the persisted group reopens");
    let raft = manager.raft(1).expect("group running");
    await_leader(&raft).await;

    // All three objects are back — rebuilt purely from log replay into the
    // fresh in-memory engine (03 §7 恢复 = 载入快照 + 重放增量 log; here there is
    // no snapshot yet, so it is a full replay).
    for key in [b"a".as_slice(), b"b", b"c"] {
        assert!(
            ns_flat::get_head(fresh_engine.as_ref(), bucket, key)
                .expect("get")
                .is_some(),
            "object {key:?} must be recovered by log replay"
        );
    }
}
