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

//! M5a 性能基线（07 §M5a: 对象灌入 + list 分页扫描）— 手动运行的基准，不进
//! 默认测试套件：
//!
//! ```bash
//! cargo test -p epoch-meta --test perf_ingest -- --ignored --nocapture
//! PERF_OBJECTS=10000000 cargo test -p epoch-meta --test perf_ingest -- --ignored --nocapture
//! ```
//!
//! Single-node single-partition: 并发 put（1-slice 对象，避开 inline 路径的
//! 变量）→ 全量 list 分页（limit=1000）扫描，输出吞吐与分页延迟。这是基线
//! 记录点，不是回归闸门——趋势对比随 M9 性能工序接入。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use epoch_meta::ns_flat::{self, ContentHead, FlatOp, ObjectHead, StorageClass};
use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::{GroupManager, MetaEntry};
use epoch_meta::ref_extractor::{EpochRefExtractor, Slice};
use epoch_meta::store::rocks::RocksEngine;
use epoch_proto::{BlobId, BucketId, ChunkId};
use openraft::{BasicNode, Config};

const BUCKET: u64 = 1;
const WRITERS: u64 = 8;
const LIST_PAGE: usize = 1000;

fn object_count() -> u64 {
    std::env::var("PERF_OBJECTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000)
}

fn put_entry(index: u64) -> MetaEntry {
    MetaEntry::Flat(FlatOp::Put {
        bucket: BucketId::new(BUCKET),
        key: format!("dataset/shard-{index:08}.bin").into_bytes(),
        head: ObjectHead {
            size: 32 << 20,
            etag: [0; 16],
            mtime: 0,
            storage: StorageClass::Standard,
            content: ContentHead::Slices(vec![Slice {
                chunk_id: ChunkId::new((index % 1024) as u32),
                blob_ids: vec![BlobId::from_raw(index)],
                blob_size: 32 << 20,
            }]),
            seg_count: 0,
            http: Default::default(),
        },
        ts_millis: 0,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "perf baseline: run manually (PERF_OBJECTS scales the ingest)"]
async fn ingest_and_list_baseline() {
    let total = object_count();
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(RocksEngine::open(&dir.path().join("sm")).expect("engine"));
    let manager = Arc::new(
        GroupManager::open(
            &dir.path().join("raft-log"),
            1,
            engine,
            Arc::new(EpochRefExtractor),
            Config {
                cluster_name: "perf".to_string(),
                ..Default::default()
            },
        )
        .expect("manager"),
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
    let deadline = Instant::now() + Duration::from_secs(10);
    while !raft.metrics().borrow().state.is_leader() {
        assert!(Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Ingest: WRITERS concurrent producers, each a contiguous index slice.
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut producers = Vec::new();
    for _ in 0..WRITERS {
        let raft = raft.clone();
        let next = Arc::clone(&next);
        producers.push(tokio::spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    break;
                }
                raft.client_write(put_entry(index))
                    .await
                    .expect("client write");
            }
        }));
    }
    for producer in producers {
        producer.await.expect("producer");
    }
    let ingest = started.elapsed();
    eprintln!(
        "INGEST  objects={total} writers={WRITERS} elapsed={ingest:?} throughput={:.0} obj/s",
        total as f64 / ingest.as_secs_f64()
    );

    // List: paginate the whole namespace with limit=1000 pages.
    let store = manager.store();
    let bucket = BucketId::new(BUCKET);
    let started = Instant::now();
    let mut pages = 0u64;
    let mut scanned = 0u64;
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = ns_flat::list(
            store.as_ref(),
            bucket,
            b"dataset/",
            cursor.as_deref(),
            LIST_PAGE,
        )
        .expect("list page");
        if page.is_empty() {
            break;
        }
        pages += 1;
        scanned += page.len() as u64;
        cursor = page.last().map(|(key, _)| key.clone());
        if page.len() < LIST_PAGE {
            break;
        }
    }
    let listing = started.elapsed();
    eprintln!(
        "LIST    objects={scanned} pages={pages} elapsed={listing:?} throughput={:.0} obj/s ({:.1} ms/page)",
        scanned as f64 / listing.as_secs_f64(),
        listing.as_secs_f64() * 1000.0 / pages.max(1) as f64,
    );
    assert_eq!(scanned, total, "list must see every ingested object");
}
