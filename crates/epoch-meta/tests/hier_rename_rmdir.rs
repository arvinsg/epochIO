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

//! Same-dir rename and rmdir through the raft apply path (03 §6.2). The state
//! machine ops (`RenameSameDir`, `RmdirUnlink`, `RmdirSentinel`) are exercised
//! end to end over a single-node group — rename is the checkpoint-publish
//! primitive, rmdir the directory-reclaim two-step — so the ops' atomicity,
//! idempotence, and rejection semantics are pinned against a real engine.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use epoch_meta::ns_common::MetaResponse;
use epoch_meta::ns_hier::{self, FsRecord, HierOp};
use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::{GroupManager, MetaEntry, MetaRaft};
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
                cluster_name: "hier-rename".to_string(),
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

/// Applies one hier op and returns its response.
async fn apply(raft: &MetaRaft, op: HierOp) -> MetaResponse {
    raft.client_write(MetaEntry::Hier(op))
        .await
        .expect("client_write")
        .data
}

/// Writes an inline file at `(parent_ino, name)`.
async fn write_file(raft: &MetaRaft, parent_ino: u64, name: &[u8], body: &[u8], ts: i64) {
    let resp = apply(
        raft,
        HierOp::Write {
            bucket: bucket(),
            parent_ino,
            name: name.to_vec(),
            size: body.len() as u64,
            etag: [9; 16],
            content: epoch_meta::ns_common::ContentHead::Inline(body.to_vec()),
            http: Default::default(),
            ts_millis: ts,
        },
    )
    .await;
    assert_eq!(resp, MetaResponse::None, "write must apply");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_dir_rename_moves_the_file_atomically() {
    let (_dir, manager, raft) = single_node_group().await;
    let store_dir = manager.store();

    // ckpt.tmp → ckpt (the checkpoint-publish pattern).
    write_file(&raft, 1, b"ckpt.tmp", b"weights", 1_000).await;
    let resp = apply(
        &raft,
        HierOp::RenameSameDir {
            bucket: bucket(),
            parent_ino: 1,
            from: b"ckpt.tmp".to_vec(),
            to: b"ckpt".to_vec(),
            ts_millis: 1_001,
        },
    )
    .await;
    assert_eq!(resp, MetaResponse::None, "same-dir rename applies");

    // `from` is gone, `to` holds the bytes.
    assert!(
        ns_hier::lookup(store_dir.as_ref(), bucket(), 1, b"ckpt.tmp")
            .expect("lookup from")
            .is_none(),
        "source name must be gone after rename"
    );
    let Some(FsRecord::File(f)) =
        ns_hier::lookup(store_dir.as_ref(), bucket(), 1, b"ckpt").expect("lookup to")
    else {
        panic!("destination must be a file");
    };
    assert_eq!(f.size, 7);

    raft.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_overwrite_captures_the_old_targets_slices() {
    let (_dir, manager, raft) = single_node_group().await;
    let store = manager.store();

    write_file(&raft, 1, b"new.tmp", b"new", 1_000).await;
    write_file(&raft, 1, b"ckpt", b"old", 1_001).await;
    let resp = apply(
        &raft,
        HierOp::RenameSameDir {
            bucket: bucket(),
            parent_ino: 1,
            from: b"new.tmp".to_vec(),
            to: b"ckpt".to_vec(),
            ts_millis: 1_002,
        },
    )
    .await;
    assert_eq!(resp, MetaResponse::None);
    // The overwrite target's content is replaced (its slices captured into delq
    // at apply — the no-leak INVARIANT, covered in ns_hier unit tests).
    let Some(FsRecord::File(f)) =
        ns_hier::lookup(store.as_ref(), bucket(), 1, b"ckpt").expect("lookup")
    else {
        panic!("ckpt must be a file");
    };
    assert_eq!(f.size, 3, "ckpt now holds the new body");

    raft.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rmdir_rejects_nonempty_then_reclaims_when_empty() {
    let (_dir, manager, raft) = single_node_group().await;
    let _ = &manager;

    // mkdir both steps for "dir".
    let ino = match apply(
        &raft,
        HierOp::MkdirSentinel {
            bucket: bucket(),
            parent_ino: 1,
            name: b"dir".to_vec(),
            ts_millis: 1_000,
        },
    )
    .await
    {
        MetaResponse::MintedIno(ino) => ino,
        other => panic!("expected a minted ino, got {other:?}"),
    };
    apply(
        &raft,
        HierOp::MkdirLink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"dir".to_vec(),
            child_ino: ino,
            ts_millis: 1_001,
        },
    )
    .await;

    // rmdir's emptiness check lives in step 2 (RmdirSentinel): with a file still
    // inside, the unlink (step 1) applies but the sentinel reclaim rejects.
    write_file(&raft, ino, b"inner", b"x", 1_002).await;
    let step1 = apply(
        &raft,
        HierOp::RmdirUnlink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"dir".to_vec(),
            ts_millis: 1_003,
        },
    )
    .await;
    assert_eq!(step1, MetaResponse::None, "step 1 unlinks the dir entry");
    let rejected = apply(
        &raft,
        HierOp::RmdirSentinel {
            bucket: bucket(),
            ino,
            ts_millis: 1_004,
        },
    )
    .await;
    assert!(
        matches!(rejected, MetaResponse::Rejected(_)),
        "rmdir of a non-empty dir must reject at the sentinel step, got {rejected:?}"
    );

    // Remove the file, then the sentinel reclaims (the dir entry is already
    // unlinked, so only step 2 needs to re-run — idempotent two-step).
    apply(
        &raft,
        HierOp::Unlink {
            bucket: bucket(),
            parent_ino: ino,
            name: b"inner".to_vec(),
            ts_millis: 1_005,
        },
    )
    .await;
    let step2 = apply(
        &raft,
        HierOp::RmdirSentinel {
            bucket: bucket(),
            ino,
            ts_millis: 1_006,
        },
    )
    .await;
    assert_eq!(step2, MetaResponse::None, "empty sentinel reclaims");

    raft.shutdown().await.expect("shutdown");
}
