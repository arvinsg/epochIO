//! Unit tests for the hierarchical namespace apply handlers (03 §6). Each op
//! is applied straight to a fresh engine (the state-machine wiring is covered
//! by the raft/cluster tests); the ino allocator is driven explicitly.

use super::*;
use crate::ref_extractor::{EpochRefExtractor, PendingDelete};
use crate::store::rocks::RocksEngine;

use epoch_proto::{BlobId, BucketId, ChunkId};

fn bucket() -> BucketId {
    // A hierarchical bucket carries the ns bit (03 §2); the handlers don't
    // require it, but keep tests faithful to real ids.
    BucketId::new(epoch_proto::consts::HIER_BUCKET_BIT | 7)
}

fn engine() -> (tempfile::TempDir, RocksEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = RocksEngine::open(dir.path()).expect("open");
    (dir, engine)
}

fn allocator() -> InoAllocator {
    // Tag 0, counter starts past ROOT_INO (=1) as a real partition would.
    InoAllocator::new(0, epoch_proto::consts::ROOT_INO + 1)
}

fn slice(chunk: u32, blob: u64) -> Slice {
    Slice {
        chunk_id: ChunkId::new(chunk),
        blob_ids: vec![BlobId::from_raw(blob)],
        blob_size: 32 << 20,
    }
}

fn slices(n: usize) -> Vec<Slice> {
    (0..n).map(|i| slice(i as u32, i as u64)).collect()
}

/// Applies an op to the engine, committing its ops; returns the response.
fn run(engine: &RocksEngine, ino: &mut InoAllocator, op: &HierOp) -> MetaResponse {
    run_seq(engine, ino, op, 1)
}

fn run_seq(engine: &RocksEngine, ino: &mut InoAllocator, op: &HierOp, seq: u64) -> MetaResponse {
    let outcome = apply(engine, &EpochRefExtractor, op, seq, ino).expect("apply");
    engine.apply(&outcome.ops).expect("commit");
    outcome.response
}

fn write_op(parent_ino: u64, name: &[u8], content: ContentHead) -> HierOp {
    HierOp::Write {
        bucket: bucket(),
        parent_ino,
        name: name.to_vec(),
        size: 100,
        etag: [7; 16],
        content,
        http: Default::default(),
        ts_millis: 1_700_000_000_000,
    }
}

fn delq_slices(
    engine: &RocksEngine,
    parent_ino: u64,
    name: &[u8],
    seq: u64,
    seg: u32,
) -> Vec<Slice> {
    let key = hier_key(
        MetaCf::Delq,
        bucket(),
        parent_ino,
        name,
        &suffix::delq_seq(seq, seg),
    );
    engine
        .get(MetaCf::Delq, &key)
        .expect("get delq")
        .map(|raw| {
            serde_json::from_slice::<PendingDelete>(&raw)
                .expect("decode")
                .slices
        })
        .unwrap_or_default()
}

/// A minted ino carries its partition tag in the high bits and is monotonic
/// (03 §6.5).
#[test]
fn ino_allocator_mints_tagged_monotonic_inodes() {
    let mut ino = InoAllocator::new(3, 0);
    let a = ino.mint().expect("mint a");
    let b = ino.mint().expect("mint b");
    assert!(b > a);
    assert_eq!(epoch_proto::consts::ino_partition_tag(a), 3);
    assert_eq!(epoch_proto::consts::ino_partition_tag(b), 3);
    assert_eq!(ino.next(), 2);
}

#[test]
fn create_write_and_lookup_roundtrip() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    let resp = run(
        &engine,
        &mut ino,
        &write_op(1, b"a.txt", ContentHead::Slices(slices(3))),
    );
    assert_eq!(resp, MetaResponse::None);

    let rec = lookup(&engine, bucket(), 1, b"a.txt")
        .expect("lookup")
        .expect("present");
    let FsRecord::File(file) = rec else {
        panic!("expected a file");
    };
    assert_eq!(file.size, 100);
    assert_eq!(file.content, ContentHead::Slices(slices(3)));
    // A fresh ino was minted (tag 0, first counter past ROOT_INO).
    assert_eq!(epoch_proto::consts::ino_partition_tag(file.ino), 0);
}

/// INVARIANT(design 03 §5): overwriting a file captures its old slices into
/// delq, and the inode identity is preserved across the overwrite.
#[test]
fn overwrite_preserves_ino_and_captures_old_slices() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"f", ContentHead::Slices(slices(2))),
        1,
    );
    let first_ino = match lookup(&engine, bucket(), 1, b"f").unwrap().unwrap() {
        FsRecord::File(f) => f.ino,
        _ => panic!("file"),
    };

    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"f", ContentHead::Inline(b"new".to_vec())),
        2,
    );
    let second = match lookup(&engine, bucket(), 1, b"f").unwrap().unwrap() {
        FsRecord::File(f) => f,
        _ => panic!("file"),
    };
    assert_eq!(second.ino, first_ino, "overwrite keeps the inode identity");
    assert_eq!(second.content, ContentHead::Inline(b"new".to_vec()));
    // The old two slices landed in delq under seq 2.
    assert_eq!(delq_slices(&engine, 1, b"f", 2, 0), slices(2));
}

#[test]
fn write_over_directory_name_is_rejected() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    // Make a directory `d` under parent 1.
    let sentinel_resp = run(
        &engine,
        &mut ino,
        &HierOp::MkdirSentinel {
            bucket: bucket(),
            parent_ino: 1,
            name: b"d".to_vec(),
            ts_millis: 1,
        },
    );
    let child_ino = parse_sentinel_ino(&sentinel_resp);
    run(
        &engine,
        &mut ino,
        &HierOp::MkdirLink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"d".to_vec(),
            child_ino,
            ts_millis: 2,
        },
    );
    // Writing a file at the same name conflicts (03 §6.4).
    let resp = run(
        &engine,
        &mut ino,
        &write_op(1, b"d", ContentHead::Inline(b"x".to_vec())),
    );
    assert!(matches!(resp, MetaResponse::Rejected(r) if r.contains("directory")));
}

#[test]
fn unlink_captures_slices_and_is_idempotent() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"g", ContentHead::Slices(slices(4))),
        1,
    );

    let del = HierOp::Unlink {
        bucket: bucket(),
        parent_ino: 1,
        name: b"g".to_vec(),
        ts_millis: 2,
    };
    run_seq(&engine, &mut ino, &del, 2);
    assert!(lookup(&engine, bucket(), 1, b"g").unwrap().is_none());
    assert_eq!(delq_slices(&engine, 1, b"g", 2, 0), slices(4));

    // Unlink of a missing name: no error, nothing new in delq.
    assert_eq!(run_seq(&engine, &mut ino, &del, 3), MetaResponse::None);
    assert!(delq_slices(&engine, 1, b"g", 3, 0).is_empty());
}

/// The sentinel response carries the minted child ino (mkdir step-1 output).
fn parse_sentinel_ino(resp: &MetaResponse) -> u64 {
    match resp {
        MetaResponse::MintedIno(ino) => *ino,
        _ => panic!("expected MintedIno response, got {resp:?}"),
    }
}

#[test]
fn mkdir_two_step_then_readdir_lists_children() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    // Root sentinel for parent 1 so its child count is maintained.
    seed_root(&engine, 1);

    let s = run(
        &engine,
        &mut ino,
        &HierOp::MkdirSentinel {
            bucket: bucket(),
            parent_ino: 1,
            name: b"sub".to_vec(),
            ts_millis: 1,
        },
    );
    let child = parse_sentinel_ino(&s);
    let link = run(
        &engine,
        &mut ino,
        &HierOp::MkdirLink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"sub".to_vec(),
            child_ino: child,
            ts_millis: 2,
        },
    );
    assert_eq!(link, MetaResponse::None);

    // A second identical link is idempotent.
    let again = run(
        &engine,
        &mut ino,
        &HierOp::MkdirLink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"sub".to_vec(),
            child_ino: child,
            ts_millis: 3,
        },
    );
    assert_eq!(again, MetaResponse::None);

    // Add a file next to the dir, then readdir sees both, sentinel excluded.
    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"a.txt", ContentHead::Slices(slices(1))),
        10,
    );
    let entries = readdir(&engine, bucket(), 1, None, 10).expect("readdir");
    let names: Vec<Vec<u8>> = entries.iter().map(|(n, _)| n.clone()).collect();
    assert_eq!(names, vec![b"a.txt".to_vec(), b"sub".to_vec()]);

    // Parent sentinel child count reflects the two entries.
    let FsRecord::Sentinel(root) = lookup(&engine, bucket(), 1, SENTINEL_NAME)
        .unwrap()
        .unwrap()
    else {
        panic!("sentinel");
    };
    assert_eq!(root.entry_count, DirCount::Exact(2));
}

/// Seeds a root sentinel for `ino` (03 §6.5 CreateBucket initializes the root).
fn seed_root(engine: &RocksEngine, dir_ino: u64) {
    let sentinel = FsRecord::Sentinel(DirRecord {
        ino: dir_ino,
        mtime: 0,
        entry_count: DirCount::Exact(0),
        parent_link: None,
        created_ts: 0,
    });
    engine
        .apply(&[StoreOp::put(
            MetaCf::Fs,
            hier_key(MetaCf::Fs, bucket(), dir_ino, SENTINEL_NAME, &[]),
            serde_json::to_vec(&sentinel).expect("encode"),
        )])
        .expect("seed root");
}

#[test]
fn rmdir_rejects_nonempty_then_succeeds_when_empty() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    seed_root(&engine, 1);
    let s = run(
        &engine,
        &mut ino,
        &HierOp::MkdirSentinel {
            bucket: bucket(),
            parent_ino: 1,
            name: b"d".to_vec(),
            ts_millis: 1,
        },
    );
    let child = parse_sentinel_ino(&s);
    run(
        &engine,
        &mut ino,
        &HierOp::MkdirLink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"d".to_vec(),
            child_ino: child,
            ts_millis: 2,
        },
    );
    // Put a file inside the child directory.
    run_seq(
        &engine,
        &mut ino,
        &write_op(child, b"inner", ContentHead::Slices(slices(1))),
        10,
    );

    // rmdir step 2 (sentinel delete) rejects a non-empty directory.
    let reject = run(
        &engine,
        &mut ino,
        &HierOp::RmdirSentinel {
            bucket: bucket(),
            ino: child,
            ts_millis: 3,
        },
    );
    assert!(matches!(reject, MetaResponse::Rejected(r) if r.contains("not empty")));

    // Empty the directory, then rmdir (unlink parent + delete sentinel).
    run_seq(
        &engine,
        &mut ino,
        &HierOp::Unlink {
            bucket: bucket(),
            parent_ino: child,
            name: b"inner".to_vec(),
            ts_millis: 4,
        },
        11,
    );
    let unlink = run(
        &engine,
        &mut ino,
        &HierOp::RmdirUnlink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"d".to_vec(),
            ts_millis: 5,
        },
    );
    assert_eq!(unlink, MetaResponse::None);
    let del = run(
        &engine,
        &mut ino,
        &HierOp::RmdirSentinel {
            bucket: bucket(),
            ino: child,
            ts_millis: 6,
        },
    );
    assert_eq!(del, MetaResponse::None);
    assert!(lookup(&engine, bucket(), 1, b"d").unwrap().is_none());
    assert!(
        lookup(&engine, bucket(), child, SENTINEL_NAME)
            .unwrap()
            .is_none()
    );
}

/// Same-directory rename is atomic and, over an existing target, captures the
/// target's slices while preserving the source inode (03 §6.2, checkpoint
/// publish).
#[test]
fn same_dir_rename_overwrites_target_and_captures_its_slices() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"model.ckpt.tmp", ContentHead::Slices(slices(2))),
        1,
    );
    let src_ino = match lookup(&engine, bucket(), 1, b"model.ckpt.tmp")
        .unwrap()
        .unwrap()
    {
        FsRecord::File(f) => f.ino,
        _ => panic!("file"),
    };
    // An existing publish target with its own slices.
    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"model.ckpt", ContentHead::Slices(vec![slice(99, 999)])),
        2,
    );

    let resp = run_seq(
        &engine,
        &mut ino,
        &HierOp::RenameSameDir {
            bucket: bucket(),
            parent_ino: 1,
            from: b"model.ckpt.tmp".to_vec(),
            to: b"model.ckpt".to_vec(),
            ts_millis: 3,
        },
        3,
    );
    assert_eq!(resp, MetaResponse::None);

    // Source gone, target now holds the source's content + inode.
    assert!(
        lookup(&engine, bucket(), 1, b"model.ckpt.tmp")
            .unwrap()
            .is_none()
    );
    let published = match lookup(&engine, bucket(), 1, b"model.ckpt")
        .unwrap()
        .unwrap()
    {
        FsRecord::File(f) => f,
        _ => panic!("file"),
    };
    assert_eq!(published.ino, src_ino, "rename preserves the source inode");
    assert_eq!(published.content, ContentHead::Slices(slices(2)));
    // The overwritten target's slice landed in delq (seq 3); the moved source
    // slices did NOT (they are still live under the new name).
    assert_eq!(
        delq_slices(&engine, 1, b"model.ckpt", 3, 0),
        vec![slice(99, 999)]
    );
}

#[test]
fn cross_type_and_missing_renames_are_rejected() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    // Missing source.
    let missing = run(
        &engine,
        &mut ino,
        &HierOp::RenameSameDir {
            bucket: bucket(),
            parent_ino: 1,
            from: b"nope".to_vec(),
            to: b"x".to_vec(),
            ts_millis: 1,
        },
    );
    assert!(matches!(missing, MetaResponse::Rejected(r) if r.contains("source not found")));

    // Destination is a directory → reject.
    run_seq(
        &engine,
        &mut ino,
        &write_op(1, b"file", ContentHead::Inline(b"x".to_vec())),
        1,
    );
    let s = run(
        &engine,
        &mut ino,
        &HierOp::MkdirSentinel {
            bucket: bucket(),
            parent_ino: 1,
            name: b"dir".to_vec(),
            ts_millis: 2,
        },
    );
    let child = parse_sentinel_ino(&s);
    run(
        &engine,
        &mut ino,
        &HierOp::MkdirLink {
            bucket: bucket(),
            parent_ino: 1,
            name: b"dir".to_vec(),
            child_ino: child,
            ts_millis: 3,
        },
    );
    let onto_dir = run(
        &engine,
        &mut ino,
        &HierOp::RenameSameDir {
            bucket: bucket(),
            parent_ino: 1,
            from: b"file".to_vec(),
            to: b"dir".to_vec(),
            ts_millis: 4,
        },
    );
    assert!(
        matches!(onto_dir, MetaResponse::Rejected(r) if r.contains("destination is a directory"))
    );
}

#[test]
fn readdir_paginates() {
    let (_dir, engine) = engine();
    let mut ino = allocator();
    seed_root(&engine, 1);
    for i in 0..5u32 {
        run_seq(
            &engine,
            &mut ino,
            &write_op(
                1,
                format!("f{i:02}").as_bytes(),
                ContentHead::Slices(slices(1)),
            ),
            i as u64 + 1,
        );
    }
    let first = readdir(&engine, bucket(), 1, None, 2).expect("page 1");
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].0, b"f00".to_vec());
    let second = readdir(&engine, bucket(), 1, Some(b"f01"), 2).expect("page 2");
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].0, b"f02".to_vec());
    let third = readdir(&engine, bucket(), 1, Some(b"f03"), 10).expect("page 3");
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].0, b"f04".to_vec());
}

/// Replaying an op batch (disableWAL tail-loss recovery) re-mints identical
/// inos and reproduces byte-identical keys (03 §6.5/§8) — a fresh allocator at
/// the same counter yields the same ino.
#[test]
fn create_is_deterministic_across_replay() {
    let (_dir, engine) = engine();
    let op = write_op(1, b"det", ContentHead::Slices(slices(1)));

    let mut ino_a = InoAllocator::new(0, 5);
    let out_a = apply(&engine, &EpochRefExtractor, &op, 1, &mut ino_a).expect("apply a");
    // A replica replaying the same entry at the same counter derives the same
    // batch (same minted ino → same FsRecord bytes).
    let mut ino_b = InoAllocator::new(0, 5);
    let out_b = apply(&engine, &EpochRefExtractor, &op, 1, &mut ino_b).expect("apply b");
    assert_eq!(out_a.ops, out_b.ops, "same counter → byte-identical ops");
    assert_eq!(ino_a.next(), ino_b.next());
}
