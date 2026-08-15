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

//! Hierarchical namespace: the `(parent_ino, name)` tree namespace (03 §3/§6
//! hier mode) — value model, raft op vocabulary, and apply-time handlers.
//!
//! ## Two structural tricks (03 §6.1)
//!
//! **① dentry + inode fused** (CephFS embedded inode): no hardlink (nlink≡1),
//! so a file's metadata lives in its one directory entry — a single
//! [`FileRecord`] is both dentry and inode, killing every cross-record
//! consistency problem.
//!
//! **② directory sentinel**: a directory's own attributes live at the head of
//! its children's key range (`name = ""` sorts first), not in the parent. A
//! directory is therefore a contiguous key interval `[ f|b|ino|"" ..
//! f|b|ino|∞ ]` — sentinel + children, naturally co-partitioned; readdir is a
//! prefix scan after the sentinel. Children key on `ino`, not name, so a
//! directory rename only rewrites the parent's [`DirEntry`] pointer — the
//! subtree never moves (rename O(1)).
//!
//! ## INVARIANT(design 03 §5): old slices captured by apply
//!
//! As in [`ns_flat`](crate::ns_flat), the proposer carries only the new value;
//! an overwrite (write / same-dir rename over an existing target) captures the
//! *current* file's slices into `delq` at apply time via
//! [`capture_old_file`], never from a proposer-supplied old value.
//!
//! ## v1 semantics (03 §6.3)
//!
//! Cross-directory rename and cross-split-boundary same-dir rename return
//! `EXDEV` (txn-lite is Phase 3, not M5b). mkdir/rmdir are the idempotent
//! two-step of 03 §6.2, but each step is its own single-partition op (its own
//! propose) — this module implements the atomic steps; the two-step
//! orchestration across partitions is a service-layer concern.
//!
//! Design: docs/design/03-metanode.md §6

use epoch_proto::BucketId;
use serde::{Deserialize, Serialize};

use crate::MetaError;
use crate::ns_common::{
    ApplyOutcome, ContentHead, HttpMeta, MetaResponse, SliceSegment, enqueue_delete_slices,
    split_slices,
};
use crate::partition::PartitionRange;
use crate::raft::MetaRaft;
use crate::ref_extractor::{RefExtractor, Slice};
use crate::store::keys::{MetaCf, hier_key, prefix_end, suffix};
use crate::store::{MetaStore, MetaStoreError, StoreOp};

/// The default orphan-sentinel age before reclamation (03 §6.2 注: 超龄默认 24h).
pub const DEFAULT_ORPHAN_SENTINEL_TTL: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

/// The default orphan-sentinel sweep interval.
pub const DEFAULT_ORPHAN_SWEEP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60 * 60);

/// The empty name of a directory sentinel (`f|bucket|ino|""`, 03 §6.1) — sorts
/// first in the directory's key range.
pub const SENTINEL_NAME: &[u8] = b"";

/// A file record: dentry and inode fused (03 §6.1). Stored at
/// `f | bucket | parent_ino | name` when `name != ""`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRecord {
    /// The file's own inode number (monotonic per partition, never reused,
    /// 03 §6.5).
    pub ino: u64,
    /// File size in bytes.
    pub size: u64,
    /// Content etag (16 bytes).
    pub etag: [u8; 16],
    /// Last-modified, wall-clock millis (proposer-supplied).
    pub mtime: i64,
    /// Inline bytes or the (possibly head-truncated) slice list — the same
    /// head/segment model as flat objects (03 §4.2, shared `ContentHead`).
    pub content: ContentHead,
    /// Number of `meta_seg` overflow entries (0 = everything embedded).
    pub seg_count: u32,
    /// HTTP metadata replayed on GET/HEAD (shared with flat, `ns_common`).
    #[serde(default)]
    pub http: HttpMeta,
}

impl FileRecord {
    /// The full slice list the record embeds (empty for inline content).
    #[must_use]
    pub fn into_slices(self) -> Vec<Slice> {
        self.content.into_slices()
    }
}

/// A subdirectory pointer stored in the parent's range at
/// `f | bucket | parent_ino | name` (03 §6.1): just the child's inode. The
/// child's attributes live in its own sentinel, not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    /// The child directory's inode.
    pub ino: u64,
}

/// A directory's child-count, exact until the directory is range-split, then
/// `Unknown` (03 §6.2 目录分裂降级): after a split the sentinel no longer
/// maintains the count, and rmdir emptiness switches to a per-partition probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirCount {
    /// The directory is unsplit; the count is authoritative.
    Exact(u64),
    /// The directory has been range-split; count/mtime are no longer
    /// maintained (03 §6.2 注).
    Unknown,
}

/// A directory sentinel: the directory's own attributes, stored at
/// `f | bucket | self_ino | ""` — the head of its children's range (03 §6.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirRecord {
    /// The directory's own inode.
    pub ino: u64,
    /// Last-modified, wall-clock millis (bumped on child add/remove while
    /// `Exact`).
    pub mtime: i64,
    /// Child count (03 §6.2): `Exact` while unsplit, else `Unknown`.
    pub entry_count: DirCount,
    /// The `(parent_ino, name)` link this directory expects in its parent
    /// (03 §6.2 mkdir 两步). Recorded by step 1 so the orphan-sentinel sweep
    /// can verify step 2 committed (the parent `DirEntry` exists); `None` for
    /// the root sentinel, which has no parent link.
    #[serde(default)]
    pub parent_link: Option<(u64, Vec<u8>)>,
    /// Sentinel creation wall-clock millis — the orphan-sweep age clock
    /// (03 §6.2: 超龄默认 24h).
    #[serde(default)]
    pub created_ts: i64,
}

/// The stored value at a hier key — a file, a subdirectory pointer, or a
/// directory sentinel — discriminated on decode by the `name` (`""` = sentinel)
/// and the payload shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FsRecord {
    /// A file (dentry+inode fused).
    File(FileRecord),
    /// A subdirectory pointer.
    Dir(DirEntry),
    /// A directory sentinel (name == "").
    Sentinel(DirRecord),
}

/// The hierarchical op vocabulary carried inside
/// [`MetaEntry::Hier`](crate::raft::MetaEntry). Every variant carries `bucket`
/// and proposer `ts_millis` (replicated, so apply reads no clock, 03 §8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HierOp {
    /// Create or overwrite a file at `(parent_ino, name)` (03 §6.2 create /
    /// write). An overwrite captures the old file's slices (INVARIANT 03 §5).
    /// The proposer supplies the new file's `ino` (minted at apply via the
    /// partition counter is *not* used here — see `state_machine`, which mints
    /// the ino for creates); `ino` is `None` for a create (apply mints it) and
    /// `Some` internally is never sent by clients.
    Write {
        /// Owning bucket.
        bucket: BucketId,
        /// Parent directory inode.
        parent_ino: u64,
        /// File name (non-empty).
        name: Vec<u8>,
        /// The new file's size.
        size: u64,
        /// The new file's etag.
        etag: [u8; 16],
        /// The new file's content (inline or the full slice list).
        content: ContentHead,
        /// HTTP metadata to persist with the record (S3 headers).
        http: HttpMeta,
        /// Proposer wall-clock millis (mtime + delq enqueue_ts on overwrite).
        ts_millis: i64,
    },
    /// Remove a file at `(parent_ino, name)`, tombstoning its data
    /// (idempotent — a missing name is a no-op, 03 §6.2 unlink).
    Unlink {
        /// Owning bucket.
        bucket: BucketId,
        /// Parent directory inode.
        parent_ino: u64,
        /// File name.
        name: Vec<u8>,
        /// Proposer wall-clock millis (delq enqueue_ts).
        ts_millis: i64,
    },
    /// Write the child directory sentinel — step 1 of the idempotent mkdir
    /// (03 §6.2). The child `ino` is minted by apply (partition counter); the
    /// sentinel records its intended `(parent_ino, name)` link so the
    /// orphan-sweep can later verify step 2 committed.
    MkdirSentinel {
        /// Owning bucket.
        bucket: BucketId,
        /// The parent the child will be linked under (recorded in the sentinel
        /// for the orphan sweep).
        parent_ino: u64,
        /// The name the child will be linked as under `parent_ino`.
        name: Vec<u8>,
        /// Proposer wall-clock millis (sentinel mtime + creation age clock).
        ts_millis: i64,
    },
    /// Link a child directory into its parent — step 2 (commit point) of mkdir
    /// (03 §6.2). Idempotent per `(parent_ino, name)`. Rejected if the name is
    /// already a file (dir/file conflict, 03 §6.4).
    MkdirLink {
        /// Owning bucket.
        bucket: BucketId,
        /// Parent directory inode.
        parent_ino: u64,
        /// Child directory name.
        name: Vec<u8>,
        /// The child directory inode (minted by the prior `MkdirSentinel`).
        child_ino: u64,
        /// Proposer wall-clock millis (parent mtime).
        ts_millis: i64,
    },
    /// Remove the parent's link to an (empty) child directory — step 1 of
    /// rmdir (03 §6.2, reverse of mkdir). Idempotent.
    RmdirUnlink {
        /// Owning bucket.
        bucket: BucketId,
        /// Parent directory inode.
        parent_ino: u64,
        /// Child directory name.
        name: Vec<u8>,
        /// Proposer wall-clock millis (parent mtime).
        ts_millis: i64,
    },
    /// Delete a child directory's sentinel — step 2 of rmdir (03 §6.2).
    /// Rejected if the directory is non-empty (has children beyond the
    /// sentinel). Idempotent for a missing sentinel.
    RmdirSentinel {
        /// Owning bucket.
        bucket: BucketId,
        /// The directory's own inode.
        ino: u64,
        /// Proposer wall-clock millis.
        ts_millis: i64,
    },
    /// Atomically rename `from` → `to` within one directory (03 §6.2 same-dir
    /// rename — the checkpoint-publish primitive). Both names share the
    /// `(parent_ino, *)` range, so it is one atomic batch; overwriting an
    /// existing `to` captures its slices (INVARIANT 03 §5). The caller must
    /// have verified both names route to *this* partition; a cross-split
    /// boundary rename is rejected (`EXDEV`) before proposal.
    RenameSameDir {
        /// Owning bucket.
        bucket: BucketId,
        /// Shared parent directory inode.
        parent_ino: u64,
        /// Source file name.
        from: Vec<u8>,
        /// Destination file name.
        to: Vec<u8>,
        /// Proposer wall-clock millis.
        ts_millis: i64,
    },
}

/// Encodes a value; failures surface as [`MetaStoreError::ValueCodec`].
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, MetaStoreError> {
    serde_json::to_vec(value).map_err(|e| MetaStoreError::ValueCodec(e.to_string()))
}

/// Decodes a value; failures surface as [`MetaStoreError::ValueCodec`].
fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, MetaStoreError> {
    serde_json::from_slice(bytes).map_err(|e| MetaStoreError::ValueCodec(e.to_string()))
}

/// The full slice list of a file record's head + overflow segments, deleting
/// every old segment key into `ops`, and enqueuing them all into `delq` under
/// `(bucket, parent_ino, name, seq)` (03 §5 capture; hier analogue of
/// `ns_flat::capture_old`). Returns the old file's `(inline_bytes,
/// inline_count, total_bytes)` for the inline guard, zeros when absent.
#[allow(clippy::too_many_arguments)]
fn capture_old_file(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    seq: u64,
    ts_millis: i64,
    ops: &mut Vec<StoreOp>,
) -> Result<(u64, u64, u64), MetaStoreError> {
    let head_key = hier_key(MetaCf::Fs, bucket, parent_ino, name, &[]);
    let Some(old) = store.get(MetaCf::Fs, &head_key)? else {
        return Ok((0, 0, 0));
    };
    // Only a file record holds slices; a dir entry or sentinel captures nothing
    // (unlink of a name that is a directory is a caller-side error, not here).
    let mut captured: Vec<Slice> = extractor.extract(MetaCf::Fs, &old);
    let account = match decode::<FsRecord>(&old) {
        Ok(FsRecord::File(file)) => {
            let (inline_bytes, inline_count) = file.content.inline_account();
            (inline_bytes, inline_count, file.size)
        }
        _ => (0, 0, 0),
    };

    // Enumerate overflow segments by prefix scan (robust to a corrupt
    // seg_count), extracting + deleting each (03 §4.2 shared meta_seg CF).
    let seg_prefix = hier_key(MetaCf::MetaSeg, bucket, parent_ino, name, &[]);
    let seg_end = prefix_end(&seg_prefix).unwrap_or_else(|| vec![MetaCf::MetaSeg.tag() + 1]);
    let mut cursor = seg_prefix.clone();
    loop {
        let page = store.scan(MetaCf::MetaSeg, &cursor, &seg_end, 1024)?;
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
        for (seg_key, value) in page {
            captured.extend(extractor.extract(MetaCf::MetaSeg, &value));
            ops.push(StoreOp::delete(MetaCf::MetaSeg, seg_key));
        }
    }

    enqueue_delete_slices(
        0,
        &captured,
        ts_millis,
        |seg_no| {
            hier_key(
                MetaCf::Delq,
                bucket,
                parent_ino,
                name,
                &suffix::delq_seq(seq, seg_no),
            )
        },
        ops,
    )?;
    Ok(account)
}

/// Reads the current record at `(parent_ino, name)`, classifying it.
fn read_record(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
) -> Result<Option<FsRecord>, MetaStoreError> {
    store
        .get(
            MetaCf::Fs,
            &hier_key(MetaCf::Fs, bucket, parent_ino, name, &[]),
        )?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// The deterministic head/segment split of a new file record (03 §4.2, shared
/// [`split_slices`]).
fn pack_file(mut file: FileRecord) -> (FileRecord, Vec<SliceSegment>) {
    let ContentHead::Slices(slices) = &file.content else {
        file.seg_count = 0;
        return (file, Vec::new());
    };
    let (embedded, segments) = split_slices(slices);
    file.content = ContentHead::Slices(embedded);
    file.seg_count = segments.len() as u32;
    (file, segments)
}

/// Mints partition-local, never-reused inodes during apply (03 §6.5): a
/// monotonic counter combined with the partition tag in the ino's high bits, so
/// every replica derives identical inos from the same op and a split (which
/// subdivides one partition's counter space) never lets a child collide with a
/// sibling. The state machine seeds it from the persisted counter, lets the
/// handler [`mint`](Self::mint) as needed, and persists [`next`](Self::next)
/// back in the same atomic batch (the delq-seq recipe, 03 §8).
pub struct InoAllocator {
    tag: u32,
    next: u64,
}

impl InoAllocator {
    /// Seeds the allocator for `partition_tag` at counter `next` (0 for a fresh
    /// partition; `ROOT_INO`'s counter is reserved at bucket creation).
    #[must_use]
    pub fn new(partition_tag: u32, next: u64) -> Self {
        Self {
            tag: partition_tag,
            next,
        }
    }

    /// Mints the next ino, bumping the counter. Deterministic across replicas.
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError::ValueCodec`] if the counter overflows its bit
    /// width (a partition minting 2^48 inodes — practically unreachable; it is
    /// the split trigger long before).
    pub fn mint(&mut self) -> Result<u64, MetaStoreError> {
        let ino = epoch_proto::consts::compose_ino(self.tag, self.next)
            .ok_or_else(|| MetaStoreError::ValueCodec("ino counter overflow".to_string()))?;
        self.next += 1;
        Ok(ino)
    }

    /// The next unused counter value (persisted by the state machine).
    #[must_use]
    pub fn next(&self) -> u64 {
        self.next
    }
}

/// Applies one hierarchical op to its outcome (03 §6.2). `seq` is the
/// partition's delete-event sequence for this entry; `ino` mints fresh inodes
/// for creates. Both are supplied by the state machine from replicated
/// counters (03 §8) so apply stays deterministic and clock-free.
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn apply(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    op: &HierOp,
    seq: u64,
    ino: &mut InoAllocator,
) -> Result<ApplyOutcome, MetaStoreError> {
    match op {
        HierOp::Write {
            bucket,
            parent_ino,
            name,
            size,
            etag,
            content,
            http,
            ts_millis,
        } => apply_write(
            store,
            extractor,
            FileAt {
                bucket: *bucket,
                parent_ino: *parent_ino,
                name,
            },
            WriteAttrs {
                size: *size,
                etag,
                content,
                http,
                ts_millis: *ts_millis,
            },
            seq,
            ino,
        ),
        HierOp::Unlink {
            bucket,
            parent_ino,
            name,
            ts_millis,
        } => apply_unlink(
            store,
            extractor,
            *bucket,
            *parent_ino,
            name,
            seq,
            *ts_millis,
        ),
        HierOp::MkdirSentinel {
            bucket,
            parent_ino,
            name,
            ts_millis,
        } => apply_mkdir_sentinel(store, *bucket, *parent_ino, name, *ts_millis, ino),
        HierOp::MkdirLink {
            bucket,
            parent_ino,
            name,
            child_ino,
            ts_millis,
        } => apply_mkdir_link(store, *bucket, *parent_ino, name, *child_ino, *ts_millis),
        HierOp::RmdirUnlink {
            bucket,
            parent_ino,
            name,
            ts_millis,
        } => apply_rmdir_unlink(store, *bucket, *parent_ino, name, *ts_millis),
        HierOp::RmdirSentinel {
            bucket,
            ino: dir_ino,
            ts_millis,
        } => apply_rmdir_sentinel(store, *bucket, *dir_ino, *ts_millis),
        HierOp::RenameSameDir {
            bucket,
            parent_ino,
            from,
            to,
            ts_millis,
        } => apply_rename_same_dir(
            store,
            extractor,
            *bucket,
            *parent_ino,
            from,
            to,
            seq,
            *ts_millis,
        ),
    }
}

/// A reject outcome (deterministic, no inline delta).
fn rejected(reason: impl Into<String>) -> ApplyOutcome {
    ApplyOutcome::no_delta(Vec::new(), MetaResponse::Rejected(reason.into()))
}

/// Writes a file record + its overflow segments, capturing an overwritten
/// file's slices (03 §6.2 create/write; INVARIANT 03 §5). A name already held
/// by a directory is a dir/file conflict (03 §6.4 → rejected).
#[allow(clippy::too_many_arguments)]
/// Where a file lives: its parent directory and name within it. These three
/// always travel together through the hier handlers.
struct FileAt<'a> {
    /// Owning bucket.
    bucket: BucketId,
    /// Parent directory inode.
    parent_ino: u64,
    /// File name within the parent.
    name: &'a [u8],
}

/// The attributes a write records on the file (grouped so `apply_write` keeps a
/// readable signature as fields accrue — AGENTS §11).
struct WriteAttrs<'a> {
    /// New size in bytes.
    size: u64,
    /// New etag.
    etag: &'a [u8; 16],
    /// Inline bytes or the full slice list.
    content: &'a ContentHead,
    /// HTTP metadata (S3 headers) to persist.
    http: &'a HttpMeta,
    /// Proposer wall-clock millis (mtime + delq enqueue_ts).
    ts_millis: i64,
}

fn apply_write(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    at: FileAt<'_>,
    attrs: WriteAttrs<'_>,
    seq: u64,
    ino: &mut InoAllocator,
) -> Result<ApplyOutcome, MetaStoreError> {
    let FileAt {
        bucket,
        parent_ino,
        name,
    } = at;
    let WriteAttrs {
        size,
        etag,
        content,
        http,
        ts_millis,
    } = attrs;
    if name.is_empty() {
        return Ok(rejected("empty file name"));
    }
    // Resolve the file's inode: reuse the existing file's (stable inode
    // identity across overwrites), reject if the name is a directory, else
    // mint a fresh one for a create. A create adds a child to the parent; an
    // overwrite does not change the child count.
    let (file_ino, is_create) = match read_record(store, bucket, parent_ino, name)? {
        Some(FsRecord::File(existing)) => (existing.ino, false),
        Some(FsRecord::Dir(_)) | Some(FsRecord::Sentinel(_)) => {
            return Ok(rejected("name is a directory"));
        }
        None => (ino.mint()?, true),
    };

    let mut ops = Vec::new();
    let (old_inline, old_count, old_total) = capture_old_file(
        store, extractor, bucket, parent_ino, name, seq, ts_millis, &mut ops,
    )?;

    let file = FileRecord {
        ino: file_ino,
        size,
        etag: *etag,
        mtime: ts_millis,
        content: content.clone(),
        seg_count: 0,
        http: http.clone(),
    };
    let (packed, segments) = pack_file(file);
    ops.push(StoreOp::put(
        MetaCf::Fs,
        hier_key(MetaCf::Fs, bucket, parent_ino, name, &[]),
        encode(&FsRecord::File(packed.clone()))?,
    ));
    for (seg_no, segment) in segments.iter().enumerate() {
        ops.push(StoreOp::put(
            MetaCf::MetaSeg,
            hier_key(
                MetaCf::MetaSeg,
                bucket,
                parent_ino,
                name,
                &suffix::seg_no(seg_no as u32),
            ),
            encode(segment)?,
        ));
    }
    bump_sentinel_child_delta(
        store,
        bucket,
        parent_ino,
        i64::from(is_create),
        ts_millis,
        &mut ops,
    )?;

    let (new_inline, new_count) = packed.content.inline_account();
    let delta = crate::guard::GuardDelta {
        inline_bytes: new_inline as i64 - old_inline as i64,
        inline_count: new_count as i64 - old_count as i64,
        total_bytes: size as i64 - old_total as i64,
    };
    Ok(ApplyOutcome::new(ops, MetaResponse::None, delta))
}

/// Removes a file, capturing its slices (03 §6.2 unlink; idempotent for a
/// missing name). Rejects unlinking a directory (use rmdir).
fn apply_unlink(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    seq: u64,
    ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    match read_record(store, bucket, parent_ino, name)? {
        None => return Ok(ApplyOutcome::no_delta(Vec::new(), MetaResponse::None)),
        Some(FsRecord::Dir(_)) | Some(FsRecord::Sentinel(_)) => {
            return Ok(rejected("name is a directory"));
        }
        Some(FsRecord::File(_)) => {}
    }
    let mut ops = Vec::new();
    let (old_inline, old_count, old_total) = capture_old_file(
        store, extractor, bucket, parent_ino, name, seq, ts_millis, &mut ops,
    )?;
    ops.push(StoreOp::delete(
        MetaCf::Fs,
        hier_key(MetaCf::Fs, bucket, parent_ino, name, &[]),
    ));
    bump_sentinel_child_delta(store, bucket, parent_ino, -1, ts_millis, &mut ops)?;
    let delta = crate::guard::GuardDelta {
        inline_bytes: -(old_inline as i64),
        inline_count: -(old_count as i64),
        total_bytes: -(old_total as i64),
    };
    Ok(ApplyOutcome::new(ops, MetaResponse::None, delta))
}

/// Writes the child directory sentinel (mkdir step 1, 03 §6.2). The sentinel's
/// ino is minted here; the returned response carries it so the caller can issue
/// step 2 (`MkdirLink`). Idempotent is not meaningful here — each call mints a
/// fresh orphan sentinel; the orphan-sentinel sweep (M5b-5) reclaims unlinked
/// ones. The caller (service two-step) mints once and retries step 2.
fn apply_mkdir_sentinel(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    ts_millis: i64,
    ino: &mut InoAllocator,
) -> Result<ApplyOutcome, MetaStoreError> {
    let child_ino = ino.mint()?;
    let sentinel = DirRecord {
        ino: child_ino,
        mtime: ts_millis,
        entry_count: DirCount::Exact(0),
        parent_link: Some((parent_ino, name.to_vec())),
        created_ts: ts_millis,
    };
    let key = hier_key(MetaCf::Fs, bucket, child_ino, SENTINEL_NAME, &[]);
    // A minted ino is unique, so this never overwrites; still tolerate a
    // present sentinel (replay) idempotently.
    let ops = if store.get(MetaCf::Fs, &key)?.is_some() {
        Vec::new()
    } else {
        vec![StoreOp::put(
            MetaCf::Fs,
            key,
            encode(&FsRecord::Sentinel(sentinel))?,
        )]
    };
    Ok(ApplyOutcome::no_delta(
        ops,
        MetaResponse::MintedIno(child_ino),
    ))
}

/// Links a child directory into its parent (mkdir step 2 / commit point,
/// 03 §6.2). Idempotent; rejects a dir/file conflict (03 §6.4).
fn apply_mkdir_link(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    child_ino: u64,
    ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    if name.is_empty() {
        return Ok(rejected("empty directory name"));
    }
    match read_record(store, bucket, parent_ino, name)? {
        Some(FsRecord::Dir(existing)) if existing.ino == child_ino => {
            return Ok(ApplyOutcome::no_delta(Vec::new(), MetaResponse::None));
        }
        Some(FsRecord::Dir(_)) => return Ok(rejected("name already links another directory")),
        Some(FsRecord::File(_)) => return Ok(rejected("name is a file")),
        Some(FsRecord::Sentinel(_)) | None => {}
    }
    let mut ops = vec![StoreOp::put(
        MetaCf::Fs,
        hier_key(MetaCf::Fs, bucket, parent_ino, name, &[]),
        encode(&FsRecord::Dir(DirEntry { ino: child_ino }))?,
    )];
    bump_sentinel_child_delta(store, bucket, parent_ino, 1, ts_millis, &mut ops)?;
    Ok(ApplyOutcome::no_delta(ops, MetaResponse::None))
}

/// Removes the parent's link to a child directory (rmdir step 1, 03 §6.2).
/// Idempotent. Does not check emptiness — that is step 2's sentinel check.
fn apply_rmdir_unlink(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    match read_record(store, bucket, parent_ino, name)? {
        Some(FsRecord::Dir(_)) => {}
        Some(FsRecord::File(_)) => return Ok(rejected("name is a file")),
        Some(FsRecord::Sentinel(_)) | None => {
            return Ok(ApplyOutcome::no_delta(Vec::new(), MetaResponse::None));
        }
    }
    let mut ops = vec![StoreOp::delete(
        MetaCf::Fs,
        hier_key(MetaCf::Fs, bucket, parent_ino, name, &[]),
    )];
    bump_sentinel_child_delta(store, bucket, parent_ino, -1, ts_millis, &mut ops)?;
    Ok(ApplyOutcome::no_delta(ops, MetaResponse::None))
}

/// Deletes an empty child directory's sentinel (rmdir step 2, 03 §6.2). Rejects
/// a non-empty directory (any child beyond the sentinel). Idempotent for a
/// missing sentinel.
fn apply_rmdir_sentinel(
    store: &dyn MetaStore,
    bucket: BucketId,
    dir_ino: u64,
    _ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    let sentinel_key = hier_key(MetaCf::Fs, bucket, dir_ino, SENTINEL_NAME, &[]);
    if store.get(MetaCf::Fs, &sentinel_key)?.is_none() {
        return Ok(ApplyOutcome::no_delta(Vec::new(), MetaResponse::None));
    }
    // Emptiness: scan the directory's range for any entry past the sentinel.
    if directory_has_children(store, bucket, dir_ino)? {
        return Ok(rejected("directory not empty"));
    }
    Ok(ApplyOutcome::no_delta(
        vec![StoreOp::delete(MetaCf::Fs, sentinel_key)],
        MetaResponse::None,
    ))
}

/// Atomically renames `from` → `to` in one directory (03 §6.2 same-dir rename).
/// Both names share the `(parent_ino, *)` range → one atomic batch; an existing
/// `to` (file) is overwritten with its slices captured (INVARIANT 03 §5).
#[allow(clippy::too_many_arguments)]
fn apply_rename_same_dir(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    parent_ino: u64,
    from: &[u8],
    to: &[u8],
    seq: u64,
    ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    if from == to {
        return Ok(ApplyOutcome::no_delta(Vec::new(), MetaResponse::None));
    }
    let source = match read_record(store, bucket, parent_ino, from)? {
        Some(FsRecord::File(file)) => file,
        Some(_) => return Ok(rejected("source is a directory (v1 renames files only)")),
        None => return Ok(rejected("source not found")),
    };
    // The destination, if a file, is overwritten (its slices captured); if a
    // directory, reject (would orphan a subtree).
    let (dest_account, dest_existed) = match read_record(store, bucket, parent_ino, to)? {
        Some(FsRecord::File(dest)) => {
            let (ib, ic) = dest.content.inline_account();
            ((ib, ic, dest.size), true)
        }
        Some(_) => return Ok(rejected("destination is a directory")),
        None => ((0, 0, 0), false),
    };

    let mut ops = Vec::new();
    // Capture the overwritten destination's slices/segments (if any).
    let (dest_inline, dest_count, dest_total) = capture_old_file(
        store, extractor, bucket, parent_ino, to, seq, ts_millis, &mut ops,
    )?;
    debug_assert_eq!((dest_inline, dest_count, dest_total), dest_account);

    // Move the source: rewrite its head under `to`, relocate its overflow
    // segments, delete the old head + segments. The inode identity (ino) is
    // preserved — rename keeps the same file.
    let (moved, segments) = pack_file(FileRecord {
        mtime: ts_millis,
        ..source
    });
    ops.push(StoreOp::put(
        MetaCf::Fs,
        hier_key(MetaCf::Fs, bucket, parent_ino, to, &[]),
        encode(&FsRecord::File(moved))?,
    ));
    for (seg_no, segment) in segments.iter().enumerate() {
        ops.push(StoreOp::put(
            MetaCf::MetaSeg,
            hier_key(
                MetaCf::MetaSeg,
                bucket,
                parent_ino,
                to,
                &suffix::seg_no(seg_no as u32),
            ),
            encode(segment)?,
        ));
    }
    // Delete the source head + its old segments (its slices are NOT captured —
    // they moved to `to`, still live).
    ops.push(StoreOp::delete(
        MetaCf::Fs,
        hier_key(MetaCf::Fs, bucket, parent_ino, from, &[]),
    ));
    delete_segments(store, bucket, parent_ino, from, &mut ops)?;
    // Child-count delta: source name removed (-1). If the destination did not
    // exist, the new name adds it back (+1) → net 0; overwriting an existing
    // destination is a net -1 (03 §6.1 sentinel count).
    let count_delta = if dest_existed { -1 } else { 0 };
    bump_sentinel_child_delta(store, bucket, parent_ino, count_delta, ts_millis, &mut ops)?;

    // Net inline delta: source moved (no change), destination removed.
    let delta = crate::guard::GuardDelta {
        inline_bytes: -(dest_inline as i64),
        inline_count: -(dest_count as i64),
        total_bytes: -(dest_total as i64),
    };
    Ok(ApplyOutcome::new(ops, MetaResponse::None, delta))
}

/// Deletes every overflow segment of `(parent_ino, name)` into `ops` (used by
/// rename to drop the moved-from segments, whose slices stay live under `to`).
fn delete_segments(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    ops: &mut Vec<StoreOp>,
) -> Result<(), MetaStoreError> {
    let seg_prefix = hier_key(MetaCf::MetaSeg, bucket, parent_ino, name, &[]);
    let seg_end = prefix_end(&seg_prefix).unwrap_or_else(|| vec![MetaCf::MetaSeg.tag() + 1]);
    let mut cursor = seg_prefix.clone();
    loop {
        let page = store.scan(MetaCf::MetaSeg, &cursor, &seg_end, 1024)?;
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
        for (seg_key, _) in page {
            ops.push(StoreOp::delete(MetaCf::MetaSeg, seg_key));
        }
    }
    Ok(())
}

/// Whether a directory holds any entry beyond its sentinel (rmdir emptiness,
/// unsplit case — 03 §6.2). Scans the two entries `[sentinel .. )` limit 2.
fn directory_has_children(
    store: &dyn MetaStore,
    bucket: BucketId,
    dir_ino: u64,
) -> Result<bool, MetaStoreError> {
    let start = hier_key(MetaCf::Fs, bucket, dir_ino, SENTINEL_NAME, &[]);
    let end = prefix_end(&hier_key(MetaCf::Fs, bucket, dir_ino, SENTINEL_NAME, &[]))
        .unwrap_or_else(|| vec![MetaCf::Fs.tag() + 1]);
    // The prefix `f|bucket|ino` bounds exactly this directory's range; the
    // sentinel (name="") is its first key, so >1 entry means it has children.
    let range_start = {
        let mut k = vec![MetaCf::Fs.tag()];
        k.extend_from_slice(&bucket.get().to_be_bytes());
        k.extend_from_slice(&dir_ino.to_be_bytes());
        k
    };
    let range_end = prefix_end(&range_start).unwrap_or_else(|| vec![MetaCf::Fs.tag() + 1]);
    let _ = (start, end);
    let page = store.scan(MetaCf::Fs, &range_start, &range_end, 2)?;
    Ok(page.len() > 1)
}

/// Applies a child-count delta and mtime bump to the parent sentinel in the
/// same batch (03 §6.1). A delta of 0 bumps only the mtime. Skips when the
/// sentinel is absent (parent lives in another partition or is not yet
/// materialized) or `Unknown` (split-degraded, 03 §6.2 — the count is no
/// longer authoritative).
fn bump_sentinel_child_delta(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    delta: i64,
    ts_millis: i64,
    ops: &mut Vec<StoreOp>,
) -> Result<(), MetaStoreError> {
    let key = hier_key(MetaCf::Fs, bucket, parent_ino, SENTINEL_NAME, &[]);
    let Some(bytes) = store.get(MetaCf::Fs, &key)? else {
        return Ok(());
    };
    let FsRecord::Sentinel(mut sentinel) = decode::<FsRecord>(&bytes)? else {
        return Ok(());
    };
    sentinel.mtime = ts_millis;
    if let DirCount::Exact(count) = sentinel.entry_count {
        let updated = count.saturating_add_signed(delta);
        sentinel.entry_count = DirCount::Exact(updated);
    }
    ops.push(StoreOp::put(
        MetaCf::Fs,
        key,
        encode(&FsRecord::Sentinel(sentinel))?,
    ));
    Ok(())
}

/// readdir: the entries of a directory (03 §6.1 prefix scan after the
/// sentinel), up to `limit`, starting strictly after `start_after` (None =
/// from the first child). Skips the sentinel; returns `(name, FsRecord)`.
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn readdir(
    store: &dyn MetaStore,
    bucket: BucketId,
    dir_ino: u64,
    start_after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, FsRecord)>, MetaStoreError> {
    let prefix_len = 1 + 8 + 8; // tag | bucket | parent_ino
    let mut start = match start_after {
        Some(name) => {
            let mut k = hier_key(MetaCf::Fs, bucket, dir_ino, name, &[]);
            k.push(0); // exclusive of start_after
            k
        }
        None => {
            // Skip the sentinel (name=""): start just past it.
            let mut k = hier_key(MetaCf::Fs, bucket, dir_ino, SENTINEL_NAME, &[]);
            k.push(0);
            k
        }
    };
    let end = prefix_end(&hier_key(MetaCf::Fs, bucket, dir_ino, SENTINEL_NAME, &[]))
        .unwrap_or_else(|| vec![MetaCf::Fs.tag() + 1]);
    let mut out = Vec::new();
    loop {
        if out.len() >= limit {
            break;
        }
        let page = store.scan(MetaCf::Fs, &start, &end, limit - out.len())?;
        if page.is_empty() {
            break;
        }
        start = page
            .last()
            .map(|(k, _)| {
                let mut next = k.clone();
                next.push(0);
                next
            })
            .expect("non-empty page");
        for (key, value) in page {
            let name = key[prefix_len..].to_vec();
            out.push((name, decode(&value)?));
        }
    }
    Ok(out)
}

/// lookup: the record at `(parent_ino, name)` — path-walk step / getattr
/// (03 §6.2).
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn lookup(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
) -> Result<Option<FsRecord>, MetaStoreError> {
    read_record(store, bucket, parent_ino, name)
}

/// One overflow segment of a hier file's slice list, by number (03 §4.2 shared
/// `meta_seg` CF — the hier mirror of [`crate::ns_flat::get_segment`]).
///
/// A file whose slice list exceeds [`crate::ns_common::HEAD_EMBEDDED_SLICES`]
/// keeps only the first slices in its head; the rest live here. A read path that
/// serves only the head-embedded slices silently truncates every large file, so
/// the service layer must walk `seg_count` segments through this function.
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn get_segment(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    seg_no: u32,
) -> Result<Option<SliceSegment>, MetaStoreError> {
    store
        .get(
            MetaCf::MetaSeg,
            &hier_key(
                MetaCf::MetaSeg,
                bucket,
                parent_ino,
                name,
                &suffix::seg_no(seg_no),
            ),
        )?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// The full slice list of a hier file: its head-embedded slices followed by
/// every overflow segment in order (03 §4.2). This is the list a GET
/// reconstructs from — an inline file has none.
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure, including a head that
/// claims a segment the engine does not hold (a torn write would be a bug: the
/// head and its segments commit in one batch).
pub fn full_slices(
    store: &dyn MetaStore,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    file: &FileRecord,
) -> Result<Vec<Slice>, MetaStoreError> {
    let ContentHead::Slices(embedded) = &file.content else {
        return Ok(Vec::new());
    };
    let mut slices = embedded.clone();
    for seg_no in 0..file.seg_count {
        let segment = get_segment(store, bucket, parent_ino, name, seg_no)?.ok_or_else(|| {
            MetaStoreError::ValueCodec(format!(
                "hier file claims segment {seg_no} but it is absent"
            ))
        })?;
        slices.extend(segment.slices);
    }
    Ok(slices)
}

/// Whether a sentinel is an orphan: its recorded parent link is missing or no
/// longer points at it (mkdir step 2 never committed, or the parent was
/// removed, 03 §6.2). The root sentinel (no `parent_link`) is never an orphan.
fn sentinel_is_orphan(
    store: &dyn MetaStore,
    bucket: BucketId,
    sentinel: &DirRecord,
) -> Result<bool, MetaStoreError> {
    let Some((parent_ino, name)) = &sentinel.parent_link else {
        return Ok(false); // root sentinel — always rooted
    };
    match read_record(store, bucket, *parent_ino, name)? {
        Some(FsRecord::Dir(entry)) => Ok(entry.ino != sentinel.ino),
        // A file at the name, or nothing → this sentinel is not linked as a dir.
        _ => Ok(true),
    }
}

/// The orphan-sentinel sweep (03 §6.2 注: 孤儿哨兵回收). Every partition leader
/// scans its `fs` range for directory sentinels whose parent `DirEntry` is
/// missing (mkdir step 1 committed but step 2 never did) and that are older
/// than `ttl`; for each, if the directory has no children, proposes a
/// `RmdirSentinel` to reclaim it. No-op on a follower.
///
/// Deletion goes through the normal `RmdirSentinel` op, so it inherits the
/// emptiness check and is deterministic + replay-safe. `now_millis` is the
/// caller's clock (never read inside apply, same discipline as the deleter).
///
/// # Errors
///
/// Returns [`MetaError`] on engine failures.
pub async fn sweep_orphan_sentinels(
    store: &std::sync::Arc<dyn MetaStore>,
    raft: &MetaRaft,
    range: &PartitionRange,
    ttl: std::time::Duration,
    now_millis: i64,
) -> Result<usize, MetaError> {
    if !raft.metrics().borrow().state.is_leader() {
        return Ok(0);
    }
    let Some(bounds) = range.key_ranges().into_iter().find(|r| r.cf == MetaCf::Fs) else {
        return Ok(0);
    };
    let expire_before = now_millis - ttl.as_millis() as i64;

    // Collect orphan sentinels (aged, unlinked, empty). A sentinel is the
    // first key of its directory range (name == ""); a directory with children
    // is skipped (rmdir on a non-empty dir would be rejected anyway).
    let mut orphans = Vec::new();
    let mut cursor = bounds.start.clone();
    loop {
        let page = store.scan(MetaCf::Fs, &cursor, &bounds.end, 1024)?;
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
        for (key, value) in page {
            // Sentinel keys end exactly at the routing key (no trailing name):
            // `tag | bucket(8) | ino(8)` = 17 bytes.
            if key.len() != 1 + 8 + 8 {
                continue;
            }
            let FsRecord::Sentinel(sentinel) = decode::<FsRecord>(&value)? else {
                continue;
            };
            if sentinel.created_ts > expire_before {
                continue; // within the intervention window
            }
            let (bucket, _) = crate::store::keys::routing_key(&key)
                .ok_or_else(|| MetaError::Raft("malformed sentinel key".to_string()))?;
            if sentinel_is_orphan(store.as_ref(), bucket, &sentinel)?
                && !directory_has_children(store.as_ref(), bucket, sentinel.ino)?
            {
                orphans.push((bucket, sentinel.ino));
            }
        }
    }

    let mut reclaimed = 0usize;
    for (bucket, ino) in orphans {
        let op = HierOp::RmdirSentinel {
            bucket,
            ino,
            ts_millis: now_millis,
        };
        if raft
            .client_write(crate::raft::MetaEntry::Hier(op))
            .await
            .is_err()
        {
            break; // leadership moved; the next sweep resumes
        }
        reclaimed += 1;
    }
    Ok(reclaimed)
}

#[cfg(test)]
mod tests;
