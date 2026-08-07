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

//! Flat namespace: the object-key dictionary-order namespace (03 §3 flat
//! mode) — value model, raft op vocabulary, and the apply-time op handlers.
//!
//! ## head + segment (03 §4.2)
//!
//! An object's blob-id list never lives in one unbounded value: the head
//! embeds the first [`HEAD_EMBEDDED_SLICES`] slices; the rest spill to
//! `meta_seg` entries of up to [`SEGMENT_SLICES`] slices each, packed at apply
//! time (the entry carries the unpacked list; packing is a pure function, so
//! every replica derives identical keys). LIST/readdir only ever scans the
//! primary `meta` CF — segments are read on demand for range GETs.
//!
//! ## INVARIANT(design 03 §5): 旧 slices 必须由 apply 捕获
//!
//! Proposes carry only the *new* value (`FlatOp::Put{head}` / `Delete`). The
//! handlers below read the *current* head/segments at apply time and enqueue
//! their slices into `delq` — in the same [`StoreOp`] batch, so the enqueue
//! is atomic with the metadata change. Apply is serial per partition, so the
//! capture is race-free by construction; a gateway-supplied old value would
//! leak under concurrent overwrites.
//!
//! Design: docs/design/03-metanode.md §4.2/§5

use epoch_proto::BucketId;
use serde::{Deserialize, Serialize};

use crate::ns_common::split_slices;
use crate::ref_extractor::{RefExtractor, Slice};
use crate::store::keys::{MetaCf, flat_key, prefix_end, suffix};
use crate::store::{MetaStore, MetaStoreError, StoreOp};

pub mod multipart;

// The head/segment content model and slice-chunking rule are namespace-neutral
// (03 §4.2) and live in `ns_common`; re-exported here so flat callers and the
// existing tests keep using `ns_flat::{ContentHead, SliceSegment, ...}`.
pub use crate::ns_common::{
    ApplyOutcome, ContentHead, HEAD_EMBEDDED_SLICES, HttpMeta, MetaResponse, SEGMENT_SLICES,
    SliceSegment, StorageClass,
};

/// The primary record of a flat object (CF `meta` value, 03 §4.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectHead {
    /// Object size in bytes.
    pub size: u64,
    /// Content etag (16 bytes; S3-compatible MD5-of-parts semantics are a
    /// gateway concern, M6).
    pub etag: [u8; 16],
    /// Last-modified, wall-clock millis (proposer-supplied, like all
    /// timestamps that enter the log).
    pub mtime: i64,
    /// Storage class.
    pub storage: StorageClass,
    /// Inline bytes or the (possibly head-truncated) slice list.
    pub content: ContentHead,
    /// Number of `meta_seg` overflow entries (0 = everything embedded).
    pub seg_count: u32,
    /// HTTP metadata replayed on GET/HEAD (Content-Type, `x-amz-meta-*`, …).
    #[serde(default)]
    pub http: HttpMeta,
}

impl ObjectHead {
    /// The full slice list the head embeds (empty for inline content).
    #[must_use]
    pub fn into_slices(self) -> Vec<Slice> {
        self.content.into_slices()
    }
}

/// The flat-namespace op vocabulary carried inside
/// [`MetaEntry::Flat`](crate::raft::MetaEntry).
///
/// Every variant that can enqueue deletes carries `ts_millis` — the
/// proposer's wall clock, replicated with the entry, so apply never reads a
/// clock and every replica enqueues byte-identical `delq` entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FlatOp {
    /// PutObject: replace `key` wholesale with the (unpacked) new head.
    Put {
        /// Owning bucket.
        bucket: BucketId,
        /// Object key (the flat routing key).
        key: Vec<u8>,
        /// The complete new head; `content` holds the *full* slice list —
        /// apply packs the head/segment split deterministically.
        head: ObjectHead,
        /// Proposer wall-clock millis (delq `enqueue_ts`).
        ts_millis: i64,
    },
    /// DeleteObject: remove `key` and tombstone its data (idempotent — a
    /// missing key is a no-op, 03 §5).
    Delete {
        /// Owning bucket.
        bucket: BucketId,
        /// Object key.
        key: Vec<u8>,
        /// Proposer wall-clock millis (delq `enqueue_ts`).
        ts_millis: i64,
    },
    /// CreateMultipartUpload: open a session (idempotent per `upload_id`,
    /// 03 §5).
    CreateMultipart {
        /// Owning bucket.
        bucket: BucketId,
        /// Object key (sessions cluster under it — Complete lands in the
        /// object's partition, 03 §4.1).
        key: Vec<u8>,
        /// Client-generated session id (16 bytes, never reused).
        upload_id: u128,
        /// Proposer wall-clock millis (session `init_ts`, the TTL clock).
        ts_millis: i64,
    },
    /// UploadPart: write one part of a session (idempotent per
    /// `(upload_id, part_no)`; an overwrite captures the old part's slices,
    /// 03 §5 同契约).
    PutPart {
        /// Owning bucket.
        bucket: BucketId,
        /// Object key.
        key: Vec<u8>,
        /// Session id.
        upload_id: u128,
        /// Part number (≥1).
        part_no: u32,
        /// The part's metadata (size/etag/slices).
        part: crate::ns_flat::multipart::PartMeta,
        /// Proposer wall-clock millis (delq `enqueue_ts` on overwrite).
        ts_millis: i64,
    },
    /// CompleteMultipartUpload: repack the listed parts into the object head
    /// and its overflow segments, discarding session state (03 §5: parts 引用
    /// 按序重打包为约 1024 条/段的连续 segment, 与 head/会话清理同一
    /// WriteBatch). Overwriting an existing object captures its slices
    /// (03 §5 同契约); uploaded but unlisted parts are abandoned and captured
    /// too.
    CompleteMultipart {
        /// Owning bucket.
        bucket: BucketId,
        /// Object key.
        key: Vec<u8>,
        /// Session id.
        upload_id: u128,
        /// The ordered part list (part_no, etag) to assemble.
        parts: Vec<crate::ns_flat::multipart::PartRef>,
        /// Proposer wall-clock millis (head `mtime` + delq `enqueue_ts`).
        ts_millis: i64,
    },
    /// AbortMultipartUpload: discard the session and tombstone its parts'
    /// data (idempotent, 03 §5).
    AbortMultipart {
        /// Owning bucket.
        bucket: BucketId,
        /// Object key.
        key: Vec<u8>,
        /// Session id.
        upload_id: u128,
        /// Proposer wall-clock millis (delq `enqueue_ts`).
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

/// Applies one flat op to its outcome.
///
/// `seq` is the partition's delete-event sequence for this entry (one op =
/// one event, 03 §8): captured slices are enqueued under
/// `q | bucket | key | seq | seg_no`.
pub fn apply(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    op: &FlatOp,
    seq: u64,
) -> Result<ApplyOutcome, MetaStoreError> {
    match op {
        FlatOp::Put {
            bucket,
            key,
            head,
            ts_millis,
        } => apply_put(store, extractor, *bucket, key, head, seq, *ts_millis),
        FlatOp::Delete {
            bucket,
            key,
            ts_millis,
        } => apply_delete(store, extractor, *bucket, key, seq, *ts_millis),
        FlatOp::CreateMultipart {
            bucket,
            key,
            upload_id,
            ts_millis,
        } => crate::ns_flat::multipart::apply_create(store, *bucket, key, *upload_id, *ts_millis),
        FlatOp::PutPart {
            bucket,
            key,
            upload_id,
            part_no,
            part,
            ts_millis,
        } => crate::ns_flat::multipart::apply_put_part(
            store, extractor, *bucket, key, *upload_id, *part_no, part, seq, *ts_millis,
        ),
        FlatOp::CompleteMultipart {
            bucket,
            key,
            upload_id,
            parts,
            ts_millis,
        } => crate::ns_flat::multipart::apply_complete(
            store, extractor, *bucket, key, *upload_id, parts, seq, *ts_millis,
        ),
        FlatOp::AbortMultipart {
            bucket,
            key,
            upload_id,
            ts_millis,
        } => crate::ns_flat::multipart::apply_abort(
            store, extractor, *bucket, key, *upload_id, seq, *ts_millis,
        ),
    }
}

/// PutObject: capture the old value, then write the packed new head and its
/// segments (03 §5 row 1).
fn apply_put(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    key: &[u8],
    new_head: &ObjectHead,
    seq: u64,
    ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    let mut ops = Vec::new();
    let CaptureOutcome {
        account: (old_inline, old_count, old_total),
        ..
    } = capture_old(store, extractor, bucket, key, seq, ts_millis, &mut ops)?;

    // Pack: the entry carries the full slice list; head embeds the first
    // HEAD_EMBEDDED_SLICES, the rest chunk into segments (03 §4.2).
    let (packed, segments) = pack(new_head);
    ops.push(StoreOp::put(
        MetaCf::Meta,
        flat_key(MetaCf::Meta, bucket, key, &[]),
        encode(&packed)?,
    ));
    for (seg_no, segment) in segments.iter().enumerate() {
        ops.push(StoreOp::put(
            MetaCf::MetaSeg,
            flat_key(MetaCf::MetaSeg, bucket, key, &suffix::seg_no(seg_no as u32)),
            encode(segment)?,
        ));
    }

    let (new_inline, new_count) = inline_account(&packed);
    let delta = crate::guard::GuardDelta {
        inline_bytes: new_inline as i64 - old_inline as i64,
        inline_count: new_count as i64 - old_count as i64,
        total_bytes: new_head.size as i64 - old_total as i64,
    };
    Ok(ApplyOutcome::new(ops, MetaResponse::None, delta))
}

/// DeleteObject: capture the old value, delete head and segments (03 §5 row
/// 3; a missing key yields just the head delete — idempotent).
fn apply_delete(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    key: &[u8],
    seq: u64,
    ts_millis: i64,
) -> Result<ApplyOutcome, MetaStoreError> {
    let mut ops = Vec::new();
    let CaptureOutcome {
        account: (old_inline, old_count, old_total),
        ..
    } = capture_old(store, extractor, bucket, key, seq, ts_millis, &mut ops)?;
    ops.push(StoreOp::delete(
        MetaCf::Meta,
        flat_key(MetaCf::Meta, bucket, key, &[]),
    ));
    let delta = crate::guard::GuardDelta {
        inline_bytes: -(old_inline as i64),
        inline_count: -(old_count as i64),
        total_bytes: -(old_total as i64),
    };
    Ok(ApplyOutcome::new(ops, MetaResponse::None, delta))
}

/// The `(inline_bytes, inline_count)` of a head value (inline objects count
/// once, even zero-length ones).
pub(crate) fn inline_account(head: &ObjectHead) -> (u64, u64) {
    head.content.inline_account()
}

/// INVARIANT(design 03 §5): reads the *current* head + segments of `key` and
/// enqueues their slices into `delq`, deleting every old segment key — all in
/// the caller's batch. The head key itself is left to the caller (overwritten
/// by put, deleted by delete).
///
/// Returns the old object's `(inline_bytes, inline_count, total_bytes)` for
/// the inline guard (zeros when absent or undecodable) plus the next free delq
/// `seg_no` (03 §8): a caller that enqueues *more* slices under the same `seq`
/// — CompleteMultipartUpload's abandoned parts — must continue numbering from
/// here, or its keys would collide with the capture's and silently overwrite
/// them (a slice leak).
pub(crate) fn capture_old(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    key: &[u8],
    seq: u64,
    ts_millis: i64,
    ops: &mut Vec<StoreOp>,
) -> Result<CaptureOutcome, MetaStoreError> {
    let mut captured: Vec<Slice> = Vec::new();
    let mut account = (0, 0, 0);
    if let Some(old_head) = store.get(MetaCf::Meta, &flat_key(MetaCf::Meta, bucket, key, &[]))? {
        captured.extend(extractor.extract(MetaCf::Meta, &old_head));
        // The guard delta decodes the head proper (the extractor stays a
        // total, slices-only lens); an undecodable head accounts as zero.
        if let Ok(head) = decode::<ObjectHead>(&old_head) {
            let (inline_bytes, inline_count) = inline_account(&head);
            account = (inline_bytes, inline_count, head.size);
        }
    } else {
        // No old object: nothing to capture (delete is idempotent). The delq
        // seg_no space for this seq is still entirely free.
        return Ok(CaptureOutcome {
            account,
            next_seg_no: 0,
        });
    }

    // Enumerate old segments by prefix scan — robust even against a corrupt
    // head seg_count — extracting and deleting each (03 §4.2: head/segments
    // share the key prefix, so the scan covers exactly this object).
    let seg_start = flat_key(MetaCf::MetaSeg, bucket, key, &[]);
    let seg_end = prefix_end(&seg_start).unwrap_or_else(|| vec![MetaCf::MetaSeg.tag() + 1]);
    let mut cursor = seg_start.clone();
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

    let next_seg_no = enqueue_delete_slices(bucket, key, seq, 0, &captured, ts_millis, ops)?;
    Ok(CaptureOutcome {
        account,
        next_seg_no,
    })
}

/// The result of [`capture_old`]: the old object's inline-guard accounting and
/// the next unused delq `seg_no` under the capture's `seq`.
pub(crate) struct CaptureOutcome {
    /// The old object's `(inline_bytes, inline_count, total_bytes)`.
    pub account: (u64, u64, u64),
    /// The next free delq `seg_no` — where a same-`seq` follow-up enqueue must
    /// start (see [`capture_old`]).
    pub next_seg_no: u32,
}

/// Enqueues `slices` into `delq` under the flat key `(bucket, key, seq)`,
/// numbering from `first_seg_no` and returning the next free `seg_no`. A thin
/// flat-key wrapper over [`crate::ns_common::enqueue_delete_slices`] (03 §8) so
/// flat callers keep their positional signature.
pub(crate) fn enqueue_delete_slices(
    bucket: BucketId,
    key: &[u8],
    seq: u64,
    first_seg_no: u32,
    slices: &[Slice],
    ts_millis: i64,
    ops: &mut Vec<StoreOp>,
) -> Result<u32, MetaStoreError> {
    crate::ns_common::enqueue_delete_slices(
        first_seg_no,
        slices,
        ts_millis,
        |seg_no| flat_key(MetaCf::Delq, bucket, key, &suffix::delq_seq(seq, seg_no)),
        ops,
    )
}

/// The deterministic head/segment split of a new head (03 §4.2): delegates the
/// slice split to [`crate::ns_common::split_slices`] and reassembles the flat
/// [`ObjectHead`] around the embedded prefix.
pub(crate) fn pack(head: &ObjectHead) -> (ObjectHead, Vec<SliceSegment>) {
    let ContentHead::Slices(slices) = &head.content else {
        return (
            ObjectHead {
                seg_count: 0,
                ..head.clone()
            },
            Vec::new(),
        );
    };
    let (embedded, segments) = split_slices(slices);
    (
        ObjectHead {
            content: ContentHead::Slices(embedded),
            seg_count: segments.len() as u32,
            ..head.clone()
        },
        segments,
    )
}

/// HeadObject / GetObject read: the head alone (segments are fetched on
/// demand for range GETs, 03 §5).
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn get_head(
    store: &dyn MetaStore,
    bucket: BucketId,
    key: &[u8],
) -> Result<Option<ObjectHead>, MetaStoreError> {
    store
        .get(MetaCf::Meta, &flat_key(MetaCf::Meta, bucket, key, &[]))?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// Range-GET support read: one overflow segment by number (03 §4.2: 由
/// offset 与各 slice 的 blob 数直接定位 seg_no，按需点查).
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn get_segment(
    store: &dyn MetaStore,
    bucket: BucketId,
    key: &[u8],
    seg_no: u32,
) -> Result<Option<SliceSegment>, MetaStoreError> {
    store
        .get(
            MetaCf::MetaSeg,
            &flat_key(MetaCf::MetaSeg, bucket, key, &suffix::seg_no(seg_no)),
        )?
        .map(|bytes| decode(&bytes))
        .transpose()
}

/// ListObjectsV2 read: up to `limit` heads under `prefix`, starting strictly
/// after `start_after` (None = from the prefix start). Scans the primary CF
/// only — never `meta_seg` (03 §4.2/§5).
///
/// # Errors
///
/// Returns [`MetaStoreError`] on engine or codec failure.
pub fn list(
    store: &dyn MetaStore,
    bucket: BucketId,
    prefix: &[u8],
    start_after: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, ObjectHead)>, MetaStoreError> {
    let mut start = flat_key(MetaCf::Meta, bucket, start_after.unwrap_or(prefix), &[]);
    if start_after.is_some() {
        start.push(0); // scan is inclusive; the start_after key itself is excluded
    }
    let end = prefix_end(&flat_key(MetaCf::Meta, bucket, prefix, &[]))
        .unwrap_or_else(|| vec![MetaCf::Meta.tag() + 1]);
    let page = store.scan(MetaCf::Meta, &start, &end, limit)?;
    page.iter()
        .map(|(key, value)| {
            let head: ObjectHead = decode(value)?;
            // Strip `tag | bucket_id` — callers want the object key.
            Ok((key[9..].to_vec(), head))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ref_extractor::{EpochRefExtractor, PendingDelete};
    use crate::store::rocks::RocksEngine;

    use epoch_proto::{BlobId, ChunkId};

    fn bucket() -> BucketId {
        BucketId::new(1)
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

    fn head_with(content: ContentHead) -> ObjectHead {
        ObjectHead {
            size: 100,
            etag: [7; 16],
            mtime: 1_700_000_000_000,
            storage: StorageClass::Standard,
            content,
            seg_count: 0,
            http: HttpMeta::default(),
        }
    }

    fn put_op(key: &[u8], head: ObjectHead) -> FlatOp {
        FlatOp::Put {
            bucket: bucket(),
            key: key.to_vec(),
            head,
            ts_millis: 1_700_000_100_000,
        }
    }

    /// Applies an op straight to the engine (the state-machine path is
    /// covered by the cluster tests; here the handlers are the unit).
    fn run(engine: &RocksEngine, op: &FlatOp, seq: u64) {
        let outcome = apply(engine, &EpochRefExtractor, op, seq).expect("apply op");
        engine.apply(&outcome.ops).expect("commit");
    }

    /// HTTP metadata must survive the write → read round trip. Dropping it is a
    /// silent data loss: the client gets a 200 and its `Content-Type` /
    /// `x-amz-meta-*` are gone for good, with nothing to retry against.
    #[test]
    fn http_metadata_round_trips_through_the_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        let mut user = std::collections::BTreeMap::new();
        user.insert("owner".to_string(), "trainer".to_string());
        user.insert("dataset".to_string(), "imagenet".to_string());
        let http = HttpMeta {
            content_type: Some("application/x-tar".to_string()),
            content_encoding: Some("gzip".to_string()),
            cache_control: Some("max-age=3600".to_string()),
            user,
        };
        let mut head = head_with(ContentHead::Inline(b"body".to_vec()));
        head.http = http.clone();
        run(&engine, &put_op(b"obj", head), 1);

        let stored = get_head(&engine, bucket(), b"obj")
            .expect("get")
            .expect("present");
        assert_eq!(stored.http, http, "every field survives the round trip");

        // A write with no metadata reads back as unset, not as empty strings —
        // the gateway must be able to omit the headers entirely.
        run(
            &engine,
            &put_op(b"bare", head_with(ContentHead::Inline(b"x".to_vec()))),
            2,
        );
        let bare = get_head(&engine, bucket(), b"bare")
            .expect("get")
            .expect("present");
        assert!(bare.http.is_empty());
        assert_eq!(bare.http.content_type, None);
    }

    #[test]
    fn put_get_list_small_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        run(
            &engine,
            &put_op(b"a/1", head_with(ContentHead::Inline(b"xyz".to_vec()))),
            1,
        );
        run(
            &engine,
            &put_op(b"a/2", head_with(ContentHead::Slices(slices(10)))),
            2,
        );

        let head = get_head(&engine, bucket(), b"a/1")
            .expect("get")
            .expect("present");
        assert_eq!(head.content, ContentHead::Inline(b"xyz".to_vec()));
        assert_eq!(head.seg_count, 0);

        let page = list(&engine, bucket(), b"a/", None, 10).expect("list");
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].0, b"a/1".to_vec());
        assert_eq!(page[1].1.content, ContentHead::Slices(slices(10)));
    }

    #[test]
    fn put_packs_overflow_into_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        let all = slices(HEAD_EMBEDDED_SLICES + SEGMENT_SLICES + 5);
        run(
            &engine,
            &put_op(b"big", head_with(ContentHead::Slices(all.clone()))),
            1,
        );

        let head = get_head(&engine, bucket(), b"big")
            .expect("get")
            .expect("present");
        assert_eq!(head.seg_count, 2);
        assert_eq!(
            head.content,
            ContentHead::Slices(all[..HEAD_EMBEDDED_SLICES].to_vec())
        );

        let seg0 = get_segment(&engine, bucket(), b"big", 0)
            .expect("get seg0")
            .expect("seg0 present");
        assert_eq!(seg0.slices.len(), SEGMENT_SLICES);
        let seg1 = get_segment(&engine, bucket(), b"big", 1)
            .expect("get seg1")
            .expect("seg1 present");
        assert_eq!(seg1.slices.len(), 5);
        // The full list round-trips in order.
        let reassembled: Vec<Slice> = head
            .into_slices()
            .into_iter()
            .chain(seg0.slices)
            .chain(seg1.slices)
            .collect();
        assert_eq!(reassembled, all);
    }

    /// INVARIANT(design 03 §5): overwritten slices are captured into delq by
    /// apply — never supplied by the proposer.
    #[test]
    fn overwrite_captures_old_slices_into_delq() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");

        // v1: 64 embedded + 5 in one segment.
        let old_slices = slices(HEAD_EMBEDDED_SLICES + 5);
        run(
            &engine,
            &put_op(b"k", head_with(ContentHead::Slices(old_slices.clone()))),
            1,
        );
        // v2: a small inline overwrite; every old slice must land in delq.
        run(
            &engine,
            &put_op(b"k", head_with(ContentHead::Inline(b"new".to_vec()))),
            2,
        );

        // Old segment keys are gone.
        assert!(
            get_segment(&engine, bucket(), b"k", 0)
                .expect("seg")
                .is_none()
        );
        // delq holds one entry under seq 2, carrying all 69 old slices.
        let delq_key = flat_key(MetaCf::Delq, bucket(), b"k", &suffix::delq_seq(2, 0));
        let pending: PendingDelete = decode(
            &engine
                .get(MetaCf::Delq, &delq_key)
                .expect("get delq")
                .expect("delq present"),
        )
        .expect("decode delq");
        assert_eq!(pending.slices, old_slices);
        assert_eq!(pending.enqueue_ts, 1_700_000_100_000);
    }

    #[test]
    fn delete_enqueues_then_tombstones_and_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        run(
            &engine,
            &put_op(b"d", head_with(ContentHead::Slices(slices(3)))),
            1,
        );

        let delete = FlatOp::Delete {
            bucket: bucket(),
            key: b"d".to_vec(),
            ts_millis: 1_700_000_200_000,
        };
        run(&engine, &delete, 2);
        assert!(get_head(&engine, bucket(), b"d").expect("get").is_none());

        let delq_key = flat_key(MetaCf::Delq, bucket(), b"d", &suffix::delq_seq(2, 0));
        let pending: PendingDelete = decode(
            &engine
                .get(MetaCf::Delq, &delq_key)
                .expect("get delq")
                .expect("delq present"),
        )
        .expect("decode delq");
        assert_eq!(pending.slices, slices(3));
        assert_eq!(pending.enqueue_ts, 1_700_000_200_000);

        // Deleting a missing key: no delq entry, no error (03 §5 幂等).
        run(&engine, &delete, 3);
        let ghost = flat_key(MetaCf::Delq, bucket(), b"d", &suffix::delq_seq(3, 0));
        assert!(engine.get(MetaCf::Delq, &ghost).expect("get").is_none());
    }

    #[test]
    fn reapply_of_same_capture_is_idempotent() {
        // The disableWAL tail-loss replay (03 §8): re-applying a batch whose
        // seq was already consumed must reproduce identical delq keys.
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        run(
            &engine,
            &put_op(b"r", head_with(ContentHead::Slices(slices(2)))),
            1,
        );
        let delete = FlatOp::Delete {
            bucket: bucket(),
            key: b"r".to_vec(),
            ts_millis: 1_700_000_300_000,
        };
        let outcome = apply(&engine, &EpochRefExtractor, &delete, 2).expect("apply");
        engine.apply(&outcome.ops).expect("commit");
        engine.apply(&outcome.ops).expect("replay");
        let page = engine
            .scan(
                MetaCf::Delq,
                &flat_key(MetaCf::Delq, bucket(), b"r", &[]),
                &flat_key(MetaCf::Delq, bucket(), b"r", &[0xFF; 12]),
                16,
            )
            .expect("scan delq");
        assert_eq!(page.len(), 1, "replay must not duplicate delq entries");
    }

    #[test]
    fn list_paginates_without_touching_segments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        for i in 0..5u32 {
            run(
                &engine,
                &put_op(
                    format!("p/{i:02}").as_bytes(),
                    head_with(ContentHead::Slices(slices(HEAD_EMBEDDED_SLICES + 1))),
                ),
                i as u64 + 1,
            );
        }
        let first = list(&engine, bucket(), b"p/", None, 2).expect("page 1");
        assert_eq!(first.len(), 2);
        let second = list(
            &engine,
            bucket(),
            b"p/",
            Some(first.last().expect("two entries").0.as_slice()),
            2,
        )
        .expect("page 2");
        assert_eq!(second.len(), 2);
        assert_eq!(second[0].0, b"p/02".to_vec());
        let third = list(
            &engine,
            bucket(),
            b"p/",
            Some(second.last().expect("two entries").0.as_slice()),
            2,
        )
        .expect("page 3");
        assert_eq!(third.len(), 1);
        // Heads carry their embedded slices; no segment keys appear in pages.
        assert!(third[0].1.seg_count > 0);
    }
}
