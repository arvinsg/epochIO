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

//! Multipart uploads (03 §5 Multipart 全家; 06 §9 ns_flat/multipart.rs).
//!
//! Sessions and parts live in the `upload` CF clustered under the object's
//! own routing key — so Complete lands in the object's partition and runs as
//! one atomic batch (03 §4.1/§1):
//!
//! ```text
//! u | bucket | object_key | upload_id(16B) | part_no(u32 BE)
//!   part_no=0 → UploadSession; part_no≥1 → PartMeta
//! ```
//!
//! Complete does **not** merge a large list value: it repacks the parts'
//! references in listed order into the head/segment layout (03 §4.2 — a pure
//! in-memory repack), writes head+segments, discards session state, and
//! captures (a) an overwritten existing object's slices and (b) uploaded but
//! unlisted parts' slices into `delq` — same batch, same 03 §5 contract as
//! Put/Delete.
//!
//! Design: docs/design/03-metanode.md §4.1/§5

use std::time::Duration;

use crate::ns_common::HttpMeta;
use epoch_proto::BucketId;
use serde::{Deserialize, Serialize};

use crate::MetaError;
use crate::ns_flat::{ContentHead, MetaResponse, ObjectHead, StorageClass, capture_old, pack};
use crate::raft::MetaRaft;
use crate::ref_extractor::{RefExtractor, Slice};
use crate::store::keys::{MetaCf, flat_key, prefix_end, suffix};
use crate::store::{MetaStore, MetaStoreError, StoreOp};

/// A multipart session record (`part_no=0` value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadSession {
    /// Session id (echo of the key's, for scan-side convenience).
    pub upload_id: u128,
    /// Session creation wall-clock millis — the TTL clock (03 §5: 废弃
    /// multipart 会话清理, default 7 days).
    pub init_ts: i64,
}

/// One uploaded part (`part_no≥1` value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartMeta {
    /// Part size in bytes.
    pub size: u64,
    /// Part etag (client-supplied; echoed by Complete validation).
    pub etag: [u8; 16],
    /// The part's data references.
    pub slices: Vec<Slice>,
}

/// One entry of a Complete request's ordered part list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRef {
    /// Part number.
    pub part_no: u32,
    /// Expected etag (validated against the stored part).
    pub etag: [u8; 16],
}

/// The abandoned-session TTL (03 §5: default 7 天, the S3
/// AbortIncompleteMultipartUpload lifecycle action).
pub const DEFAULT_UPLOAD_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The default expired-session sweep interval (03 §5: 每分区 leader 后台扫描).
pub const DEFAULT_UPLOAD_TTL_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// `u | bucket | key | upload_id | part_no`.
fn upload_key(bucket: BucketId, key: &[u8], upload_id: u128, part_no: u32) -> Vec<u8> {
    flat_key(
        MetaCf::Upload,
        bucket,
        key,
        &suffix::upload_part(upload_id, part_no),
    )
}

/// The prefix covering one session's every entry (`u | bucket | key | upload_id`).
fn upload_prefix(bucket: BucketId, key: &[u8], upload_id: u128) -> Vec<u8> {
    flat_key(MetaCf::Upload, bucket, key, &upload_id.to_be_bytes())
}

/// The part number inside an upload key (its last 4 bytes).
fn part_no_of(key: &[u8]) -> Option<u32> {
    key.get(key.len().saturating_sub(4)..)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .map(u32::from_be_bytes)
}

/// The `(bucket, object_key, upload_id)` embedded in an upload key — the TTL
/// sweep's parse (layout: `u | bucket(8B) | key | upload_id(16B) | part(4B)`).
fn parse_upload_key(key: &[u8]) -> Option<(BucketId, Vec<u8>, u128)> {
    let (&tag, rest) = key.split_first()?;
    if tag != MetaCf::Upload.tag() || rest.len() < 8 + 16 + 4 {
        return None;
    }
    let (bucket, rest) = rest.split_at(8);
    let (object_key, rest) = rest.split_at(rest.len().checked_sub(20)?);
    let upload_id = u128::from_be_bytes(rest[..16].try_into().ok()?);
    Some((
        BucketId::new(u64::from_be_bytes(bucket.try_into().ok()?)),
        object_key.to_vec(),
        upload_id,
    ))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, MetaStoreError> {
    serde_json::to_vec(value).map_err(|e| MetaStoreError::ValueCodec(e.to_string()))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, MetaStoreError> {
    serde_json::from_slice(bytes).map_err(|e| MetaStoreError::ValueCodec(e.to_string()))
}

/// Scans every entry of one session (session + all parts, in part order).
fn scan_session(
    store: &dyn MetaStore,
    bucket: BucketId,
    key: &[u8],
    upload_id: u128,
) -> Result<crate::store::KvPage, MetaStoreError> {
    let prefix = upload_prefix(bucket, key, upload_id);
    let end = prefix_end(&prefix).unwrap_or_else(|| vec![MetaCf::Upload.tag() + 1]);
    // A session's entry count is bounded by the S3 part limit (10_000), so
    // one generous page suffices; a pathological overflow just continues at
    // the boundary below.
    let mut out = Vec::new();
    let mut cursor = prefix;
    loop {
        let page = store.scan(MetaCf::Upload, &cursor, &end, 4096)?;
        if page.is_empty() {
            return Ok(out);
        }
        cursor = page
            .last()
            .map(|(k, _)| {
                let mut next = k.clone();
                next.push(0);
                next
            })
            .expect("non-empty page");
        out.extend(page);
    }
}

/// CreateMultipartUpload: write the session if absent (idempotent per
/// `upload_id`; the read-then-put is atomic because apply is serial, 03 §1).
pub fn apply_create(
    store: &dyn MetaStore,
    bucket: BucketId,
    key: &[u8],
    upload_id: u128,
    ts_millis: i64,
) -> Result<crate::ns_flat::ApplyOutcome, MetaStoreError> {
    let session_key = upload_key(bucket, key, upload_id, 0);
    if store.get(MetaCf::Upload, &session_key)?.is_some() {
        return Ok(crate::ns_flat::ApplyOutcome::no_delta(
            Vec::new(),
            MetaResponse::None,
        ));
    }
    let session = UploadSession {
        upload_id,
        init_ts: ts_millis,
    };
    Ok(crate::ns_flat::ApplyOutcome::no_delta(
        vec![StoreOp::put(MetaCf::Upload, session_key, encode(&session)?)],
        MetaResponse::None,
    ))
}

/// UploadPart: write the part; an overwrite captures the old part's slices
/// (03 §5 同契约 — apply 捕获, never proposer-supplied).
#[allow(clippy::too_many_arguments)]
pub fn apply_put_part(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    key: &[u8],
    upload_id: u128,
    part_no: u32,
    part: &PartMeta,
    seq: u64,
    ts_millis: i64,
) -> Result<crate::ns_flat::ApplyOutcome, MetaStoreError> {
    let session_key = upload_key(bucket, key, upload_id, 0);
    if store.get(MetaCf::Upload, &session_key)?.is_none() {
        return Ok(crate::ns_flat::ApplyOutcome::no_delta(
            Vec::new(),
            MetaResponse::Rejected("unknown upload id".to_string()),
        ));
    }
    let part_key = upload_key(bucket, key, upload_id, part_no);
    let mut ops = Vec::new();
    if let Some(old) = store.get(MetaCf::Upload, &part_key)? {
        let old_slices = extractor.extract(MetaCf::Upload, &old);
        crate::ns_flat::enqueue_delete_slices(
            bucket,
            key,
            seq,
            0,
            &old_slices,
            ts_millis,
            &mut ops,
        )?;
    }
    ops.push(StoreOp::put(MetaCf::Upload, part_key, encode(part)?));
    Ok(crate::ns_flat::ApplyOutcome::no_delta(
        ops,
        MetaResponse::None,
    ))
}

/// CompleteMultipartUpload: validate session + listed parts, repack into the
/// object head, discard session state, capture old-object and unlisted-part
/// slices — one batch (03 §5).
#[allow(clippy::too_many_arguments)]
pub fn apply_complete(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    key: &[u8],
    upload_id: u128,
    parts: &[PartRef],
    seq: u64,
    ts_millis: i64,
) -> Result<crate::ns_flat::ApplyOutcome, MetaStoreError> {
    let session_key = upload_key(bucket, key, upload_id, 0);
    if store.get(MetaCf::Upload, &session_key)?.is_none() {
        return Ok(crate::ns_flat::ApplyOutcome::no_delta(
            Vec::new(),
            MetaResponse::Rejected("unknown upload id".to_string()),
        ));
    }
    if parts.is_empty() {
        return Ok(crate::ns_flat::ApplyOutcome::no_delta(
            Vec::new(),
            MetaResponse::Rejected("empty part list".to_string()),
        ));
    }

    // Validate every listed part and assemble the full slice list in order.
    let mut slices = Vec::new();
    let mut size = 0u64;
    for part_ref in parts {
        let part_key = upload_key(bucket, key, upload_id, part_ref.part_no);
        let Some(raw) = store.get(MetaCf::Upload, &part_key)? else {
            return Ok(crate::ns_flat::ApplyOutcome::no_delta(
                Vec::new(),
                MetaResponse::Rejected(format!("part {} missing", part_ref.part_no)),
            ));
        };
        let part: PartMeta = decode(&raw)?;
        if part.etag != part_ref.etag {
            return Ok(crate::ns_flat::ApplyOutcome::no_delta(
                Vec::new(),
                MetaResponse::Rejected(format!("part {} etag mismatch", part_ref.part_no)),
            ));
        }
        size = size.saturating_add(part.size);
        slices.extend(part.slices);
    }

    let mut ops = Vec::new();
    // 覆盖契约: an existing object's slices are captured here, at apply. The
    // capture uses delq seg_no 0..next_seg_no under this seq; abandoned parts
    // below continue from next_seg_no so their keys never collide with the
    // capture's (a collision would overwrite captured slices and leak them).
    let crate::ns_flat::CaptureOutcome {
        account: (old_inline, old_count, old_total),
        mut next_seg_no,
    } = capture_old(store, extractor, bucket, key, seq, ts_millis, &mut ops)?;

    // Discard session state: every entry of this upload goes, and uploaded
    // but *unlisted* parts are abandoned data — captured, not leaked.
    let listed: std::collections::BTreeSet<u32> = parts.iter().map(|p| p.part_no).collect();
    for (entry_key, raw) in scan_session(store, bucket, key, upload_id)? {
        if let Some(part_no) = part_no_of(&entry_key)
            && part_no != 0
            && !listed.contains(&part_no)
        {
            let abandoned = extractor.extract(MetaCf::Upload, &raw);
            next_seg_no = crate::ns_flat::enqueue_delete_slices(
                bucket,
                key,
                seq,
                next_seg_no,
                &abandoned,
                ts_millis,
                &mut ops,
            )?;
        }
        ops.push(StoreOp::delete(MetaCf::Upload, entry_key));
    }

    // Write the assembled head (+segments), packed deterministically (03 §4.2).
    let head = ObjectHead {
        size,
        etag: multipart_etag(parts),
        mtime: ts_millis,
        storage: StorageClass::Standard,
        content: ContentHead::Slices(slices),
        seg_count: 0,
        http: HttpMeta::default(),
    };
    let (packed, segments) = pack(&head);
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
    // The completed object is all slices (never inline); the old object's
    // inline bytes leave with it (03 §4.3 计数).
    let delta = crate::guard::GuardDelta {
        inline_bytes: -(old_inline as i64),
        inline_count: -(old_count as i64),
        total_bytes: size as i64 - old_total as i64,
    };
    Ok(crate::ns_flat::ApplyOutcome::new(
        ops,
        MetaResponse::None,
        delta,
    ))
}

/// AbortMultipartUpload: discard the session, tombstoning every part's data
/// (idempotent, 03 §5).
#[allow(clippy::too_many_arguments)]
pub fn apply_abort(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    bucket: BucketId,
    key: &[u8],
    upload_id: u128,
    seq: u64,
    ts_millis: i64,
) -> Result<crate::ns_flat::ApplyOutcome, MetaStoreError> {
    let session_key = upload_key(bucket, key, upload_id, 0);
    if store.get(MetaCf::Upload, &session_key)?.is_none() {
        return Ok(crate::ns_flat::ApplyOutcome::no_delta(
            Vec::new(),
            MetaResponse::None,
        ));
    }
    let mut ops = Vec::new();
    let mut next_seg_no = 0u32;
    for (entry_key, raw) in scan_session(store, bucket, key, upload_id)? {
        if let Some(part_no) = part_no_of(&entry_key)
            && part_no != 0
        {
            let slices = extractor.extract(MetaCf::Upload, &raw);
            next_seg_no = crate::ns_flat::enqueue_delete_slices(
                bucket,
                key,
                seq,
                next_seg_no,
                &slices,
                ts_millis,
                &mut ops,
            )?;
        }
        ops.push(StoreOp::delete(MetaCf::Upload, entry_key));
    }
    Ok(crate::ns_flat::ApplyOutcome::no_delta(
        ops,
        MetaResponse::None,
    ))
}

/// The completed object's etag: the BLAKE3 digest of the concatenated binary
/// part etags, truncated to the 16-byte head field (99 M5a: etag =
/// blake3(part etags 拼接) 截 16B). BLAKE3 is the project-wide digest (00 §2);
/// this is deliberately **not** an S3-compatible multipart ETag — S3's is
/// `md5(concat(part md5s))-<n>`, so a gateway cannot reproduce the S3 string
/// form from this digest. The `-<n>` part-count suffix and any S3 string
/// projection are a gateway concern (M6); v1 stores the raw digest only.
pub(crate) fn multipart_etag(parts: &[PartRef]) -> [u8; 16] {
    let mut concatenated = Vec::with_capacity(parts.len() * 16);
    for part in parts {
        concatenated.extend_from_slice(&part.etag);
    }
    let digest = blake3::hash(&concatenated);
    let mut etag = [0u8; 16];
    etag.copy_from_slice(&digest.as_bytes()[..16]);
    etag
}

/// The expired-session sweep (03 §5 废弃 multipart 会话清理): every expired
/// session in the partition's upload range is aborted via one propose —
/// equivalent to an explicit Abort (parts captured into delq, session
/// discarded, idempotent).
///
/// No-op on a follower. Returns how many sessions were aborted.
///
/// # Errors
///
/// Returns [`MetaError`] on engine failures.
pub async fn sweep_expired(
    store: &std::sync::Arc<dyn MetaStore>,
    raft: &MetaRaft,
    range: &crate::partition::PartitionRange,
    ttl: Duration,
    now_millis: i64,
) -> Result<usize, MetaError> {
    if !raft.metrics().borrow().state.is_leader() {
        return Ok(0);
    }
    let Some(bounds) = range
        .key_ranges()
        .into_iter()
        .find(|r| r.cf == MetaCf::Upload)
    else {
        return Ok(0);
    };
    let expire_before = now_millis - ttl.as_millis() as i64;

    let mut expired = Vec::new();
    let mut cursor = bounds.start.clone();
    loop {
        let page = store.scan(MetaCf::Upload, &cursor, &bounds.end, 1024)?;
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
            if part_no_of(&key) != Some(0) {
                continue;
            }
            let session: UploadSession = serde_json::from_slice(&value)
                .map_err(|e| MetaError::Raft(format!("malformed upload session: {e}")))?;
            if session.init_ts <= expire_before {
                let Some((bucket, object_key, upload_id)) = parse_upload_key(&key) else {
                    continue;
                };
                expired.push((bucket, object_key, upload_id));
            }
        }
    }

    let mut aborted = 0usize;
    for (bucket, object_key, upload_id) in expired {
        let op = crate::ns_flat::FlatOp::AbortMultipart {
            bucket,
            key: object_key,
            upload_id,
            ts_millis: now_millis,
        };
        if raft
            .client_write(crate::raft::MetaEntry::Flat(op))
            .await
            .is_err()
        {
            break; // leadership moved; the next sweep resumes
        }
        aborted += 1;
    }
    Ok(aborted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ns_flat;
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

    fn part(size: u64, etag_byte: u8, blobs: &[u64]) -> PartMeta {
        PartMeta {
            size,
            etag: [etag_byte; 16],
            slices: blobs.iter().map(|&b| slice(b as u32, b)).collect(),
        }
    }

    fn engine() -> (tempfile::TempDir, RocksEngine) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = RocksEngine::open(dir.path()).expect("open");
        (dir, engine)
    }

    fn commit(engine: &RocksEngine, ops: Vec<StoreOp>) {
        engine.apply(&ops).expect("commit");
    }

    fn get_head(engine: &RocksEngine, key: &[u8]) -> Option<ObjectHead> {
        ns_flat::get_head(engine, bucket(), key).expect("get head")
    }

    fn delq_slices(engine: &RocksEngine, key: &[u8], seq: u64, seg: u32) -> Vec<Slice> {
        let delq_key = flat_key(MetaCf::Delq, bucket(), key, &suffix::delq_seq(seq, seg));
        engine
            .get(MetaCf::Delq, &delq_key)
            .expect("get delq")
            .map(|raw| {
                let pending: PendingDelete = serde_json::from_slice(&raw).expect("decode");
                pending.slices
            })
            .unwrap_or_default()
    }

    #[test]
    fn create_is_idempotent_and_part_requires_session() {
        let (_dir, engine) = engine();
        let outcome = apply_create(&engine, bucket(), b"k", 7, 1_000).expect("create");
        assert_eq!(outcome.response, MetaResponse::None);
        assert_eq!(outcome.ops.len(), 1);
        commit(&engine, outcome.ops);
        // Retry: no-op.
        let outcome = apply_create(&engine, bucket(), b"k", 7, 2_000).expect("recreate");
        assert!(outcome.ops.is_empty());

        // PutPart against an unknown upload is rejected.
        let outcome = apply_put_part(
            &engine,
            &EpochRefExtractor,
            bucket(),
            b"k",
            99,
            1,
            &part(1, 1, &[1]),
            1,
            3_000,
        )
        .expect("put part");
        assert!(matches!(outcome.response, MetaResponse::Rejected(_)));
    }

    #[test]
    fn complete_repacks_parts_captures_old_object_and_abandoned_parts() {
        let (_dir, engine) = engine();
        let upload = 42u128;
        let outcome = apply_create(&engine, bucket(), b"obj", upload, 1_000).expect("create");
        commit(&engine, outcome.ops);

        // An existing object gets overwritten by the complete (03 §5 同契约).
        ns_flat::apply(
            &engine,
            &EpochRefExtractor,
            &ns_flat::FlatOp::Put {
                bucket: bucket(),
                key: b"obj".to_vec(),
                head: ns_flat::ObjectHead {
                    size: 1,
                    etag: [9; 16],
                    mtime: 0,
                    storage: StorageClass::Standard,
                    content: ContentHead::Slices(vec![slice(1, 900)]),
                    seg_count: 0,
                    http: HttpMeta::default(),
                },
                ts_millis: 500,
            },
            1,
        )
        .map(|outcome| commit(&engine, outcome.ops))
        .expect("put old object");

        // Three parts; part 3 is never listed (abandoned data).
        for (no, etag_byte, blobs) in [
            (1u32, 1u8, &[100u64][..]),
            (2, 2, &[200][..]),
            (3, 3, &[300][..]),
        ] {
            let outcome = apply_put_part(
                &engine,
                &EpochRefExtractor,
                bucket(),
                b"obj",
                upload,
                no,
                &part(10, etag_byte, blobs),
                2,
                1_100,
            )
            .expect("put part");
            assert_eq!(outcome.response, MetaResponse::None);
            commit(&engine, outcome.ops);
        }

        let outcome = apply_complete(
            &engine,
            &EpochRefExtractor,
            bucket(),
            b"obj",
            upload,
            &[
                PartRef {
                    part_no: 2,
                    etag: [2; 16],
                },
                PartRef {
                    part_no: 1,
                    etag: [1; 16],
                },
            ],
            3,
            2_000,
        )
        .expect("complete");
        assert_eq!(outcome.response, MetaResponse::None);
        commit(&engine, outcome.ops);

        // The head holds the listed parts in listed order (2 then 1).
        let head = get_head(&engine, b"obj").expect("head");
        assert_eq!(head.size, 20);
        assert_eq!(
            head.content,
            ContentHead::Slices(vec![slice(200, 200), slice(100, 100)])
        );
        assert_eq!(head.mtime, 2_000);

        // Session state is gone.
        assert!(
            scan_session(&engine, bucket(), b"obj", upload)
                .expect("scan")
                .is_empty()
        );

        // Old object slices captured first (seq 3, seg 0), then the abandoned
        // part continues from the next free seg_no (seq 3, seg 1) — the two
        // capture sources share one contiguous seg_no space so their delq keys
        // never collide (a collision would overwrite and leak, see the
        // dedicated regression test below).
        assert_eq!(delq_slices(&engine, b"obj", 3, 0), vec![slice(1, 900)]);
        assert_eq!(delq_slices(&engine, b"obj", 3, 1), vec![slice(300, 300)]);
    }

    /// Regression (delq key collision): a Complete overwriting an old object
    /// with more than [`SEGMENT_SLICES`] slices makes `capture_old` emit delq
    /// chunks at seg_no 0,1,2,…; an abandoned part must NOT reuse one of those
    /// seg_no values (it once used `part_no`, colliding with capture chunk N
    /// and overwriting it — a silent slice leak). Every old slice and every
    /// abandoned slice must survive in delq.
    #[test]
    fn complete_over_large_old_object_does_not_collide_delq_with_abandoned_parts() {
        let (_dir, engine) = engine();
        let upload = 7u128;

        // Old object: HEAD_EMBEDDED + 2*SEGMENT_SLICES slices → capture chunks
        // the list into 3 delq segments (seg_no 0,1,2).
        let old_count = ns_flat::HEAD_EMBEDDED_SLICES + 2 * ns_flat::SEGMENT_SLICES;
        let old_slices: Vec<Slice> = (0..old_count).map(|i| slice(i as u32, i as u64)).collect();
        let expected_old_chunks = old_slices.chunks(ns_flat::SEGMENT_SLICES).count();
        assert_eq!(expected_old_chunks, 3, "old object spans 3 delq segments");
        ns_flat::apply(
            &engine,
            &EpochRefExtractor,
            &ns_flat::FlatOp::Put {
                bucket: bucket(),
                key: b"big".to_vec(),
                head: ns_flat::ObjectHead {
                    size: 10,
                    etag: [9; 16],
                    mtime: 0,
                    storage: StorageClass::Standard,
                    content: ContentHead::Slices(old_slices.clone()),
                    seg_count: 0,
                    http: HttpMeta::default(),
                },
                ts_millis: 500,
            },
            1,
        )
        .map(|outcome| commit(&engine, outcome.ops))
        .expect("put big old object");

        let outcome = apply_create(&engine, bucket(), b"big", upload, 1_000).expect("create");
        commit(&engine, outcome.ops);
        // Part 1 is listed; part 2 is abandoned. Part 2's seg_no once equalled
        // its part_no (2), which is exactly a capture chunk seg_no → collision.
        // Its blobs use chunk ids well above the old object's range so the
        // reconciliation below stays unambiguous.
        for (no, etag_byte, blob) in [(1u32, 1u8, 100_000u64), (2, 2, 200_000)] {
            let outcome = apply_put_part(
                &engine,
                &EpochRefExtractor,
                bucket(),
                b"big",
                upload,
                no,
                &part(10, etag_byte, &[blob]),
                2,
                1_100,
            )
            .expect("put part");
            commit(&engine, outcome.ops);
        }

        let outcome = apply_complete(
            &engine,
            &EpochRefExtractor,
            bucket(),
            b"big",
            upload,
            &[PartRef {
                part_no: 1,
                etag: [1; 16],
            }],
            3,
            2_000,
        )
        .expect("complete");
        assert_eq!(outcome.response, MetaResponse::None);
        commit(&engine, outcome.ops);

        // Reconcile: gather every slice enqueued under seq 3 across all delq
        // segments and assert it equals old-object slices ∪ abandoned part 2.
        let mut enqueued: Vec<Slice> = Vec::new();
        let start = flat_key(MetaCf::Delq, bucket(), b"big", &suffix::delq_seq(3, 0));
        let end = flat_key(MetaCf::Delq, bucket(), b"big", &suffix::delq_seq(4, 0));
        for (_, raw) in engine
            .scan(MetaCf::Delq, &start, &end, 4096)
            .expect("scan delq")
        {
            let pending: PendingDelete = serde_json::from_slice(&raw).expect("decode");
            enqueued.extend(pending.slices);
        }
        let mut expected = old_slices;
        expected.push(slice(200_000, 200_000)); // abandoned part 2
        enqueued.sort_by_key(|s| s.chunk_id.get());
        expected.sort_by_key(|s| s.chunk_id.get());
        assert_eq!(
            enqueued, expected,
            "every old slice and the abandoned part must survive in delq — no collision leak"
        );
    }

    #[test]
    fn complete_validates_parts_and_etag() {
        let (_dir, engine) = engine();
        let outcome = apply_create(&engine, bucket(), b"o", 5, 1_000).expect("create");
        commit(&engine, outcome.ops);
        let outcome = apply_put_part(
            &engine,
            &EpochRefExtractor,
            bucket(),
            b"o",
            5,
            1,
            &part(1, 1, &[1]),
            1,
            1_100,
        )
        .expect("part");
        commit(&engine, outcome.ops);

        // Missing part 2.
        let outcome = apply_complete(
            &engine,
            &EpochRefExtractor,
            bucket(),
            b"o",
            5,
            &[PartRef {
                part_no: 2,
                etag: [2; 16],
            }],
            2,
            2_000,
        )
        .expect("complete");
        assert!(matches!(outcome.response, MetaResponse::Rejected(r) if r.contains("missing")));

        // Etag mismatch on part 1.
        let outcome = apply_complete(
            &engine,
            &EpochRefExtractor,
            bucket(),
            b"o",
            5,
            &[PartRef {
                part_no: 1,
                etag: [9; 16],
            }],
            2,
            2_000,
        )
        .expect("complete");
        assert!(matches!(outcome.response, MetaResponse::Rejected(r) if r.contains("etag")));
    }

    #[test]
    fn abort_tombstones_parts_and_is_idempotent() {
        let (_dir, engine) = engine();
        let outcome = apply_create(&engine, bucket(), b"a", 8, 1_000).expect("create");
        commit(&engine, outcome.ops);
        for no in 1..=2u32 {
            let outcome = apply_put_part(
                &engine,
                &EpochRefExtractor,
                bucket(),
                b"a",
                8,
                no,
                &part(1, no as u8, &[no as u64 * 10]),
                1,
                1_100,
            )
            .expect("part");
            commit(&engine, outcome.ops);
        }

        let outcome =
            apply_abort(&engine, &EpochRefExtractor, bucket(), b"a", 8, 2, 5_000).expect("abort");
        assert_eq!(outcome.response, MetaResponse::None);
        commit(&engine, outcome.ops);

        assert!(
            scan_session(&engine, bucket(), b"a", 8)
                .expect("scan")
                .is_empty()
        );
        // Parts are captured in scan (part-number) order under contiguous
        // seg_no starting at 0 — part 1 → seg 0, part 2 → seg 1.
        assert_eq!(delq_slices(&engine, b"a", 2, 0), vec![slice(10, 10)]);
        assert_eq!(delq_slices(&engine, b"a", 2, 1), vec![slice(20, 20)]);
        // The abort timestamp lands in delq (the TTL/deleter clock).
        let key = flat_key(MetaCf::Delq, bucket(), b"a", &suffix::delq_seq(2, 0));
        let pending: PendingDelete = serde_json::from_slice(
            &engine
                .get(MetaCf::Delq, &key)
                .expect("get")
                .expect("present"),
        )
        .expect("decode");
        assert_eq!(pending.enqueue_ts, 5_000);

        // Idempotent: second abort is a no-op.
        let outcome = apply_abort(&engine, &EpochRefExtractor, bucket(), b"a", 8, 3, 6_000)
            .expect("re-abort");
        assert_eq!(outcome.response, MetaResponse::None);
        assert!(outcome.ops.is_empty());
    }

    #[test]
    fn put_part_overwrite_captures_old_part_slices() {
        let (_dir, engine) = engine();
        let outcome = apply_create(&engine, bucket(), b"w", 9, 1_000).expect("create");
        commit(&engine, outcome.ops);
        for (blobs, seq) in [(&[50u64][..], 1u64), (&[60u64][..], 2u64)] {
            let outcome = apply_put_part(
                &engine,
                &EpochRefExtractor,
                bucket(),
                b"w",
                9,
                1,
                &part(1, 1, blobs),
                seq,
                1_100,
            )
            .expect("part");
            commit(&engine, outcome.ops);
        }
        // The first part version's blob 50 is captured by the overwrite.
        assert_eq!(delq_slices(&engine, b"w", 2, 0), vec![slice(50, 50)]);
    }

    #[test]
    fn upload_key_parse_round_trip() {
        let key = upload_key(bucket(), b"dir/f.bin", 0xAB, 7);
        assert_eq!(part_no_of(&key), Some(7));
        let (parsed_bucket, object_key, upload_id) = parse_upload_key(&key).expect("parse");
        assert_eq!(parsed_bucket, bucket());
        assert_eq!(object_key, b"dir/f.bin".to_vec());
        assert_eq!(upload_id, 0xAB);
        // A session key parses too (part_no = 0).
        let session = upload_key(bucket(), b"k", 3, 0);
        assert_eq!(part_no_of(&session), Some(0));
        assert!(parse_upload_key(&session).is_some());
    }
}
