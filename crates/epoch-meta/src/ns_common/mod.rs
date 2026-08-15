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

//! Shared namespace value types and pure helpers used by both the flat
//! ([`ns_flat`](crate::ns_flat)) and hierarchical ([`ns_hier`](crate::ns_hier))
//! namespaces: the head/segment content model (03 §4.2) and the delete-queue
//! chunking rule (03 §8). Neither type nor helper is namespace-specific — the
//! flat object head and the hier file record both embed the same
//! [`ContentHead`] and split it the same way, and both enqueue captured slices
//! into `delq` the same way.

use serde::{Deserialize, Serialize};

use epoch_proto::BucketId;

use crate::ref_extractor::{PendingDelete, Slice};
use crate::store::keys::MetaCf;
use crate::store::{MetaStore, MetaStoreError, StoreOp};

/// Slices embedded in the head before spilling to segments (03 §4.2: ≈2 GB
/// objects stay segment-free).
pub const HEAD_EMBEDDED_SLICES: usize = 64;

/// Slices per `meta_seg` entry (03 §4.2: ~4 KB values) and per `delq` entry
/// (03 §8: 大对象 slices 分 seg 段承载，单条 value 有界).
pub const SEGMENT_SLICES: usize = 1024;

/// The storage class of a stored object/file (03 §4.2; v1 keeps the EC
/// standard tier only, further tiers land with lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageClass {
    /// Erasure-coded standard tier.
    Standard,
}

/// The HTTP metadata an object carries, replayed verbatim on GET/HEAD.
///
/// Grouped rather than spread across the head records (§3.4) so both namespaces
/// embed one field and the "which headers do we persist" question has a single
/// answer. Empty/absent means unset — the gateway then omits the header rather
/// than sending an empty one, because `Content-Type: ` is not the same as no
/// Content-Type to a browser or SDK.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpMeta {
    /// `Content-Type` (absent ⇒ the client's own default applies).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// `Content-Encoding` (e.g. pre-compressed uploads).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
    /// `Cache-Control`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<String>,
    /// User metadata (`x-amz-meta-*`), lowercased name → value. Silently
    /// dropping these is worse than rejecting them: the client sees a success
    /// and the metadata is gone for good.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub user: std::collections::BTreeMap<String, String>,
}

impl HttpMeta {
    /// Whether nothing was recorded (the common case for internal writes).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.content_type.is_none()
            && self.content_encoding.is_none()
            && self.cache_control.is_none()
            && self.user.is_empty()
    }
}

/// The inline-or-reference content of a head record (03 §4.2). Shared by the
/// flat `ObjectHead` and the hier `FileRecord`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ContentHead {
    /// Small-object content stored in the metadata itself (≤ the inline
    /// threshold, 03 §4.3; guarded by `guard.rs`).
    Inline(Vec<u8>),
    /// The object's slice list — first [`HEAD_EMBEDDED_SLICES`] when spilled.
    Slices(Vec<Slice>),
}

impl ContentHead {
    /// The full slice list this content embeds (empty for inline content).
    #[must_use]
    pub fn into_slices(self) -> Vec<Slice> {
        match self {
            ContentHead::Inline(_) => Vec::new(),
            ContentHead::Slices(slices) => slices,
        }
    }

    /// The `(inline_bytes, inline_count)` this content contributes to the
    /// inline guard (03 §4.3): inline content counts once (even zero-length),
    /// slice content contributes nothing.
    #[must_use]
    pub fn inline_account(&self) -> (u64, u64) {
        match self {
            ContentHead::Inline(bytes) => (bytes.len() as u64, 1),
            ContentHead::Slices(_) => (0, 0),
        }
    }
}

/// Purges every record of one bucket from this partition's owned key space
/// (99-Q15 bucket deletion: 各 CF 按前缀 DeleteRange). Called by the MetaNode's
/// `DeleteRange` RPC once the bucket is tombstoned (DELETING) at PD.
///
/// Only metadata is removed here — the blob bodies this orphans are reclaimed
/// separately by GcRound (01 §6.3). The bucket's records are purged from every
/// bucket-scoped CF the partition owns (`meta`/`fs` primary index, `meta_seg`
/// overflow, `upload` in-flight multipart, `delq` pending deletes, `ttl` expiry
/// keys). `delq` is purged too: its slices are already scheduled for deletion
/// and must not survive the bucket.
///
/// The scan is paged and each page is committed as its own batch so a huge
/// bucket never builds an unbounded write batch.
///
/// # Errors
///
/// Returns [`MetaStoreError`] if a scan or batch apply fails.
pub fn purge_bucket_records(
    store: &dyn MetaStore,
    range: &crate::partition::PartitionRange,
    bucket: BucketId,
    apply_batch: &dyn Fn(&[StoreOp]) -> Result<(), MetaStoreError>,
) -> Result<u64, MetaStoreError> {
    // The bucket's key prefix in any CF: the leading type tag + the big-endian
    // bucket id (03 §2: `type | bucket_id | …`). Intersecting that prefix range
    // with the partition's owned range yields exactly the keys to purge — a
    // partition that owns a slice of the bucket purges only its own slice.
    let mut purged = 0u64;
    for kr in range.key_ranges() {
        let prefix = [kr.cf.tag()]
            .into_iter()
            .chain(bucket.get().to_be_bytes())
            .collect::<Vec<u8>>();
        let prefix_end =
            crate::store::keys::prefix_end(&prefix).unwrap_or_else(|| vec![kr.cf.tag() + 1]);
        let start = kr.start.clone().max(prefix.clone());
        let end = kr.end.clone().min(prefix_end);
        if start >= end {
            continue; // this partition owns nothing of this bucket in this CF
        }
        let mut cursor = start;
        loop {
            let page = store.scan(kr.cf, &cursor, &end, SCAN_PURGE_PAGE)?;
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
            let ops: Vec<StoreOp> = page
                .into_iter()
                .map(|(key, _)| StoreOp::delete(kr.cf, key))
                .collect();
            purged += ops.len() as u64;
            apply_batch(&ops)?;
        }
    }
    Ok(purged)
}

/// The purge page size — small enough that one batch stays cheap on the apply
/// path (the same bound the delete-queue sweep uses).
const SCAN_PURGE_PAGE: usize = 1024;

/// One overflow segment of a large object's slice list (CF `meta_seg` value,
/// 03 §4.2). Shared by both namespaces (the `g` CF holds either's overflow).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SliceSegment {
    /// Up to [`SEGMENT_SLICES`] slices, continuing the head's list.
    pub slices: Vec<Slice>,
}

/// The raft apply response carried back to `client_write` callers (the `R`
/// type of every partition group, shared by both namespaces). Namespace
/// handlers produce it at apply time; validation rejections are deterministic
/// — derived from committed state only, identical on every replica.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetaResponse {
    /// The entry produced no reply payload.
    None,
    /// The op was valid raft input but not applicable to current state (e.g.
    /// an unknown upload id, or a rejected cross-directory rename); carries
    /// the reason for the caller.
    Rejected(String),
    /// A hier `MkdirSentinel` minted this child directory inode (03 §6.2 step
    /// 1); the caller issues step 2 (`MkdirLink`) with it.
    MintedIno(u64),
}

/// The full outcome of applying one namespace op: the mutation batch (data
/// ops, delq capture, and segment cleanup — the state machine appends the
/// applied record and commits; 03 §8), the caller-facing response, and the
/// inline-guard accounting delta (03 §4.3 — signed, since overwrites and
/// deletes subtract the old record's bytes).
pub struct ApplyOutcome {
    /// The mutation batch.
    pub ops: Vec<StoreOp>,
    /// The caller-facing apply response.
    pub response: MetaResponse,
    /// The inline-guard accounting delta.
    pub delta: crate::guard::GuardDelta,
}

impl ApplyOutcome {
    /// An outcome with an explicit inline-guard delta.
    #[must_use]
    pub fn new(ops: Vec<StoreOp>, response: MetaResponse, delta: crate::guard::GuardDelta) -> Self {
        Self {
            ops,
            response,
            delta,
        }
    }

    /// An outcome that moves no inline counters (session ops, pure directory
    /// ops).
    #[must_use]
    pub fn no_delta(ops: Vec<StoreOp>, response: MetaResponse) -> Self {
        Self::new(ops, response, crate::guard::GuardDelta::default())
    }
}

/// Splits a full slice list into the head-embedded prefix and the overflow
/// segments (03 §4.2): the first [`HEAD_EMBEDDED_SLICES`] stay embedded, the
/// rest chunk into [`SEGMENT_SLICES`]-sized segments. A pure function, so every
/// replica derives byte-identical keys from the same op.
#[must_use]
pub fn split_slices(slices: &[Slice]) -> (Vec<Slice>, Vec<SliceSegment>) {
    if slices.len() <= HEAD_EMBEDDED_SLICES {
        return (slices.to_vec(), Vec::new());
    }
    let (embedded, rest) = slices.split_at(HEAD_EMBEDDED_SLICES);
    let segments = rest
        .chunks(SEGMENT_SLICES)
        .map(|chunk| SliceSegment {
            slices: chunk.to_vec(),
        })
        .collect();
    (embedded.to_vec(), segments)
}

/// Enqueues `slices` into `delq`, chunked into bounded values of
/// [`SEGMENT_SLICES`] each (03 §8: 大对象 slices 分 seg 段承载，单条 value 有界),
/// numbering entries from `first_seg_no`. `delq_key(seg_no)` builds the full
/// delq key for one segment — the caller supplies the namespace encoding
/// (`flat_key` / `hier_key`) with `seq` already closed over.
///
/// Returns the next free `seg_no`, so several enqueues can share one `seq`
/// without key collision (CompleteMultipartUpload captures the old object and
/// its abandoned parts under one `seq`). Empty `slices` is a no-op that returns
/// `first_seg_no` unchanged.
///
/// # Errors
///
/// Returns [`MetaStoreError::ValueCodec`] if a pending-delete value fails to
/// encode.
pub fn enqueue_delete_slices(
    first_seg_no: u32,
    slices: &[Slice],
    ts_millis: i64,
    mut delq_key: impl FnMut(u32) -> Vec<u8>,
    ops: &mut Vec<StoreOp>,
) -> Result<u32, MetaStoreError> {
    let mut seg_no = first_seg_no;
    for chunk in slices.chunks(SEGMENT_SLICES) {
        let pending = PendingDelete {
            slices: chunk.to_vec(),
            enqueue_ts: ts_millis,
        };
        let value =
            serde_json::to_vec(&pending).map_err(|e| MetaStoreError::ValueCodec(e.to_string()))?;
        ops.push(StoreOp::put(MetaCf::Delq, delq_key(seg_no), value));
        seg_no += 1;
    }
    Ok(seg_no)
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::{BlobId, ChunkId};

    fn slices(n: usize) -> Vec<Slice> {
        (0..n)
            .map(|i| Slice {
                chunk_id: ChunkId::new(i as u32),
                blob_ids: vec![BlobId::from_raw(i as u64)],
                blob_size: 32 << 20,
            })
            .collect()
    }

    #[test]
    fn split_slices_keeps_small_lists_embedded() {
        let (embedded, segments) = split_slices(&slices(HEAD_EMBEDDED_SLICES));
        assert_eq!(embedded.len(), HEAD_EMBEDDED_SLICES);
        assert!(segments.is_empty());
    }

    #[test]
    fn split_slices_chunks_overflow_into_bounded_segments() {
        let all = slices(HEAD_EMBEDDED_SLICES + SEGMENT_SLICES + 5);
        let (embedded, segments) = split_slices(&all);
        assert_eq!(embedded, all[..HEAD_EMBEDDED_SLICES]);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].slices.len(), SEGMENT_SLICES);
        assert_eq!(segments[1].slices.len(), 5);
        // The full list round-trips in order.
        let reassembled: Vec<Slice> = embedded
            .into_iter()
            .chain(segments.into_iter().flat_map(|s| s.slices))
            .collect();
        assert_eq!(reassembled, all);
    }

    fn flat_head(bucket: u64, key: &[u8]) -> StoreOp {
        StoreOp::put(
            MetaCf::Meta,
            crate::store::keys::flat_key(MetaCf::Meta, BucketId::new(bucket), key, &[]),
            b"head".to_vec(),
        )
    }

    #[test]
    fn purge_bucket_records_removes_only_the_target_bucket() {
        let engine = crate::store::mem::MemEngine::new();
        // Two buckets in one flat partition; bucket 2 is the deletion target.
        engine
            .apply(&[
                flat_head(1, b"keep/a"),
                flat_head(1, b"keep/b"),
                flat_head(2, b"drop/x"),
                flat_head(2, b"drop/y"),
                flat_head(2, b"drop/z"),
            ])
            .expect("seed");
        let range = crate::partition::PartitionRange::full(crate::partition::Namespace::Flat);

        let purged =
            purge_bucket_records(&engine, &range, BucketId::new(2), &|ops| engine.apply(ops))
                .expect("purge");
        assert_eq!(purged, 3, "exactly the target bucket's records are purged");

        // Bucket 1 is untouched.
        for key in [b"keep/a".as_slice(), b"keep/b"] {
            let k = crate::store::keys::flat_key(MetaCf::Meta, BucketId::new(1), key, &[]);
            assert!(engine.get(MetaCf::Meta, &k).expect("get").is_some());
        }
        // Bucket 2 is gone.
        for key in [b"drop/x".as_slice(), b"drop/y", b"drop/z"] {
            let k = crate::store::keys::flat_key(MetaCf::Meta, BucketId::new(2), key, &[]);
            assert!(engine.get(MetaCf::Meta, &k).expect("get").is_none());
        }

        // Idempotent: a second purge finds nothing left.
        let again =
            purge_bucket_records(&engine, &range, BucketId::new(2), &|ops| engine.apply(ops))
                .expect("re-purge");
        assert_eq!(again, 0);
    }

    #[test]
    fn purge_bucket_records_respects_the_partition_range() {
        let engine = crate::store::mem::MemEngine::new();
        // A partition that owns only a slice of the namespace purges only its
        // own slice of the bucket — records outside its range belong to another
        // partition and must not be touched.
        engine
            .apply(&[flat_head(5, b"a"), flat_head(5, b"m"), flat_head(5, b"z")])
            .expect("seed");
        // Partition owning [bucket 5 key "m", +∞).
        let range = crate::partition::PartitionRange {
            ns: crate::partition::Namespace::Flat,
            start: Some((BucketId::new(5), b"m".to_vec())),
            end: None,
        };
        let purged =
            purge_bucket_records(&engine, &range, BucketId::new(5), &|ops| engine.apply(ops))
                .expect("purge");
        assert_eq!(purged, 2, "only the in-range records (m, z) purge");
        // "a" is before the partition's start → survives.
        let k = crate::store::keys::flat_key(MetaCf::Meta, BucketId::new(5), b"a", &[]);
        assert!(engine.get(MetaCf::Meta, &k).expect("get").is_some());
    }

    #[test]
    fn enqueue_numbers_segments_from_first_and_returns_next() {
        let mut ops = Vec::new();
        let next = enqueue_delete_slices(
            3,
            &slices(2 * SEGMENT_SLICES + 1),
            1_000,
            |seg| vec![seg as u8],
            &mut ops,
        )
        .expect("enqueue");
        // 2049 slices → 3 chunks, numbered 3,4,5 → next is 6.
        assert_eq!(next, 6);
        assert_eq!(ops.len(), 3);

        // Empty input is a no-op returning the start unchanged.
        let mut ops = Vec::new();
        assert_eq!(
            enqueue_delete_slices(7, &[], 1_000, |seg| vec![seg as u8], &mut ops).expect("empty"),
            7
        );
        assert!(ops.is_empty());
    }
}
