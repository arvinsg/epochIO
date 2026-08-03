//! Shared namespace value types and pure helpers used by both the flat
//! ([`ns_flat`](crate::ns_flat)) and hierarchical ([`ns_hier`](crate::ns_hier))
//! namespaces: the head/segment content model (03 §4.2) and the delete-queue
//! chunking rule (03 §8). Neither type nor helper is namespace-specific — the
//! flat object head and the hier file record both embed the same
//! [`ContentHead`] and split it the same way, and both enqueue captured slices
//! into `delq` the same way.
//!
//! Design: docs/design/03-metanode.md §4.2 (head+segment), §8 (delq value 有界)

use serde::{Deserialize, Serialize};

use crate::ref_extractor::{PendingDelete, Slice};
use crate::store::keys::MetaCf;
use crate::store::{MetaStoreError, StoreOp};

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
