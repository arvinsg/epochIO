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

//! GcRound reference export (01 §6.3 / Q20 / Q27): a MetaNode partition leader
//! scans every column family that can hold a live `SliceRef` — the flat heads
//! (`meta`) or hier file records (`fs`), plus slice overflow segments
//! (`meta_seg`) and in-flight multipart parts (`upload`) — and returns the set
//! of *referenced* blob ids: the "keep set" a DataNode diffs its on-disk blobs
//! against to find orphans.
//!
//! This is the metadata half of the token-watermark GC (Q27): a blob `(t, s)` on
//! a DataNode is reclaimable iff it is absent from this reference set AND either
//! its token is dead (+grace) or `s ≤ W(t)`. The export is leader-only and read
//! only — it mutates nothing (the reclaim tombstone happens on the DataNode).
//!
//! Reuses [`RefExtractor`] (the same per-value slice decoder the deleter uses,
//! 03 §5) and the partition's [`primary scan bounds`](PartitionRange::key_ranges)
//! so a split's narrowed range exports exactly the keys it still owns. v1
//! collects the id set in memory — partitions split before a range outgrows RAM
//! (03 §2), the same bound the snapshot export relies on.

use std::collections::BTreeSet;

use epoch_proto::BlobId;

use crate::partition::PartitionRange;
use crate::ref_extractor::RefExtractor;
use crate::store::{MetaCf, MetaStore, MetaStoreError};

/// Page size for the primary-index scan (rows per `scan` call).
const SCAN_PAGE: usize = 4096;

/// Exports the set of blob ids referenced by every live object in `range`,
/// scanning every ref-bearing column family (namespace primary index +
/// `meta_seg` + `upload`), in ascending id order.
///
/// The returned set is the GC keep-set for the partition: any DataNode blob not
/// in it (and past its token watermark) is an orphan. Scans in bounded pages so
/// a large partition never loads its whole key space at once.
///
/// # Errors
///
/// Returns [`MetaStoreError`] if a scan fails.
pub fn export_references(
    store: &dyn MetaStore,
    extractor: &dyn RefExtractor,
    range: &PartitionRange,
) -> Result<BTreeSet<u64>, MetaStoreError> {
    let mut refs: BTreeSet<u64> = BTreeSet::new();
    // INVARIANT(design 01 §6.3 / Q27): the keep-set must cover EVERY column
    // family that can hold a live `SliceRef` — omitting one makes its blobs look
    // unreferenced, and GC tombstones live data. So this is an *exclusion* list,
    // not an allowlist: a newly added ref-bearing CF is included automatically,
    // and a CF that holds no slices costs only a scan whose `extract` yields
    // nothing. `Delq` is excluded deliberately (its slices are already scheduled
    // for deletion — keeping them alive would strand every orphan forever) and
    // `Ttl` holds expiry keys with no slices.
    //
    // Which CFs are the primary index depends on the namespace, and
    // `key_ranges` already encodes that: `meta` for flat, `fs` for hier, plus
    // `meta_seg` (slice overflow) and `upload` (multipart parts in flight) for
    // both. It also yields exactly the sub-ranges this partition owns, so a
    // split's narrowed range exports only its own keys.
    for kr in range
        .key_ranges()
        .into_iter()
        .filter(|r| !matches!(r.cf, MetaCf::Delq | MetaCf::Ttl))
    {
        let mut cursor = kr.start.clone();
        loop {
            let page = store.scan(kr.cf, &cursor, &kr.end, SCAN_PAGE)?;
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
                .unwrap_or_else(|| kr.end.clone());
            for (_key, value) in page {
                for slice in extractor.extract(kr.cf, &value) {
                    for blob_id in slice.blob_ids {
                        refs.insert(blob_id.as_u64());
                    }
                }
            }
        }
    }
    Ok(refs)
}

/// Whether `blob` is referenced by the keep-set (a convenience for the diff).
#[must_use]
pub fn is_referenced(refs: &BTreeSet<u64>, blob: BlobId) -> bool {
    refs.contains(&blob.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use epoch_proto::{BucketId, ChunkId, WriterToken};

    use crate::ns_common::{ContentHead, HttpMeta, StorageClass};
    use crate::ns_flat::FlatOp;
    use crate::ns_flat::ObjectHead;
    use crate::ref_extractor::{EpochRefExtractor, Slice};
    use crate::store::rocks::RocksEngine;

    fn slice(chunk: u32, blob: BlobId) -> Slice {
        Slice {
            chunk_id: ChunkId::new(chunk),
            blob_ids: vec![blob],
            blob_size: 50,
        }
    }

    #[test]
    fn export_collects_referenced_blob_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(RocksEngine::open(dir.path()).expect("open"));
        let range = PartitionRange::full(crate::partition::Namespace::Flat);

        // Put an object whose head references two slice blobs.
        let b0 = BlobId::new(WriterToken::new(1), 10);
        let b1 = BlobId::new(WriterToken::new(1), 11);
        let head = ObjectHead {
            size: 100,
            etag: [0u8; 16],
            mtime: 1,
            storage: StorageClass::Standard,
            content: ContentHead::Slices(vec![slice(1, b0), slice(2, b1)]),
            seg_count: 0,
            http: HttpMeta::default(),
        };
        let op = FlatOp::Put {
            bucket: BucketId::new(1),
            key: b"k".to_vec(),
            head,
            ts_millis: 1,
        };
        let outcome =
            crate::ns_flat::apply(engine.as_ref(), &EpochRefExtractor, &op, 1).expect("apply");
        engine.apply(&outcome.ops).expect("commit");

        let refs = export_references(engine.as_ref(), &EpochRefExtractor, &range).expect("export");
        assert!(is_referenced(&refs, b0), "blob 0 is referenced");
        assert!(is_referenced(&refs, b1), "blob 1 is referenced");
        assert!(
            !is_referenced(&refs, BlobId::new(WriterToken::new(1), 99)),
            "an unwritten blob is not referenced (would be an orphan)"
        );
    }

    /// INVARIANT(design 01 §6.3 / Q27): a hier file's blobs must be in the
    /// keep-set. The hier primary index is the `fs` CF, not `meta` — scanning
    /// only `meta`/`meta_seg` yields an empty keep-set for a hier partition, so
    /// GC would reclaim every live hier file that has no overflow segment.
    #[test]
    fn export_covers_hier_file_records() {
        use crate::ns_hier::{HierOp, InoAllocator};

        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(RocksEngine::open(dir.path()).expect("open"));
        let range = PartitionRange::full(crate::partition::Namespace::Hier);
        let bucket = BucketId::new(epoch_proto::consts::HIER_BUCKET_BIT | 7);
        let mut ino = InoAllocator::new(0, epoch_proto::consts::ROOT_INO + 1);

        // A hier file whose body is EC (slices embedded in the head, no segment).
        let b0 = BlobId::new(WriterToken::new(2), 20);
        let op = HierOp::Write {
            bucket,
            parent_ino: epoch_proto::consts::ROOT_INO,
            name: b"f".to_vec(),
            size: 50,
            etag: [1u8; 16],
            content: ContentHead::Slices(vec![slice(3, b0)]),
            http: Default::default(),
            ts_millis: 1_700_000_000_000,
        };
        let outcome = crate::ns_hier::apply(engine.as_ref(), &EpochRefExtractor, &op, 1, &mut ino)
            .expect("apply");
        engine.apply(&outcome.ops).expect("commit");

        let refs = export_references(engine.as_ref(), &EpochRefExtractor, &range).expect("export");
        assert!(
            is_referenced(&refs, b0),
            "a live hier file's blob must be in the keep-set, else GC deletes it"
        );
    }

    /// INVARIANT(design 01 §6.3 / Q27): an in-flight multipart part's blobs live
    /// only in the `upload` CF until Complete repacks them. The gateway resolves
    /// each part off the in-flight set right after PutPart, so `W(t)` advances
    /// past those seqs — omitting `upload` from the keep-set makes them orphans
    /// and GC deletes completed parts before the upload finishes.
    #[test]
    fn export_covers_in_flight_multipart_parts() {
        use crate::ns_flat::multipart::PartMeta;

        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(RocksEngine::open(dir.path()).expect("open"));
        let range = PartitionRange::full(crate::partition::Namespace::Flat);
        let bucket = BucketId::new(1);

        let create = FlatOp::CreateMultipart {
            bucket,
            key: b"big".to_vec(),
            upload_id: 7,
            ts_millis: 1_000,
        };
        let outcome =
            crate::ns_flat::apply(engine.as_ref(), &EpochRefExtractor, &create, 1).expect("create");
        engine.apply(&outcome.ops).expect("commit");

        let b0 = BlobId::new(WriterToken::new(3), 30);
        let put_part = FlatOp::PutPart {
            bucket,
            key: b"big".to_vec(),
            upload_id: 7,
            part_no: 1,
            part: PartMeta {
                size: 64,
                etag: [2u8; 16],
                slices: vec![slice(4, b0)],
            },
            ts_millis: 2_000,
        };
        let outcome = crate::ns_flat::apply(engine.as_ref(), &EpochRefExtractor, &put_part, 2)
            .expect("put_part");
        engine.apply(&outcome.ops).expect("commit");

        let refs = export_references(engine.as_ref(), &EpochRefExtractor, &range).expect("export");
        assert!(
            is_referenced(&refs, b0),
            "an uploaded part's blob must be in the keep-set before Complete"
        );
    }

    /// The delete queue is deliberately NOT in the keep-set: its slices are
    /// already scheduled for reclaim, so keeping them referenced would strand
    /// every orphan forever.
    #[test]
    fn export_excludes_the_delete_queue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(RocksEngine::open(dir.path()).expect("open"));
        let range = PartitionRange::full(crate::partition::Namespace::Flat);
        let bucket = BucketId::new(1);

        let old = BlobId::new(WriterToken::new(4), 40);
        let new = BlobId::new(WriterToken::new(4), 41);
        let head = |blob| ObjectHead {
            size: 100,
            etag: [0u8; 16],
            mtime: 1,
            storage: StorageClass::Standard,
            content: ContentHead::Slices(vec![slice(5, blob)]),
            seg_count: 0,
            http: HttpMeta::default(),
        };
        for (seq, blob) in [(1, old), (2, new)] {
            let op = FlatOp::Put {
                bucket,
                key: b"k".to_vec(),
                head: head(blob),
                ts_millis: seq as i64,
            };
            let outcome = crate::ns_flat::apply(engine.as_ref(), &EpochRefExtractor, &op, seq)
                .expect("apply");
            engine.apply(&outcome.ops).expect("commit");
        }

        let refs = export_references(engine.as_ref(), &EpochRefExtractor, &range).expect("export");
        assert!(is_referenced(&refs, new), "the live head's blob is kept");
        assert!(
            !is_referenced(&refs, old),
            "the overwritten blob sits in delq and must NOT be kept alive"
        );
    }
}
