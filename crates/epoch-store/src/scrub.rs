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

//! Local scrub (02 §1.9): a low-rate integrity pass over an extent's live
//! records, plus reclamation of dropped (orphaned) extents.
//!
//! [`scrub_extent`] re-reads every live blob record, relying on
//! [`ExtentFile::read_record`]'s built-in header/body CRC32C checks to surface
//! bitrot, throttled on the Background QoS class like compaction (02 §1.7);
//! corrupt blobs are collected into a [`ScrubReport`] (reporting to PD →
//! ShardRepair arrives with the control plane, M4/M7). [`reclaim_dropped`]
//! finishes retiring extents left `Dropped` by compaction — moving their files
//! to `.trash` and clearing their index entries. [`reclaim_orphans`] retires
//! extents no shard binds to (e.g. a compaction destination orphaned by a crash
//! between copy and rebind, 02 §1.6).
//!
//! epoch/PD-map reconciliation (forced release of epoch-stale extents, 02 §1.9)
//! needs the PD mapping and lands with control-plane integration (M4+).
//!
//! Design: docs/design/02-datanode.md §1.9

use std::collections::BTreeMap;

use epoch_proto::{BlobId, ExtentId};

use crate::compact::reclaim_extent;
use crate::disk::Disk;
use crate::error::StoreError;
use crate::extent::file::{ExtentError, ExtentFile};
use crate::extent::state::ExtentStatus;
use crate::index::DiskIndex;
use crate::qos::RateLimiter;

/// Outcome of scrubbing one extent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScrubReport {
    /// The extent scrubbed.
    pub extent: ExtentId,
    /// Number of live (non-tombstoned) blobs read and checked.
    pub scanned: usize,
    /// Blobs whose record failed integrity checks (header or body CRC32C).
    pub corrupt: Vec<BlobId>,
}

impl ScrubReport {
    /// Whether every scanned blob passed its integrity checks.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.corrupt.is_empty()
    }
}

/// Scrubs one extent: re-reads every live blob record and records those that
/// fail their integrity checks. Tombstoned blobs are skipped (they await
/// compaction, 02 §1.6).
///
/// # Errors
///
/// [`StoreError::Index`] on an index failure, or [`StoreError::Extent`] on an
/// underlying I/O error (a *checksum* failure is recorded, not returned).
pub fn scrub_extent(
    index: &DiskIndex,
    extent: &ExtentFile,
    limiter: &mut RateLimiter,
) -> Result<ScrubReport, StoreError> {
    let extent_id = extent.extent_id();
    let mut scanned = 0;
    let mut corrupt = Vec::new();
    for (blob_id, idx) in index.list_blobs(extent_id)? {
        if idx.is_tombstoned() {
            continue;
        }
        scanned += 1;
        limiter.throttle(u64::from(idx.size));
        match extent.read_record(idx.offset) {
            Ok(_) => {}
            // Header or body corruption → record the blob and keep scrubbing.
            Err(ExtentError::BodyChecksum { .. } | ExtentError::Record { .. }) => {
                corrupt.push(blob_id);
            }
            // A genuine I/O failure aborts the pass.
            Err(other) => return Err(other.into()),
        }
    }
    Ok(ScrubReport {
        extent: extent_id,
        scanned,
        corrupt,
    })
}

/// Reclaims every extent left `Dropped` (e.g. a compaction source, or one whose
/// retirement was interrupted by a crash): retires each via
/// [`reclaim_extent`], which is idempotent. Returns the reclaimed extent ids.
///
/// # Errors
///
/// [`StoreError::Index`] / [`StoreError::Disk`] on a storage failure.
pub fn reclaim_dropped(
    disk: &Disk,
    index: &DiskIndex,
    extents: &mut BTreeMap<ExtentId, ExtentFile>,
) -> Result<Vec<ExtentId>, StoreError> {
    let dropped: Vec<ExtentId> = index
        .list_extents()?
        .into_iter()
        .filter(|(_, meta)| meta.status == ExtentStatus::Dropped)
        .map(|(id, _)| id)
        .collect();
    for id in &dropped {
        reclaim_extent(disk, index, extents, *id)?;
    }
    Ok(dropped)
}

/// Reclaims every extent no shard binds to while it is not `Dropped` — the
/// orphan a compaction crash leaves behind when the destination was created and
/// filled but the shard was never rebound (compact.rs step 2–4, 02 §1.6). Such
/// an extent is intact and `Writable` yet unreachable: PD never named it, and
/// `reclaim_dropped` skips it (it is not `Dropped`). Idempotent; safe to run at
/// startup and periodically.
///
/// An extent is kept iff the binding for its meta's shard points back at it —
/// compaction sources (bound until the atomic swap) and destinations (bound
/// from the swap on) are both safe.
///
/// # Errors
///
/// [`StoreError::Index`] / [`StoreError::Disk`] on a storage failure.
pub fn reclaim_orphans(
    disk: &Disk,
    index: &DiskIndex,
    extents: &mut BTreeMap<ExtentId, ExtentFile>,
) -> Result<Vec<ExtentId>, StoreError> {
    let mut orphans = Vec::new();
    for (id, meta) in index.list_extents()? {
        if meta.status == ExtentStatus::Dropped {
            continue;
        }
        if index.get_shard_binding(meta.shard_id)? != Some(id) {
            orphans.push(id);
        }
    }
    for id in &orphans {
        reclaim_extent(disk, index, extents, *id)?;
    }
    Ok(orphans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{blob, writable_extent};
    use crate::write::write_blob;

    #[test]
    fn scrub_of_healthy_extent_is_clean() {
        let mut fx = writable_extent();
        for seq in 0..5u32 {
            write_blob(&fx.index, &mut fx.extent, blob(seq), &[seq as u8; 500]).expect("write");
        }
        let mut limiter = RateLimiter::new(0, 0);
        let report = scrub_extent(&fx.index, &fx.extent, &mut limiter).expect("scrub");
        assert_eq!(report.scanned, 5);
        assert!(report.is_clean());
    }

    #[test]
    fn scrub_skips_tombstoned_blobs() {
        let mut fx = writable_extent();
        write_blob(&fx.index, &mut fx.extent, blob(0), b"live").expect("write");
        write_blob(&fx.index, &mut fx.extent, blob(1), b"dead").expect("write");
        assert!(
            fx.index
                .tombstone_blob(fx.extent_id, blob(1))
                .expect("tombstone")
        );

        let mut limiter = RateLimiter::new(0, 0);
        let report = scrub_extent(&fx.index, &fx.extent, &mut limiter).expect("scrub");
        assert_eq!(report.scanned, 1, "tombstoned blob is not scanned");
        assert!(report.is_clean());
    }

    #[test]
    fn reclaim_orphans_retires_unbound_but_keeps_bound_extents() {
        use crate::disk::Disk;
        use crate::index::ExtentMeta;
        use crate::superblock::Superblock;
        use crate::testutil::CLUSTER;
        use epoch_proto::consts::DEFAULT_EXTENT_SIZE;
        use epoch_proto::{ChunkId, DiskId, ShardId};

        let dir = tempfile::tempdir().expect("tempdir");
        let disk = Disk::format(
            dir.path(),
            Superblock {
                disk_id: DiskId::new(1),
                cluster_id: CLUSTER,
                created_at: 0,
                flags: 0,
                extent_size: DEFAULT_EXTENT_SIZE,
            },
        )
        .expect("format");
        let index = DiskIndex::open(&disk.index_dir()).expect("index");

        let shard = ShardId::new(ChunkId::new(1), 0, 0);
        let bound = ExtentId::new(shard, 1);
        let orphan = ExtentId::new(shard, 2);
        let meta = ExtentMeta {
            shard_id: shard,
            status: ExtentStatus::Writable,
            size: 4096,
            deleted_bytes: 0,
            create_ts: 0,
        };
        let mut extents = BTreeMap::new();
        extents.insert(
            bound,
            ExtentFile::create(disk.extent_path(bound), bound).expect("create bound"),
        );
        extents.insert(
            orphan,
            ExtentFile::create(disk.extent_path(orphan), orphan).expect("create orphan"),
        );
        index.put_extent_meta(bound, &meta).expect("meta bound");
        index.put_extent_meta(orphan, &meta).expect("meta orphan");
        // Only `bound` has a shard binding (the orphan simulates a compaction
        // destination abandoned before the atomic rebind).
        index.set_shard_binding(shard, bound).expect("binding");

        let reclaimed = reclaim_orphans(&disk, &index, &mut extents).expect("reclaim");
        assert_eq!(reclaimed, vec![orphan]);
        assert!(!disk.extent_path(orphan).exists(), "orphan file retired");
        assert_eq!(index.get_extent_meta(orphan).expect("meta"), None);
        // The bound extent is untouched, and a second run is a no-op.
        assert!(extents.contains_key(&bound));
        assert!(index.get_extent_meta(bound).expect("meta").is_some());
        assert!(
            reclaim_orphans(&disk, &index, &mut extents)
                .expect("reclaim")
                .is_empty()
        );
    }

    #[test]
    fn reclaim_dropped_retires_only_dropped_extents() {
        use crate::disk::Disk;
        use crate::index::ExtentMeta;
        use crate::superblock::Superblock;
        use crate::testutil::CLUSTER;
        use epoch_proto::consts::DEFAULT_EXTENT_SIZE;
        use epoch_proto::{ChunkId, DiskId, ShardId};

        let dir = tempfile::tempdir().expect("tempdir");
        let disk = Disk::format(
            dir.path(),
            Superblock {
                disk_id: DiskId::new(1),
                cluster_id: CLUSTER,
                created_at: 0,
                flags: 0,
                extent_size: DEFAULT_EXTENT_SIZE,
            },
        )
        .expect("format");
        let index = DiskIndex::open(&disk.index_dir()).expect("index");

        let shard = ShardId::new(ChunkId::new(1), 0, 0);
        let dropped = ExtentId::new(shard, 1);
        let live = ExtentId::new(shard, 2);
        let meta = |status| ExtentMeta {
            shard_id: shard,
            status,
            size: 4096,
            deleted_bytes: 0,
            create_ts: 0,
        };
        let mut extents = BTreeMap::new();
        extents.insert(
            dropped,
            ExtentFile::create(disk.extent_path(dropped), dropped).expect("create dropped"),
        );
        extents.insert(
            live,
            ExtentFile::create(disk.extent_path(live), live).expect("create live"),
        );
        index
            .put_extent_meta(dropped, &meta(ExtentStatus::Dropped))
            .expect("meta");
        index
            .put_extent_meta(live, &meta(ExtentStatus::Writable))
            .expect("meta");

        let reclaimed = reclaim_dropped(&disk, &index, &mut extents).expect("reclaim");
        assert_eq!(reclaimed, vec![dropped]);

        // The dropped extent is gone from the open set, its index, and extents/.
        assert!(!extents.contains_key(&dropped));
        assert_eq!(index.get_extent_meta(dropped).expect("meta"), None);
        assert!(!disk.extent_path(dropped).exists());
        // The live extent is untouched.
        assert!(extents.contains_key(&live));
        assert!(index.get_extent_meta(live).expect("meta").is_some());
    }
}
