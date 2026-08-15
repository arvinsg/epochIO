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

//! Compaction: reclaim tombstoned space by copying an extent's live blobs into
//! a fresh extent and atomically rebinding the shard to it (02 §1.6).
//!
//! Crash-safety model (single atomic commit point):
//!   1. mark the source `Full` (pause writes, 02 §1.6);
//!   2. create the destination extent and install its metadata *without*
//!      rebinding — so recovery treats it as index-authority, never rebuilding
//!      and mis-rebinding a half-filled file (see `service::recover_extent`);
//!   3. copy every live (non-tombstoned) blob into it via [`write_blob`]
//!      (append → fsync → index commit each), throttled on the Background QoS
//!      class;
//!   4. re-scan the source for tombstones that landed *during* the copy and
//!      carry them into the destination inside the swap batch —
//!      [`DiskIndex::swap_shard_binding_with`], the single commit point:
//!      rebind `shard → dest`, mark the source `Dropped`, and tombstone the
//!      carried blobs on the destination, atomically (see the INVARIANT
//!      below);
//!   5. retire the source (file → `.trash`, index entries removed).
//!
//! INVARIANT(design 02 §1.6): an acked delete must survive compaction. A
//! tombstone landing on the source after step 3's live-set snapshot would be
//! copied as live and then erased with the source — resurrecting the blob —
//! unless carried into the destination atomically with the rebind (step 4).
//! The engine additionally serializes deletes against this commit section per
//! shard (see `service::EngineInner`), so no tombstone can land between the
//! re-scan and the swap.
//!
//! A crash before step 4 leaves the binding on the intact source (the
//! destination is an index-authority orphan); a crash after leaves a `Dropped`
//! source that scrub reclaims (02 §1.9). Either way no committed blob is lost.

use std::collections::BTreeMap;
use std::io;

use epoch_proto::{BlobId, ExtentId};

use crate::disk::{Disk, DiskError};
use crate::error::StoreError;
use crate::extent::file::ExtentFile;
use crate::extent::state::ExtentStatus;
use crate::index::{BlobIndex, DiskIndex, ExtentMeta};
use crate::qos::RateLimiter;
use crate::write::write_blob;

/// Default compaction trigger: reclaim once tombstoned bytes reach 30% of the
/// extent (02 §1.6). Capacity-pressure escalation (>80% → 0.1, >90% → forced)
/// needs live disk-usage stats and lands with PD integration (M4).
pub const DEFAULT_COMPACT_RATIO_PERCENT: u8 = 30;

/// Result of a successful [`compact_extent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactOutcome {
    /// The freshly written extent the shard now binds to.
    pub new_extent: ExtentId,
    /// Number of live blobs carried over.
    pub live_blobs: usize,
    /// On-disk bytes reclaimed (old valid size minus new valid size).
    pub reclaimed_bytes: u64,
}

/// Whether an extent is worth compacting: it has tombstoned bytes and they are
/// at least `ratio_percent` of its size. Integer comparison (no float) keeps
/// the decision exact.
#[must_use]
pub fn should_compact(meta: &ExtentMeta, ratio_percent: u8) -> bool {
    meta.deleted_bytes > 0
        && meta
            .deleted_bytes
            .saturating_mul(100)
            .ge(&meta.size.saturating_mul(u64::from(ratio_percent)))
}

/// Compacts `extent_id`: copies its live blobs into a new extent stamped with
/// `new_create_ts`, rebinds the shard, and retires the source. See the module
/// docs for the crash-safety model.
///
/// # Errors
///
/// - [`StoreError::ExtentNotFound`] if the extent is not open;
/// - [`StoreError::NotWritable`] if it is not `Writable`/`Full`;
/// - [`StoreError::Index`]/[`StoreError::Extent`]/[`StoreError::Disk`] on a
///   storage failure.
pub(crate) fn compact_extent(
    disk: &Disk,
    index: &DiskIndex,
    extents: &mut BTreeMap<ExtentId, ExtentFile>,
    limiter: &mut RateLimiter,
    extent_id: ExtentId,
    new_create_ts: i64,
) -> Result<CompactOutcome, StoreError> {
    let src_meta = index
        .get_extent_meta(extent_id)?
        .ok_or(StoreError::ExtentNotFound(extent_id))?;
    match src_meta.status {
        ExtentStatus::Writable | ExtentStatus::Full => {}
        other => return Err(StoreError::NotWritable(other)),
    }
    let shard = extent_id.shard_id();

    let live: Vec<(BlobId, BlobIndex)> = index
        .list_blobs(extent_id)?
        .into_iter()
        .filter(|(_, idx)| !idx.is_tombstoned())
        .collect();

    // 1. Pause writes to the source (02 §1.6) — field-scoped status merge.
    index.set_extent_status(extent_id, ExtentStatus::Full)?;

    // 2. Create the destination and install its metadata WITHOUT rebinding, so
    //    recovery never rebuilds/rebinds this half-filled file.
    let new_id = ExtentId::new(shard, new_create_ts);
    let mut dest = ExtentFile::create(disk.extent_path(new_id), new_id)?;
    // Reserve the new extent's full capacity up front (02 §1.7); no-op off Linux.
    dest.preallocate(disk.superblock().extent_size)?;
    index.put_extent_meta(
        new_id,
        &ExtentMeta {
            shard_id: shard,
            status: ExtentStatus::Writable,
            size: dest.write_offset(),
            deleted_bytes: 0,
            create_ts: new_create_ts,
        },
    )?;

    // 3. Copy live blobs (Background-throttled).
    for (blob_id, src_idx) in &live {
        let body = {
            let src = extents
                .get(&extent_id)
                .ok_or(StoreError::ExtentNotFound(extent_id))?;
            let (_header, body) = src.read_record(src_idx.offset)?;
            body
        };
        write_blob(index, &mut dest, *blob_id, &body)?;
        limiter.throttle(body.len() as u64);
    }
    let new_size = dest.write_offset();

    // 4. Commit point: carry over tombstones that landed on the source since
    //    the step-3 snapshot, rebind shard → dest, and drop the source — one
    //    synced batch (see the module INVARIANT). The caller holds the shard's
    //    delete lock across this call, so the re-scan is race-free.
    let carried: Vec<BlobId> = index
        .list_blobs(extent_id)?
        .into_iter()
        .filter(|(_, idx)| idx.is_tombstoned())
        .map(|(blob, _)| blob)
        .collect();
    index.swap_shard_binding_with(shard, new_id, extent_id, &carried)?;
    extents.insert(new_id, dest);

    // 5. Retire the source (idempotent; scrub can finish it after a crash).
    reclaim_extent(disk, index, extents, extent_id)?;

    Ok(CompactOutcome {
        new_extent: new_id,
        live_blobs: live.len(),
        reclaimed_bytes: src_meta.size.saturating_sub(new_size),
    })
}

/// Retires an extent that no shard binds anymore: drops it from the open set,
/// moves its file to `.trash`, and removes its index entries. Idempotent — an
/// already-trashed file is tolerated — so it is safe to re-run after a crash
/// mid-compaction and is shared with scrub's dropped-extent reclamation.
pub(crate) fn reclaim_extent(
    disk: &Disk,
    index: &DiskIndex,
    extents: &mut BTreeMap<ExtentId, ExtentFile>,
    extent_id: ExtentId,
) -> Result<(), StoreError> {
    extents.remove(&extent_id);
    match disk.move_extent_to_trash(extent_id) {
        Ok(()) => {}
        Err(DiskError::Io(err)) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }
    index.remove_extent(extent_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::state::ExtentStatus;

    fn meta(size: u64, deleted: u64) -> ExtentMeta {
        ExtentMeta {
            shard_id: epoch_proto::ShardId::from_raw(1),
            status: ExtentStatus::Writable,
            size,
            deleted_bytes: deleted,
            create_ts: 0,
        }
    }

    #[test]
    fn should_compact_respects_ratio_threshold() {
        // 30% threshold.
        assert!(!should_compact(&meta(1000, 0), 30), "no tombstones");
        assert!(!should_compact(&meta(1000, 299), 30), "just under 30%");
        assert!(should_compact(&meta(1000, 300), 30), "exactly 30%");
        assert!(should_compact(&meta(1000, 900), 30), "well over");
    }

    #[test]
    fn should_compact_handles_empty_extent() {
        assert!(!should_compact(&meta(0, 0), 30));
    }
}
