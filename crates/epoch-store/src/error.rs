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

//! The epoch-store crate error type.
//!
//! Aggregates the lower-layer failures (index, extent file) and the
//! write-path rejection reasons (sealed / full / not writable) so the local
//! blob API and the future RPC service share one error surface. Mapping to the
//! cross-component `epoch_proto::EpochError` happens at the RPC boundary (M3).

use std::io;

use epoch_proto::{BlobId, EpochError, ExtentId, ShardId};

use crate::disk::DiskError;
use crate::extent::file::ExtentError;
use crate::extent::state::ExtentStatus;
use crate::index::IndexError;

/// A blob write/read failure.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A per-disk index failure.
    #[error("index: {0}")]
    Index(#[from] IndexError),
    /// An extent-file failure.
    #[error("extent: {0}")]
    Extent(#[from] ExtentError),
    /// A disk-lifecycle failure (load / directory layout).
    #[error("disk: {0}")]
    Disk(#[from] DiskError),
    /// The target extent has no metadata (not created/registered).
    #[error("extent {0:?} is not registered")]
    ExtentNotFound(ExtentId),
    /// The shard slot has no extent bound to it yet (no extent created).
    #[error("shard {0:?} has no bound extent")]
    ShardUnbound(ShardId),
    /// The extent is sealed and rejects writes (maps to `EpochError::Sealed`).
    #[error("extent is sealed")]
    Sealed,
    /// The extent is full and rejects writes (maps to `EpochError::ChunkFull`).
    #[error("extent is full")]
    Full,
    /// The extent is in a non-writable status other than sealed/full.
    #[error("extent is not writable (status {0:?})")]
    NotWritable(ExtentStatus),
    /// The blob body exceeds the record size field.
    #[error("blob body too large: {0} bytes")]
    BodyTooLarge(usize),
    /// The index pointed at a record that describes a different blob (index/file desync).
    #[error("index/record mismatch: requested {requested:?}, record holds {found:?}")]
    BlobMismatch {
        /// Blob id the caller asked for.
        requested: BlobId,
        /// Blob id found in the record the index pointed at.
        found: BlobId,
    },
}

impl StoreError {
    /// Maps this local error to the cross-component [`EpochError`] returned over
    /// the data-plane RPC (02 §2.1/§5). Rejections carry their meaning across
    /// the wire (they drive gateway cache invalidation); genuine faults collapse
    /// to [`EpochError::Internal`] / [`EpochError::DiskBroken`], whose full
    /// detail this method logs since those wire errors carry no payload (§9.3).
    pub(crate) fn to_epoch(&self) -> EpochError {
        let epoch = self.classify();
        if matches!(epoch, EpochError::Internal | EpochError::DiskBroken) {
            tracing::warn!(error = %self, mapped = %epoch, "store error surfaced to data plane");
        }
        epoch
    }

    /// The pure error-code mapping (no logging), separated so the mapping table
    /// itself is unit-testable.
    fn classify(&self) -> EpochError {
        match self {
            // Write-path rejections keep their cross-component meaning.
            StoreError::Sealed => EpochError::Sealed,
            StoreError::Full => EpochError::ChunkFull,
            StoreError::NotWritable(status) => match status {
                ExtentStatus::Rebuilding => EpochError::Rebuilding,
                ExtentStatus::Dropped => EpochError::ShardNotFound,
                // Writable/Full never reach here (they are not "not writable").
                ExtentStatus::Writable | ExtentStatus::Full | ExtentStatus::Sealed => {
                    EpochError::Sealed
                }
            },
            StoreError::ExtentNotFound(_) => EpochError::ShardNotFound,
            StoreError::ShardUnbound(_) => EpochError::ShardNotFound,
            // A full disk is healthy hardware: it must map to OutOfSpace, never
            // DiskBroken (02 §1.7). Conflating them makes the cluster answer
            // exhaustion by starting repairs that need the capacity that ran out.
            StoreError::Disk(DiskError::Io(err)) if is_out_of_space(err) => EpochError::OutOfSpace,
            StoreError::Extent(ExtentError::Io(err)) if is_out_of_space(err) => {
                EpochError::OutOfSpace
            }
            // Raw file / disk-lifecycle I/O failure is a disk-broken signal so the
            // gateway evicts the chunk and retries elsewhere (02 §2.1).
            StoreError::Disk(_) => EpochError::DiskBroken,
            StoreError::Extent(ExtentError::Io(_)) => EpochError::DiskBroken,
            // Local corruption / index-layer / oversize / desync: no distinct
            // cross-component meaning.
            StoreError::Extent(_)
            | StoreError::Index(_)
            | StoreError::BodyTooLarge(_)
            | StoreError::BlobMismatch { .. } => EpochError::Internal,
        }
    }
}

/// Whether an I/O error means "the filesystem has no room left".
///
/// `io::ErrorKind::StorageFull` is the portable form; the raw `ENOSPC` check is
/// the fallback for platforms/versions where the kind is not surfaced (and for
/// `EDQUOT`, a quota exhaustion that is equally not a hardware fault).
fn is_out_of_space(err: &io::Error) -> bool {
    if err.kind() == io::ErrorKind::StorageFull {
        return true;
    }
    // 28 = ENOSPC, 69 = EDQUOT on Linux (122 on macOS/BSD).
    matches!(err.raw_os_error(), Some(28) | Some(69) | Some(122))
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::BlobId;
    use std::io;

    #[test]
    fn rejections_map_to_their_wire_meaning() {
        assert_eq!(StoreError::Sealed.classify(), EpochError::Sealed);
        assert_eq!(StoreError::Full.classify(), EpochError::ChunkFull);
        assert_eq!(
            StoreError::NotWritable(ExtentStatus::Rebuilding).classify(),
            EpochError::Rebuilding
        );
        assert_eq!(
            StoreError::NotWritable(ExtentStatus::Dropped).classify(),
            EpochError::ShardNotFound
        );
        assert_eq!(
            StoreError::ExtentNotFound(ExtentId::from_bytes([0u8; 16])).classify(),
            EpochError::ShardNotFound
        );
    }

    #[test]
    fn io_faults_map_to_disk_broken() {
        assert_eq!(
            StoreError::Extent(ExtentError::Io(io::Error::from(
                io::ErrorKind::PermissionDenied
            )))
            .classify(),
            EpochError::DiskBroken
        );
    }

    /// INVARIANT(design 02 §1.7): a full disk is not a broken disk.
    ///
    /// If ENOSPC mapped to `DiskBroken`, PD would mark the disk failed and start a
    /// RepairDisk job — spending capacity to react to running out of capacity. A
    /// filling cluster would see every disk "fail" at once, with repair traffic
    /// compounding the exhaustion.
    #[test]
    fn out_of_space_is_not_disk_broken() {
        assert_eq!(
            StoreError::Extent(ExtentError::Io(io::Error::from(io::ErrorKind::StorageFull)))
                .classify(),
            EpochError::OutOfSpace
        );
        // The raw errno path (platforms that do not surface the kind).
        assert_eq!(
            StoreError::Extent(ExtentError::Io(io::Error::from_raw_os_error(28))).classify(),
            EpochError::OutOfSpace,
            "ENOSPC"
        );
        // Disk-lifecycle I/O (e.g. creating an extent file) too.
        assert_eq!(
            StoreError::Disk(DiskError::Io(io::Error::from(io::ErrorKind::StorageFull))).classify(),
            EpochError::OutOfSpace
        );
        // A genuine hardware fault still trips broken.
        assert_eq!(
            StoreError::Disk(DiskError::Io(io::Error::from(io::ErrorKind::Other))).classify(),
            EpochError::DiskBroken
        );
    }

    #[test]
    fn corruption_and_index_faults_map_to_internal() {
        assert_eq!(
            StoreError::Extent(ExtentError::BodyChecksum { offset: 0 }).classify(),
            EpochError::Internal
        );
        assert_eq!(
            StoreError::BodyTooLarge(1 << 40).classify(),
            EpochError::Internal
        );
        assert_eq!(
            StoreError::BlobMismatch {
                requested: BlobId::from_raw(1),
                found: BlobId::from_raw(2),
            }
            .classify(),
            EpochError::Internal
        );
    }
}
