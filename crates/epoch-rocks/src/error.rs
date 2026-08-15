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

//! Error classification for the RocksDB wrapper.
//!
//! Splits `rocksdb::Error` into a disk-level failure (I/O error or on-disk
//! corruption — the evidence that drives DataNode disk-broken reporting) versus
//! any other error. The *stateful* disk-broken decision ("trip once per disk,
//! process fatal at >=3 disks") lives in the disk-state owner (epoch-store),
//! not here; this crate only classifies.

use rocksdb::{Error as RawError, ErrorKind};

/// Result alias for RocksDB wrapper operations.
pub type Result<T> = std::result::Result<T, RocksError>;

/// A classified RocksDB failure.
///
/// The [`DiskFailure`](RocksError::DiskFailure) / [`Other`](RocksError::Other)
/// split is the EIO-identification contract of 06 §5: I/O and corruption errors
/// signal a possibly-broken disk. The stateful disk-broken *policy* ("trip once
/// per disk, process fatal at >=3 disks", 02 §1.8) is applied by the disk-state
/// owner in a later milestone, which matches on these variants.
#[derive(Debug, thiserror::Error)]
pub enum RocksError {
    /// A disk-level failure (I/O error or on-disk corruption).
    #[error("rocksdb disk failure: {source}")]
    DiskFailure {
        /// The originating RocksDB error.
        #[source]
        source: RawError,
    },
    /// Any other RocksDB error (invalid argument, busy, shutdown, ...).
    #[error("rocksdb error: {source}")]
    Other {
        /// The originating RocksDB error.
        #[source]
        source: RawError,
    },
}

impl From<RawError> for RocksError {
    fn from(source: RawError) -> Self {
        if is_disk_failure_kind(&source.kind()) {
            RocksError::DiskFailure { source }
        } else {
            RocksError::Other { source }
        }
    }
}

/// Whether a RocksDB error kind denotes a disk-level failure.
fn is_disk_failure_kind(kind: &ErrorKind) -> bool {
    matches!(kind, ErrorKind::IOError | ErrorKind::Corruption)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_and_corruption_are_disk_failures() {
        assert!(is_disk_failure_kind(&ErrorKind::IOError));
        assert!(is_disk_failure_kind(&ErrorKind::Corruption));
    }

    #[test]
    fn other_kinds_are_not_disk_failures() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::InvalidArgument,
            ErrorKind::Busy,
            ErrorKind::TryAgain,
            ErrorKind::ShutdownInProgress,
        ] {
            assert!(!is_disk_failure_kind(&kind), "kind {kind:?} must not trip");
        }
    }
}
