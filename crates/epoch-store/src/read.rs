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

//! Blob shard read: resolve the index entry, then read and verify the record.
//!
//! Reads ignore the tombstone flag — a logically deleted blob stays physically
//! readable until compaction, so an in-flight GET that still holds an old slice
//! is never interrupted (02 §1.6). The record is fully integrity-checked by
//! [`ExtentFile::read_record`] (header CRC, footer, body CRC32C), and the
//! record's self-described `blob_id` is cross-checked against the request to
//! catch any index/file desync.
//!
//! Design: docs/design/02-datanode.md §1.3/§1.6

use epoch_proto::BlobId;

use crate::error::StoreError;
use crate::extent::file::ExtentFile;
use crate::index::DiskIndex;

/// Reads a blob shard `body` from `extent`, or `None` if the blob is not indexed.
///
/// # Errors
///
/// - [`StoreError::Index`] on an index failure;
/// - [`StoreError::Extent`] if the record is missing or fails integrity checks;
/// - [`StoreError::BlobMismatch`] if the record at the indexed offset describes
///   a different blob (index/file desync).
pub fn read_blob(
    index: &DiskIndex,
    extent: &ExtentFile,
    blob_id: BlobId,
) -> Result<Option<Vec<u8>>, StoreError> {
    let Some(entry) = index.get_blob(extent.extent_id(), blob_id)? else {
        return Ok(None);
    };
    let (header, body) = extent.read_record(entry.offset)?;
    if header.blob_id != blob_id {
        return Err(StoreError::BlobMismatch {
            requested: blob_id,
            found: header.blob_id,
        });
    }
    Ok(Some(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{blob, writable_extent};
    use crate::write::write_blob;

    #[test]
    fn reading_absent_blob_is_none() {
        let fx = writable_extent();
        assert_eq!(
            read_blob(&fx.index, &fx.extent, blob(404)).expect("read"),
            None
        );
    }

    #[test]
    fn tombstoned_blob_is_still_readable() {
        let mut fx = writable_extent();
        let body = b"still-here";
        write_blob(&fx.index, &mut fx.extent, blob(0), body).expect("write");

        assert!(
            fx.index
                .tombstone_blob(fx.extent_id, blob(0))
                .expect("tombstone")
        );
        // Read ignores the tombstone (02 §1.6): the body is still returned.
        assert_eq!(
            read_blob(&fx.index, &fx.extent, blob(0))
                .expect("read")
                .as_deref(),
            Some(&body[..])
        );
    }
}
