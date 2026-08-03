//! Blob shard write: append the record, fsync, then commit the index entry.
//!
//! The append→fsync→index-commit order is the local realization of the
//! persistence contract (02 §1.7): the shard data is durable before the index
//! commit that acknowledges it. Re-writing an already-committed `blob_id` is an
//! idempotent no-op ([`WriteOutcome::AlreadyExists`], 02 §1.4) — the index is
//! the commit authority, so an uncommitted half-write is simply re-appended.
//!
//! M2 exposes a whole-body local API; streamed OPEN/END framing arrives with
//! the data-plane RPC (M3).
//!
//! Design: docs/design/02-datanode.md §1.4/§1.7

use epoch_proto::BlobId;

use crate::error::StoreError;
use crate::extent::file::ExtentFile;
use crate::extent::state::ExtentStatus;
use crate::index::{BlobIndex, DiskIndex};

/// Outcome of a [`write_blob`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The blob was newly written and committed.
    Written,
    /// The blob was already committed under this id (idempotent retry).
    AlreadyExists,
}

/// The write-path status gate (02 §1.4): a blob may only be appended to a
/// `Writable` extent. Shared by [`write_blob`], the per-extent writer's batched
/// path, and the writable-extent resolver so the mapping lives in one place.
///
/// # Errors
///
/// [`StoreError::Sealed`] / [`StoreError::Full`] / [`StoreError::NotWritable`]
/// for the respective non-writable status.
pub(crate) fn writable_gate(status: ExtentStatus) -> Result<(), StoreError> {
    match status {
        // `Rebuilding` accepts writes too: it is a repair target that receives
        // rebuilt blobs by explicit extent id (02 §1.5). It never serves
        // *foreground* writes because it is unbound — `writable_extent(shard)`
        // resolves the shard's bound extent, never a rebuild target — so the
        // "no foreground writes" guarantee comes from binding, not this gate.
        ExtentStatus::Writable | ExtentStatus::Rebuilding => Ok(()),
        ExtentStatus::Sealed => Err(StoreError::Sealed),
        ExtentStatus::Full => Err(StoreError::Full),
        ExtentStatus::Dropped => Err(StoreError::NotWritable(ExtentStatus::Dropped)),
    }
}

/// Writes one blob shard `body` into `extent` and commits it to `index`.
///
/// The shard slot is taken from the extent's own id (an extent backs a single
/// shard). Rejects the write if the extent is not `Writable`.
///
/// # Errors
///
/// - [`StoreError::ExtentNotFound`] if the extent has no metadata;
/// - [`StoreError::Sealed`] / [`StoreError::Full`] / [`StoreError::NotWritable`]
///   if the extent does not accept writes;
/// - [`StoreError::BodyTooLarge`] if `body` exceeds the record size field;
/// - [`StoreError::Index`] / [`StoreError::Extent`] on a storage failure.
pub fn write_blob(
    index: &DiskIndex,
    extent: &mut ExtentFile,
    blob_id: BlobId,
    body: &[u8],
) -> Result<WriteOutcome, StoreError> {
    let extent_id = extent.extent_id();
    let meta = index
        .get_extent_meta(extent_id)?
        .ok_or(StoreError::ExtentNotFound(extent_id))?;

    writable_gate(meta.status)?;

    // Idempotent retry: an already-committed blob id needs no rewrite.
    if index.get_blob(extent_id, blob_id)?.is_some() {
        return Ok(WriteOutcome::AlreadyExists);
    }

    let size = u32::try_from(body.len()).map_err(|_| StoreError::BodyTooLarge(body.len()))?;
    let (offset, crc) = extent.append_record(blob_id, extent_id.shard_id(), body)?;
    // Data durable before the index commit that acknowledges it (02 §1.7).
    extent.sync()?;

    // The size merge is field-scoped (crate::meta_merge): a concurrent
    // delete's `deleted_bytes` or a landed seal's `status` survives.
    index.commit_blob(
        extent_id,
        blob_id,
        &BlobIndex::new(offset, size, crc),
        extent.write_offset(),
    )?;
    Ok(WriteOutcome::Written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::read_blob;
    use crate::testutil::{blob, writable_extent};

    #[test]
    fn write_then_read_round_trip() {
        let mut fx = writable_extent();
        let body = vec![0x5Au8; 4096];
        assert_eq!(
            write_blob(&fx.index, &mut fx.extent, blob(0), &body).expect("write"),
            WriteOutcome::Written
        );
        assert_eq!(
            read_blob(&fx.index, &fx.extent, blob(0)).expect("read"),
            Some(body)
        );
    }

    #[test]
    fn rewriting_same_blob_id_is_idempotent() {
        let mut fx = writable_extent();
        let body = b"original";
        write_blob(&fx.index, &mut fx.extent, blob(7), body).expect("first");
        let cursor_after_first = fx.extent.write_offset();

        // A retry with the same id neither appends nor changes the committed data.
        assert_eq!(
            write_blob(&fx.index, &mut fx.extent, blob(7), b"different").expect("retry"),
            WriteOutcome::AlreadyExists
        );
        assert_eq!(fx.extent.write_offset(), cursor_after_first);
        assert_eq!(
            read_blob(&fx.index, &fx.extent, blob(7))
                .expect("read")
                .as_deref(),
            Some(&body[..])
        );
    }

    #[test]
    fn writes_to_sealed_extent_are_rejected() {
        let mut fx = writable_extent();
        let mut meta = fx
            .index
            .get_extent_meta(fx.extent_id)
            .expect("meta")
            .unwrap();
        meta.status = ExtentStatus::Sealed;
        fx.index.put_extent_meta(fx.extent_id, &meta).expect("seal");

        assert!(matches!(
            write_blob(&fx.index, &mut fx.extent, blob(0), b"x"),
            Err(StoreError::Sealed)
        ));
    }

    #[test]
    fn multiple_blobs_are_independently_readable() {
        let mut fx = writable_extent();
        for seq in 0..5u32 {
            let body = vec![seq as u8; 1000 + seq as usize];
            write_blob(&fx.index, &mut fx.extent, blob(seq), &body).expect("write");
        }
        for seq in 0..5u32 {
            let expected = vec![seq as u8; 1000 + seq as usize];
            assert_eq!(
                read_blob(&fx.index, &fx.extent, blob(seq)).expect("read"),
                Some(expected)
            );
        }
    }
}
