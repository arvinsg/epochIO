//! Per-disk RocksDB index (02 §1.3): the local source of truth binding shard
//! slots to extents and blobs to their on-disk records.
//!
//! Three keyspaces share one column family, distinguished by a leading byte:
//! ```text
//! e{extent_id:16}                -> ExtentMeta{shard_id,status,size,deleted_bytes,ctime}
//! s{shard_prefix:8 BE}           -> extent_id (current shard->extent binding)
//! b{extent_id:16}{blob_id:8 BE}  -> BlobIndex{offset,size,crc,flags}
//! ```
//! Blob keys embed `blob_id` big-endian so a range scan over `b{extent_id}`
//! lists a blob's records in id order. Values are little-endian (only keys need
//! big-endian ordering). A blob CLOSE commits the blob record and the updated
//! extent size in one synced [`WriteBatch`] — the shard-level commit point.
//!
//! Extent-meta mutations are field-scoped merge operands
//! ([`crate::meta_merge`]): the writer merges `size`, deletes merge
//! `deleted_bytes`, seal/compaction merge `status`. Full-value puts are
//! reserved for creation/recovery installs, so concurrent owners can never
//! roll back each other's fields (02 §1.6/§1.7).
//!
//! Design: docs/design/02-datanode.md §1.3/§1.4/§1.6

use std::path::Path;

use epoch_proto::{BlobId, ExtentId, ShardId};
use epoch_rocks::RocksError;
use rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};

use crate::codec::read_array;
use crate::extent::record::record_on_disk_len;
use crate::extent::state::ExtentStatus;
use crate::meta_merge::{MetaMergeOp, merge_meta};

/// `TOMBSTONE`: the blob is logically deleted; space is reclaimed by compaction
/// (02 §1.6). (`INLINE` — small data stored in the value — lands with inline
/// support in M6.)
pub const FLAG_TOMBSTONE: u8 = 0x01;

/// Encoded length of an [`ExtentMeta`] value.
const EXTENT_META_LEN: usize = 33;
/// Encoded length of a [`BlobIndex`] value.
const BLOB_INDEX_LEN: usize = 17;

const EXTENT_KEY_LEN: usize = 1 + 16;
const SHARD_KEY_LEN: usize = 1 + 8;
const BLOB_KEY_PREFIX_LEN: usize = 1 + 16;
const BLOB_KEY_LEN: usize = 1 + 16 + 8;

/// An index operation failure.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    /// Underlying RocksDB storage error (classified for disk-broken reporting).
    #[error("index storage: {0}")]
    Storage(#[from] RocksError),
    /// A stored value could not be decoded.
    #[error("corrupt {kind} value ({len} bytes): {reason}")]
    Corrupt {
        /// Which value kind failed to decode.
        kind: &'static str,
        /// Byte length of the offending value.
        len: usize,
        /// Human-readable reason.
        reason: &'static str,
    },
}

impl From<rocksdb::Error> for IndexError {
    fn from(err: rocksdb::Error) -> Self {
        IndexError::Storage(RocksError::from(err))
    }
}

/// Per-extent metadata stored under the `e` keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentMeta {
    /// Shard slot this extent is bound to.
    pub shard_id: ShardId,
    /// Lifecycle status.
    pub status: ExtentStatus,
    /// Append cursor / valid file length (recovery truncates to this, 02 §1.7).
    pub size: u64,
    /// Sum of on-disk lengths of tombstoned records (compaction trigger).
    pub deleted_bytes: u64,
    /// Creation timestamp.
    pub create_ts: i64,
}

impl ExtentMeta {
    /// Encodes to the fixed 33-byte value form.
    #[must_use]
    pub fn encode(&self) -> [u8; EXTENT_META_LEN] {
        let mut buf = [0u8; EXTENT_META_LEN];
        buf[0..8].copy_from_slice(&self.shard_id.as_u64().to_le_bytes());
        buf[8] = self.status.to_u8();
        buf[9..17].copy_from_slice(&self.size.to_le_bytes());
        buf[17..25].copy_from_slice(&self.deleted_bytes.to_le_bytes());
        buf[25..33].copy_from_slice(&self.create_ts.to_le_bytes());
        buf
    }

    /// Decodes the fixed 33-byte value form (shared with the merge operator).
    pub(crate) fn decode_value(buf: &[u8]) -> Result<Self, &'static str> {
        let buf = buf
            .first_chunk::<EXTENT_META_LEN>()
            .ok_or("short extent meta")?;
        let status = ExtentStatus::from_u8(buf[8]).ok_or("unknown extent status")?;
        Ok(Self {
            shard_id: ShardId::from_raw(u64::from_le_bytes(read_array::<8>(buf, 0))),
            status,
            size: u64::from_le_bytes(read_array::<8>(buf, 9)),
            deleted_bytes: u64::from_le_bytes(read_array::<8>(buf, 17)),
            create_ts: i64::from_le_bytes(read_array::<8>(buf, 25)),
        })
    }
}

/// A blob record's index entry stored under the `b` keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobIndex {
    /// Record start offset within the extent file.
    pub offset: u64,
    /// Framed body length (bytes of bitrot-framed shard data).
    pub size: u32,
    /// CRC32C of the body (matches the record footer).
    pub crc: u32,
    /// Flag bits ([`FLAG_TOMBSTONE`]).
    pub flags: u8,
}

impl BlobIndex {
    /// A live (non-tombstoned) blob index entry.
    #[must_use]
    pub fn new(offset: u64, size: u32, crc: u32) -> Self {
        Self {
            offset,
            size,
            crc,
            flags: 0,
        }
    }

    /// Whether this entry is tombstoned (logically deleted).
    #[must_use]
    pub fn is_tombstoned(&self) -> bool {
        self.flags & FLAG_TOMBSTONE != 0
    }

    /// Encodes to the fixed 17-byte value form.
    #[must_use]
    pub fn encode(&self) -> [u8; BLOB_INDEX_LEN] {
        let mut buf = [0u8; BLOB_INDEX_LEN];
        buf[0..8].copy_from_slice(&self.offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.size.to_le_bytes());
        buf[12..16].copy_from_slice(&self.crc.to_le_bytes());
        buf[16] = self.flags;
        buf
    }

    fn decode(buf: &[u8]) -> Result<Self, &'static str> {
        let buf = buf
            .first_chunk::<BLOB_INDEX_LEN>()
            .ok_or("short blob index")?;
        Ok(Self {
            offset: u64::from_le_bytes(read_array::<8>(buf, 0)),
            size: u32::from_le_bytes(read_array::<4>(buf, 8)),
            crc: u32::from_le_bytes(read_array::<4>(buf, 12)),
            flags: buf[16],
        })
    }
}

/// The per-disk RocksDB index.
#[derive(Debug)]
pub struct DiskIndex {
    db: DB,
}

impl DiskIndex {
    /// Opens (creating if absent) the index at `dir` with the epoch-rocks
    /// `DiskIndex` profile.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] if RocksDB cannot open the database.
    pub fn open(dir: &Path) -> Result<Self, IndexError> {
        Ok(Self {
            db: epoch_rocks::open_disk_index(dir, Some(merge_meta))?,
        })
    }

    /// Writes an extent's whole metadata value (unsynced).
    ///
    /// INVARIANT(design 02 §1.6/§1.7): reserved for extent creation and
    /// recovery installs, where the caller owns the entire value. Field
    /// changes on a live extent must go through the merge-based mutators
    /// ([`commit_blobs`](Self::commit_blobs),
    /// [`tombstone_blob`](Self::tombstone_blob),
    /// [`seal_extent`](Self::seal_extent),
    /// [`swap_shard_binding`](Self::swap_shard_binding)) — a whole-value put
    /// can roll back a concurrent owner's field.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a write failure.
    pub fn put_extent_meta(&self, extent: ExtentId, meta: &ExtentMeta) -> Result<(), IndexError> {
        self.db.put(extent_key(extent), meta.encode())?;
        Ok(())
    }

    /// Reads an extent's metadata.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a read failure, [`IndexError::Corrupt`] if the
    /// stored value cannot be decoded.
    pub fn get_extent_meta(&self, extent: ExtentId) -> Result<Option<ExtentMeta>, IndexError> {
        let Some(value) = self.db.get(extent_key(extent))? else {
            return Ok(None);
        };
        ExtentMeta::decode_value(&value)
            .map(Some)
            .map_err(|reason| IndexError::Corrupt {
                kind: "extent_meta",
                len: value.len(),
                reason,
            })
    }

    /// Binds a shard slot to its current extent (unsynced; set at creation).
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a write failure.
    pub fn set_shard_binding(&self, shard: ShardId, extent: ExtentId) -> Result<(), IndexError> {
        self.db.put(shard_key(shard), extent.as_bytes())?;
        Ok(())
    }

    /// Resolves a shard slot to its current extent.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a read failure, [`IndexError::Corrupt`] if the
    /// stored binding is not a 16-byte extent id.
    pub fn get_shard_binding(&self, shard: ShardId) -> Result<Option<ExtentId>, IndexError> {
        let Some(value) = self.db.get(shard_key(shard))? else {
            return Ok(None);
        };
        let bytes: [u8; 16] = value
            .as_slice()
            .try_into()
            .map_err(|_| IndexError::Corrupt {
                kind: "shard_binding",
                len: value.len(),
                reason: "expected 16-byte extent id",
            })?;
        Ok(Some(ExtentId::from_bytes(bytes)))
    }

    /// Reads a blob's index entry.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a read failure, [`IndexError::Corrupt`] if the
    /// stored value cannot be decoded.
    pub fn get_blob(
        &self,
        extent: ExtentId,
        blob: BlobId,
    ) -> Result<Option<BlobIndex>, IndexError> {
        let Some(value) = self.db.get(blob_key(extent, blob))? else {
            return Ok(None);
        };
        BlobIndex::decode(&value)
            .map(Some)
            .map_err(|reason| IndexError::Corrupt {
                kind: "blob_index",
                len: value.len(),
                reason,
            })
    }

    /// Lists all blob entries of an extent in ascending `blob_id` order.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a read failure, [`IndexError::Corrupt`] on a
    /// malformed key or value.
    pub fn list_blobs(&self, extent: ExtentId) -> Result<Vec<(BlobId, BlobIndex)>, IndexError> {
        let prefix = blob_key_prefix(extent);
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let blob_be: [u8; 8] = key
                .get(BLOB_KEY_PREFIX_LEN..BLOB_KEY_LEN)
                .and_then(|s| s.try_into().ok())
                .ok_or(IndexError::Corrupt {
                    kind: "blob_key",
                    len: key.len(),
                    reason: "expected 25-byte blob key",
                })?;
            let blob = BlobId::from_raw(u64::from_be_bytes(blob_be));
            let index = BlobIndex::decode(&value).map_err(|reason| IndexError::Corrupt {
                kind: "blob_index",
                len: value.len(),
                reason,
            })?;
            out.push((blob, index));
        }
        Ok(out)
    }

    /// Commits a blob at its CLOSE point: writes the blob index entry and
    /// merges the advanced extent `size` in one synced batch (02 §1.4 shard
    /// commit point).
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] if the synced batch write fails.
    pub fn commit_blob(
        &self,
        extent: ExtentId,
        blob: BlobId,
        blob_index: &BlobIndex,
        size: u64,
    ) -> Result<(), IndexError> {
        self.commit_blobs(extent, &[(blob, *blob_index)], size)
    }

    /// Commits a group of blobs and merges the advanced extent `size` in one
    /// synced batch: the group-commit form of [`commit_blob`] used by the
    /// per-extent writer to amortize one fsync + one index batch across several
    /// queued writes (02 §1.4). All blob entries and the size merge are made
    /// durable atomically, so a crash leaves either none or all of the group
    /// committed.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] if the synced batch write fails.
    pub fn commit_blobs(
        &self,
        extent: ExtentId,
        blobs: &[(BlobId, BlobIndex)],
        size: u64,
    ) -> Result<(), IndexError> {
        let mut batch = WriteBatch::default();
        for (blob, blob_index) in blobs {
            batch.put(blob_key(extent, *blob), blob_index.encode());
        }
        // Field-scoped: the writer owns `size` only — a concurrent delete's
        // `deleted_bytes` or a landed seal's `status` is never rolled back
        // (02 §1.6/§1.7; see crate::meta_merge).
        batch.merge(extent_key(extent), MetaMergeOp::SetSize(size).encode());
        self.write_synced(batch)
    }

    /// Atomically installs an extent's whole index state — its metadata, its
    /// shard binding, and every supplied blob entry — in one synced batch. The
    /// binding is taken from `meta.shard_id`.
    ///
    /// Used at extent creation (empty `blobs`) and to rebuild the index after
    /// loss (`blobs` = the records recovered by a stream scan, 02 §1.7). Writing
    /// all keys atomically makes both crash-safe: after a crash the extent's
    /// metadata is either absent (so recovery re-derives the whole extent) or
    /// present together with its binding and blobs — never a torn subset.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] if the synced batch write fails.
    pub fn install_extent(
        &self,
        extent: ExtentId,
        meta: &ExtentMeta,
        blobs: &[(BlobId, BlobIndex)],
    ) -> Result<(), IndexError> {
        let mut batch = WriteBatch::default();
        batch.put(extent_key(extent), meta.encode());
        batch.put(shard_key(meta.shard_id), extent.as_bytes());
        for (blob, blob_index) in blobs {
            batch.put(blob_key(extent, *blob), blob_index.encode());
        }
        self.write_synced(batch)
    }

    /// Tombstones a blob (idempotent): sets [`FLAG_TOMBSTONE`] and merges the
    /// record's on-disk length into the extent's `deleted_bytes`, in one synced
    /// batch. Returns `true` if this call newly tombstoned the blob, `false` if
    /// it was already tombstoned or absent (at-least-once delete is safe, 02 §1.6).
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a failure.
    pub fn tombstone_blob(&self, extent: ExtentId, blob: BlobId) -> Result<bool, IndexError> {
        let Some(mut index) = self.get_blob(extent, blob)? else {
            return Ok(false);
        };
        if index.is_tombstoned() {
            return Ok(false);
        }
        // The blob entry read-modify-write is safe: after CLOSE the entry has a
        // single mutator (this path; the engine serializes deletes per shard).
        index.flags |= FLAG_TOMBSTONE;

        let mut batch = WriteBatch::default();
        batch.put(blob_key(extent, blob), index.encode());
        // Field-scoped: only `deleted_bytes` moves — a concurrent writer's
        // `size` commit is never rolled back (02 §1.6; see crate::meta_merge).
        batch.merge(
            extent_key(extent),
            MetaMergeOp::AddDeletedBytes(record_on_disk_len(index.size) as u64).encode(),
        );
        self.write_synced(batch)?;
        Ok(true)
    }

    /// Atomically rebinds `shard` from `old_extent` to `new_extent` and marks
    /// the old extent `Dropped`, in one synced batch — the compaction commit
    /// point (02 §1.6). The old extent's other metadata fields are preserved.
    ///
    /// # Errors
    ///
    /// [`IndexError::Corrupt`] if `old_extent` has no metadata,
    /// [`IndexError::Storage`] on read or the synced write.
    pub fn swap_shard_binding(
        &self,
        shard: ShardId,
        new_extent: ExtentId,
        old_extent: ExtentId,
    ) -> Result<(), IndexError> {
        self.swap_shard_binding_with(shard, new_extent, old_extent, &[])
    }

    /// [`swap_shard_binding`](Self::swap_shard_binding) plus tombstone
    /// carry-over: atomically rebinds the shard, drops the source, and marks
    /// `carried` blobs tombstoned on the destination in the same synced batch.
    ///
    /// INVARIANT(design 02 §1.6): deletes that landed on the compaction source
    /// after its live-set snapshot must be carried into the destination inside
    /// the swap batch, or removing the source erases the acked delete and the
    /// copied blob resurrects.
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on read or the synced write,
    /// [`IndexError::Corrupt`] on a malformed carried blob entry.
    pub fn swap_shard_binding_with(
        &self,
        shard: ShardId,
        new_extent: ExtentId,
        old_extent: ExtentId,
        carried: &[BlobId],
    ) -> Result<(), IndexError> {
        let mut batch = WriteBatch::default();
        batch.put(shard_key(shard), new_extent.as_bytes());
        // Field-scoped: only the source's `status` moves (02 §1.6).
        batch.merge(
            extent_key(old_extent),
            MetaMergeOp::SetStatus(ExtentStatus::Dropped).encode(),
        );
        let mut carried_bytes: u64 = 0;
        for blob in carried {
            let Some(mut entry) = self.get_blob(new_extent, *blob)? else {
                // Not copied (deleted before the copy loop reached it): the
                // destination simply has no entry — nothing to tombstone.
                continue;
            };
            if entry.is_tombstoned() {
                continue;
            }
            entry.flags |= FLAG_TOMBSTONE;
            batch.put(blob_key(new_extent, *blob), entry.encode());
            carried_bytes += record_on_disk_len(entry.size) as u64;
        }
        if carried_bytes > 0 {
            batch.merge(
                extent_key(new_extent),
                MetaMergeOp::AddDeletedBytes(carried_bytes).encode(),
            );
        }
        self.write_synced(batch)
    }

    /// Durably transitions `extent` to `status` via a field-scoped merge (other
    /// metadata fields are preserved; 02 §1.5). Used by compaction's pause step
    /// (`Full`) and future lifecycle transitions.
    ///
    /// # Errors
    ///
    /// [`IndexError::Corrupt`] if `extent` has no metadata,
    /// [`IndexError::Storage`] on read or the synced write.
    pub fn set_extent_status(
        &self,
        extent: ExtentId,
        status: ExtentStatus,
    ) -> Result<(), IndexError> {
        self.get_extent_meta(extent)?.ok_or(IndexError::Corrupt {
            kind: "extent_meta",
            len: 0,
            reason: "missing metadata for status transition",
        })?;
        let mut batch = WriteBatch::default();
        batch.merge(extent_key(extent), MetaMergeOp::SetStatus(status).encode());
        self.write_synced(batch)
    }

    /// Durably transitions `extent` to [`ExtentStatus::Sealed`] (the seal commit
    /// point, 01 §4.2). Idempotent; other metadata fields are preserved by the
    /// field-scoped status merge.
    ///
    /// # Errors
    ///
    /// [`IndexError::Corrupt`] if `extent` has no metadata,
    /// [`IndexError::Storage`] on read or the synced write.
    pub fn seal_extent(&self, extent: ExtentId) -> Result<(), IndexError> {
        // Presence check keeps the "seal an unknown extent" error visible.
        self.get_extent_meta(extent)?.ok_or(IndexError::Corrupt {
            kind: "extent_meta",
            len: 0,
            reason: "missing metadata for seal",
        })?;
        let mut batch = WriteBatch::default();
        // Field-scoped: only `status` moves — an in-flight writer commit's
        // `size` merge is never rolled back (02 §1.6; the Q17 drain window:
        // queued data commits, the status stays Sealed).
        batch.merge(
            extent_key(extent),
            MetaMergeOp::SetStatus(ExtentStatus::Sealed).encode(),
        );
        self.write_synced(batch)
    }

    /// Lists every extent's `(id, metadata)` in ascending extent-id order.
    /// Used by scrub reconciliation and dropped-extent reclamation (02 §1.9).
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a read failure, [`IndexError::Corrupt`] on a
    /// malformed key or value.
    pub fn list_extents(&self) -> Result<Vec<(ExtentId, ExtentMeta)>, IndexError> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator(IteratorMode::From(b"e", Direction::Forward))
        {
            let (key, value) = item?;
            if key.first() != Some(&b'e') {
                break;
            }
            let bytes: [u8; 16] = key
                .get(1..EXTENT_KEY_LEN)
                .and_then(|s| s.try_into().ok())
                .ok_or(IndexError::Corrupt {
                    kind: "extent_key",
                    len: key.len(),
                    reason: "expected 17-byte extent key",
                })?;
            let meta = ExtentMeta::decode_value(&value).map_err(|reason| IndexError::Corrupt {
                kind: "extent_meta",
                len: value.len(),
                reason,
            })?;
            out.push((ExtentId::from_bytes(bytes), meta));
        }
        Ok(out)
    }

    /// Removes an extent's metadata and all its blob entries in one synced
    /// batch. The shard binding is left untouched (a compacted source is
    /// removed only after its shard has been rebound elsewhere, 02 §1.6).
    ///
    /// # Errors
    ///
    /// [`IndexError::Storage`] on a read or the synced write.
    pub fn remove_extent(&self, extent: ExtentId) -> Result<(), IndexError> {
        let prefix = blob_key_prefix(extent);
        let mut batch = WriteBatch::default();
        batch.delete(extent_key(extent));
        for item in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete(key);
        }
        self.write_synced(batch)
    }

    fn write_synced(&self, batch: WriteBatch) -> Result<(), IndexError> {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db.write_opt(batch, &opts)?;
        Ok(())
    }
}

fn extent_key(extent: ExtentId) -> [u8; EXTENT_KEY_LEN] {
    let mut key = [0u8; EXTENT_KEY_LEN];
    key[0] = b'e';
    key[1..].copy_from_slice(extent.as_bytes());
    key
}

fn shard_key(shard: ShardId) -> [u8; SHARD_KEY_LEN] {
    let mut key = [0u8; SHARD_KEY_LEN];
    key[0] = b's';
    key[1..].copy_from_slice(&shard.shard_prefix().to_be_bytes());
    key
}

fn blob_key_prefix(extent: ExtentId) -> [u8; BLOB_KEY_PREFIX_LEN] {
    let mut key = [0u8; BLOB_KEY_PREFIX_LEN];
    key[0] = b'b';
    key[1..].copy_from_slice(extent.as_bytes());
    key
}

fn blob_key(extent: ExtentId, blob: BlobId) -> [u8; BLOB_KEY_LEN] {
    let mut key = [0u8; BLOB_KEY_LEN];
    key[0] = b'b';
    key[1..BLOB_KEY_PREFIX_LEN].copy_from_slice(extent.as_bytes());
    key[BLOB_KEY_PREFIX_LEN..].copy_from_slice(&blob.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::{ChunkId, WriterToken};

    fn shard() -> ShardId {
        ShardId::new(ChunkId::new(3), 2, 7)
    }

    fn extent() -> ExtentId {
        ExtentId::new(shard(), 1_700_000_000)
    }

    fn blob(seq: u32) -> BlobId {
        BlobId::new(WriterToken::new(1), seq)
    }

    fn meta(size: u64) -> ExtentMeta {
        ExtentMeta {
            shard_id: shard(),
            status: ExtentStatus::Writable,
            size,
            deleted_bytes: 0,
            create_ts: 1_700_000_000,
        }
    }

    fn open() -> (tempfile::TempDir, DiskIndex) {
        let dir = tempfile::tempdir().expect("tempdir");
        let index = DiskIndex::open(dir.path()).expect("open");
        (dir, index)
    }

    #[test]
    fn extent_meta_value_round_trips() {
        let m = ExtentMeta {
            shard_id: ShardId::from_raw(u64::MAX),
            status: ExtentStatus::Sealed,
            size: u64::MAX,
            deleted_bytes: 123,
            create_ts: i64::MIN,
        };
        assert_eq!(ExtentMeta::decode_value(&m.encode()), Ok(m));
    }

    #[test]
    fn blob_index_value_round_trips() {
        let b = BlobIndex {
            offset: 4096,
            size: 1_048_576,
            crc: 0xDEAD_BEEF,
            flags: FLAG_TOMBSTONE,
        };
        assert_eq!(BlobIndex::decode(&b.encode()), Ok(b));
        assert!(b.is_tombstoned());
        assert!(!BlobIndex::new(0, 0, 0).is_tombstoned());
    }

    #[test]
    fn unknown_status_byte_reports_corrupt() {
        let (_dir, index) = open();
        // Write a raw extent value with an out-of-range status byte.
        let mut bytes = meta(0).encode();
        bytes[8] = 99;
        index.db.put(extent_key(extent()), bytes).expect("put raw");
        assert!(matches!(
            index.get_extent_meta(extent()),
            Err(IndexError::Corrupt {
                kind: "extent_meta",
                ..
            })
        ));
    }

    #[test]
    fn extent_meta_and_shard_binding_round_trip() {
        let (_dir, index) = open();
        index
            .put_extent_meta(extent(), &meta(4096))
            .expect("put meta");
        assert_eq!(
            index.get_extent_meta(extent()).expect("get"),
            Some(meta(4096))
        );

        index.set_shard_binding(shard(), extent()).expect("bind");
        assert_eq!(
            index.get_shard_binding(shard()).expect("get"),
            Some(extent())
        );
        assert_eq!(
            index
                .get_extent_meta(ExtentId::new(shard(), 1))
                .expect("miss"),
            None
        );
    }

    #[test]
    fn commit_blob_writes_index_and_updates_size() {
        let (_dir, index) = open();
        index.put_extent_meta(extent(), &meta(4096)).expect("base");
        let bi = BlobIndex::new(4096, 1000, 0x1234);
        index
            .commit_blob(extent(), blob(0), &bi, 8192)
            .expect("commit");

        assert_eq!(index.get_blob(extent(), blob(0)).expect("get"), Some(bi));
        assert_eq!(
            index
                .get_extent_meta(extent())
                .expect("meta")
                .map(|m| m.size),
            Some(8192)
        );
    }

    #[test]
    fn commit_blobs_writes_all_entries_and_meta_in_one_batch() {
        let (_dir, index) = open();
        index.put_extent_meta(extent(), &meta(4096)).expect("base");
        let group = [
            (blob(0), BlobIndex::new(4096, 100, 0xA1)),
            (blob(1), BlobIndex::new(8192, 200, 0xB2)),
            (blob(2), BlobIndex::new(12288, 300, 0xC3)),
        ];
        index
            .commit_blobs(extent(), &group, 16384)
            .expect("commit group");

        for (id, bi) in group {
            assert_eq!(index.get_blob(extent(), id).expect("get"), Some(bi));
        }
        assert_eq!(
            index
                .get_extent_meta(extent())
                .expect("meta")
                .map(|m| m.size),
            Some(16384)
        );
    }

    #[test]
    fn install_extent_writes_meta_binding_and_blobs_atomically() {
        let (_dir, index) = open();
        let blobs = [
            (blob(0), BlobIndex::new(4096, 100, 0x11)),
            (blob(1), BlobIndex::new(8192, 200, 0x22)),
        ];
        index
            .install_extent(extent(), &meta(12288), &blobs)
            .expect("install");

        assert_eq!(
            index.get_extent_meta(extent()).expect("meta"),
            Some(meta(12288))
        );
        assert_eq!(
            index.get_shard_binding(shard()).expect("bind"),
            Some(extent())
        );
        for (id, bi) in blobs {
            assert_eq!(index.get_blob(extent(), id).expect("blob"), Some(bi));
        }
    }

    #[test]
    fn install_extent_with_no_blobs_still_binds_shard() {
        let (_dir, index) = open();
        index
            .install_extent(extent(), &meta(4096), &[])
            .expect("install");
        assert_eq!(
            index.get_shard_binding(shard()).expect("bind"),
            Some(extent())
        );
        assert_eq!(index.list_blobs(extent()).expect("list"), Vec::new());
    }

    #[test]
    fn swap_shard_binding_rebinds_and_drops_source() {
        let (_dir, index) = open();
        let old = extent();
        let new = ExtentId::new(shard(), 1_700_000_100);
        index
            .install_extent(old, &meta(8192), &[])
            .expect("install old");
        index.put_extent_meta(new, &meta(4096)).expect("new meta");

        index.swap_shard_binding(shard(), new, old).expect("swap");

        assert_eq!(index.get_shard_binding(shard()).expect("bind"), Some(new));
        assert_eq!(
            index.get_extent_meta(old).expect("old").map(|m| m.status),
            Some(ExtentStatus::Dropped)
        );
    }

    #[test]
    fn list_extents_returns_all_in_id_order() {
        let (_dir, index) = open();
        let a = ExtentId::new(shard(), 1);
        let b = ExtentId::new(shard(), 2);
        index.put_extent_meta(b, &meta(2)).expect("put b");
        index.put_extent_meta(a, &meta(1)).expect("put a");

        let ids: Vec<ExtentId> = index
            .list_extents()
            .expect("list")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec![a, b]);
    }

    #[test]
    fn remove_extent_clears_meta_and_blobs_but_keeps_binding() {
        let (_dir, index) = open();
        let blobs = [
            (blob(0), BlobIndex::new(4096, 10, 0)),
            (blob(1), BlobIndex::new(8192, 10, 0)),
        ];
        index
            .install_extent(extent(), &meta(12288), &blobs)
            .expect("install");

        index.remove_extent(extent()).expect("remove");

        assert_eq!(index.get_extent_meta(extent()).expect("meta"), None);
        assert_eq!(index.list_blobs(extent()).expect("list"), Vec::new());
        // The shard binding is intentionally left in place (caller rebinds first).
        assert_eq!(
            index.get_shard_binding(shard()).expect("bind"),
            Some(extent())
        );
    }

    #[test]
    fn list_blobs_is_ordered_by_blob_id() {
        let (_dir, index) = open();
        index.put_extent_meta(extent(), &meta(4096)).expect("base");
        for seq in [3u32, 1, 2] {
            let bi = BlobIndex::new(u64::from(seq) * 4096, 10, 0);
            index
                .commit_blob(extent(), blob(seq), &bi, 4096)
                .expect("commit");
        }
        let listed: Vec<u32> = index
            .list_blobs(extent())
            .expect("list")
            .into_iter()
            .map(|(b, _)| b.seq())
            .collect();
        assert_eq!(listed, vec![1, 2, 3]);
    }

    #[test]
    fn tombstone_is_idempotent_and_accounts_deleted_bytes() {
        let (_dir, index) = open();
        index.put_extent_meta(extent(), &meta(4096)).expect("base");
        let bi = BlobIndex::new(4096, 1000, 0);
        index
            .commit_blob(extent(), blob(0), &bi, 4096)
            .expect("commit");

        assert!(index.tombstone_blob(extent(), blob(0)).expect("tombstone"));
        assert!(
            index
                .get_blob(extent(), blob(0))
                .expect("get")
                .unwrap()
                .is_tombstoned()
        );
        let deleted = index
            .get_extent_meta(extent())
            .expect("meta")
            .unwrap()
            .deleted_bytes;
        assert_eq!(deleted, record_on_disk_len(1000) as u64);

        // Repeat: no-op, deleted_bytes unchanged.
        assert!(
            !index
                .tombstone_blob(extent(), blob(0))
                .expect("re-tombstone")
        );
        assert_eq!(
            index
                .get_extent_meta(extent())
                .expect("meta")
                .unwrap()
                .deleted_bytes,
            deleted
        );

        // Absent blob: safe no-op.
        assert!(!index.tombstone_blob(extent(), blob(99)).expect("absent"));
    }

    #[test]
    fn committed_blob_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bi = BlobIndex::new(4096, 500, 0xABCD);
        {
            let index = DiskIndex::open(dir.path()).expect("open");
            index.put_extent_meta(extent(), &meta(4096)).expect("base");
            index
                .commit_blob(extent(), blob(0), &bi, 8192)
                .expect("commit");
        }
        let index = DiskIndex::open(dir.path()).expect("reopen");
        assert_eq!(index.get_blob(extent(), blob(0)).expect("get"), Some(bi));
    }
}
