//! Shared test fixtures for epoch-store (AGENTS §9.1a: one `testutil` per crate,
//! not copied between test files).

use epoch_proto::consts::DEFAULT_EXTENT_SIZE;
use epoch_proto::{BlobId, ChunkId, DiskId, ExtentId, ShardId, WriterToken};
use tempfile::TempDir;

use crate::disk::Disk;
use crate::extent::file::ExtentFile;
use crate::extent::state::ExtentStatus;
use crate::index::{DiskIndex, ExtentMeta};
use crate::service::StorageEngine;
use crate::superblock::Superblock;

/// Test cluster id used by fixtures.
pub const CLUSTER: u128 = 0x00C0_FFEE;

/// Extent size for engine fixtures: small so the per-extent `fallocate`
/// reservation (02 §1.7) stays cheap on Linux CI — every engine-created extent
/// is preallocated.
const TEST_EXTENT_SIZE: u64 = 8 * 1024 * 1024;

/// A test blob id under a fixed writer token.
pub fn blob(seq: u32) -> BlobId {
    BlobId::new(WriterToken::new(1), seq)
}

/// The shard slot engine fixtures bind their extents to.
pub fn shard() -> ShardId {
    ShardId::new(ChunkId::new(1), 0, 0)
}

/// A formatted (registered) but freshly opened engine on a temp disk.
///
/// The `TempDir` must outlive the engine — bind it, don't drop it.
pub fn fresh_engine() -> (TempDir, StorageEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    let superblock = Superblock {
        disk_id: DiskId::new(1),
        cluster_id: CLUSTER,
        created_at: 0,
        flags: 0,
        extent_size: TEST_EXTENT_SIZE,
    };
    Disk::format(dir.path(), superblock).expect("format");
    let engine = StorageEngine::open(dir.path(), CLUSTER).expect("open");
    (dir, engine)
}

/// A freshly formatted disk with an open index and one registered, writable
/// extent bound to a single shard slot.
pub struct Fixture {
    /// The per-disk index.
    pub index: DiskIndex,
    /// The open, writable extent.
    pub extent: ExtentFile,
    /// The extent's id.
    pub extent_id: ExtentId,
    // Roots the temp directory's lifetime (RAII cleanup on drop); never read.
    #[allow(dead_code)]
    dir: TempDir,
}

/// Builds a [`Fixture`] with one writable extent ready for blob writes.
pub fn writable_extent() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let superblock = Superblock {
        disk_id: DiskId::new(1),
        cluster_id: CLUSTER,
        created_at: 0,
        flags: 0,
        extent_size: DEFAULT_EXTENT_SIZE,
    };
    let disk = Disk::format(dir.path(), superblock).expect("format");
    let index = DiskIndex::open(&disk.index_dir()).expect("open index");

    let shard = ShardId::new(ChunkId::new(1), 0, 0);
    let extent_id = ExtentId::new(shard, 1);
    let extent = ExtentFile::create(disk.extent_path(extent_id), extent_id).expect("create extent");

    index
        .install_extent(
            extent_id,
            &ExtentMeta {
                shard_id: shard,
                status: ExtentStatus::Writable,
                size: extent.write_offset(),
                deleted_bytes: 0,
                create_ts: 0,
            },
            &[],
        )
        .expect("install extent");

    Fixture {
        index,
        extent,
        extent_id,
        dir,
    }
}
