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

//! Single-disk lifecycle: directory layout, crash-safe superblock persistence,
//! registration (format) and load with cluster-identity validation.
//!
//! Disk layout (02 §1.1):
//! ```text
//! ${root}/
//!   .superblock   two 4 KiB copies (head [0..4K), tail [4K..8K)); update writes
//!                 the tail first, then the head, so one intact copy always survives
//!   extents/      one file per extent
//!   index/        per-disk RocksDB (blob index + extent metadata)
//!   .trash/       extents awaiting physical deletion after the protection window
//! ```

use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use epoch_proto::{DiskId, ExtentId};

use crate::superblock::{SUPERBLOCK_SIZE, Superblock, SuperblockError};

const SUPERBLOCK_FILE: &str = ".superblock";
const EXTENTS_DIR: &str = "extents";
const INDEX_DIR: &str = "index";
const TRASH_DIR: &str = ".trash";

/// On-disk size of the `.superblock` file: two 4 KiB copies (head + tail).
const SUPERBLOCK_FILE_SIZE: u64 = 2 * SUPERBLOCK_SIZE as u64;

/// A disk lifecycle failure.
#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    /// An underlying filesystem I/O error.
    #[error("disk I/O error: {0}")]
    Io(#[from] io::Error),
    /// No `.superblock` present: the disk is unformatted and must be registered.
    #[error("disk is not registered (no superblock at {0})")]
    Unregistered(PathBuf),
    /// Both superblock copies failed to decode (torn/rotted beyond recovery).
    #[error("both superblock copies are unreadable; head copy: {0}")]
    SuperblockUnreadable(SuperblockError),
    /// The superblock belongs to a different cluster (guards against misplugging).
    #[error("cluster id mismatch: superblock {found:#034x}, expected {expected:#034x}")]
    ClusterMismatch {
        /// Cluster id stored on the disk.
        found: u128,
        /// Cluster id this node expected.
        expected: u128,
    },
}

/// A loaded, identity-validated disk and its directory layout.
#[derive(Debug)]
pub struct Disk {
    root: PathBuf,
    superblock: Superblock,
}

impl Disk {
    /// Formats `root` as a fresh epochIO disk: creates the directory layout and
    /// writes both superblock copies. **Destructive** — overwrites any existing
    /// superblock; callers register a disk only after [`load`](Self::load)
    /// returns [`DiskError::Unregistered`].
    ///
    /// # Errors
    ///
    /// Propagates filesystem errors from directory creation or the superblock write.
    pub fn format(root: impl AsRef<Path>, superblock: Superblock) -> Result<Self, DiskError> {
        let root = root.as_ref().to_path_buf();
        for dir in [
            &root,
            &root.join(EXTENTS_DIR),
            &root.join(INDEX_DIR),
            &root.join(TRASH_DIR),
        ] {
            fs::create_dir_all(dir)?;
        }
        write_superblock(&root.join(SUPERBLOCK_FILE), &superblock)?;
        Ok(Self { root, superblock })
    }

    /// Loads an already-registered disk at `root`, validating that its
    /// superblock belongs to `expected_cluster_id`.
    ///
    /// # Errors
    ///
    /// - [`DiskError::Unregistered`] if no superblock file exists;
    /// - [`DiskError::SuperblockUnreadable`] if both copies fail to decode;
    /// - [`DiskError::ClusterMismatch`] if the disk belongs to another cluster.
    pub fn load(root: impl AsRef<Path>, expected_cluster_id: u128) -> Result<Self, DiskError> {
        let root = root.as_ref().to_path_buf();
        let sb_path = root.join(SUPERBLOCK_FILE);
        if !sb_path.exists() {
            return Err(DiskError::Unregistered(sb_path));
        }
        let superblock = read_superblock(&sb_path)?;
        if superblock.cluster_id != expected_cluster_id {
            return Err(DiskError::ClusterMismatch {
                found: superblock.cluster_id,
                expected: expected_cluster_id,
            });
        }
        Ok(Self { root, superblock })
    }

    /// The validated superblock (disk identity and layout).
    #[must_use]
    pub fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    /// This disk's id.
    #[must_use]
    pub fn disk_id(&self) -> DiskId {
        self.superblock.disk_id
    }

    /// The disk root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Directory holding one file per extent.
    #[must_use]
    pub fn extents_dir(&self) -> PathBuf {
        self.root.join(EXTENTS_DIR)
    }

    /// Path of the extent file named by `extent_id` in lowercase hex (02 §1.1).
    #[must_use]
    pub fn extent_path(&self, extent_id: ExtentId) -> PathBuf {
        self.extents_dir().join(extent_file_name(extent_id))
    }

    /// Moves an extent file from `extents/` into `.trash/`, keeping its name.
    /// Physical deletion happens after the protection window (02 §1.1/§1.5);
    /// compaction and scrub use this to retire a superseded/orphaned extent.
    ///
    /// # Errors
    ///
    /// [`DiskError::Io`] if the rename fails (e.g. the source is absent).
    pub fn move_extent_to_trash(&self, extent_id: ExtentId) -> Result<(), DiskError> {
        let from = self.extent_path(extent_id);
        let to = self.trash_dir().join(extent_file_name(extent_id));
        fs::rename(from, to)?;
        Ok(())
    }

    /// Lists the paths of all extent files under `extents/` (order unspecified).
    /// The storage engine opens each one at startup and validates the header;
    /// non-file entries are skipped.
    ///
    /// # Errors
    ///
    /// [`DiskError::Io`] if the extents directory cannot be enumerated.
    pub fn list_extent_paths(&self) -> Result<Vec<PathBuf>, DiskError> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(self.extents_dir())? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
        Ok(paths)
    }

    /// Directory holding the per-disk RocksDB index.
    #[must_use]
    pub fn index_dir(&self) -> PathBuf {
        self.root.join(INDEX_DIR)
    }

    /// Directory holding extents awaiting physical deletion.
    #[must_use]
    pub fn trash_dir(&self) -> PathBuf {
        self.root.join(TRASH_DIR)
    }

    /// Capacity of the filesystem hosting this disk: `(total, free, used)` in
    /// bytes from `statvfs` (Linux production). Best-effort telemetry for the
    /// PD heartbeat; zeros on non-Linux (macOS dev) or a stat failure.
    #[must_use]
    pub fn space_stats(&self) -> (u64, u64, u64) {
        space_stats(&self.root)
    }
}

/// The lowercase-hex file name of an extent (02 §1.1): the 16 raw id bytes.
fn extent_file_name(extent_id: ExtentId) -> String {
    extent_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Writes both superblock copies crash-safely: the tail copy first (fsync), then
/// the head copy (fsync). A crash between the two leaves the tail intact, and a
/// torn head is detected by its checksum and superseded by the tail on read.
fn write_superblock(path: &Path, superblock: &Superblock) -> Result<(), DiskError> {
    let bytes = superblock.encode();
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    file.set_len(SUPERBLOCK_FILE_SIZE)?;

    file.seek(SeekFrom::Start(SUPERBLOCK_SIZE as u64))?;
    file.write_all(&bytes)?;
    file.sync_all()?;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Reads the superblock, preferring the head copy and falling back to the tail
/// copy if the head is unreadable.
fn read_superblock(path: &Path) -> Result<Superblock, DiskError> {
    let data = fs::read(path)?;
    match Superblock::decode(&data) {
        Ok(sb) => Ok(sb),
        Err(head_err) => data
            .get(SUPERBLOCK_SIZE..)
            .and_then(|tail| Superblock::decode(tail).ok())
            .ok_or(DiskError::SuperblockUnreadable(head_err)),
    }
}

/// Capacity of the filesystem at `root`: `(total, free, used)` in bytes from
/// `statvfs(2)` (Linux). Best-effort telemetry for the PD heartbeat; zeros on
/// non-Linux (macOS dev) or a stat failure.
#[must_use]
pub fn space_stats(root: &Path) -> (u64, u64, u64) {
    space_stats_impl(root)
}

/// `statvfs(2)` for the disk's filesystem (Linux): `(total, free, used)` bytes.
/// Errors degrade to zeros — heartbeat capacity is telemetry, never a gate.
#[cfg(target_os = "linux")]
fn space_stats_impl(root: &Path) -> (u64, u64, u64) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = CString::new(root.as_os_str().as_bytes()) else {
        return (0, 0, 0);
    };
    // SAFETY: `stat` is a valid statvfs out-buffer; `path` is a valid C string.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
        return (0, 0, 0);
    }
    // `f_blocks`/`f_bavail`/`f_frsize` are all `fsblkcnt_t`/`c_ulong` = u64 on
    // the supported target (x86_64 Linux, 99 v0.12 Linux-first), so the products
    // need no widening conversion.
    let total = stat.f_blocks * stat.f_frsize;
    let free = stat.f_bavail * stat.f_frsize;
    (total, free, total.saturating_sub(free))
}

/// `statfs(2)` for the disk's filesystem (macOS dev): `(total, free, used)`
/// bytes via `f_bsize` (no `f_frsize` on this platform).
#[cfg(target_os = "macos")]
fn space_stats_impl(root: &Path) -> (u64, u64, u64) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = CString::new(root.as_os_str().as_bytes()) else {
        return (0, 0, 0);
    };
    // SAFETY: `stat` is a valid statfs out-buffer; `path` is a valid C string.
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(path.as_ptr(), &mut stat) } != 0 {
        return (0, 0, 0);
    }
    let total = stat.f_blocks * u64::from(stat.f_bsize);
    let free = stat.f_bavail * u64::from(stat.f_bsize);
    (total, free, total.saturating_sub(free))
}

/// Non-Linux/macOS fallback: no statfs wired; heartbeat reports zeros.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn space_stats_impl(_root: &Path) -> (u64, u64, u64) {
    (0, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLUSTER: u128 = 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10;

    fn sample_superblock() -> Superblock {
        Superblock {
            disk_id: DiskId::new(7),
            cluster_id: CLUSTER,
            created_at: 1_700_000_000,
            flags: 0,
            extent_size: epoch_proto::consts::DEFAULT_EXTENT_SIZE,
        }
    }

    #[test]
    fn format_then_load_round_trips_identity_and_layout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let formatted = Disk::format(dir.path(), sample_superblock()).expect("format");
        assert!(formatted.extents_dir().is_dir());
        assert!(formatted.index_dir().is_dir());
        assert!(formatted.trash_dir().is_dir());

        let loaded = Disk::load(dir.path(), CLUSTER).expect("load");
        assert_eq!(loaded.disk_id(), DiskId::new(7));
        assert_eq!(*loaded.superblock(), sample_superblock());
    }

    #[test]
    fn load_unregistered_disk_reports_unregistered() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            Disk::load(dir.path(), CLUSTER),
            Err(DiskError::Unregistered(_))
        ));
    }

    #[test]
    fn load_rejects_foreign_cluster() {
        let dir = tempfile::tempdir().expect("tempdir");
        Disk::format(dir.path(), sample_superblock()).expect("format");
        assert!(matches!(
            Disk::load(dir.path(), CLUSTER ^ 1),
            Err(DiskError::ClusterMismatch { expected, .. }) if expected == CLUSTER ^ 1
        ));
    }

    #[test]
    fn corrupt_head_copy_falls_back_to_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        Disk::format(dir.path(), sample_superblock()).expect("format");

        // Clobber the head copy's magic; the tail copy must still load.
        let sb_path = dir.path().join(SUPERBLOCK_FILE);
        let mut data = fs::read(&sb_path).expect("read");
        data[0..8].fill(0xFF);
        fs::write(&sb_path, &data).expect("write");

        let loaded = Disk::load(dir.path(), CLUSTER).expect("load via tail");
        assert_eq!(*loaded.superblock(), sample_superblock());
    }

    #[test]
    fn both_copies_corrupt_is_unreadable() {
        let dir = tempfile::tempdir().expect("tempdir");
        Disk::format(dir.path(), sample_superblock()).expect("format");

        let sb_path = dir.path().join(SUPERBLOCK_FILE);
        let mut data = fs::read(&sb_path).expect("read");
        data[0..8].fill(0xFF); // head magic
        data[SUPERBLOCK_SIZE..SUPERBLOCK_SIZE + 8].fill(0xFF); // tail magic
        fs::write(&sb_path, &data).expect("write");

        assert!(matches!(
            Disk::load(dir.path(), CLUSTER),
            Err(DiskError::SuperblockUnreadable(_))
        ));
    }

    #[test]
    fn list_extent_paths_returns_only_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = Disk::format(dir.path(), sample_superblock()).expect("format");
        assert!(disk.list_extent_paths().expect("empty").is_empty());

        let extent_id = ExtentId::new(epoch_proto::ShardId::from_raw(1), 1);
        let path = disk.extent_path(extent_id);
        fs::write(&path, b"x").expect("write extent file");
        fs::create_dir(disk.extents_dir().join("subdir")).expect("mkdir");

        let listed = disk.list_extent_paths().expect("list");
        assert_eq!(listed, vec![path]);
    }

    #[test]
    fn move_extent_to_trash_relocates_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let disk = Disk::format(dir.path(), sample_superblock()).expect("format");
        let extent_id = ExtentId::new(epoch_proto::ShardId::from_raw(1), 1);
        let path = disk.extent_path(extent_id);
        fs::write(&path, b"data").expect("write extent file");

        disk.move_extent_to_trash(extent_id).expect("trash");
        assert!(!path.exists(), "source removed from extents/");
        let name = path.file_name().expect("name");
        assert!(disk.trash_dir().join(name).exists(), "present in .trash/");
    }
}
