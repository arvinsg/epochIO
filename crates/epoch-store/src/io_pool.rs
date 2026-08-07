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

//! Per-disk read fd cache: a bounded LRU of open, read-only [`ExtentFile`]
//! handles shared across the data plane's read path (02 §1.3). Opening an extent
//! file per read costs an `open()` syscall plus a header read + CRC check; the
//! pool amortizes that across the many blobs a hot shard serves while capping
//! the disk's open descriptors.
//!
//! Handles are shared as `Arc<ExtentFile>` so a read performs its positioned
//! record read *outside* the pool lock (the lock only guards the small
//! lookup/insert). Positioned reads never move a shared cursor, so one handle
//! serves concurrent readers safely, and it also coexists with the extent's
//! writer thread (a separate fd appending past the committed tail, 02 §1.7).
//!
//! Invalidation contract: any operation that trashes or rebinds an extent
//! (compaction, reclamation) must [`FdPool::evict`] it, otherwise a later
//! by-extent-id read could serve a cached handle to a superseded file. Reads by
//! shard are unaffected — they resolve the current binding before touching the
//! pool.
//!
//! Design: docs/design/02-datanode.md §1.3/§1.7; docs/design/07-iteration-plan.md (M3)

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use epoch_proto::ExtentId;

use crate::error::StoreError;
use crate::extent::file::{ExtentError, ExtentFile};

/// Default per-disk read-fd budget (02 §1.3). Sized well under a typical
/// process fd rlimit while covering the hot working set of a busy disk.
pub(crate) const DEFAULT_READ_FD_CAPACITY: usize = 600;

/// Opens an extent file read-only, mapping a missing file to
/// [`StoreError::ExtentNotFound`] (the extent was never created or has been
/// trashed) rather than a raw I/O error. Shared by the pool and the owned-open
/// maintenance paths (compaction/scrub) so the mapping lives in one place.
pub(crate) fn open_read(extent_id: ExtentId, path: &Path) -> Result<ExtentFile, StoreError> {
    match ExtentFile::open(path) {
        Ok(extent) => Ok(extent),
        Err(ExtentError::Io(err)) if err.kind() == io::ErrorKind::NotFound => {
            Err(StoreError::ExtentNotFound(extent_id))
        }
        Err(err) => Err(err.into()),
    }
}

/// A bounded LRU cache of read-only extent handles for one disk.
#[derive(Debug)]
pub(crate) struct FdPool {
    capacity: usize,
    inner: StdMutex<PoolInner>,
}

#[derive(Debug)]
struct PoolInner {
    handles: HashMap<ExtentId, Entry>,
    /// Monotonic access clock; the entry with the smallest stamp is the LRU.
    tick: u64,
}

#[derive(Debug)]
struct Entry {
    file: Arc<ExtentFile>,
    last_used: u64,
}

impl FdPool {
    /// A pool holding at most `capacity` open handles.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero (a zero-capacity cache cannot hold the
    /// handle it just opened — a configuration bug, not a runtime condition).
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "read fd pool capacity must be non-zero");
        Self {
            capacity,
            inner: StdMutex::new(PoolInner {
                handles: HashMap::new(),
                tick: 0,
            }),
        }
    }

    /// Returns the cached handle for `extent_id`, opening (and caching) it from
    /// `path` on a miss. The record read the caller performs on the returned
    /// handle happens outside the pool lock.
    ///
    /// # Errors
    ///
    /// [`StoreError::ExtentNotFound`] if the file is absent, or
    /// [`StoreError::Extent`] on a header/open failure.
    pub(crate) fn get_or_open(
        &self,
        extent_id: ExtentId,
        path: &Path,
    ) -> Result<Arc<ExtentFile>, StoreError> {
        if let Some(file) = self.touch(extent_id) {
            return Ok(file);
        }
        // Miss: open outside the lock. A concurrent miss for the same id may
        // also open; the later insert wins and the extra handle is dropped when
        // its caller finishes — a rare, harmless duplicate open.
        let file = Arc::new(open_read(extent_id, path)?);
        self.insert(extent_id, Arc::clone(&file));
        Ok(file)
    }

    /// Drops any cached handle for `extent_id`. Called when an extent is trashed
    /// or rebound so a later by-id read reopens instead of serving a stale file.
    pub(crate) fn evict(&self, extent_id: ExtentId) {
        self.lock().handles.remove(&extent_id);
    }

    fn touch(&self, extent_id: ExtentId) -> Option<Arc<ExtentFile>> {
        let mut inner = self.lock();
        let tick = inner.next_tick();
        let entry = inner.handles.get_mut(&extent_id)?;
        entry.last_used = tick;
        Some(Arc::clone(&entry.file))
    }

    fn insert(&self, extent_id: ExtentId, file: Arc<ExtentFile>) {
        let mut inner = self.lock();
        let tick = inner.next_tick();
        inner.handles.insert(
            extent_id,
            Entry {
                file,
                last_used: tick,
            },
        );
        if inner.handles.len() > self.capacity {
            inner.evict_lru();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PoolInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().handles.len()
    }

    #[cfg(test)]
    fn contains(&self, extent_id: ExtentId) -> bool {
        self.lock().handles.contains_key(&extent_id)
    }
}

impl PoolInner {
    fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }

    /// Removes the least-recently-used entry. The scan is O(capacity) but runs
    /// only on an over-capacity insert, whose accompanying `open()` syscall
    /// dominates the cost.
    fn evict_lru(&mut self) {
        let victim = self
            .handles
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(id, _)| *id);
        if let Some(id) = victim {
            self.handles.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::Disk;
    use crate::superblock::Superblock;
    use crate::testutil::CLUSTER;
    use epoch_proto::consts::DEFAULT_EXTENT_SIZE;
    use epoch_proto::{ChunkId, DiskId, ShardId};
    use tempfile::TempDir;

    fn extent(seq: i64) -> ExtentId {
        ExtentId::new(ShardId::new(ChunkId::new(1), 0, 0), seq)
    }

    /// A formatted disk with `count` real extent files, ids `extent(0..count)`.
    fn disk_with_extents(count: i64) -> (TempDir, Disk, Vec<ExtentId>) {
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
        let ids: Vec<ExtentId> = (0..count).map(extent).collect();
        for id in &ids {
            ExtentFile::create(disk.extent_path(*id), *id).expect("create");
        }
        (dir, disk, ids)
    }

    #[test]
    fn open_read_maps_missing_file_to_extent_not_found() {
        let (_dir, disk, _ids) = disk_with_extents(0);
        let ghost = extent(99);
        assert!(matches!(
            open_read(ghost, &disk.extent_path(ghost)),
            Err(StoreError::ExtentNotFound(id)) if id == ghost
        ));
    }

    #[test]
    fn repeated_get_reuses_one_cached_handle() {
        let (_dir, disk, ids) = disk_with_extents(1);
        let pool = FdPool::new(4);
        let a = pool
            .get_or_open(ids[0], &disk.extent_path(ids[0]))
            .expect("open");
        let b = pool
            .get_or_open(ids[0], &disk.extent_path(ids[0]))
            .expect("open");
        assert_eq!(pool.len(), 1, "second get is a cache hit");
        assert!(Arc::ptr_eq(&a, &b), "same underlying handle is shared");
    }

    #[test]
    fn insert_over_capacity_evicts_least_recently_used() {
        let (_dir, disk, ids) = disk_with_extents(3);
        let pool = FdPool::new(2);
        pool.get_or_open(ids[0], &disk.extent_path(ids[0]))
            .expect("open 0");
        pool.get_or_open(ids[1], &disk.extent_path(ids[1]))
            .expect("open 1");
        // Touch id0 so id1 becomes the least-recently-used, then overflow.
        pool.get_or_open(ids[0], &disk.extent_path(ids[0]))
            .expect("touch 0");
        pool.get_or_open(ids[2], &disk.extent_path(ids[2]))
            .expect("open 2");

        assert_eq!(pool.len(), 2);
        assert!(pool.contains(ids[0]), "recently used kept");
        assert!(pool.contains(ids[2]), "newest kept");
        assert!(!pool.contains(ids[1]), "LRU evicted");
    }

    #[test]
    fn evict_drops_the_cached_handle() {
        let (_dir, disk, ids) = disk_with_extents(1);
        let pool = FdPool::new(4);
        pool.get_or_open(ids[0], &disk.extent_path(ids[0]))
            .expect("open");
        assert!(pool.contains(ids[0]));
        pool.evict(ids[0]);
        assert!(!pool.contains(ids[0]));
        assert_eq!(pool.len(), 0);
    }
}
