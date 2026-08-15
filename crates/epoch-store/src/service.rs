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

//! `StorageEngine`: the per-disk local API the DataNode data plane drives. It
//! owns the disk, the per-disk index, and one dedicated writer thread per
//! writable extent, plus the startup crash-recovery reconciliation.
//!
//! Concurrency model (M3): the engine is `Clone` (an `Arc` handle) and every
//! method is `&self`, so the async RPC layer shares one engine across
//! connections. Because appending needs `&mut ExtentFile`, each writable extent
//! is served by a single [`writer`] thread; writes are sent over a bounded
//! channel and awaited, giving per-shard ordering and group commit without a
//! global lock. Reads and maintenance (create/read/compact/scrub/reclaim) run on
//! `spawn_blocking` threads so no filesystem or RocksDB call ever blocks the
//! async executor (§5). Reads open the extent file on demand (a per-disk fd pool
//! replaces this in a later step).
//!
//! Errors returned to the data plane are the cross-component [`EpochError`]
//! (rejections keep their meaning for gateway cache invalidation, 02 §2.1);
//! [`StorageEngine::open`] still returns the local [`StoreError`] since startup
//! is not a wire boundary.
//!
//! Startup recovery (02 §1.7) reconciles each extent file against the index:
//!   - index present  → the index is the commit authority; a synced-but-
//!     uncommitted torn tail past the committed size is truncated away;
//!   - index absent   → the extent is rebuilt from its self-describing record
//!     stream (02 §1.2). Tombstones live only in the index and are therefore
//!     lost on rebuild (resurrecting logically-deleted blobs); GC reconciles
//!     this later (02 §1.6). Sealed/Full status is likewise not stream-derivable
//!     and a rebuilt extent returns to `Writable`.
//!
//! Only writable, currently-bound extents get a writer thread; others (sealed,
//! dropped, or unbound compaction orphans) stay closed and are read on demand or
//! reclaimed by maintenance.
//!
//! Design: docs/design/02-datanode.md §1.4/§1.5/§1.7; docs/design/07-iteration-plan.md (M3)

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use epoch_proto::{BlobId, EpochError, ExtentId, ShardId};
use tokio::sync::{mpsc, oneshot};

use crate::compact::{self, CompactOutcome};
use crate::disk::Disk;
use crate::error::StoreError;
use crate::extent::file::{ExtentError, ExtentFile};
use crate::extent::state::ExtentStatus;
use crate::index::{BlobIndex, DiskIndex, ExtentMeta};
use crate::io_pool::{self, FdPool};
use crate::qos::{IoClass, Qos, QosConfig};
use crate::read::read_blob;
use crate::scrub::{self, ScrubReport};
use crate::write::{self, WriteOutcome};
use crate::writer::{self, WriteRequest};

/// A running per-extent writer: the channel the engine feeds it and the thread
/// handle used to stop and join it.
#[derive(Debug)]
struct WriterEntry {
    sender: mpsc::Sender<WriteRequest>,
    handle: JoinHandle<()>,
}

/// Shared engine state behind the [`StorageEngine`] handle.
#[derive(Debug)]
struct EngineInner {
    disk: Disk,
    index: Arc<DiskIndex>,
    /// Set once an I/O fault trips this disk to Broken (02 §1.8). Sticky: a
    /// broken disk stays broken until PD detaches it and repair reprovisions.
    /// The heartbeat reads it so PD transitions `DiskStatus::Broken` and starts
    /// a RepairDisk job.
    broken: std::sync::atomic::AtomicBool,
    /// Live writer threads, keyed by the extent each one owns.
    writers: StdMutex<HashMap<ExtentId, WriterEntry>>,
    /// Bounded LRU of read-only extent handles for the read path.
    read_pool: FdPool,
    /// Per-disk QoS limiters, shared serially by background maintenance ops.
    qos: StdMutex<Qos>,
    /// Per-shard exclusion between the delete path and compaction's commit
    /// section (both rare): a delete resolves binding + tombstones under this
    /// lock, and compaction holds it across its tombstone re-scan + rebind, so
    /// no tombstone can slip between the re-scan and the swap (02 §1.6
    /// INVARIANT in `crate::compact`). Sharded by `shard_prefix` to keep the
    /// map bounded; never held across `.await` (blocking sections only).
    shard_delete_locks: [StdMutex<()>; SHARD_LOCK_SHARDS],
}

/// Shard-lock striping width (power of two; deletes and compactions are rare,
/// contention is per-shard correctness only, not throughput).
const SHARD_LOCK_SHARDS: usize = 16;

impl EngineInner {
    /// The stripe lock guarding deletes/compaction-commit for `shard`.
    fn shard_delete_lock(&self, shard: ShardId) -> &StdMutex<()> {
        let stripe = (shard.shard_prefix() as usize) % SHARD_LOCK_SHARDS;
        &self.shard_delete_locks[stripe]
    }
}

/// The single-disk storage engine handle: a cheap `Arc` clone shared across the
/// data plane. Owns the disk, its index, and every per-extent writer thread.
#[derive(Debug, Clone)]
pub struct StorageEngine {
    inner: Arc<EngineInner>,
}

impl StorageEngine {
    /// Opens the engine on an already-registered disk at `root`, validating the
    /// disk's cluster identity, opening the index, recovering every extent file
    /// (02 §1.7), then spawning a writer thread for each writable, bound extent.
    ///
    /// This is a synchronous startup path (it performs blocking recovery I/O and
    /// is called before the data plane starts serving); it returns the local
    /// [`StoreError`] rather than the wire error.
    ///
    /// # Errors
    ///
    /// - [`StoreError::Disk`] if the disk is unregistered, foreign, or its
    ///   layout cannot be enumerated;
    /// - [`StoreError::Index`] if the index cannot be opened;
    /// - [`StoreError::Extent`] if an extent header is invalid or recovery I/O
    ///   fails.
    pub fn open(root: impl AsRef<Path>, cluster_id: u128) -> Result<Self, StoreError> {
        Self::open_with_qos(root, cluster_id, QosConfig::default())
    }

    /// Opens the engine with explicit QoS byte-rate budgets (02 §1.7). The budgets
    /// come from cluster config (the repair budget is the MTTR knob, 01 §6.3);
    /// [`open`](Self::open) defaults them to unlimited.
    ///
    /// # Errors
    ///
    /// As [`open`](Self::open).
    pub fn open_with_qos(
        root: impl AsRef<Path>,
        cluster_id: u128,
        qos: QosConfig,
    ) -> Result<Self, StoreError> {
        let disk = Disk::load(root, cluster_id)?;
        let index = Arc::new(DiskIndex::open(&disk.index_dir())?);
        let mut recovered = Vec::new();
        for path in disk.list_extent_paths()? {
            let mut extent = match ExtentFile::open(&path) {
                Ok(extent) => extent,
                // A file that fails to decode is quarantined (02 §1.9): one bad
                // file must not block the whole disk from coming online.
                // Genuine I/O errors still abort startup as disk-failure
                // evidence (02 §1.8).
                Err(err) if is_format_error(&err) => {
                    quarantine(&disk, &path, &err)?;
                    continue;
                }
                Err(err) => return Err(err.into()),
            };
            recover_extent(&index, &mut extent)?;
            recovered.push(extent);
        }
        let inner = Arc::new(EngineInner {
            disk,
            index,
            broken: std::sync::atomic::AtomicBool::new(false),
            writers: StdMutex::new(HashMap::new()),
            read_pool: FdPool::new(io_pool::DEFAULT_READ_FD_CAPACITY),
            qos: StdMutex::new(Qos::new(&qos)),
            shard_delete_locks: Default::default(),
        });
        inner.spawn_writers_for(recovered)?;
        Ok(Self { inner })
    }

    /// Whether this disk has tripped to Broken on an I/O fault (02 §1.8). The
    /// heartbeat reports this so PD transitions the disk and starts repair.
    #[must_use]
    pub fn is_broken(&self) -> bool {
        self.inner.broken.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Trips this disk to Broken (sticky). Called when an I/O fault surfaces as
    /// [`EpochError::DiskBroken`]; idempotent.
    pub fn mark_broken(&self) {
        if !self
            .inner
            .broken
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            tracing::error!(
                disk = self.inner.disk.disk_id().get(),
                "disk tripped to broken"
            );
        }
    }

    /// Observes an operation result: trips the disk to Broken on a
    /// [`EpochError::DiskBroken`] fault, then returns the result unchanged.
    /// Engine write/create paths funnel their result through this so a single
    /// I/O fault flips the broken flag the heartbeat reports.
    ///
    /// INVARIANT(design 02 §1.7): only `DiskBroken` trips the flag.
    /// [`EpochError::OutOfSpace`] must not — a full disk is healthy hardware, and
    /// marking it broken would make PD start a RepairDisk job that needs the very
    /// capacity that ran out. When a cluster fills, every disk would "fail" at
    /// once and repair traffic would compound the exhaustion.
    fn observe<T>(&self, result: Result<T, EpochError>) -> Result<T, EpochError> {
        if matches!(result, Err(EpochError::DiskBroken)) {
            self.mark_broken();
        }
        result
    }

    /// The disk this engine manages (identity and directory layout).
    #[must_use]
    pub fn disk(&self) -> &Disk {
        &self.inner.disk
    }

    /// The number of extents currently accepting writes (live writer threads) —
    /// the watermark PD's placement loop targets (01 §4.1).
    #[must_use]
    pub fn writable_extent_count(&self) -> usize {
        self.inner
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Every extent id on this disk (GcRound self-scan enumeration, 01 §6.3).
    /// The GC daemon lists each extent's live blobs and diffs them against the
    /// reference keep-set.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] if the index cannot be enumerated.
    pub fn list_extent_ids(&self) -> Result<Vec<ExtentId>, EpochError> {
        self.inner
            .index
            .list_extents()
            .map(|extents| extents.into_iter().map(|(id, _)| id).collect())
            .map_err(|_| EpochError::Internal)
    }

    /// Creates a fresh writable extent bound to `shard`, spawns its writer, and
    /// returns its id (the CREATE_EXTENT data-plane op), using a caller-chosen
    /// creation timestamp so PD-driven chunk creation is deterministic
    /// (01 §4.1). Metadata and binding are installed atomically (02 §1.7).
    ///
    /// Idempotent for a retried identical id: an already-installed extent is a
    /// no-op success; a half-created one (file without index) is orphaned and
    /// recreated.
    ///
    /// # Errors
    ///
    /// [`EpochError::DiskBroken`] if the extent file cannot be created,
    /// [`EpochError::Internal`] on an index failure.
    pub async fn create_extent_at(
        &self,
        shard: ShardId,
        create_ts: i64,
    ) -> Result<ExtentId, EpochError> {
        let inner = Arc::clone(&self.inner);
        self.observe(run_blocking(move || inner.create_extent_at_blocking(shard, create_ts)).await)
    }

    /// Creates a fresh writable extent bound to `shard` with a local timestamp
    /// (local provisioning; PD-driven creation goes through [`create_extent_at`]).
    ///
    /// # Errors
    ///
    /// As [`create_extent_at`](Self::create_extent_at).
    pub async fn create_extent(&self, shard: ShardId) -> Result<ExtentId, EpochError> {
        self.create_extent_at(shard, now_ts_nanos()).await
    }

    /// Creates a `Rebuilding` extent bound to `shard` (02 §1.5): a repair target
    /// that receives rebuilt blobs by explicit extent id but is never a
    /// foreground write target. Promote it with [`promote_rebuilt`] once the
    /// rebuild is complete.
    ///
    /// [`promote_rebuilt`]: Self::promote_rebuilt
    ///
    /// # Errors
    ///
    /// As [`create_extent_at`](Self::create_extent_at).
    pub async fn create_rebuilding_extent(
        &self,
        shard: ShardId,
        create_ts: i64,
    ) -> Result<ExtentId, EpochError> {
        let inner = Arc::clone(&self.inner);
        self.observe(
            run_blocking(move || {
                inner.create_extent_with_status(shard, create_ts, ExtentStatus::Rebuilding)
            })
            .await,
        )
    }

    /// Promotes a fully-rebuilt extent from `Rebuilding` to `Writable` (02 §1.5),
    /// so it can serve reads/writes once PD rebinds the shard to it. Idempotent
    /// on an already-`Writable` extent.
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the extent has no metadata; storage
    /// faults mapped to the wire.
    pub async fn promote_rebuilt(&self, extent_id: ExtentId) -> Result<(), EpochError> {
        let inner = Arc::clone(&self.inner);
        self.observe(run_blocking(move || inner.promote_rebuilt_blocking(extent_id)).await)
    }

    /// Resolves the writable extent currently bound to `shard` (used by the
    /// handler's OPEN to target a blob write). Rejects a shard with no bound
    /// extent or a non-writable one.
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the shard is unbound,
    /// [`EpochError::Sealed`] / [`EpochError::ChunkFull`] /
    /// [`EpochError::Rebuilding`] if the bound extent does not accept writes.
    pub async fn writable_extent(&self, shard: ShardId) -> Result<ExtentId, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.writable_extent_blocking(shard)).await
    }

    /// Meters `bytes` of IO through the `class` rate limiter, blocking (on the
    /// blocking pool) just long enough to stay within the class budget (02 §1.7).
    /// A no-op for an unlimited class. Repair/migrate paths call this before
    /// writing a rebuilt shard so a rebuild storm cannot starve foreground IO
    /// (01 §6.3 MTTR budget = the Repair class rate).
    pub async fn throttle(&self, class: IoClass, bytes: u64) {
        let inner = Arc::clone(&self.inner);
        let _ = run_blocking(move || {
            let start = std::time::Instant::now();
            {
                let mut qos = inner.qos.lock().unwrap_or_else(PoisonError::into_inner);
                qos.limiter(class).throttle(bytes);
            }
            // Record the class's IO volume + any time the limiter blocked
            // (08 §5.1 store counters). The write itself follows on the caller.
            crate::metrics::record_io(class, "write", bytes);
            crate::metrics::record_throttle_wait(class, start.elapsed().as_secs_f64());
            Ok::<(), StoreError>(())
        })
        .await;
    }

    /// Seals `extent_id`: drains its in-flight writes (by stopping and joining
    /// the writer thread) then durably transitions it to `Sealed`, so further
    /// OPENs and writes are rejected while reads continue (01 §4.2 / Q17).
    /// Idempotent — an already-sealed extent succeeds; a `Rebuilding` /
    /// `Dropped` extent is not sealable.
    ///
    /// The drain is to completion (the writer finishes its queued batch before
    /// the seal commits); a bounded "seal within one blob-write timeout then
    /// force" grace is a refinement deferred until a stuck-writer watchdog
    /// exists (a stuck append surfaces as `DiskBroken` on its own path).
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the extent is unknown,
    /// [`EpochError::Rebuilding`] if it is still rebuilding, and
    /// [`EpochError::Internal`] on an index failure.
    pub async fn seal_extent(&self, extent_id: ExtentId) -> Result<(), EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.seal_extent_blocking(extent_id)).await
    }

    /// Writes one blob shard `body` into `extent_id` (idempotent by blob id,
    /// 02 §1.4) by handing it to the extent's writer thread and awaiting the
    /// group-commit reply.
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the extent has no live writer, plus the
    /// write rejections / faults surfaced by the writer (`Sealed`, `ChunkFull`,
    /// `DiskBroken`, `Internal`).
    pub async fn write(
        &self,
        extent_id: ExtentId,
        blob_id: BlobId,
        body: Bytes,
    ) -> Result<WriteOutcome, EpochError> {
        let sender = {
            let writers = self
                .inner
                .writers
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            writers.get(&extent_id).map(|entry| entry.sender.clone())
        };
        let Some(sender) = sender else {
            // No live writer ⇒ this extent is not an active writable target.
            return Err(EpochError::ShardNotFound);
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = WriteRequest {
            blob_id,
            body,
            reply: reply_tx,
        };
        if sender.send(request).await.is_err() {
            return Err(EpochError::Internal);
        }
        self.observe(reply_rx.await.unwrap_or(Err(EpochError::Internal)))
    }

    /// Reads a blob shard from a named `extent_id`, or `None` if not indexed.
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the extent file is absent, plus the read
    /// faults from [`read_blob`] mapped to the wire.
    pub async fn read(
        &self,
        extent_id: ExtentId,
        blob_id: BlobId,
    ) -> Result<Option<Vec<u8>>, EpochError> {
        let inner = Arc::clone(&self.inner);
        let start = std::time::Instant::now();
        let out = run_blocking(move || inner.read_blocking(extent_id, blob_id)).await;
        // Foreground read metering (08 §5.1): the read bytes + latency feed the
        // same store IO families the write path already records into.
        if let Ok(Some(body)) = &out {
            crate::metrics::record_io(IoClass::Foreground, "read", body.len() as u64);
        }
        crate::metrics::record_io_duration("read", start.elapsed().as_secs_f64());
        out
    }

    /// Reads a blob shard by `shard` slot: resolves the shard's current extent
    /// through the index binding (02 §1.3), then reads. Returns `None` if the
    /// shard is unbound or the blob is not indexed.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] on an index failure, [`EpochError::ShardNotFound`]
    /// if the binding points at an absent extent, plus read faults from
    /// [`read_blob`].
    pub async fn read_shard(
        &self,
        shard: ShardId,
        blob_id: BlobId,
    ) -> Result<Option<Vec<u8>>, EpochError> {
        let inner = Arc::clone(&self.inner);
        let start = std::time::Instant::now();
        let out = run_blocking(move || inner.read_shard_blocking(shard, blob_id)).await;
        if let Ok(Some(body)) = &out {
            crate::metrics::record_io(IoClass::Foreground, "read", body.len() as u64);
        }
        crate::metrics::record_io_duration("read", start.elapsed().as_secs_f64());
        out
    }

    /// Lists the live (non-tombstoned) blob ids in `extent_id`, ascending — the
    /// repair coordinator's enumeration of a surviving shard's blobs (01 §6.3).
    /// Tombstoned blobs are omitted so repair never resurrects deleted data
    /// (02 §1.6). An absent/empty extent yields an empty list.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] on an index failure.
    pub async fn list_live_blobs(&self, extent_id: ExtentId) -> Result<Vec<BlobId>, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || {
            let blobs = inner.index.list_blobs(extent_id)?;
            Ok(blobs
                .into_iter()
                .filter(|(_, idx)| !idx.is_tombstoned())
                .map(|(id, _)| id)
                .collect())
        })
        .await
    }

    /// Tombstones a blob (idempotent, 02 §1.6). Returns `true` if this call
    /// newly tombstoned it. The record stays physically readable until
    /// compaction.
    ///
    /// Safe against concurrent writers and compaction: `deleted_bytes` is a
    /// field-scoped merge (`crate::meta_merge`), and the shard's stripe lock
    /// orders the binding resolution + tombstone against compaction's rebind
    /// (02 §1.6 INVARIANT in `crate::compact`).
    ///
    /// INVARIANT(design 02 §1.6): this is the *only* delete entry point, because
    /// a caller may hold a **pre-compaction extent id** — the MetaNode deleter
    /// resolves placement from PD, which can race a rebind. The shard's current
    /// binding is resolved here rather than trusting the caller, or the tombstone
    /// lands on the extent compaction is about to drop and the delete is
    /// silently lost (the RPC still reports success and the delq record is
    /// consumed, so it is unrecoverable). Do not add a variant that takes the
    /// caller's extent id at face value.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] on an index failure.
    pub async fn delete(&self, extent_id: ExtentId, blob_id: BlobId) -> Result<bool, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.delete_blocking(extent_id, blob_id)).await
    }

    /// Lists the extents whose tombstoned fraction has reached
    /// `ratio_percent`, worst-first (02 §1.6 compaction trigger).
    ///
    /// The threshold decision stays here, next to [`compact::should_compact`], so
    /// the maintenance ticker never needs `ExtentMeta`. Worst-first ordering
    /// means a bounded per-round budget always reclaims the most space.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] on an index failure.
    pub fn list_compaction_candidates(
        &self,
        ratio_percent: u8,
    ) -> Result<Vec<ExtentId>, EpochError> {
        let mut candidates: Vec<(u64, ExtentId)> = self
            .inner
            .index
            .list_extents()
            .map_err(|_| EpochError::Internal)?
            .into_iter()
            .filter(|(_, meta)| compact::should_compact(meta, ratio_percent))
            .map(|(id, meta)| (meta.deleted_bytes, id))
            .collect();
        // Worst-first, then by id so a round is deterministic across replicas of
        // the same disk state (aids reproducing a bad round in tests).
        candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        Ok(candidates.into_iter().map(|(_, id)| id).collect())
    }

    /// Compacts `extent_id`, reclaiming tombstoned space by copying its live
    /// blobs into a fresh extent and rebinding the shard (02 §1.6). Stops the
    /// source's writer first, then hands the new extent its own writer. Returns
    /// the new extent id and reclamation stats.
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the extent is absent,
    /// [`EpochError`] mappings of the storage faults otherwise.
    pub async fn compact_extent(&self, extent_id: ExtentId) -> Result<CompactOutcome, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.compact_blocking(extent_id)).await
    }

    /// Scrubs `extent_id` for bitrot, returning which live blobs (if any) fail
    /// their integrity checks (02 §1.9). Throttled on the Background QoS class.
    ///
    /// # Errors
    ///
    /// [`EpochError::ShardNotFound`] if the extent file is absent, plus mapped
    /// storage faults from the scan.
    pub async fn scrub_extent(&self, extent_id: ExtentId) -> Result<ScrubReport, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.scrub_blocking(extent_id)).await
    }

    /// Reclaims every `Dropped` extent (compaction leftovers): moves files to
    /// `.trash` and clears index entries. Returns the reclaimed ids (02 §1.9).
    /// Assumes no live writer holds a `Dropped` extent (true: writers exist only
    /// for writable, bound extents).
    ///
    /// # Errors
    ///
    /// [`EpochError`] mappings of index / disk faults.
    pub async fn reclaim_dropped(&self) -> Result<Vec<ExtentId>, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.reclaim_dropped_blocking()).await
    }

    /// Reclaims every extent no shard binds to (compaction-crash orphans,
    /// 02 §1.6/§1.9). Returns the reclaimed ids. Assumes the orphans hold no live
    /// writer (true for the compaction-crash destinations M3 produces: they are
    /// created unbound and so never get a writer).
    ///
    /// # Errors
    ///
    /// [`EpochError`] mappings of index / disk faults.
    pub async fn reclaim_orphans(&self) -> Result<Vec<ExtentId>, EpochError> {
        let inner = Arc::clone(&self.inner);
        run_blocking(move || inner.reclaim_orphans_blocking()).await
    }

    /// Stops every writer thread (dropping its channel and joining it) so all
    /// extent files are released. Synchronous and quick — writers exit as soon
    /// as their queues drain. Committed data stays readable (reads reopen on
    /// demand); further writes find no writer until the engine is reopened.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl EngineInner {
    /// Spawns writer threads for the writable, currently-bound extents among
    /// `recovered`; the rest are dropped (files closed) — reads reopen them on
    /// demand and maintenance reclaims dropped/orphan extents.
    fn spawn_writers_for(&self, recovered: Vec<ExtentFile>) -> Result<(), StoreError> {
        for extent in recovered {
            let extent_id = extent.extent_id();
            let keep = match self.index.get_extent_meta(extent_id)? {
                Some(meta) => {
                    meta.status == ExtentStatus::Writable
                        && self.index.get_shard_binding(meta.shard_id)? == Some(extent_id)
                }
                None => false,
            };
            if keep {
                self.spawn_writer(extent_id, extent);
            }
        }
        Ok(())
    }

    /// Spawns and registers a writer thread owning `extent`.
    fn spawn_writer(&self, extent_id: ExtentId, extent: ExtentFile) {
        let (sender, handle) = writer::spawn(Arc::clone(&self.index), extent);
        self.writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(extent_id, WriterEntry { sender, handle });
    }

    /// Stops the writer for `extent_id` if present: drops its channel (so it
    /// drains and exits) and joins it, releasing the file handle. A no-op if the
    /// extent has no writer.
    fn stop_writer(&self, extent_id: ExtentId) {
        let entry = self
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&extent_id);
        if let Some(WriterEntry { sender, handle }) = entry {
            drop(sender);
            let _ = handle.join();
        }
    }

    fn shutdown(&self) {
        let entries: Vec<WriterEntry> = self
            .writers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, entry)| entry)
            .collect();
        // Drop every channel first so all writers begin draining, then join.
        let handles: Vec<JoinHandle<()>> = entries
            .into_iter()
            .map(|WriterEntry { sender, handle }| {
                drop(sender);
                handle
            })
            .collect();
        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Reads a blob from an extent's cached (or freshly opened) read handle.
    fn read_blocking(
        &self,
        extent_id: ExtentId,
        blob_id: BlobId,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let extent = self
            .read_pool
            .get_or_open(extent_id, &self.disk.extent_path(extent_id))?;
        read_blob(&self.index, &extent, blob_id)
    }

    fn read_shard_blocking(
        &self,
        shard: ShardId,
        blob_id: BlobId,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(extent_id) = self.index.get_shard_binding(shard)? else {
            return Ok(None);
        };
        let extent = self
            .read_pool
            .get_or_open(extent_id, &self.disk.extent_path(extent_id))?;
        read_blob(&self.index, &extent, blob_id)
    }

    fn create_extent_at_blocking(
        &self,
        shard: ShardId,
        create_ts: i64,
    ) -> Result<ExtentId, StoreError> {
        self.create_extent_with_status(shard, create_ts, ExtentStatus::Writable)
    }

    /// Creates an extent bound to `shard` with `status` (either `Writable` for a
    /// normal target or `Rebuilding` for a repair target), spawning its writer.
    fn create_extent_with_status(
        &self,
        shard: ShardId,
        create_ts: i64,
        status: ExtentStatus,
    ) -> Result<ExtentId, StoreError> {
        let extent_id = ExtentId::new(shard, create_ts);
        let extent = match ExtentFile::create(self.disk.extent_path(extent_id), extent_id) {
            Ok(extent) => extent,
            // PD-driven creation retries with the same deterministic id: an
            // already-installed extent is an idempotent success; a half-created
            // one (file without index metadata) is orphaned and recreated.
            Err(ExtentError::Io(err)) if err.kind() == io::ErrorKind::AlreadyExists => {
                if self.index.get_extent_meta(extent_id)?.is_some() {
                    return Ok(extent_id);
                }
                tracing::warn!(extent = ?extent_id, "orphaning half-created extent file");
                self.disk.move_extent_to_trash(extent_id)?;
                ExtentFile::create(self.disk.extent_path(extent_id), extent_id)?
            }
            Err(err) => return Err(err.into()),
        };
        // Reserve the extent's full capacity up front (02 §1.7); no-op off Linux.
        extent.preallocate(self.disk.superblock().extent_size)?;
        let meta = ExtentMeta {
            shard_id: shard,
            status,
            size: extent.write_offset(),
            deleted_bytes: 0,
            create_ts,
        };
        self.index.install_extent(extent_id, &meta, &[])?;
        self.spawn_writer(extent_id, extent);
        Ok(extent_id)
    }

    /// Flips a `Rebuilding` extent to `Writable` (02 §1.5 promote). Idempotent on
    /// an already-`Writable` extent; rejects a missing one.
    fn promote_rebuilt_blocking(&self, extent_id: ExtentId) -> Result<(), StoreError> {
        let meta = self
            .index
            .get_extent_meta(extent_id)?
            .ok_or(StoreError::ExtentNotFound(extent_id))?;
        match meta.status {
            ExtentStatus::Writable => Ok(()), // idempotent
            ExtentStatus::Rebuilding => {
                self.index
                    .set_extent_status(extent_id, ExtentStatus::Writable)?;
                Ok(())
            }
            other => Err(StoreError::NotWritable(other)),
        }
    }

    fn writable_extent_blocking(&self, shard: ShardId) -> Result<ExtentId, StoreError> {
        let extent_id = self
            .index
            .get_shard_binding(shard)?
            .ok_or(StoreError::ShardUnbound(shard))?;
        let meta = self
            .index
            .get_extent_meta(extent_id)?
            .ok_or(StoreError::ExtentNotFound(extent_id))?;
        // Foreground OPEN never targets a rebuild extent: a bound `Rebuilding`
        // extent is not a valid foreground target (02 §1.5). The direct rebuild
        // write path (by explicit extent id) is what feeds it.
        if meta.status == ExtentStatus::Rebuilding {
            return Err(StoreError::NotWritable(ExtentStatus::Rebuilding));
        }
        write::writable_gate(meta.status)?;
        Ok(extent_id)
    }

    fn seal_extent_blocking(&self, extent_id: ExtentId) -> Result<(), StoreError> {
        let meta = self
            .index
            .get_extent_meta(extent_id)?
            .ok_or(StoreError::ExtentNotFound(extent_id))?;
        match meta.status {
            // Already sealed: idempotent success.
            ExtentStatus::Sealed => return Ok(()),
            // Writable / Full extents can be sealed.
            ExtentStatus::Writable | ExtentStatus::Full => {}
            // A rebuilding or dropped extent is not a seal target.
            status @ (ExtentStatus::Rebuilding | ExtentStatus::Dropped) => {
                return Err(StoreError::NotWritable(status));
            }
        }
        // Drain in-flight writes to completion, releasing the file handle, then
        // commit the transition durably (order matters: the queued writes must
        // finish before the seal is visible, or they would be gate-rejected).
        self.stop_writer(extent_id);
        self.index.seal_extent(extent_id)?;
        Ok(())
    }

    fn delete_blocking(&self, extent_id: ExtentId, blob_id: BlobId) -> Result<bool, StoreError> {
        // Resolve the shard's *current* extent under the stripe lock: a caller
        // holding a pre-compaction extent id must tombstone the destination,
        // not the (about to be removed) source (02 §1.6 INVARIANT in
        // crate::compact). Fall back to the named extent when the shard is
        // unbound (recovery windows).
        let shard = extent_id.shard_id();
        let _guard = self
            .shard_delete_lock(shard)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let target = self.index.get_shard_binding(shard)?.unwrap_or(extent_id);
        Ok(self.index.tombstone_blob(target, blob_id)?)
    }

    fn compact_blocking(&self, extent_id: ExtentId) -> Result<CompactOutcome, StoreError> {
        // Release the source's writer so compaction owns the file exclusively.
        self.stop_writer(extent_id);
        // Exclude deletes for this shard across the copy + tombstone re-scan +
        // rebind (02 §1.6 INVARIANT in crate::compact). Coarse (whole
        // compaction) rather than commit-section-only: deletes are rare and a
        // blocked delete simply waits; revisit if compaction of a large extent
        // ever needs finer scoping.
        let _guard = self
            .shard_delete_lock(extent_id.shard_id())
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut extents = BTreeMap::new();
        extents.insert(
            extent_id,
            io_pool::open_read(extent_id, &self.disk.extent_path(extent_id))?,
        );
        let new_create_ts = now_ts_nanos();
        let outcome = {
            let mut qos = self.qos.lock().unwrap_or_else(PoisonError::into_inner);
            compact::compact_extent(
                &self.disk,
                &self.index,
                &mut extents,
                qos.limiter(IoClass::Background),
                extent_id,
                new_create_ts,
            )?
        };
        // The source is now trashed: drop any cached read handle so a stale
        // by-id read cannot serve the superseded file (io_pool invalidation
        // contract).
        self.read_pool.evict(extent_id);
        // The destination is now the shard's writable extent: give it a writer.
        if let Some(dest) = extents.remove(&outcome.new_extent) {
            self.spawn_writer(outcome.new_extent, dest);
        }
        Ok(outcome)
    }

    fn scrub_blocking(&self, extent_id: ExtentId) -> Result<ScrubReport, StoreError> {
        let extent = io_pool::open_read(extent_id, &self.disk.extent_path(extent_id))?;
        let mut qos = self.qos.lock().unwrap_or_else(PoisonError::into_inner);
        scrub::scrub_extent(&self.index, &extent, qos.limiter(IoClass::Background))
    }

    fn reclaim_dropped_blocking(&self) -> Result<Vec<ExtentId>, StoreError> {
        // Dropped extents never hold a live writer; on-demand reads keep no open
        // handle, so an empty open-set is the complete view for reclamation.
        let mut extents = BTreeMap::new();
        let reclaimed = scrub::reclaim_dropped(&self.disk, &self.index, &mut extents)?;
        for id in &reclaimed {
            self.read_pool.evict(*id);
        }
        Ok(reclaimed)
    }

    fn reclaim_orphans_blocking(&self) -> Result<Vec<ExtentId>, StoreError> {
        let mut extents = BTreeMap::new();
        let reclaimed = scrub::reclaim_orphans(&self.disk, &self.index, &mut extents)?;
        for id in &reclaimed {
            self.read_pool.evict(*id);
        }
        Ok(reclaimed)
    }
}

/// Runs `task` on a blocking thread and maps its result to the wire error: a
/// local [`StoreError`] via [`StoreError::to_epoch`], a task panic/cancel to
/// [`EpochError::Internal`].
async fn run_blocking<F, T>(task: F) -> Result<T, EpochError>
where
    F: FnOnce() -> Result<T, StoreError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(task).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(err.to_epoch()),
        Err(join_err) => {
            tracing::error!(error = %join_err, "storage blocking task panicked");
            Err(EpochError::Internal)
        }
    }
}

/// Reconciles one opened extent file against the index at startup (02 §1.7).
fn recover_extent(index: &DiskIndex, extent: &mut ExtentFile) -> Result<(), StoreError> {
    let extent_id = extent.extent_id();
    match index.get_extent_meta(extent_id)? {
        Some(meta) => {
            // INVARIANT(design 02 §1.7): the index is the commit authority. A
            // committed blob always fsynced its record before the index commit,
            // so a correct file is never shorter than the committed size; only a
            // synced-but-uncommitted torn tail makes it longer, and that tail is
            // truncated away.
            if extent.write_offset() > meta.size {
                extent.truncate_to(meta.size)?;
            }
        }
        None => {
            // Index lost for this extent: rebuild from the self-describing record
            // stream (02 §1.2/§1.7). Tombstones and Sealed/Full status are not
            // stream-derivable, so a rebuilt extent resurrects deleted blobs and
            // returns to Writable; GC reconciles the former later (02 §1.6).
            let recovery = extent.scan()?;
            if recovery.truncated {
                extent.truncate_to(recovery.valid_end)?;
            }
            let meta = ExtentMeta {
                shard_id: extent_id.shard_id(),
                status: ExtentStatus::Writable,
                size: recovery.valid_end,
                deleted_bytes: 0,
                create_ts: extent_id.create_ts(),
            };
            // Defense in depth: an extent backs exactly one shard, so a record
            // naming a different shard is skipped rather than indexed here.
            let mut foreign = 0usize;
            let blobs: Vec<(BlobId, BlobIndex)> = recovery
                .records
                .iter()
                .filter(|r| {
                    let own = r.shard_id == extent_id.shard_id();
                    foreign += usize::from(!own);
                    own
                })
                .map(|r| (r.blob_id, BlobIndex::new(r.offset, r.body_len, r.crc)))
                .collect();
            if foreign > 0 {
                tracing::warn!(extent = ?extent_id, foreign, "skipped foreign-shard records during rebuild");
            }
            index.install_extent(extent_id, &meta, &blobs)?;
        }
    }
    Ok(())
}

/// Whether an extent-open failure is a format/decode problem (quarantinable,
/// 02 §1.9) rather than an I/O problem (disk-failure evidence, 02 §1.8). A
/// short read (`UnexpectedEof`) means the file is truncated garbage, not a
/// failing disk, so it is quarantinable too.
fn is_format_error(err: &ExtentError) -> bool {
    match err {
        ExtentError::BadHeaderMagic
        | ExtentError::Version { .. }
        | ExtentError::HeaderChecksum { .. } => true,
        ExtentError::Io(e) => e.kind() == io::ErrorKind::UnexpectedEof,
        _ => false,
    }
}

/// Moves an undecodable extent file into `.trash` and logs the quarantine.
fn quarantine(disk: &Disk, path: &Path, err: &ExtentError) -> Result<(), StoreError> {
    tracing::warn!(path = %path.display(), error = %err, "quarantining undecodable extent file");
    let Some(name) = path.file_name() else {
        return Err(StoreError::Extent(ExtentError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "extent path has no file name",
        ))));
    };
    std::fs::rename(path, disk.trash_dir().join(name))
        .map_err(|e| StoreError::from(ExtentError::Io(e)))
}

/// Local wall-clock nanoseconds since the Unix epoch, saturated into `i64`
/// (used only for disk-local extent identity, never replicated state).
fn now_ts_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{CLUSTER, blob, fresh_engine, shard};
    use epoch_proto::ChunkId;

    #[tokio::test]
    async fn create_write_read_round_trip() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        let body = vec![0x5Au8; 4096];

        assert_eq!(
            engine
                .write(extent_id, blob(0), Bytes::from(body.clone()))
                .await
                .expect("write"),
            WriteOutcome::Written
        );
        assert_eq!(
            engine.read(extent_id, blob(0)).await.expect("read"),
            Some(body.clone())
        );
        // Read addressing by shard resolves the same data through the binding.
        assert_eq!(
            engine
                .read_shard(shard(), blob(0))
                .await
                .expect("read shard"),
            Some(body)
        );
    }

    #[tokio::test]
    async fn write_to_unknown_extent_is_not_found() {
        let (_dir, engine) = fresh_engine();
        let ghost = ExtentId::new(shard(), 42);
        assert_eq!(
            engine.write(ghost, blob(0), Bytes::from_static(b"x")).await,
            Err(EpochError::ShardNotFound)
        );
    }

    #[tokio::test]
    async fn read_shard_of_unbound_slot_is_none() {
        let (_dir, engine) = fresh_engine();
        assert_eq!(
            engine.read_shard(shard(), blob(0)).await.expect("read"),
            None
        );
    }

    #[tokio::test]
    async fn writable_extent_resolves_binding_and_rejects_unbound() {
        let (_dir, engine) = fresh_engine();
        assert_eq!(
            engine.writable_extent(shard()).await,
            Err(EpochError::ShardNotFound)
        );
        let extent_id = engine.create_extent(shard()).await.expect("create");
        assert_eq!(
            engine.writable_extent(shard()).await.expect("resolve"),
            extent_id
        );
    }

    #[tokio::test]
    async fn delete_tombstones_but_read_still_returns() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"live"))
            .await
            .expect("write");

        assert!(engine.delete(extent_id, blob(0)).await.expect("delete"));
        // Read ignores the tombstone until compaction (02 §1.6).
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"live"[..])
        );
        // Idempotent: a second delete reports no new change.
        assert!(!engine.delete(extent_id, blob(0)).await.expect("re-delete"));
    }

    #[tokio::test]
    async fn reopen_recovers_committed_blobs() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        for seq in 0..4u32 {
            let body = vec![seq as u8; 500 + seq as usize];
            engine
                .write(extent_id, blob(seq), Bytes::from(body))
                .await
                .expect("write");
        }
        engine.shutdown();
        drop(engine);

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        for seq in 0..4u32 {
            let expected = vec![seq as u8; 500 + seq as usize];
            assert_eq!(
                engine.read(extent_id, blob(seq)).await.expect("read"),
                Some(expected.clone())
            );
            assert_eq!(
                engine.read_shard(shard(), blob(seq)).await.expect("shard"),
                Some(expected)
            );
        }
        // The recovered extent still accepts appends.
        assert_eq!(
            engine
                .write(extent_id, blob(100), Bytes::from_static(b"after-recovery"))
                .await
                .expect("write"),
            WriteOutcome::Written
        );
    }

    #[tokio::test]
    async fn reopen_truncates_synced_uncommitted_tail() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"committed"))
            .await
            .expect("write");
        let path = engine.disk().extent_path(extent_id);
        let committed_end = ExtentFile::open(&path).expect("open raw").write_offset();
        engine.shutdown();
        drop(engine);

        // Simulate a crash after append+fsync but before the index commit: a
        // fully valid record exists on disk that the index never acknowledged.
        {
            let mut raw = ExtentFile::open(&path).expect("open raw");
            raw.append_record(blob(99), shard(), b"never-committed")
                .expect("append");
            raw.sync().expect("sync");
            assert!(raw.write_offset() > committed_end);
        }

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        // Committed blob survives; the uncommitted tail is gone from index...
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"committed"[..])
        );
        assert_eq!(engine.read(extent_id, blob(99)).await.expect("read"), None);
        // ...and the file was physically truncated back to the committed size.
        let reopened = ExtentFile::open(&path).expect("open raw");
        assert_eq!(reopened.write_offset(), committed_end);
    }

    #[tokio::test]
    async fn list_live_blobs_omits_tombstoned() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"a"))
            .await
            .expect("w0");
        engine
            .write(extent_id, blob(1), Bytes::from_static(b"b"))
            .await
            .expect("w1");
        engine
            .write(extent_id, blob(2), Bytes::from_static(b"c"))
            .await
            .expect("w2");
        // Tombstone blob(1): it must not appear in the live list (repair must
        // never rebuild a deleted blob, 02 §1.6).
        engine.delete(extent_id, blob(1)).await.expect("tomb");

        let mut live = engine.list_live_blobs(extent_id).await.expect("list");
        live.sort_by_key(|b| b.as_u64());
        assert_eq!(live, vec![blob(0), blob(2)]);

        // An unknown extent lists empty, not an error.
        let absent = ExtentId::new(ShardId::new(ChunkId::new(9), 0, 0), 1);
        assert!(
            engine
                .list_live_blobs(absent)
                .await
                .expect("absent")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn rebuilding_extent_accepts_rebuild_writes_then_promotes() {
        let (_dir, engine) = fresh_engine();
        // A repair target for a different shard index than the (broken) original.
        let rebuild_shard = ShardId::new(ChunkId::new(1), 1, 1);
        let extent_id = engine
            .create_rebuilding_extent(rebuild_shard, 1_700_000_000_001)
            .await
            .expect("create rebuilding");

        // A Rebuilding extent accepts rebuilt blobs by explicit extent id...
        engine
            .write(
                extent_id,
                blob(0),
                Bytes::from_static(b"rebuilt-shard-body"),
            )
            .await
            .expect("rebuild write");
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"rebuilt-shard-body"[..])
        );

        // ...but is not a foreground write target (unbound + Rebuilding status).
        assert!(matches!(
            engine.writable_extent(rebuild_shard).await,
            Err(EpochError::Rebuilding) | Err(EpochError::ShardNotFound)
        ));

        // Promote flips it to Writable so it can serve after PD rebinds.
        engine.promote_rebuilt(extent_id).await.expect("promote");
        // Idempotent second promote.
        engine
            .promote_rebuilt(extent_id)
            .await
            .expect("promote again");
        // The rebuilt blob is still readable after promotion.
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"rebuilt-shard-body"[..])
        );
    }

    #[tokio::test]
    async fn reopen_rebuilds_index_after_loss_and_resurrects_deleted() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"kept"))
            .await
            .expect("write");
        engine
            .write(extent_id, blob(1), Bytes::from_static(b"deleted"))
            .await
            .expect("write");
        assert!(engine.delete(extent_id, blob(1)).await.expect("delete"));
        let index_dir = engine.disk().index_dir();
        engine.shutdown();
        drop(engine);

        // Lose the index entirely, then reopen: recovery rebuilds it from the
        // self-describing record stream (02 §1.7).
        std::fs::remove_dir_all(&index_dir).expect("drop index");

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"kept"[..])
        );
        // Documented caveat: tombstones live only in the index, so a rebuild
        // resurrects the deleted blob (GC reconciles later, 02 §1.6).
        assert_eq!(
            engine
                .read(extent_id, blob(1))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"deleted"[..])
        );
        // The shard binding was rebuilt too.
        assert_eq!(
            engine
                .read_shard(shard(), blob(0))
                .await
                .expect("shard")
                .as_deref(),
            Some(&b"kept"[..])
        );
    }

    /// The maintenance sweep's selection contract (02 §1.6): only extents past
    /// the tombstone ratio are candidates, worst-first, and compacting them
    /// actually returns bytes. This is the gate that makes DELETE eventually free
    /// space — without a candidate list nothing schedules compaction, and usage
    /// tracks bytes-ever-written.
    #[tokio::test]
    async fn compaction_candidates_are_ratio_gated_and_worst_first() {
        let (_dir, engine) = fresh_engine();
        // Two shards so two extents coexist with different dirtiness.
        let clean = engine.create_extent(shard()).await.expect("create clean");
        let dirty = engine
            .create_extent(ShardId::new(ChunkId::new(2), 0, 0))
            .await
            .expect("create dirty");

        for (extent, count) in [(clean, 4u32), (dirty, 4)] {
            for seq in 0..count {
                engine
                    .write(extent, blob(seq), Bytes::from(vec![seq as u8; 2000]))
                    .await
                    .expect("write");
            }
        }
        // `clean` loses one of four (25%), `dirty` loses three of four (75%).
        engine.delete(clean, blob(0)).await.expect("delete");
        for seq in 0..3u32 {
            engine.delete(dirty, blob(seq)).await.expect("delete");
        }

        // At a 30% threshold only `dirty` qualifies.
        let candidates = engine
            .list_compaction_candidates(30)
            .expect("list candidates");
        assert_eq!(
            candidates,
            vec![dirty],
            "only the extent past the ratio is a candidate"
        );
        // Lowering the threshold admits both, dirtiest first.
        let candidates = engine
            .list_compaction_candidates(10)
            .expect("list candidates");
        assert_eq!(
            candidates,
            vec![dirty, clean],
            "candidates are ordered worst-first so a bounded budget reclaims most"
        );

        // Compacting the candidate reclaims real bytes and the extent leaves the
        // candidate set (its tombstones are gone).
        let outcome = engine.compact_extent(dirty).await.expect("compact");
        assert!(outcome.reclaimed_bytes > 0, "space was returned");
        let after = engine
            .list_compaction_candidates(30)
            .expect("list candidates");
        assert!(
            after.is_empty(),
            "a compacted extent is no longer dirty: {after:?}"
        );
    }

    #[tokio::test]
    async fn compact_reclaims_space_keeps_live_and_drops_tombstoned() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        for seq in 0..6u32 {
            engine
                .write(extent_id, blob(seq), Bytes::from(vec![seq as u8; 2000]))
                .await
                .expect("write");
        }
        // Delete the even half.
        for seq in [0u32, 2, 4] {
            assert!(engine.delete(extent_id, blob(seq)).await.expect("delete"));
        }

        let outcome = engine.compact_extent(extent_id).await.expect("compact");
        assert_eq!(outcome.live_blobs, 3);
        assert!(outcome.reclaimed_bytes > 0, "space was reclaimed");
        assert_ne!(outcome.new_extent, extent_id);

        // Live blobs remain readable through the rebound shard.
        for seq in [1u32, 3, 5] {
            assert_eq!(
                engine.read_shard(shard(), blob(seq)).await.expect("read"),
                Some(vec![seq as u8; 2000])
            );
        }
        // Tombstoned blobs did not survive into the new extent.
        for seq in [0u32, 2, 4] {
            assert_eq!(
                engine
                    .read(outcome.new_extent, blob(seq))
                    .await
                    .expect("read"),
                None
            );
        }
        // The source extent has been retired (file trashed, writer stopped).
        assert_eq!(
            engine.read(extent_id, blob(1)).await,
            Err(EpochError::ShardNotFound)
        );
    }

    #[tokio::test]
    async fn compacted_state_survives_reopen() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        for seq in 0..4u32 {
            engine
                .write(extent_id, blob(seq), Bytes::from(vec![seq as u8; 1000]))
                .await
                .expect("write");
        }
        engine.delete(extent_id, blob(0)).await.expect("delete");
        engine.delete(extent_id, blob(1)).await.expect("delete");
        let outcome = engine.compact_extent(extent_id).await.expect("compact");
        engine.shutdown();
        drop(engine);

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        for seq in [2u32, 3] {
            assert_eq!(
                engine.read_shard(shard(), blob(seq)).await.expect("read"),
                Some(vec![seq as u8; 1000])
            );
        }
        assert_eq!(
            engine
                .read(outcome.new_extent, blob(2))
                .await
                .expect("read"),
            Some(vec![2u8; 1000])
        );
        // The trashed source is gone from the open set after reopen.
        assert_eq!(
            engine.read(extent_id, blob(2)).await,
            Err(EpochError::ShardNotFound)
        );
    }

    #[tokio::test]
    async fn scrub_reports_healthy_then_detects_corruption() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"healthy-body"))
            .await
            .expect("write");
        engine
            .write(extent_id, blob(1), Bytes::from(vec![7u8; 400]))
            .await
            .expect("write");
        assert!(
            engine
                .scrub_extent(extent_id)
                .await
                .expect("scrub")
                .is_clean()
        );

        let path = engine.disk().extent_path(extent_id);
        engine.shutdown();
        drop(engine);
        // Flip a byte inside the first record's body (past the extent + record
        // headers) so its footer CRC32C no longer matches.
        {
            use crate::extent::file::EXTENT_HEADER_LEN;
            use crate::extent::record::RECORD_HEADER_LEN;
            let mut data = std::fs::read(&path).expect("read");
            data[EXTENT_HEADER_LEN + RECORD_HEADER_LEN + 1] ^= 0xFF;
            std::fs::write(&path, &data).expect("write");
        }

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        let report = engine.scrub_extent(extent_id).await.expect("scrub");
        assert_eq!(report.scanned, 2);
        assert_eq!(report.corrupt, vec![blob(0)]);
        assert!(!report.is_clean());
    }

    #[tokio::test]
    async fn open_quarantines_undecodable_file_and_still_starts() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"ok"))
            .await
            .expect("write");
        let extents_dir = engine.disk().extents_dir();
        let trash_dir = engine.disk().trash_dir();
        engine.shutdown();
        drop(engine);

        let junk = extents_dir.join("deadbeef");
        std::fs::write(&junk, b"not an extent").expect("write junk");

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("open");
        // The good extent still serves; the junk file was quarantined to .trash.
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"ok"[..])
        );
        assert!(!junk.exists());
        assert!(trash_dir.join("deadbeef").exists());
    }

    #[tokio::test]
    async fn rebuild_skips_records_from_a_foreign_shard() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"own"))
            .await
            .expect("write");
        let index_dir = engine.disk().index_dir();
        let path = engine.disk().extent_path(extent_id);
        engine.shutdown();
        drop(engine);

        // Append a record that names a different shard directly at the file level.
        {
            let mut raw = ExtentFile::open(&path).expect("open raw");
            let foreign = ShardId::new(ChunkId::new(9), 9, 9);
            raw.append_record(blob(7), foreign, b"foreign")
                .expect("append");
            raw.sync().expect("sync");
        }
        std::fs::remove_dir_all(&index_dir).expect("drop index");

        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"own"[..])
        );
        // The foreign record was not indexed into this extent's rebuilt index.
        assert_eq!(engine.read(extent_id, blob(7)).await.expect("read"), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes_all_persist_through_group_commit() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");

        // Fan many concurrent writes at one extent's single writer thread, which
        // coalesces the ready backlog into group commits (writer::process_batch).
        let mut handles = Vec::new();
        for seq in 0..64u32 {
            let engine = engine.clone();
            handles.push(tokio::spawn(async move {
                engine
                    .write(extent_id, blob(seq), Bytes::from(vec![seq as u8; 100]))
                    .await
            }));
        }
        for handle in handles {
            let outcome = handle.await.expect("join").expect("write");
            assert_eq!(outcome, WriteOutcome::Written);
        }

        for seq in 0..64u32 {
            assert_eq!(
                engine.read(extent_id, blob(seq)).await.expect("read"),
                Some(vec![seq as u8; 100])
            );
        }
    }

    #[tokio::test]
    async fn shutdown_stops_writers_but_keeps_reads() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"before"))
            .await
            .expect("write");

        engine.shutdown();

        // No writer remains, so a further write finds no target.
        assert_eq!(
            engine
                .write(extent_id, blob(1), Bytes::from_static(b"after"))
                .await,
            Err(EpochError::ShardNotFound)
        );
        // Committed data is still readable (reads reopen on demand).
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"before"[..])
        );
    }

    #[tokio::test]
    async fn seal_rejects_new_opens_and_keeps_reads() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"kept"))
            .await
            .expect("write");

        engine.seal_extent(extent_id).await.expect("seal");

        // A new OPEN (writable resolution) is rejected as Sealed.
        assert_eq!(
            engine.writable_extent(shard()).await,
            Err(EpochError::Sealed)
        );
        // The drained pre-seal write is still readable.
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"kept"[..])
        );
    }

    #[tokio::test]
    async fn seal_is_idempotent() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine.seal_extent(extent_id).await.expect("first seal");
        // Sealing again is a no-op success.
        engine.seal_extent(extent_id).await.expect("second seal");
        assert_eq!(
            engine.writable_extent(shard()).await,
            Err(EpochError::Sealed)
        );
    }

    #[tokio::test]
    async fn seal_survives_reopen() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"kept"))
            .await
            .expect("write");
        engine.seal_extent(extent_id).await.expect("seal");
        engine.shutdown();
        drop(engine);

        // The seal is durable: after reopen the extent still rejects OPEN.
        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        assert_eq!(
            engine.writable_extent(shard()).await,
            Err(EpochError::Sealed)
        );
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"kept"[..])
        );
    }

    #[tokio::test]
    async fn seal_unknown_extent_is_shard_not_found() {
        let (_dir, engine) = fresh_engine();
        let missing = ExtentId::new(shard(), 42);
        assert_eq!(
            engine.seal_extent(missing).await,
            Err(EpochError::ShardNotFound)
        );
    }

    /// C1 regression (02 §1.6/§1.7): concurrent deletes and writer commits on
    /// one extent must never roll back `meta.size` — a stale size would make
    /// recovery truncate committed blobs. Field-scoped merges make the two
    /// owners commute; this hammers them together and then reopens to verify.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_deletes_and_writes_never_roll_back_size() {
        let (dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");

        // Seed blobs 0..32 so deletes have targets while 32..64 are written.
        for seq in 0..32u32 {
            engine
                .write(extent_id, blob(seq), Bytes::from(vec![seq as u8; 512]))
                .await
                .expect("seed");
        }

        let writer = {
            let engine = engine.clone();
            tokio::spawn(async move {
                for seq in 32..64u32 {
                    engine
                        .write(extent_id, blob(seq), Bytes::from(vec![seq as u8; 512]))
                        .await
                        .expect("write");
                }
            })
        };
        let deleter = {
            let engine = engine.clone();
            tokio::spawn(async move {
                for seq in 0..32u32 {
                    engine.delete(extent_id, blob(seq)).await.expect("delete");
                }
            })
        };
        writer.await.expect("writer task");
        deleter.await.expect("deleter task");

        engine.shutdown();
        drop(engine);

        // Reopen: recovery must keep every committed blob readable (a rolled
        // back size would have truncated the tail ones away).
        let engine = StorageEngine::open(dir.path(), CLUSTER).expect("reopen");
        for seq in 32..64u32 {
            assert_eq!(
                engine
                    .read(extent_id, blob(seq))
                    .await
                    .expect("read")
                    .as_deref(),
                Some(&vec![seq as u8; 512][..]),
                "blob {seq} lost after concurrent delete/write + reopen"
            );
        }
        // And the deletes stuck (tombstoned, still physically readable).
        for seq in 0..32u32 {
            assert!(
                !engine
                    .delete(extent_id, blob(seq))
                    .await
                    .expect("re-delete"),
                "blob {seq} lost its tombstone"
            );
        }
    }

    /// C1 regression (Q17 drain window): a seal landing while a write is in
    /// flight must not be overwritten by the write's commit — the data commits,
    /// the status stays Sealed (field-scoped status merge).
    #[tokio::test]
    async fn seal_survives_in_flight_write_commit() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");

        // Interleave: write, seal, write (second write races the seal's
        // visibility; whichever way the gate falls, the status must be Sealed
        // afterwards and the first write must be intact).
        engine
            .write(extent_id, blob(0), Bytes::from_static(b"before"))
            .await
            .expect("write before seal");
        engine.seal_extent(extent_id).await.expect("seal");
        let _ = engine
            .write(extent_id, blob(1), Bytes::from_static(b"racer"))
            .await;

        assert_eq!(
            engine.writable_extent(shard()).await,
            Err(EpochError::Sealed),
            "seal was rolled back by a concurrent commit"
        );
        assert_eq!(
            engine
                .read(extent_id, blob(0))
                .await
                .expect("read")
                .as_deref(),
            Some(&b"before"[..])
        );
    }

    /// C2 regression (02 §1.6 INVARIANT in `crate::compact`): a delete that
    /// lands during/around compaction must survive it — never resurrected by
    /// the copy, whether the caller holds the pre- or post-compaction extent id.
    #[tokio::test]
    async fn delete_survives_compaction_and_stale_extent_ids() {
        let (_dir, engine) = fresh_engine();
        let extent_id = engine.create_extent(shard()).await.expect("create");

        for seq in 0..8u32 {
            engine
                .write(extent_id, blob(seq), Bytes::from(vec![seq as u8; 256]))
                .await
                .expect("write");
        }
        // Delete half before compaction.
        for seq in 0..4u32 {
            assert!(engine.delete(extent_id, blob(seq)).await.expect("delete"));
        }

        let outcome = engine.compact_extent(extent_id).await.expect("compact");
        assert_eq!(outcome.live_blobs, 4);

        // A late delete arriving with the STALE (pre-compaction) extent id must
        // land on the shard's current extent, not vanish with the source.
        assert!(
            engine
                .delete(extent_id, blob(4))
                .await
                .expect("late delete"),
            "stale-extent-id delete was lost"
        );
        // Idempotent re-delete against the new extent agrees.
        assert!(
            !engine
                .delete(outcome.new_extent, blob(4))
                .await
                .expect("re-delete"),
            "delete did not land on the current extent"
        );
        // Deleted-before-compaction blobs stayed deleted (not resurrected):
        // a fresh delete of them reports "already tombstoned or absent".
        for seq in 0..4u32 {
            assert!(
                !engine
                    .delete(outcome.new_extent, blob(seq))
                    .await
                    .expect("check"),
                "blob {seq} resurrected by compaction"
            );
        }
        // Survivors are intact.
        for seq in 5..8u32 {
            assert_eq!(
                engine
                    .read(outcome.new_extent, blob(seq))
                    .await
                    .expect("read")
                    .as_deref(),
                Some(&vec![seq as u8; 256][..])
            );
        }
    }
}
