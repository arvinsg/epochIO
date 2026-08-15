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

//! Chunk manager: the committed chunk records, in-flight staging plans, and the
//! per-disk shard secondary index, all backed by the PD state machine.
//!
//! [`ChunkManager`] owns two column families: `chunk` holds committed [`Chunk`]
//! records (keyed by chunk id, 4-byte BE) plus the chunk-id allocation counter,
//! and `chunk_staging` holds in-flight [`StagingChunk`] plans (keyed by chunk id,
//! no counter). It mirrors the node / disk managers (persistence mechanics in
//! [`id_record`](crate::cluster::id_record)) and adds the two-phase creation flow
//! and a `disk_id → {shard_prefix}` secondary index used to enumerate a disk's
//! shards during repair (design 01 §3).
//!
//! Two-phase creation (01 §4.1): [`apply_create_staging`](ChunkManager::apply_create_staging)
//! allocates the id and writes the plan to `chunk_staging`; the extents are
//! created out-of-band; [`apply_commit`](ChunkManager::apply_commit) promotes the
//! plan to a `Writable` chunk. A crash between the two is recovered by scanning
//! [`pending_staging`](ChunkManager::pending_staging) and re-planning with
//! [`apply_rebump`](ChunkManager::apply_rebump) (epoch jump, 01 §4.1).
//!
//! INVARIANT(design 01 §2 / AGENTS §8): apply is deterministic — chunk ids come
//! from the persisted counter and shard / extent ids are derived from the
//! command's leader-chosen `epoch` / `create_ts`, never from a clock read here.

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::{ChunkId, CodeMode, CodeModeId, DiskId, ExtentId, ShardId};
use rocksdb::{DB, WriteBatch};

use crate::chunk::model::{
    Chunk, ChunkStatus, ChunkStatusHistogram, CommitChunk, CommitShardMapping, CreateChunkStaging,
    EPOCH_JUMP, RebumpStaging, SealChunk, ShardSlot, SlotPlan, StagingChunk,
};
use crate::cluster::RejectReason;
use crate::cluster::id_record;
use crate::raft::{SmError, sm_corrupt};

/// The `chunk` column family: one committed chunk record per id (4-byte BE),
/// plus the chunk-id allocation counter (mechanics in [`id_record`]).
pub(crate) const CHUNK_CF: &str = "chunk";

/// The `chunk_staging` column family: one in-flight plan per chunk id (4-byte
/// BE). No allocation counter — ids are minted against [`CHUNK_CF`].
pub(crate) const STAGING_CF: &str = "chunk_staging";

/// The in-memory chunk index: committed chunks, staging plans, the next id, and
/// the `disk → shard_prefix` secondary index.
#[derive(Debug)]
struct ChunkIndex {
    chunks: BTreeMap<ChunkId, Chunk>,
    staging: BTreeMap<ChunkId, StagingChunk>,
    next_id: u32,
    disk_shards: BTreeMap<DiskId, BTreeSet<u64>>,
}

impl ChunkIndex {
    fn index_shards(&mut self, chunk: &Chunk) {
        for slot in &chunk.shards {
            self.disk_shards
                .entry(slot.disk_id)
                .or_default()
                .insert(slot.shard_prefix);
        }
    }
}

/// Validates a placement plan's shape against its code mode.
///
/// Returns [`RejectReason::InvalidChunkPlan`] unless the plan holds exactly one
/// slot for each shard index `0..shards_total`.
pub(crate) fn validate_slots(code_mode: &CodeMode, slots: &[SlotPlan]) -> Option<RejectReason> {
    let total = code_mode.shards_total();
    if slots.len() != usize::from(total) {
        return Some(RejectReason::InvalidChunkPlan);
    }
    let mut seen: BTreeSet<u8> = BTreeSet::new();
    for slot in slots {
        if u16::from(slot.index) >= total || !seen.insert(slot.index) {
            return Some(RejectReason::InvalidChunkPlan);
        }
    }
    None
}

/// Composes the shard slots for a chunk plan in shard-index order.
///
/// `epoch` and `create_ts` come from the command (deterministic). Assumes the
/// plan already passed [`validate_slots`].
fn compose_slots(
    chunk_id: ChunkId,
    slots: &[SlotPlan],
    epoch: u32,
    create_ts: i64,
) -> Result<Vec<ShardSlot>, SmError> {
    let mut ordered: Vec<SlotPlan> = slots.to_vec();
    ordered.sort_by_key(|slot| slot.index);
    let mut shards = Vec::with_capacity(ordered.len());
    for slot in ordered {
        shards.push(compose_slot(
            chunk_id,
            slot.index,
            epoch,
            slot.disk_id,
            create_ts,
        )?);
    }
    Ok(shards)
}

/// Composes one shard slot, failing (not panicking) on shard-epoch overflow so
/// the raft apply stays panic-free (AGENTS §8).
fn compose_slot(
    chunk_id: ChunkId,
    index: u8,
    epoch: u32,
    disk_id: DiskId,
    create_ts: i64,
) -> Result<ShardSlot, SmError> {
    let shard_id = ShardId::try_new(chunk_id, index, epoch)
        .map_err(|_| sm_corrupt("shard epoch exceeds 24-bit max"))?;
    Ok(ShardSlot {
        shard_prefix: shard_id.shard_prefix(),
        epoch,
        disk_id,
        extent_id: ExtentId::new(shard_id, create_ts),
    })
}

/// Owns the chunk column families and the chunk index (cloneable; clones share
/// the same database and in-memory index).
#[derive(Clone)]
pub struct ChunkManager {
    db: Arc<DB>,
    index: Arc<RwLock<ChunkIndex>>,
}

impl ChunkManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted chunks.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(ChunkIndex {
                chunks: BTreeMap::new(),
                staging: BTreeMap::new(),
                next_id: id_record::FIRST_ID,
                disk_shards: BTreeMap::new(),
            })),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, ChunkIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, ChunkIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuilds the in-memory index from the `chunk` and `chunk_staging` column
    /// families.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if a column family is missing or a
    /// persisted record cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let (chunks, next_id) = id_record::load_records::<Chunk>(&self.db, CHUNK_CF)?;
        let (staging, _) = id_record::load_records::<StagingChunk>(&self.db, STAGING_CF)?;

        let mut index = self.write_index();
        index.chunks = chunks.into_iter().map(|c| (c.chunk_id, c)).collect();
        index.staging = staging.into_iter().map(|s| (s.chunk_id, s)).collect();
        index.next_id = next_id;
        index.disk_shards.clear();
        let committed: Vec<Chunk> = index.chunks.values().cloned().collect();
        for chunk in &committed {
            index.index_shards(chunk);
        }
        Ok(())
    }

    /// Applies a chunk-staging command: allocates the next chunk id, composes the
    /// shard slots, and writes the staging plan. Stages durable writes into
    /// `batch`; the caller flushes. Assumes the plan already passed
    /// [`validate_slots`] and its disks were validated by the aggregation point.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the id counter overflows, a shard epoch
    /// overflows, or a record cannot be serialized.
    pub(crate) fn apply_create_staging(
        &self,
        batch: &mut WriteBatch,
        cmd: &CreateChunkStaging,
    ) -> Result<ChunkId, SmError> {
        let chunk_cf = id_record::open_cf(&self.db, CHUNK_CF)?;
        let staging_cf = id_record::open_cf(&self.db, STAGING_CF)?;
        let mut index = self.write_index();

        let chunk_id = ChunkId::new(index.next_id);
        let next_id = id_record::advance_counter(index.next_id)?;
        let shards = compose_slots(chunk_id, &cmd.slots, cmd.epoch, cmd.create_ts)?;
        let staging = StagingChunk {
            chunk_id,
            code_mode: cmd.code_mode,
            shards,
        };

        id_record::stage_put_record(batch, staging_cf, chunk_id.get(), &staging)?;
        id_record::stage_put_counter(batch, chunk_cf, next_id);
        index.next_id = next_id;
        index.staging.insert(chunk_id, staging);
        Ok(chunk_id)
    }

    /// Applies a commit: promotes a staging plan to a committed `Writable` chunk.
    /// Returns `Ok(None)` when committed or `Ok(Some(reason))` when no staging
    /// plan exists for the id.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the committed record cannot be
    /// serialized.
    pub(crate) fn apply_commit(
        &self,
        batch: &mut WriteBatch,
        cmd: &CommitChunk,
    ) -> Result<Option<RejectReason>, SmError> {
        let chunk_cf = id_record::open_cf(&self.db, CHUNK_CF)?;
        let staging_cf = id_record::open_cf(&self.db, STAGING_CF)?;
        let mut index = self.write_index();
        let Some(staging) = index.staging.get(&cmd.chunk_id).cloned() else {
            return Ok(Some(RejectReason::NotFound));
        };

        let chunk = Chunk {
            chunk_id: staging.chunk_id,
            code_mode: staging.code_mode,
            status: ChunkStatus::Writable,
            shards: staging.shards,
        };
        id_record::stage_put_record(batch, chunk_cf, chunk.chunk_id.get(), &chunk)?;
        batch.delete_cf(staging_cf, id_record::record_key(cmd.chunk_id.get()));

        index.staging.remove(&cmd.chunk_id);
        index.index_shards(&chunk);
        index.chunks.insert(chunk.chunk_id, chunk);
        Ok(None)
    }

    /// Applies a re-bump: advances a staging plan's shard epoch by [`EPOCH_JUMP`]
    /// (re-deriving each shard / extent id) so a retried extent creation cannot
    /// collide with a half-created one (01 §4.1). Returns `Ok(None)` when
    /// re-planned or `Ok(Some(reason))` when no staging plan exists.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if a shard epoch overflows or the record
    /// cannot be serialized.
    pub(crate) fn apply_rebump(
        &self,
        batch: &mut WriteBatch,
        cmd: &RebumpStaging,
    ) -> Result<Option<RejectReason>, SmError> {
        let staging_cf = id_record::open_cf(&self.db, STAGING_CF)?;
        let mut index = self.write_index();
        let Some(staging) = index.staging.get(&cmd.chunk_id).cloned() else {
            return Ok(Some(RejectReason::NotFound));
        };

        let mut rebumped = staging;
        for slot in &mut rebumped.shards {
            let epoch = slot
                .epoch
                .checked_add(EPOCH_JUMP)
                .ok_or_else(|| sm_corrupt("shard epoch overflow on recovery"))?;
            *slot = compose_slot(
                cmd.chunk_id,
                slot.index(),
                epoch,
                slot.disk_id,
                slot.extent_id.create_ts(),
            )?;
        }

        id_record::stage_put_record(batch, staging_cf, cmd.chunk_id.get(), &rebumped)?;
        index.staging.insert(cmd.chunk_id, rebumped);
        Ok(None)
    }

    /// A snapshot of the in-flight staging plans (the crash-recovery scan, 01
    /// §4.1). Plans come out in chunk-id order.
    #[must_use]
    pub fn pending_staging(&self) -> Vec<StagingChunk> {
        self.read_index().staging.values().cloned().collect()
    }

    /// Applies a seal: transition a committed chunk to [`ChunkStatus::Sealed`]
    /// (01 §4.2). Idempotent — an already-sealed chunk applies cleanly; a
    /// `Migrating` / `Broken` chunk is rejected, as is an unknown id.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the record cannot be serialized.
    pub(crate) fn apply_seal_chunk(
        &self,
        batch: &mut WriteBatch,
        cmd: &SealChunk,
    ) -> Result<Option<RejectReason>, SmError> {
        let chunk_cf = id_record::open_cf(&self.db, CHUNK_CF)?;
        let mut index = self.write_index();
        let Some(chunk) = index.chunks.get_mut(&cmd.chunk_id) else {
            return Ok(Some(RejectReason::NotFound));
        };
        match chunk.status {
            ChunkStatus::Sealed => return Ok(None),
            ChunkStatus::Writable | ChunkStatus::Full => {}
            ChunkStatus::Migrating | ChunkStatus::Broken => {
                return Ok(Some(RejectReason::InvalidTransition));
            }
        }
        chunk.status = ChunkStatus::Sealed;
        let chunk = chunk.clone();
        id_record::stage_put_record(batch, chunk_cf, chunk.chunk_id.get(), &chunk)?;
        Ok(None)
    }

    /// Rebinds shard slot `index` of a committed chunk to a rebuilt extent on
    /// `cmd.new_disk` and bumps the slot epoch (01 §6.4 invariant 2/5). The caller
    /// ([`PdState::apply_command`]) has already verified the committer's Job
    /// authorization; this delegates to the shared mechanical rebind.
    ///
    /// Returns `Some(RejectReason)` when the chunk or slot is missing, the
    /// observed epoch is stale, or the slot epoch would overflow — all
    /// deterministic non-error outcomes.
    ///
    /// [`PdState::apply_command`]: crate::state::PdState::apply_command
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the updated record cannot be serialized.
    pub(crate) fn apply_commit_shard_mapping(
        &self,
        batch: &mut WriteBatch,
        cmd: &CommitShardMapping,
    ) -> Result<Option<RejectReason>, SmError> {
        self.apply_rebind(
            batch,
            cmd.chunk_id,
            cmd.index,
            cmd.expected_epoch,
            cmd.new_disk,
            cmd.new_create_ts,
        )
    }

    /// The mechanical, deterministic shard-slot rebind shared by both rebind
    /// authorization paths — a Job-authorized [`CommitShardMapping`] (RepairDisk /
    /// migrate) and a ticket-authorized `CommitShardRepair`
    /// ([`ShardRepairRegistry`](crate::shard_repair::ShardRepairRegistry)). The
    /// *authorization* differs and is the caller's responsibility (01 §6.4
    /// invariant 2); this repoints slot `index` of `chunk_id` to `new_disk` +
    /// `new_create_ts`, bumps the epoch, and migrates the `disk_shards` secondary
    /// index. For an in-place ShardRepair rebuild, `new_disk` equals the slot's
    /// current disk — the epoch still bumps so gateways invalidate their cache
    /// (invariant 5).
    ///
    /// Returns `Some(RejectReason)` for a missing chunk/slot, stale observed
    /// epoch, or epoch overflow.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the updated record cannot be serialized.
    pub(crate) fn apply_rebind(
        &self,
        batch: &mut WriteBatch,
        chunk_id: ChunkId,
        slot_index: u8,
        expected_epoch: u32,
        new_disk: DiskId,
        new_create_ts: i64,
    ) -> Result<Option<RejectReason>, SmError> {
        let chunk_cf = id_record::open_cf(&self.db, CHUNK_CF)?;
        let mut index = self.write_index();
        let Some(chunk) = index.chunks.get(&chunk_id).cloned() else {
            return Ok(Some(RejectReason::NotFound));
        };
        let Some(slot) = chunk
            .shards
            .iter()
            .find(|s| s.index() == slot_index)
            .copied()
        else {
            return Ok(Some(RejectReason::NotFound));
        };
        // Staleness guard: the coordinator must have rebuilt from the epoch it
        // observed; a mismatch means another rebind/migrate landed first.
        if slot.epoch != expected_epoch {
            return Ok(Some(RejectReason::InvalidTransition));
        }
        // Rebind: new epoch, new disk, new extent (derived from the new shard id
        // + the coordinator's create_ts, matching the DataNode's rebuilt extent).
        let Some(new_epoch) = slot
            .epoch
            .checked_add(1)
            .filter(|e| *e <= epoch_proto::id::MAX_SHARD_EPOCH)
        else {
            return Ok(Some(RejectReason::InvalidTransition));
        };
        let rebuilt = compose_slot(chunk_id, slot_index, new_epoch, new_disk, new_create_ts)?;

        let mut updated = chunk.clone();
        if let Some(target) = updated.shards.iter_mut().find(|s| s.index() == slot_index) {
            *target = rebuilt;
        }
        // Move the slot's stable prefix from the old disk's set to the new one
        // (shard_prefix is epoch-zeroed, so it is identical before/after). An
        // in-place rebind (same disk) removes and re-inserts the same prefix.
        if let Some(set) = index.disk_shards.get_mut(&slot.disk_id) {
            set.remove(&slot.shard_prefix);
        }
        index
            .disk_shards
            .entry(new_disk)
            .or_default()
            .insert(rebuilt.shard_prefix);

        id_record::stage_put_record(batch, chunk_cf, updated.chunk_id.get(), &updated)?;
        index.chunks.insert(updated.chunk_id, updated);
        Ok(None)
    }

    /// The committed chunks, next id, and staging plans for a snapshot. All come
    /// out in id order (deterministic snapshot bytes, AGENTS §8).
    pub(crate) fn snapshot_view(&self) -> (Vec<Chunk>, u32, Vec<StagingChunk>) {
        let index = self.read_index();
        (
            index.chunks.values().cloned().collect(),
            index.next_id,
            index.staging.values().cloned().collect(),
        )
    }

    /// Replaces the full chunk + staging sets from an installed snapshot: stages
    /// the record rewrite into `batch` and rebuilds the in-memory index. The
    /// caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if a column family is missing or a record
    /// cannot be (de)serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        chunks: Vec<Chunk>,
        next_id: u32,
        staging: Vec<StagingChunk>,
    ) -> Result<(), SmError> {
        let chunk_cf = id_record::open_cf(&self.db, CHUNK_CF)?;
        let staging_cf = id_record::open_cf(&self.db, STAGING_CF)?;
        id_record::stage_clear(batch, &self.db, chunk_cf)?;
        id_record::stage_clear(batch, &self.db, staging_cf)?;
        for chunk in &chunks {
            id_record::stage_put_record(batch, chunk_cf, chunk.chunk_id.get(), chunk)?;
        }
        id_record::stage_put_counter(batch, chunk_cf, next_id);
        for plan in &staging {
            id_record::stage_put_record(batch, staging_cf, plan.chunk_id.get(), plan)?;
        }

        let mut index = self.write_index();
        index.chunks = chunks.into_iter().map(|c| (c.chunk_id, c)).collect();
        index.staging = staging.into_iter().map(|s| (s.chunk_id, s)).collect();
        index.next_id = next_id;
        index.disk_shards.clear();
        let committed: Vec<Chunk> = index.chunks.values().cloned().collect();
        for chunk in &committed {
            index.index_shards(chunk);
        }
        Ok(())
    }

    /// Looks up a committed chunk by id (read path).
    #[must_use]
    pub fn get(&self, chunk_id: ChunkId) -> Option<Chunk> {
        self.read_index().chunks.get(&chunk_id).cloned()
    }

    /// The committed `Writable` chunks of the given code mode, in id order (the
    /// gateway's writable-set pull, 01 §7 `GetWritableChunks`).
    #[must_use]
    pub fn writable_chunks(&self, code_mode_id: CodeModeId) -> Vec<Chunk> {
        self.read_index()
            .chunks
            .values()
            .filter(|chunk| {
                chunk.status == ChunkStatus::Writable && chunk.code_mode.id == code_mode_id
            })
            .cloned()
            .collect()
    }

    /// The committed shard slots bound to `disk_id`, in chunk-id then shard-index
    /// order (01 §7 `ListDiskShards`). Unlike
    /// [`shard_prefixes_on_disk`](Self::shard_prefixes_on_disk), each slot carries
    /// its current epoch and extent binding.
    #[must_use]
    pub fn shard_slots_on_disk(&self, disk_id: DiskId) -> Vec<ShardSlot> {
        let index = self.read_index();
        let mut slots = Vec::new();
        for chunk in index.chunks.values() {
            for slot in &chunk.shards {
                if slot.disk_id == disk_id {
                    slots.push(*slot);
                }
            }
        }
        slots
    }

    /// Looks up an in-flight staging plan by id (read path).
    #[must_use]
    pub fn staging(&self, chunk_id: ChunkId) -> Option<StagingChunk> {
        self.read_index().staging.get(&chunk_id).cloned()
    }

    /// A page of committed chunks with id strictly greater than `after`, in
    /// ascending id order, capped at `limit` (01 §6.3 InspectRound enumeration).
    ///
    /// Paging by `(after, limit)` lets the inspect coordinator walk every chunk
    /// in bounded segments and resume from its progress watermark across a
    /// coordinator reassignment. `after = 0` starts from the first chunk (ids
    /// begin at [`FIRST_ID`](crate::cluster::id_record::FIRST_ID) = 1). An empty
    /// page marks the end of the round.
    #[must_use]
    pub fn list_chunks(&self, after: ChunkId, limit: usize) -> Vec<Chunk> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.read_index()
            .chunks
            .range((Excluded(after), Unbounded))
            .take(limit)
            .map(|(_, chunk)| chunk.clone())
            .collect()
    }

    /// The stable shard prefixes of committed shards on `disk_id`, in order
    /// (repair enumerates these to rebuild a failed disk, 01 §3).
    #[must_use]
    pub fn shard_prefixes_on_disk(&self, disk_id: DiskId) -> Vec<u64> {
        self.read_index()
            .disk_shards
            .get(&disk_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Counts committed chunks by lifecycle status (08 §5 console COUNTS): the
    /// state distribution the overview + Chunk admin pages render, without
    /// listing the whole table (§5 red line — no `list_all`). Returns
    /// `(total, per-status counts)`.
    #[must_use]
    pub fn status_histogram(&self) -> ChunkStatusHistogram {
        let index = self.read_index();
        let mut h = ChunkStatusHistogram::default();
        for chunk in index.chunks.values() {
            h.total += 1;
            match chunk.status {
                ChunkStatus::Writable => h.writable += 1,
                ChunkStatus::Full => h.full += 1,
                ChunkStatus::Sealed => h.sealed += 1,
                ChunkStatus::Migrating => h.migrating += 1,
                ChunkStatus::Broken => h.broken += 1,
            }
        }
        h
    }

    /// The number of committed chunks (read path).
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_index().chunks.len()
    }

    /// Whether no chunk is committed (read path).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_index().chunks.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use epoch_proto::CodeModeId;
    use rocksdb::WriteBatch;
    use tempfile::TempDir;

    use super::*;

    const CREATE_TS: i64 = 1_700_000_000_000;

    fn code_mode() -> CodeMode {
        CodeMode {
            id: CodeModeId::new(1),
            data: 2,
            parity: 1,
            stripe_size: 1 << 20,
            blob_size: 32 << 20,
            write_quorum: None,
        }
    }

    fn open_manager(dir: &TempDir) -> ChunkManager {
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[CHUNK_CF, STAGING_CF],
        )
        .expect("open chunk cfs");
        let manager = ChunkManager::new(Arc::new(db));
        manager.restore().expect("restore");
        manager
    }

    fn commit(manager: &ChunkManager, batch: WriteBatch) {
        manager.db.write(batch).expect("flush batch");
    }

    fn slots(disks: &[u32]) -> Vec<SlotPlan> {
        disks
            .iter()
            .enumerate()
            .map(|(index, &disk)| SlotPlan {
                index: u8::try_from(index).expect("small index"),
                disk_id: DiskId::new(disk),
            })
            .collect()
    }

    fn stage(manager: &ChunkManager, epoch: u32, disks: &[u32]) -> ChunkId {
        let mut batch = WriteBatch::default();
        let cmd = CreateChunkStaging {
            code_mode: code_mode(),
            slots: slots(disks),
            create_ts: CREATE_TS,
            epoch,
        };
        let chunk_id = manager
            .apply_create_staging(&mut batch, &cmd)
            .expect("stage");
        commit(manager, batch);
        chunk_id
    }

    fn do_commit(manager: &ChunkManager, chunk_id: ChunkId) -> Option<RejectReason> {
        let mut batch = WriteBatch::default();
        let reject = manager
            .apply_commit(&mut batch, &CommitChunk { chunk_id })
            .expect("commit");
        commit(manager, batch);
        reject
    }

    #[test]
    fn validate_slots_enforces_plan_shape() {
        let mode = code_mode(); // shards_total == 3
        assert_eq!(validate_slots(&mode, &slots(&[1, 2, 3])), None);
        // Too few slots.
        assert_eq!(
            validate_slots(&mode, &slots(&[1, 2])),
            Some(RejectReason::InvalidChunkPlan)
        );
        // Duplicate index.
        let dup = vec![
            SlotPlan {
                index: 0,
                disk_id: DiskId::new(1),
            },
            SlotPlan {
                index: 0,
                disk_id: DiskId::new(2),
            },
            SlotPlan {
                index: 1,
                disk_id: DiskId::new(3),
            },
        ];
        assert_eq!(
            validate_slots(&mode, &dup),
            Some(RejectReason::InvalidChunkPlan)
        );
        // Index out of range.
        let oor = vec![
            SlotPlan {
                index: 0,
                disk_id: DiskId::new(1),
            },
            SlotPlan {
                index: 1,
                disk_id: DiskId::new(2),
            },
            SlotPlan {
                index: 9,
                disk_id: DiskId::new(3),
            },
        ];
        assert_eq!(
            validate_slots(&mode, &oor),
            Some(RejectReason::InvalidChunkPlan)
        );
    }

    #[test]
    fn staging_allocates_id_and_composes_ordered_slots() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);

        // Slots supplied out of index order; composed shards must be index-ordered.
        let mut batch = WriteBatch::default();
        let unordered = vec![
            SlotPlan {
                index: 2,
                disk_id: DiskId::new(30),
            },
            SlotPlan {
                index: 0,
                disk_id: DiskId::new(10),
            },
            SlotPlan {
                index: 1,
                disk_id: DiskId::new(20),
            },
        ];
        let cmd = CreateChunkStaging {
            code_mode: code_mode(),
            slots: unordered,
            create_ts: CREATE_TS,
            epoch: 0,
        };
        let chunk_id = manager
            .apply_create_staging(&mut batch, &cmd)
            .expect("stage");
        commit(&manager, batch);
        assert_eq!(chunk_id, ChunkId::new(1));

        let staging = manager.staging(chunk_id).expect("staging present");
        assert_eq!(staging.shards.len(), 3);
        for (i, slot) in staging.shards.iter().enumerate() {
            assert_eq!(slot.index(), u8::try_from(i).unwrap());
            assert_eq!(slot.epoch, 0);
            assert_eq!(slot.shard_id().chunk_id(), chunk_id);
        }
        assert_eq!(staging.shards[0].disk_id, DiskId::new(10));
        assert_eq!(staging.shards[2].disk_id, DiskId::new(30));
        // Not yet committed.
        assert!(manager.get(chunk_id).is_none());
        assert_eq!(manager.pending_staging().len(), 1);
    }

    #[test]
    fn commit_promotes_staging_and_indexes_shards() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let chunk_id = stage(&manager, 0, &[10, 20, 30]);

        assert_eq!(do_commit(&manager, chunk_id), None);
        let chunk = manager.get(chunk_id).expect("committed");
        assert_eq!(chunk.status, ChunkStatus::Writable);
        assert_eq!(chunk.shards.len(), 3);
        assert!(manager.staging(chunk_id).is_none());
        assert!(manager.pending_staging().is_empty());

        // Secondary index enumerates each disk's shard prefix.
        let prefix = ShardId::new(chunk_id, 1, 0).shard_prefix();
        assert_eq!(
            manager.shard_prefixes_on_disk(DiskId::new(20)),
            vec![prefix]
        );
        assert!(manager.shard_prefixes_on_disk(DiskId::new(99)).is_empty());

        // Committing an unknown / already-committed id is rejected.
        assert_eq!(do_commit(&manager, chunk_id), Some(RejectReason::NotFound));
    }

    #[test]
    fn rebump_advances_epoch_and_rederives_extents() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let chunk_id = stage(&manager, 0, &[10, 20, 30]);

        let before = manager.staging(chunk_id).expect("staging");
        let old_extent = before.shards[0].extent_id;

        let mut batch = WriteBatch::default();
        assert_eq!(
            manager
                .apply_rebump(&mut batch, &RebumpStaging { chunk_id })
                .expect("rebump"),
            None
        );
        commit(&manager, batch);

        let after = manager.staging(chunk_id).expect("staging");
        for slot in &after.shards {
            assert_eq!(slot.epoch, EPOCH_JUMP);
        }
        // Same disk, same create_ts, but a fresh extent id (epoch changed).
        assert_eq!(after.shards[0].disk_id, before.shards[0].disk_id);
        assert_eq!(
            after.shards[0].extent_id.create_ts(),
            old_extent.create_ts()
        );
        assert_ne!(after.shards[0].extent_id, old_extent);

        // Rebump on an unknown id is rejected.
        let mut batch = WriteBatch::default();
        assert_eq!(
            manager
                .apply_rebump(
                    &mut batch,
                    &RebumpStaging {
                        chunk_id: ChunkId::new(999)
                    }
                )
                .expect("rebump"),
            Some(RejectReason::NotFound)
        );
    }

    #[test]
    fn restore_rebuilds_committed_staging_and_secondary_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let committed_id = {
            let manager = open_manager(&dir);
            let id = stage(&manager, 0, &[10, 20, 30]);
            do_commit(&manager, id);
            // Leave a second chunk in staging (uncommitted).
            stage(&manager, 0, &[11, 21, 31]);
            id
        };

        let manager = open_manager(&dir);
        assert_eq!(manager.len(), 1);
        assert_eq!(manager.pending_staging().len(), 1);
        assert!(manager.get(committed_id).is_some());
        let prefix = ShardId::new(committed_id, 0, 0).shard_prefix();
        assert_eq!(
            manager.shard_prefixes_on_disk(DiskId::new(10)),
            vec![prefix]
        );

        // The counter was restored: the next stage continues the sequence.
        let next = stage(&manager, 0, &[12, 22, 32]);
        assert_eq!(next, ChunkId::new(3));
    }

    #[test]
    fn snapshot_view_and_install_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = open_manager(&dir);
        let committed_id = stage(&source, 0, &[10, 20, 30]);
        do_commit(&source, committed_id);
        stage(&source, 0, &[11, 21, 31]); // staging
        let (chunks, next_id, staging) = source.snapshot_view();
        assert_eq!(chunks.len(), 1);
        assert_eq!(staging.len(), 1);
        assert_eq!(next_id, 3);

        let target_dir = tempfile::tempdir().expect("tempdir");
        let target = open_manager(&target_dir);
        stage(&target, 0, &[99, 98, 97]); // stale staging to be replaced

        let mut batch = WriteBatch::default();
        target
            .stage_and_apply_snapshot(&mut batch, chunks, next_id, staging)
            .expect("install snapshot");
        commit(&target, batch);

        assert_eq!(target.len(), 1);
        assert_eq!(target.pending_staging().len(), 1);
        assert!(target.get(committed_id).is_some());
        let prefix = ShardId::new(committed_id, 2, 0).shard_prefix();
        assert_eq!(target.shard_prefixes_on_disk(DiskId::new(30)), vec![prefix]);

        // Disk from the replaced stale staging is gone; restore agrees with memory.
        target.restore().expect("restore");
        assert_eq!(target.len(), 1);
        assert_eq!(stage(&target, 0, &[1, 2, 3]), ChunkId::new(3));
    }

    fn rebind(
        manager: &ChunkManager,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        new_disk: u32,
    ) -> Result<Option<RejectReason>, SmError> {
        let mut batch = WriteBatch::default();
        let reject = manager.apply_commit_shard_mapping(
            &mut batch,
            &CommitShardMapping {
                job_id: 1,
                chunk_id,
                index,
                expected_epoch,
                new_disk: DiskId::new(new_disk),
                new_create_ts: CREATE_TS + 1,
                committer: epoch_proto::NodeId::new(1),
            },
        )?;
        commit(manager, batch);
        Ok(reject)
    }

    #[test]
    fn commit_shard_mapping_rebinds_slot_and_bumps_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let chunk_id = stage(&manager, 0, &[1, 2, 3]);
        assert_eq!(do_commit(&manager, chunk_id), None);

        // Rebind shard index 1 (was on disk 2, epoch 0) to disk 5.
        assert_eq!(rebind(&manager, chunk_id, 1, 0, 5).expect("rebind"), None);

        let chunk = manager.get(chunk_id).expect("chunk");
        let slot = chunk.shards.iter().find(|s| s.index() == 1).expect("slot");
        assert_eq!(slot.disk_id, DiskId::new(5), "slot moved to the new disk");
        assert_eq!(
            slot.epoch, 1,
            "epoch bumped on rebind (01 §6.4 invariant 5)"
        );
        assert_eq!(
            slot.extent_id,
            ExtentId::new(slot.shard_id(), CREATE_TS + 1),
            "extent derived from new shard id + coordinator create_ts"
        );

        // The prefix moved disks in the secondary index.
        let prefix = ShardId::new(chunk_id, 1, 0).shard_prefix();
        assert_eq!(manager.shard_prefixes_on_disk(DiskId::new(5)), vec![prefix]);
        assert!(manager.shard_prefixes_on_disk(DiskId::new(2)).is_empty());

        // Survives restore (persisted).
        manager.restore().expect("restore");
        let slot = manager
            .get(chunk_id)
            .expect("chunk")
            .shards
            .into_iter()
            .find(|s| s.index() == 1)
            .expect("slot");
        assert_eq!(slot.disk_id, DiskId::new(5));
        assert_eq!(slot.epoch, 1);
    }

    #[test]
    fn commit_shard_mapping_rejects_stale_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let chunk_id = stage(&manager, 0, &[1, 2, 3]);
        do_commit(&manager, chunk_id);

        // The slot is at epoch 0; a commit claiming epoch 1 is stale.
        assert_eq!(
            rebind(&manager, chunk_id, 1, 1, 5).expect("rebind"),
            Some(RejectReason::InvalidTransition)
        );
        // Unchanged.
        let slot = manager
            .get(chunk_id)
            .expect("chunk")
            .shards
            .into_iter()
            .find(|s| s.index() == 1)
            .expect("slot");
        assert_eq!(slot.disk_id, DiskId::new(2));
        assert_eq!(slot.epoch, 0);
    }

    #[test]
    fn commit_shard_mapping_rejects_unknown_chunk_or_index() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        let chunk_id = stage(&manager, 0, &[1, 2, 3]);
        do_commit(&manager, chunk_id);

        assert_eq!(
            rebind(&manager, ChunkId::new(999), 0, 0, 5).expect("rebind"),
            Some(RejectReason::NotFound),
            "unknown chunk"
        );
        assert_eq!(
            rebind(&manager, chunk_id, 9, 0, 5).expect("rebind"),
            Some(RejectReason::NotFound),
            "index out of range"
        );
    }

    #[test]
    fn list_chunks_pages_in_id_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        // Stage + commit three chunks (ids 1,2,3 in creation order).
        for _ in 0..3 {
            let id = stage(&manager, 0, &[1, 2, 3]);
            do_commit(&manager, id);
        }

        // First page (after 0, limit 2) → chunks 1,2.
        let page = manager.list_chunks(ChunkId::new(0), 2);
        assert_eq!(
            page.iter().map(|c| c.chunk_id.get()).collect::<Vec<_>>(),
            vec![1, 2],
            "first page in ascending id order, capped at limit"
        );
        // Next page (after 2) → chunk 3, then empty.
        let page = manager.list_chunks(ChunkId::new(2), 2);
        assert_eq!(
            page.iter().map(|c| c.chunk_id.get()).collect::<Vec<_>>(),
            vec![3]
        );
        assert!(
            manager.list_chunks(ChunkId::new(3), 2).is_empty(),
            "past the last chunk the page is empty (round end)"
        );
    }

    #[test]
    fn status_histogram_counts_by_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manager = open_manager(&dir);
        // Three committed chunks — all Writable at creation.
        let mut ids = Vec::new();
        for _ in 0..3 {
            let id = stage(&manager, 0, &[1, 2, 3]);
            do_commit(&manager, id);
            ids.push(id);
        }
        let h = manager.status_histogram();
        assert_eq!(h.total, 3);
        assert_eq!(h.writable, 3, "committed chunks start Writable");
        assert_eq!(h.sealed + h.full + h.migrating + h.broken, 0);

        // Seal one → the histogram reflects the transition.
        let mut batch = WriteBatch::default();
        manager
            .apply_seal_chunk(&mut batch, &SealChunk { chunk_id: ids[0] })
            .expect("seal");
        commit(&manager, batch);
        let h = manager.status_histogram();
        assert_eq!(h.total, 3);
        assert_eq!(h.writable, 2);
        assert_eq!(h.sealed, 1);
    }
}
