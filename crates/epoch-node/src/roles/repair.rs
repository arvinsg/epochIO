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

//! The `repair` role wiring (M7, 01 §6.3): the DataNode-embedded RepairDisk
//! coordinator daemon + single-shard repair driver, and the production
//! [`RepairBackend`] they drive.
//!
//! Two pull loops share one daemon (both keyed on this node, latency-tolerant):
//!
//! - **RepairDisk** (Job-driven): poll PD (`ListNodeJobs`) for RepairDisk jobs
//!   assigned to this node as coordinator, expand each into per-shard
//!   [`RepairSubtask`]s, run them through [`epoch_worker::run_job`], then mark
//!   the Job Done (`CommitJobProgress{done}`). The rebind is Job-authorized
//!   (`CommitShardMapping`).
//! - **ShardRepair** (no Job, 01 §6.3): poll PD (`ListNodeShardRepairs`) for
//!   single-shard repairs PD dispatched to this node, rebuild each in place
//!   ([`ShardRepairTask`]), and commit the completion-receipt rebind
//!   (`CommitShardRepair`), authorized by the pending ShardRepair *ticket*
//!   rather than a Job (01 §6.4 invariant 2).
//!
//! Both rebuild onto the coordinator's *own* disk (the simplest healthy target;
//! data-affinity placement is a follow-up) and are idempotent end to end, so a
//! crash + re-dispatch simply re-runs from surviving state (already-rebound
//! slots report done).
//!
//! Design: docs/design/01-pd.md §6.1/§6.3; docs/design/02-datanode.md §3.1/§3.2

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use epoch_client::PdClient;
use epoch_proto::grpc::pd;
use epoch_proto::{BlobId, ChunkId, DiskId, ExtentId, NodeId, ShardId};
use epoch_rpc::{ListBlobsReq, ReadShardReq, ShardTransport};
use epoch_store::StorageEngine;
use epoch_worker::repair::{
    ChunkLayout, CommitOutcome, RepairBackend, RepairSubtask, ShardRepairTask,
};
use epoch_worker::{InspectBackend, InspectSubtask};
use epoch_worker::{Subtask, SubtaskError, run_job};
use tokio::task::JoinHandle;

/// Poll interval for the coordinator daemon (01 §6.1). A RepairDisk job is not
/// latency-critical; poll modestly to keep PD read load low.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Owns the coordinator daemon task; aborts it on drop.
#[derive(Debug)]
pub struct RepairCoordinatorHandle {
    task: JoinHandle<()>,
}

impl Drop for RepairCoordinatorHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the DataNode-embedded RepairDisk coordinator: polls PD for jobs
/// assigned to `node_id` and drives each to completion. `transport` reads
/// surviving shards from peers; `engine` is this node's local store (the rebuild
/// target); `local_disk` is the disk rebuilt extents are created on.
#[must_use]
pub fn spawn_repair_coordinator(
    pd: PdClient,
    transport: Arc<dyn ShardTransport>,
    engine: StorageEngine,
    node_id: NodeId,
    local_disk: DiskId,
) -> RepairCoordinatorHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(POLL_INTERVAL);
        loop {
            ticker.tick().await;
            // RepairDisk Jobs assigned to this node as coordinator.
            match pd.list_node_jobs(node_id).await {
                Ok(jobs) => {
                    for job in jobs {
                        // RepairDisk / DropDisk / Balance all move every shard off
                        // `job.disk_id` onto healthy disks; they share the expand +
                        // reconstruct + rebind executor and differ only in the PD
                        // trigger + completion (drop → Dropped, balance → done).
                        let moves_disk_shards = job.kind == pd::JobKind::RepairDisk as i32
                            || job.kind == pd::JobKind::DropDisk as i32
                            || job.kind == pd::JobKind::Balance as i32;
                        if moves_disk_shards {
                            if let Err(err) = drive_repair_job(
                                &pd, &transport, &engine, node_id, local_disk, &job,
                            )
                            .await
                            {
                                tracing::warn!(job = job.job_id, kind = job.kind, error = %err, "shard-move job drive failed; will retry");
                            }
                        } else if job.kind == pd::JobKind::InspectRound as i32
                            && let Err(err) = drive_inspect_job(&pd, &transport, &job).await
                        {
                            tracing::warn!(job = job.job_id, error = %err, "inspect job drive failed; will retry");
                        }
                        // GcRound is driven by its own node-self-scan path (not a
                        // coordinator Job).
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "repair coordinator: list_node_jobs failed");
                }
            }
            // Single-shard repairs (no Job) PD dispatched to this node (01 §6.3).
            match pd.list_node_shard_repairs(node_id).await {
                Ok(repairs) => {
                    for repair in repairs {
                        if let Err(err) = drive_shard_repair(
                            &pd, &transport, &engine, node_id, local_disk, &repair,
                        )
                        .await
                        {
                            tracing::warn!(
                                chunk = repair.chunk_id,
                                index = repair.index,
                                error = %err,
                                "shard repair drive failed; will retry"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "repair coordinator: list_node_shard_repairs failed");
                }
            }
        }
    });
    RepairCoordinatorHandle { task }
}

/// Drives one RepairDisk job: expand the broken disk's shards into subtasks, run
/// them through the coordinator, then mark the job Done.
async fn drive_repair_job(
    pd: &PdClient,
    transport: &Arc<dyn ShardTransport>,
    engine: &StorageEngine,
    node_id: NodeId,
    local_disk: DiskId,
    job: &pd::AssignedJob,
) -> Result<(), SubtaskError> {
    let broken_disk = DiskId::new(job.disk_id);
    let backend = Arc::new(PdRepairBackend {
        pd: pd.clone(),
        transport: Arc::clone(transport),
        engine: engine.clone(),
        job_id: job.job_id,
        committer: node_id,
        local_disk,
    });
    let expander = RepairExpander {
        pd: pd.clone(),
        transport: Arc::clone(transport),
        backend: Arc::clone(&backend),
        broken_disk,
    };
    // Progress checkpoints ride the coordinator's `run_job`; the sink commits
    // them + completion to PD.
    let sink = PdProgressSink {
        pd: pd.clone(),
        job_id: job.job_id,
        last_watermark: std::sync::atomic::AtomicU64::new(0),
    };
    run_job(&expander, &sink, job.progress_watermark).await?;
    // All subtasks done → mark the job complete.
    pd.commit_job_progress(job.job_id, job.progress_watermark, true)
        .await
        .map_err(|e| SubtaskError::Failed(format!("complete job: {e}")))?;
    Ok(())
}

/// Drives one single-shard repair (01 §6.3, no Job): resolve the chunk layout,
/// enumerate the target shard's blobs from a survivor, rebuild in place, and
/// commit the ticket-authorized rebind. Idempotent — a re-dispatch after a crash
/// finds the already-rebound slot (ticket cleared) and reports it done.
async fn drive_shard_repair(
    pd: &PdClient,
    transport: &Arc<dyn ShardTransport>,
    engine: &StorageEngine,
    node_id: NodeId,
    local_disk: DiskId,
    repair: &pd::ShardRepairAssignment,
) -> Result<(), SubtaskError> {
    let chunk_id = ChunkId::new(repair.chunk_id);
    let index = u8::try_from(repair.index)
        .map_err(|_| SubtaskError::Failed("shard index exceeds u8".into()))?;
    let (_nodes, disks) = pd
        .list_nodes()
        .await
        .map_err(|e| SubtaskError::Failed(format!("list_nodes: {e}")))?;
    let layout = chunk_layout_from_pd(pd, chunk_id, &disks).await?;
    let blobs = survivor_blob_ids(pd, transport, &layout, index).await?;

    // The backend rebuilds onto the target's own disk (in place); job_id is
    // unused on the ShardRepair path (the rebind is ticket-authorized).
    let backend = Arc::new(PdRepairBackend {
        pd: pd.clone(),
        transport: Arc::clone(transport),
        engine: engine.clone(),
        job_id: 0,
        committer: node_id,
        local_disk,
    });
    let task = ShardRepairTask::new(backend, layout, index, repair.expected_epoch, blobs);
    task.run().await.map(|_| ())
}

/// Drives one InspectRound job: walk the cluster's chunks in segments, probing
/// each shard's presence and reporting missing ones to PD (raises ShardRepair).
/// Runs through the coordinator framework so a reassigned coordinator resumes
/// from the round's progress watermark.
async fn drive_inspect_job(
    pd: &PdClient,
    transport: &Arc<dyn ShardTransport>,
    job: &pd::AssignedJob,
) -> Result<(), SubtaskError> {
    let backend = Arc::new(PdInspectBackend {
        pd: pd.clone(),
        transport: Arc::clone(transport),
    });
    let expander = InspectExpander {
        pd: pd.clone(),
        backend: Arc::clone(&backend),
    };
    let sink = PdProgressSink {
        pd: pd.clone(),
        job_id: job.job_id,
        last_watermark: std::sync::atomic::AtomicU64::new(0),
    };
    run_job(&expander, &sink, job.progress_watermark).await?;
    pd.commit_job_progress(job.job_id, job.progress_watermark, true)
        .await
        .map_err(|e| SubtaskError::Failed(format!("complete inspect job: {e}")))?;
    Ok(())
}

/// Expands an InspectRound job into one [`InspectSubtask`] per committed chunk,
/// paging through PD's `ListChunks`. Each subtask probes its chunk's shards and
/// reports any missing one.
struct InspectExpander {
    pd: PdClient,
    backend: Arc<PdInspectBackend>,
}

/// How many chunks to fetch per `ListChunks` page.
const INSPECT_PAGE: u32 = 256;

#[async_trait]
impl epoch_worker::JobExpander for InspectExpander {
    /// Expands the round into one [`InspectSubtask`] per committed chunk, paging
    /// PD's `ListChunks`. `resume_from` is the count of chunks already scanned in
    /// a prior (crashed) attempt — the watermark `run_job` checkpoints — so a
    /// reassigned coordinator resumes *past* them instead of restarting from
    /// chunk 0 (01 §6.3: PD 记 checkpoint 续扫). Without this a long round that
    /// loses its coordinator near the end could never reach the tail chunks.
    async fn expand(&self, resume_from: u64) -> Result<Vec<Arc<dyn Subtask>>, SubtaskError> {
        let (_nodes, disks) = self
            .pd
            .list_nodes()
            .await
            .map_err(|e| SubtaskError::Failed(format!("list_nodes: {e}")))?;
        let mut subtasks: Vec<Arc<dyn Subtask>> = Vec::new();
        let mut after = ChunkId::new(0);
        // Skip the first `resume_from` chunks (already scanned). Chunk ids are
        // monotonic, so counting off the page stream is a stable resume cursor.
        let mut skipped = 0u64;
        loop {
            let page = self
                .pd
                .list_chunks(after, INSPECT_PAGE)
                .await
                .map_err(|e| SubtaskError::Failed(format!("list_chunks: {e}")))?;
            if page.is_empty() {
                break;
            }
            for view in &page {
                let chunk_id = ChunkId::new(view.chunk_id);
                after = chunk_id;
                if skipped < resume_from {
                    skipped += 1;
                    continue;
                }
                let layout = chunk_layout_from_pd(&self.pd, chunk_id, &disks).await?;
                subtasks.push(Arc::new(InspectSubtask::new(
                    Arc::clone(&self.backend),
                    layout,
                )));
            }
        }
        Ok(subtasks)
    }
}

/// The production [`InspectBackend`]: probe presence via a data-plane `ListBlobs`
/// per shard extent, report misses via PD's `ReportShardRepair`.
struct PdInspectBackend {
    pd: PdClient,
    transport: Arc<dyn ShardTransport>,
}

#[async_trait]
impl InspectBackend for PdInspectBackend {
    async fn probe_present(&self, layout: &ChunkLayout, index: u8) -> bool {
        let Some(&(shard, node)) = layout.shards.get(usize::from(index)) else {
            return false;
        };
        // Resolve the shard's extent from PD's chunk view, then probe it. Any
        // error (chunk gone, extent missing, node unreachable) reads as absent —
        // a spurious report is harmless (the repair path re-checks).
        let Ok(shards) = self.pd.get_chunk(layout.chunk_id).await.map(|c| c.shards) else {
            return false;
        };
        let Ok(extent) = extent_of(&shards, shard) else {
            return false;
        };
        self.transport
            .list_blobs(node, ListBlobsReq { extent_id: extent })
            .await
            .is_ok()
    }

    async fn report_missing(&self, chunk_id: ChunkId, index: u8) -> Result<(), SubtaskError> {
        self.pd
            .report_shard_repair(chunk_id, index)
            .await
            .map(|_| ())
            .map_err(|e| SubtaskError::Failed(format!("report_shard_repair: {e}")))
    }
}

/// Expands a RepairDisk job into one [`RepairSubtask`] per shard the broken disk
/// held: lists the broken disk's shard slots (PD), and for each resolves the
/// chunk's placement + surviving blob set.
struct RepairExpander {
    pd: PdClient,
    transport: Arc<dyn ShardTransport>,
    backend: Arc<PdRepairBackend>,
    broken_disk: DiskId,
}

#[async_trait]
impl epoch_worker::JobExpander for RepairExpander {
    async fn expand(&self, _resume_from: u64) -> Result<Vec<Arc<dyn Subtask>>, SubtaskError> {
        // Every shard slot the broken disk currently holds (PD route view).
        let slots = self
            .pd
            .list_disk_shards(self.broken_disk)
            .await
            .map_err(|e| SubtaskError::Failed(format!("list_disk_shards: {e}")))?;

        // Topology for disk→node on the survivor reads (built once per expand).
        let (nodes, disks) = self
            .pd
            .list_nodes()
            .await
            .map_err(|e| SubtaskError::Failed(format!("list_nodes: {e}")))?;

        let mut subtasks: Vec<Arc<dyn Subtask>> = Vec::new();
        for slot in slots {
            let shard = ShardId::from_raw(slot.shard_prefix | u64::from(slot.epoch));
            let chunk_id = shard.chunk_id();
            let layout = self.chunk_layout(chunk_id, &nodes, &disks).await?;
            // Enumerate the blobs the broken shard held from a surviving peer of
            // the same chunk (EC stripe families share blob ids).
            let blobs = self.survivor_blob_ids(&layout, shard.index()).await?;
            subtasks.push(Arc::new(RepairSubtask::new(
                Arc::clone(&self.backend),
                layout,
                shard.index(),
                slot.epoch,
                self.broken_disk,
                blobs,
            )));
        }
        Ok(subtasks)
    }
}

impl RepairExpander {
    /// Builds a [`ChunkLayout`] from PD's `GetChunk`, resolving each shard's
    /// current node via the disk table (`disk_id → node_id`).
    async fn chunk_layout(
        &self,
        chunk_id: ChunkId,
        _nodes: &[pd::NodeInfo],
        disks: &[pd::DiskInfo],
    ) -> Result<ChunkLayout, SubtaskError> {
        chunk_layout_from_pd(&self.pd, chunk_id, disks).await
    }

    /// The blob ids held by a surviving shard of `layout` (any index but the
    /// one being rebuilt), via the data-plane `ListBlobs`.
    async fn survivor_blob_ids(
        &self,
        layout: &ChunkLayout,
        target: u8,
    ) -> Result<Vec<BlobId>, SubtaskError> {
        survivor_blob_ids(&self.pd, &self.transport, layout, target).await
    }
}

/// Builds a [`ChunkLayout`] from PD's `GetChunk`, resolving each shard's current
/// node via the disk table (`disk_id → node_id`). Shared by the RepairDisk
/// expander and the single-shard repair driver.
async fn chunk_layout_from_pd(
    pd: &PdClient,
    chunk_id: ChunkId,
    disks: &[pd::DiskInfo],
) -> Result<ChunkLayout, SubtaskError> {
    let chunk = pd
        .get_chunk(chunk_id)
        .await
        .map_err(|e| SubtaskError::Failed(format!("get_chunk: {e}")))?;
    let code = chunk
        .code_mode
        .ok_or_else(|| SubtaskError::Failed("chunk missing code mode".into()))?;
    let mut shards = vec![(ShardId::from_raw(0), NodeId::new(0)); chunk.shards.len()];
    for view in &chunk.shards {
        let shard = ShardId::from_raw(view.shard_prefix | u64::from(view.epoch));
        let node = disk_node(disks, view.disk_id)?;
        let index = usize::from(shard.index());
        if index >= shards.len() {
            return Err(SubtaskError::Failed("shard index out of range".into()));
        }
        shards[index] = (shard, node);
    }
    Ok(ChunkLayout {
        chunk_id,
        data: code.data as usize,
        parity: code.parity as usize,
        stripe_size: code.stripe_size as usize,
        shards,
    })
}

/// The blob ids held by a surviving shard of `layout` (any index but `target`),
/// via the data-plane `ListBlobs`. Shared by the RepairDisk expander and the
/// single-shard repair driver.
async fn survivor_blob_ids(
    pd: &PdClient,
    transport: &Arc<dyn ShardTransport>,
    layout: &ChunkLayout,
    target: u8,
) -> Result<Vec<BlobId>, SubtaskError> {
    let chunk = pd
        .get_chunk(layout.chunk_id)
        .await
        .map_err(|e| SubtaskError::Failed(format!("get_chunk: {e}")))?;
    for (j, &(shard, node)) in layout.shards.iter().enumerate() {
        if j == usize::from(target) {
            continue;
        }
        // Resolve the survivor's extent from PD's chunk view. Its extent id is
        // `shard | create_ts`; ListBlobs takes the extent id.
        let extent = extent_of(&chunk.shards, shard)?;
        match transport
            .list_blobs(node, ListBlobsReq { extent_id: extent })
            .await
        {
            Ok(ids) => return Ok(ids),
            Err(_) => continue, // try the next survivor
        }
    }
    Err(SubtaskError::NotReady(
        "no surviving shard to enumerate blobs".into(),
    ))
}

/// The production [`RepairBackend`]: PD control RPCs + data-plane reads + the
/// local engine (rebuild target).
struct PdRepairBackend {
    pd: PdClient,
    transport: Arc<dyn ShardTransport>,
    engine: StorageEngine,
    job_id: u64,
    committer: NodeId,
    local_disk: DiskId,
}

#[async_trait]
impl RepairBackend for PdRepairBackend {
    async fn seal_chunk(&self, chunk_id: ChunkId) -> Result<(), SubtaskError> {
        // 01 §6.3 不变量 4 (Seal 先行): fence the chunk before the rebuild reads
        // a single survivor, so the blob set enumerated at expand time is closed.
        // `SealChunk` is idempotent at PD and on the store, so a retried subtask
        // re-sealing is a no-op.
        self.pd
            .seal_chunk(chunk_id)
            .await
            .map_err(|e| SubtaskError::NotReady(format!("seal_chunk: {e}")))
    }

    async fn read_shard(
        &self,
        node: NodeId,
        shard: ShardId,
        blob_id: BlobId,
    ) -> Result<Option<Vec<u8>>, SubtaskError> {
        self.transport
            .read_shard(
                node,
                ReadShardReq {
                    shard_id: shard,
                    blob_id,
                },
            )
            .await
            .map(|opt| opt.map(|b| b.to_vec()))
            .map_err(|e| SubtaskError::NotReady(format!("read_shard: {e}")))
    }

    async fn ensure_rebuild_extent(&self, shard: ShardId) -> Result<ExtentId, SubtaskError> {
        // The rebuilt slot gets a fresh epoch (old + 1) so its extent id differs
        // from the broken one. The create_ts is derived deterministically from
        // the job id so a retry (after a crash) reuses the same extent id — the
        // engine treats a re-create of an installed extent as idempotent.
        let rebuilt_shard = ShardId::try_new(shard.chunk_id(), shard.index(), shard.epoch() + 1)
            .map_err(|_| SubtaskError::Failed("shard epoch overflow".into()))?;
        let create_ts = i64::try_from(self.job_id).unwrap_or(i64::MAX).max(1);
        self.engine
            .create_rebuilding_extent(rebuilt_shard, create_ts)
            .await
            .map_err(|e| SubtaskError::Failed(format!("create rebuilding extent: {e}")))
    }

    async fn write_rebuilt(
        &self,
        extent: ExtentId,
        blob_id: BlobId,
        body: Vec<u8>,
    ) -> Result<(), SubtaskError> {
        // Meter the rebuild write through the Repair QoS class (02 §1.7 / 01
        // §6.3 MTTR budget) so a repair storm yields to foreground IO.
        self.engine
            .throttle(epoch_store::IoClass::Repair, body.len() as u64)
            .await;
        self.engine
            .write(extent, blob_id, body.into())
            .await
            .map(|_| ())
            .map_err(|e| SubtaskError::Failed(format!("write rebuilt: {e}")))
    }

    async fn promote(&self, extent: ExtentId) -> Result<(), SubtaskError> {
        self.engine
            .promote_rebuilt(extent)
            .await
            .map_err(|e| SubtaskError::Failed(format!("promote: {e}")))
    }

    async fn commit_mapping(
        &self,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        extent: ExtentId,
    ) -> Result<CommitOutcome, SubtaskError> {
        let (_new_epoch, rebound) = self
            .pd
            .commit_shard_mapping(
                self.job_id,
                chunk_id,
                index,
                expected_epoch,
                self.local_disk,
                extent.create_ts(),
                self.committer,
            )
            .await
            .map_err(|e| SubtaskError::Failed(format!("commit_shard_mapping: {e}")))?;
        Ok(if rebound {
            CommitOutcome::Rebound
        } else {
            CommitOutcome::AlreadyRebound
        })
    }

    async fn commit_shard_repair(
        &self,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        extent: ExtentId,
    ) -> Result<CommitOutcome, SubtaskError> {
        let (_new_epoch, rebound) = self
            .pd
            .commit_shard_repair(
                chunk_id,
                index,
                expected_epoch,
                self.local_disk,
                extent.create_ts(),
                self.committer,
            )
            .await
            .map_err(|e| SubtaskError::Failed(format!("commit_shard_repair: {e}")))?;
        Ok(if rebound {
            CommitOutcome::Rebound
        } else {
            CommitOutcome::AlreadyRebound
        })
    }

    async fn slot_rebound(
        &self,
        chunk_id: ChunkId,
        index: u8,
        broken_disk: DiskId,
    ) -> Result<bool, SubtaskError> {
        let chunk = self
            .pd
            .get_chunk(chunk_id)
            .await
            .map_err(|e| SubtaskError::Failed(format!("get_chunk: {e}")))?;
        for view in &chunk.shards {
            let shard = ShardId::from_raw(view.shard_prefix | u64::from(view.epoch));
            if shard.index() == index {
                // Rebound once the slot no longer lives on the broken disk.
                return Ok(DiskId::new(view.disk_id) != broken_disk);
            }
        }
        Ok(false)
    }
}

/// The coordinator's [`ProgressSink`] over PD (`CommitJobProgress`).
struct PdProgressSink {
    pd: PdClient,
    job_id: u64,
    /// Last watermark this coordinator committed. A standalone `renew_lease`
    /// re-sends it so the renew never rewinds progress (PD's apply is monotonic
    /// anyway, but re-sending the current value keeps the two in step).
    last_watermark: std::sync::atomic::AtomicU64,
}

#[async_trait]
impl epoch_worker::ProgressSink for PdProgressSink {
    async fn checkpoint(&self, watermark: u64) -> Result<(), SubtaskError> {
        self.last_watermark
            .store(watermark, std::sync::atomic::Ordering::Relaxed);
        self.pd
            .commit_job_progress(self.job_id, watermark, false)
            .await
            .map_err(|e| SubtaskError::Failed(format!("checkpoint: {e}")))
    }

    async fn renew_lease(&self) -> Result<(), SubtaskError> {
        // A progress commit renews the lease on PD (01 §6.4), so a standalone
        // renew re-sends the current watermark. This matters between checkpoints:
        // the coordinator batches one every `CHECKPOINT_EVERY` subtasks, and a
        // long subtask run between two batches would otherwise let the 30s lease
        // lapse and get the job reassigned mid-flight.
        let watermark = self
            .last_watermark
            .load(std::sync::atomic::Ordering::Relaxed);
        self.pd
            .commit_job_progress(self.job_id, watermark, false)
            .await
            .map_err(|e| SubtaskError::Failed(format!("renew_lease: {e}")))
    }
}

/// Resolves the node currently hosting `disk_id` from the disk table.
fn disk_node(disks: &[pd::DiskInfo], disk_id: u32) -> Result<NodeId, SubtaskError> {
    disks
        .iter()
        .find(|d| d.disk_id == disk_id)
        .map(|d| NodeId::new(d.node_id))
        .ok_or_else(|| SubtaskError::Failed(format!("disk {disk_id} not in topology")))
}

/// The bound extent id of `shard` from a chunk's shard views.
fn extent_of(shards: &[pd::ShardView], shard: ShardId) -> Result<ExtentId, SubtaskError> {
    for view in shards {
        let vshard = ShardId::from_raw(view.shard_prefix | u64::from(view.epoch));
        if vshard == shard {
            let bytes: [u8; 16] = view
                .extent_id
                .as_slice()
                .try_into()
                .map_err(|_| SubtaskError::Failed("bad extent id length".into()))?;
            return Ok(ExtentId::from_bytes(bytes));
        }
    }
    Err(SubtaskError::Failed("shard not in chunk view".into()))
}
