//! The `gc` role wiring (M7, 01 §6.3 / Q27): the DataNode-embedded GcRound
//! self-scan daemon.
//!
//! GcRound is coordinator-less (01 §6.3 "各节点自扫本盘, PD 只发 Round 号"): each
//! DataNode periodically scans its own disk for orphaned blobs and tombstones
//! them. A blob `(t, s)` is reclaimable iff no live object references it AND its
//! write is terminal — its token is dead (+grace) or `s ≤ W(t)` (the commit
//! watermark, Q27). The daemon assembles that decision:
//!
//! 1. PD `GetLiveWriters` → the live-token table with each token's `W(t)`;
//! 2. PD `ListPartitions` → every MetaNode partition leader, then MetaNode
//!    `ExportReferences` on each → the union reference keep-set;
//! 3. per local extent, [`epoch_worker::GcSubtask`] diffs and tombstones.
//!
//! Reclaim is idempotent (tombstone is idempotent; physical space returns via
//! the existing compaction pass). A missing partition/live-table read aborts the
//! round (better to skip a round than reclaim against a partial keep-set —
//! reclaiming a referenced blob would be data loss).
//!
//! Design: docs/design/01-pd.md §6.3; docs/design/99-open-questions.md Q20/Q27

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use epoch_client::{MetaClient, PdClient};
use epoch_proto::{BlobId, ExtentId, NodeId, WriterToken};
use epoch_store::StorageEngine;
use epoch_worker::{GcBackend, GcSubtask, LiveTokens, SubtaskError};
use tokio::task::JoinHandle;

/// Owns the GC daemon task; aborts it on drop.
#[derive(Debug)]
pub struct GcHandle {
    task: JoinHandle<()>,
}

impl Drop for GcHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the DataNode GcRound self-scan daemon: every `interval` it builds the
/// reference keep-set + live-token table and reclaims orphaned blobs on this
/// node's disk. Best-effort — a round that cannot assemble a complete keep-set
/// is skipped (never reclaims against partial references).
#[must_use]
pub fn spawn_gc_daemon(
    pd: PdClient,
    meta: MetaClient,
    engine: StorageEngine,
    node_id: NodeId,
    interval: Duration,
) -> GcHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(err) = run_gc_round(&pd, &meta, &engine).await {
                tracing::debug!(node = node_id.get(), error = %err, "gc round skipped");
            }
        }
    });
    GcHandle { task }
}

/// Runs one GC round over this node's extents. Returns the number of orphans
/// tombstoned, or an error if the keep-set could not be fully assembled (the
/// round is then skipped rather than reclaiming against partial references).
async fn run_gc_round(
    pd: &PdClient,
    meta: &MetaClient,
    engine: &StorageEngine,
) -> Result<usize, SubtaskError> {
    // 1. Live-token table (token → W(t)); a dead token is absent.
    let live: LiveTokens = pd
        .get_live_writers()
        .await
        .map_err(|e| SubtaskError::NotReady(format!("get_live_writers: {e}")))?
        .into_iter()
        .map(|w| (WriterToken::new(w.writer_token), w.commit_watermark))
        .collect();

    // 2. Reference keep-set: the union of every partition leader's export. A
    //    partition that cannot be read aborts the round — reclaiming against a
    //    partial keep-set could delete a referenced blob (data loss).
    let partitions = pd
        .list_partitions()
        .await
        .map_err(|e| SubtaskError::NotReady(format!("list_partitions: {e}")))?;
    let mut referenced: BTreeSet<u64> = BTreeSet::new();
    for view in partitions {
        if view.leader_addr.is_empty() {
            return Err(SubtaskError::NotReady(format!(
                "partition {} has no leader yet",
                view.partition_id
            )));
        }
        match meta
            .export_references(&view.leader_addr, view.partition_id)
            .await
            .map_err(|e| SubtaskError::NotReady(format!("export_references: {e}")))?
        {
            Some(ids) => referenced.extend(ids),
            // The addressed node is not the leader (route moved) — retry next
            // round rather than reclaim against an incomplete set.
            None => {
                return Err(SubtaskError::NotReady(format!(
                    "partition {} leader moved",
                    view.partition_id
                )));
            }
        }
    }

    // 3. Diff each local extent and tombstone its orphans.
    let referenced = Arc::new(referenced);
    let live = Arc::new(live);
    let backend = Arc::new(EngineGcBackend {
        engine: engine.clone(),
    });
    let extents = engine
        .list_extent_ids()
        .map_err(|e| SubtaskError::Failed(format!("list_extent_ids: {e}")))?;
    let mut reclaimed = 0;
    for extent in extents {
        let task = GcSubtask::new(
            Arc::clone(&backend),
            extent,
            Arc::clone(&referenced),
            Arc::clone(&live),
        );
        reclaimed += task.run().await?;
    }
    if reclaimed > 0 {
        tracing::info!(reclaimed, "gc round reclaimed orphaned blobs");
    }
    Ok(reclaimed)
}

/// The production [`GcBackend`]: enumerate/tombstone via the local store engine.
struct EngineGcBackend {
    engine: StorageEngine,
}

#[async_trait]
impl GcBackend for EngineGcBackend {
    async fn list_live_blobs(&self, extent: ExtentId) -> Result<Vec<BlobId>, SubtaskError> {
        self.engine
            .list_live_blobs(extent)
            .await
            .map_err(|e| SubtaskError::Failed(format!("list_live_blobs: {e}")))
    }

    async fn tombstone(&self, extent: ExtentId, blob: BlobId) -> Result<(), SubtaskError> {
        // `delete` resolves the shard's current binding under the stripe lock —
        // the extent id came from a PD placement read that can race a compaction
        // rebind (02 §1.6 INVARIANT).
        self.engine
            .delete(extent, blob)
            .await
            .map(|_| ())
            .map_err(|e| SubtaskError::Failed(format!("delete blob: {e}")))
    }
}
