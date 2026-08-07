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

//! DataNode maintenance daemon: the production driver for compaction, scrub, and
//! extent reclaim (02 §1.6 / §1.9).
//!
//! Every sweep, on this node's own disk:
//!   1. **reclaim** — delete `.trash` extents retired by an earlier compaction
//!      and unbound orphans left by an interrupted one;
//!   2. **compact** — rebuild the dirtiest extents so tombstoned bytes become
//!      free space. This is the *only* path that returns disk space: DELETE and
//!      GcRound only set tombstones, so without this sweep usage tracks
//!      bytes-ever-written and an extent that fills stays wedged at `Full`;
//!   3. **scrub** — verify bitrot frames on a rotating slice of the disk so cold
//!      data's corruption is found before a client reads it (and before enough
//!      redundancy erodes to make it unrecoverable). Corrupt blobs are reported
//!      to PD as shard-repair tickets, the same channel heal-on-read uses.
//!
//! Each phase has a per-sweep budget so background I/O stays bounded; work that
//! does not fit waits for the next sweep. Scrub keeps a cursor across sweeps
//! (round-robin over extents) rather than restarting, so a disk larger than one
//! sweep's budget is still covered completely.
//!
//! Design: docs/design/02-datanode.md §1.6/§1.9; docs/design/01-pd.md §6.3

use std::sync::Arc;

use epoch_client::PdClient;
use epoch_proto::{DiskId, ExtentId, NodeId};
use epoch_store::StorageEngine;
use tokio::task::JoinHandle;

use crate::config::MaintenanceSpec;

/// Owns the maintenance daemon task; aborts it on drop.
#[derive(Debug)]
pub struct MaintenanceHandle {
    task: JoinHandle<()>,
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Picks this sweep's scrub targets, advancing `cursor` round-robin.
///
/// Pure so the rotation is directly testable: the cursor is an index into the
/// (sorted, stable) extent list, and a sweep takes at most `budget` ids from it,
/// wrapping once. Returns the picked ids and the next cursor.
///
/// INVARIANT(design 02 §1.9): scrub must eventually visit *every* extent — cold
/// data is exactly where bitrot goes unnoticed, since no client read will
/// surface it. A sweep that always restarted at 0 would never reach the tail of a
/// disk whose extent count exceeds the budget.
#[must_use]
pub fn scrub_targets(extents: &[ExtentId], cursor: usize, budget: usize) -> (Vec<ExtentId>, usize) {
    if extents.is_empty() || budget == 0 {
        return (Vec::new(), 0);
    }
    let start = cursor % extents.len();
    let take = budget.min(extents.len());
    let picked: Vec<ExtentId> = extents
        .iter()
        .cycle()
        .skip(start)
        .take(take)
        .copied()
        .collect();
    ((picked), (start + take) % extents.len())
}

/// Spawns the maintenance daemon: every `spec.interval()` it reclaims retired
/// extents, compacts the dirtiest ones, and scrubs a rotating slice of the disk.
///
/// Best-effort: a failing phase is logged and the sweep continues, so one bad
/// extent never stops space reclamation. `pd` is optional — without it, scrub
/// still detects corruption but has nowhere to report it (blueprint mode).
#[must_use]
pub fn spawn_maintenance(
    engine: StorageEngine,
    spec: MaintenanceSpec,
    node_id: NodeId,
    disk_id: DiskId,
    pd: Option<Arc<PdClient>>,
) -> MaintenanceHandle {
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(spec.interval());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut scrub_cursor = 0usize;
        loop {
            ticker.tick().await;
            scrub_cursor = run_sweep(
                &engine,
                &spec,
                node_id,
                disk_id,
                pd.as_deref(),
                scrub_cursor,
            )
            .await;
        }
    });
    MaintenanceHandle { task }
}

/// Runs one maintenance sweep; returns the next scrub cursor.
async fn run_sweep(
    engine: &StorageEngine,
    spec: &MaintenanceSpec,
    node_id: NodeId,
    disk_id: DiskId,
    pd: Option<&PdClient>,
    scrub_cursor: usize,
) -> usize {
    // 1. Reclaim: physically remove what an earlier sweep retired.
    match engine.reclaim_dropped().await {
        Ok(ids) if !ids.is_empty() => {
            tracing::info!(
                node = node_id.get(),
                reclaimed = ids.len(),
                "trash reclaimed"
            );
        }
        Ok(_) => {}
        Err(err) => tracing::warn!(node = node_id.get(), error = %err, "reclaim_dropped failed"),
    }
    match engine.reclaim_orphans().await {
        Ok(ids) if !ids.is_empty() => {
            tracing::info!(
                node = node_id.get(),
                reclaimed = ids.len(),
                "orphans reclaimed"
            );
        }
        Ok(_) => {}
        Err(err) => tracing::warn!(node = node_id.get(), error = %err, "reclaim_orphans failed"),
    }

    // 2. Compact: the only path that turns tombstones into free bytes.
    match engine.list_compaction_candidates(spec.compact_ratio_percent) {
        Ok(candidates) => {
            let total = candidates.len();
            for extent in candidates.into_iter().take(spec.compact_per_sweep) {
                match engine.compact_extent(extent).await {
                    Ok(outcome) => tracing::info!(
                        node = node_id.get(),
                        disk = disk_id.get(),
                        reclaimed_bytes = outcome.reclaimed_bytes,
                        live_blobs = outcome.live_blobs,
                        "extent compacted"
                    ),
                    Err(err) => {
                        tracing::warn!(node = node_id.get(), error = %err, "compaction failed");
                    }
                }
            }
            if total > spec.compact_per_sweep {
                // Never let a bounded budget look like "nothing left to do".
                tracing::info!(
                    node = node_id.get(),
                    deferred = total - spec.compact_per_sweep,
                    "compaction candidates deferred to the next sweep"
                );
            }
        }
        Err(err) => tracing::warn!(node = node_id.get(), error = %err, "candidate scan failed"),
    }

    // 3. Scrub a rotating slice; report corrupt blobs as repair tickets.
    let extents = match engine.list_extent_ids() {
        Ok(mut ids) => {
            ids.sort();
            ids
        }
        Err(err) => {
            tracing::warn!(node = node_id.get(), error = %err, "list_extent_ids failed");
            return scrub_cursor;
        }
    };
    let (targets, next_cursor) = scrub_targets(&extents, scrub_cursor, spec.scrub_per_sweep);
    for extent in targets {
        match engine.scrub_extent(extent).await {
            Ok(report) if !report.corrupt.is_empty() => {
                tracing::warn!(
                    node = node_id.get(),
                    disk = disk_id.get(),
                    corrupt = report.corrupt.len(),
                    "scrub found corrupt blobs"
                );
                report_corruption(pd, node_id, extent).await;
            }
            Ok(_) => {}
            Err(err) => tracing::warn!(node = node_id.get(), error = %err, "scrub failed"),
        }
    }
    next_cursor
}

/// Reports a scrub-detected corrupt shard to PD, reusing the heal-on-read
/// ticket channel (01 §6.3): the shard is rebuilt from its survivors.
async fn report_corruption(pd: Option<&PdClient>, node_id: NodeId, extent: ExtentId) {
    let Some(pd) = pd else {
        // Blueprint mode: detection without a control plane to repair it.
        tracing::warn!(
            node = node_id.get(),
            "corruption detected but no PD to report to"
        );
        return;
    };
    let shard = extent.shard_id();
    if let Err(err) = pd
        .report_shard_repair(shard.chunk_id(), shard.index())
        .await
    {
        tracing::warn!(node = node_id.get(), error = %err, "shard repair report failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::{ChunkId, ShardId};

    fn extent(seq: u64) -> ExtentId {
        ExtentId::new(ShardId::new(ChunkId::new(1), 0, 0), seq as i64)
    }

    #[test]
    fn scrub_rotates_across_sweeps_and_covers_every_extent() {
        let extents: Vec<ExtentId> = (1..=5).map(extent).collect();
        let mut cursor = 0;
        let mut seen = Vec::new();
        // Three sweeps at budget 2 must cover all five (wrapping once).
        for _ in 0..3 {
            let (picked, next) = scrub_targets(&extents, cursor, 2);
            seen.extend(picked);
            cursor = next;
        }
        for e in &extents {
            assert!(
                seen.contains(e),
                "extent {e:?} was never scrubbed: {seen:?}"
            );
        }
    }

    #[test]
    fn scrub_budget_never_exceeds_the_extent_count() {
        let extents: Vec<ExtentId> = (1..=2).map(extent).collect();
        let (picked, _) = scrub_targets(&extents, 0, 10);
        assert_eq!(picked.len(), 2, "no extent is scrubbed twice in one sweep");
    }

    #[test]
    fn scrub_handles_empty_disk_and_disabled_budget() {
        assert_eq!(scrub_targets(&[], 3, 2), (Vec::new(), 0));
        let extents: Vec<ExtentId> = vec![extent(1)];
        assert_eq!(scrub_targets(&extents, 0, 0), (Vec::new(), 0), "0 disables");
    }
}
