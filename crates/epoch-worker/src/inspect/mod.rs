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

//! InspectRound execution (01 §6.3 / §6.4): the stripe-presence backstop that
//! upgrades from optional to a **correctness component**. A coordinator walks the
//! cluster's chunks in segments; for each chunk it probes every shard's presence
//! and reports any missing shard to PD, which raises a ShardRepair ticket — the
//! same intake heal-on-read uses (04 §4). This is the backstop that catches a
//! quorum-write gap or a silently-lost shard that no read has touched (01 §6.4
//! Q5): the scan period bounds the maximum exposure window of a missing shard.
//!
//! One [`InspectSubtask`] inspects one chunk. Presence is probed via the
//! injected [`InspectBackend`] (production: a `ListBlobs` per shard extent — a
//! shard whose extent is missing/unreadable reads as absent). A subtask is
//! idempotent and side-effect-light: it only *reports* (best-effort), never
//! mutates, so re-running one is always safe.

use std::sync::Arc;

use async_trait::async_trait;
use epoch_proto::ChunkId;

use crate::repair::ChunkLayout;
use crate::subtask::{Subtask, SubtaskError};

/// The injected cluster seam the inspect subtask drives. Production maps
/// `probe_present` to a data-plane `ListBlobs` (extent present ⇒ shard present)
/// and `report_missing` to PD's `ReportShardRepair`; tests use an in-memory
/// double.
#[async_trait]
pub trait InspectBackend: Send + Sync {
    /// Whether shard `index` of `chunk_id` is present (its extent exists and is
    /// readable on its hosting node). A transient probe failure is reported as
    /// `false` — a spurious ShardRepair report is harmless (the repair path
    /// re-checks and no-ops if the shard is actually fine).
    async fn probe_present(&self, layout: &ChunkLayout, index: u8) -> bool;

    /// Reports shard `index` of `chunk_id` missing to PD (raises a ShardRepair
    /// ticket). Best-effort — a lost report is caught by the next round.
    async fn report_missing(&self, chunk_id: ChunkId, index: u8) -> Result<(), SubtaskError>;
}

/// One chunk's stripe-presence inspection: probe every shard, report the missing
/// ones. Idempotent (report-only, no mutation).
pub struct InspectSubtask<B: InspectBackend> {
    backend: Arc<B>,
    layout: ChunkLayout,
}

impl<B: InspectBackend> InspectSubtask<B> {
    /// Builds an inspect subtask for one chunk.
    #[must_use]
    pub fn new(backend: Arc<B>, layout: ChunkLayout) -> Self {
        Self { backend, layout }
    }
}

#[async_trait]
impl<B: InspectBackend> Subtask for InspectSubtask<B> {
    fn id(&self) -> u64 {
        // The chunk id identifies this subtask within the round.
        u64::from(self.layout.chunk_id.get())
    }

    async fn is_done(&self) -> Result<bool, SubtaskError> {
        // Inspection has no persistent "done" state — the round's progress
        // watermark (segment cursor) governs resumption, not per-chunk state.
        // Re-inspecting a chunk is cheap and idempotent, so never skip.
        Ok(false)
    }

    async fn execute(&self) -> Result<(), SubtaskError> {
        let total = self.layout.shards.len();
        for index in 0..total {
            let idx = u8::try_from(index)
                .map_err(|_| SubtaskError::Failed("shard index exceeds u8".into()))?;
            if !self.backend.probe_present(&self.layout, idx).await {
                // A missing shard is the backstop's finding — report it. A report
                // failure is surfaced so the coordinator retries the round; the
                // report itself is idempotent at PD (ticket keyed by shard).
                self.backend
                    .report_missing(self.layout.chunk_id, idx)
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use epoch_proto::{NodeId, ShardId};

    fn layout() -> ChunkLayout {
        let chunk_id = ChunkId::new(7);
        ChunkLayout {
            chunk_id,
            data: 2,
            parity: 1,
            stripe_size: 128,
            shards: vec![
                (ShardId::new(chunk_id, 0, 0), NodeId::new(1)),
                (ShardId::new(chunk_id, 1, 0), NodeId::new(2)),
                (ShardId::new(chunk_id, 2, 0), NodeId::new(3)),
            ],
        }
    }

    /// A backend where a fixed set of indices probe as missing; records reports.
    struct FakeBackend {
        missing: Vec<u8>,
        reported: Mutex<Vec<(ChunkId, u8)>>,
    }

    #[async_trait]
    impl InspectBackend for FakeBackend {
        async fn probe_present(&self, _layout: &ChunkLayout, index: u8) -> bool {
            !self.missing.contains(&index)
        }
        async fn report_missing(&self, chunk_id: ChunkId, index: u8) -> Result<(), SubtaskError> {
            self.reported.lock().unwrap().push((chunk_id, index));
            Ok(())
        }
    }

    #[tokio::test]
    async fn reports_only_the_missing_shards() {
        let backend = Arc::new(FakeBackend {
            missing: vec![1],
            reported: Mutex::new(Vec::new()),
        });
        let task = InspectSubtask::new(Arc::clone(&backend), layout());
        task.execute().await.expect("inspect");
        assert_eq!(
            *backend.reported.lock().unwrap(),
            vec![(ChunkId::new(7), 1)],
            "exactly the missing shard 1 is reported"
        );
    }

    #[tokio::test]
    async fn all_present_reports_nothing() {
        let backend = Arc::new(FakeBackend {
            missing: vec![],
            reported: Mutex::new(Vec::new()),
        });
        let task = InspectSubtask::new(Arc::clone(&backend), layout());
        task.execute().await.expect("inspect");
        assert!(
            backend.reported.lock().unwrap().is_empty(),
            "a fully-present chunk raises no repair"
        );
    }
}
