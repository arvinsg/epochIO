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

//! GcRound diff + reclaim (01 §6.3 / Q20 / Q27): a DataNode self-scans its
//! blobs, diffs them against the MetaNode reference keep-set, and tombstones the
//! orphans — the space-reclaim half of the token-watermark GC.
//!
//! A local blob `(t, s)` is a reclaimable orphan iff it is **not referenced** by
//! any live object AND its write is provably terminal:
//!
//! - `t` is not a live writer token (dead + grace, so no in-flight write can
//!   still reference it), **or**
//! - `s ≤ W(t)` — the token's commit watermark, so this seq has committed or
//!   been abandoned (an in-flight `s > W(t)` is exempt: a long PUT's early blob
//!   is not yet referenced but must not be reclaimed, Q27).
//!
//! [`orphans`] is the pure decision (fully testable); [`GcSubtask`] drives it
//! over a [`GcBackend`] seam (list local blobs → tombstone), mirroring the
//! repair subtask's injected-backend shape. Reclaim is idempotent (tombstone is
//! idempotent; the physical space returns via the existing compaction path).
//!
//! Design: docs/design/01-pd.md §6.3; docs/design/99-open-questions.md Q20/Q27

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use epoch_proto::{BlobId, ExtentId, WriterToken};

use crate::subtask::SubtaskError;

/// The live-writer table a GcRound diffs against (01 §6.3): per live token, its
/// commit watermark `W(t)`. A token absent from the map is dead (+grace) — all
/// its blobs are terminal and reclaimable if unreferenced.
pub type LiveTokens = HashMap<WriterToken, i64>;

/// Selects the reclaimable orphans among `local` blobs: those neither in the
/// `referenced` keep-set nor still possibly-in-flight (Q27 predicate). Pure and
/// deterministic. Returns the orphans in input order.
#[must_use]
pub fn orphans(local: &[BlobId], referenced: &BTreeSet<u64>, live: &LiveTokens) -> Vec<BlobId> {
    local
        .iter()
        .copied()
        .filter(|blob| is_orphan(*blob, referenced, live))
        .collect()
}

/// Whether one local blob is a reclaimable orphan (the Q27 predicate).
fn is_orphan(blob: BlobId, referenced: &BTreeSet<u64>, live: &LiveTokens) -> bool {
    if referenced.contains(&blob.as_u64()) {
        return false; // a live object still references it
    }
    match live.get(&blob.writer_token()) {
        // Live token: reclaimable only at or below its commit watermark; an
        // in-flight seq above the watermark is exempt (Q27 long-PUT safety).
        Some(&watermark) => i64::from(blob.seq()) <= watermark,
        // Dead token (absent from the live table, past its grace): every blob is
        // terminal, so an unreferenced one is a true orphan.
        None => true,
    }
}

/// The injected DataNode seam the GC subtask drives: enumerate the live blobs of
/// one local extent, and tombstone the orphans. Production maps these to the
/// store engine; tests use an in-memory double.
#[async_trait]
pub trait GcBackend: Send + Sync {
    /// The live (non-tombstoned) blobs of `extent` on the local disk.
    async fn list_live_blobs(&self, extent: ExtentId) -> Result<Vec<BlobId>, SubtaskError>;

    /// Tombstones `blob` in `extent` (idempotent; physical space is reclaimed by
    /// the later compaction pass).
    async fn tombstone(&self, extent: ExtentId, blob: BlobId) -> Result<(), SubtaskError>;
}

/// One extent's GC pass: list its live blobs, diff against the reference set +
/// live-token table, tombstone the orphans. Idempotent — a re-run finds the
/// orphans already tombstoned (absent from `list_live_blobs`) and does nothing.
pub struct GcSubtask<B: GcBackend> {
    backend: Arc<B>,
    extent: ExtentId,
    referenced: Arc<BTreeSet<u64>>,
    live: Arc<LiveTokens>,
}

impl<B: GcBackend> GcSubtask<B> {
    /// Builds a GC subtask for one extent against the round's keep-set + live
    /// tokens (shared across the round's extents via `Arc`).
    #[must_use]
    pub fn new(
        backend: Arc<B>,
        extent: ExtentId,
        referenced: Arc<BTreeSet<u64>>,
        live: Arc<LiveTokens>,
    ) -> Self {
        Self {
            backend,
            extent,
            referenced,
            live,
        }
    }

    /// Runs the pass, returning the number of orphans tombstoned.
    ///
    /// # Errors
    ///
    /// [`SubtaskError`] from the backend (list or tombstone); a partial pass is
    /// safe to re-run (tombstone is idempotent).
    pub async fn run(&self) -> Result<usize, SubtaskError> {
        let local = self.backend.list_live_blobs(self.extent).await?;
        let orphans = orphans(&local, &self.referenced, &self.live);
        let count = orphans.len();
        for blob in orphans {
            self.backend.tombstone(self.extent, blob).await?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn blob(token: u32, seq: u32) -> BlobId {
        BlobId::new(WriterToken::new(token), seq)
    }

    #[test]
    fn referenced_blob_is_never_an_orphan() {
        let referenced: BTreeSet<u64> = [blob(1, 5).as_u64()].into_iter().collect();
        let live: LiveTokens = [(WriterToken::new(1), 100)].into_iter().collect();
        // Referenced → kept even though it is well below the watermark.
        assert!(!is_orphan(blob(1, 5), &referenced, &live));
    }

    #[test]
    fn unreferenced_below_watermark_is_an_orphan() {
        let referenced = BTreeSet::new();
        let live: LiveTokens = [(WriterToken::new(1), 10)].into_iter().collect();
        assert!(is_orphan(blob(1, 5), &referenced, &live), "s=5 ≤ W=10");
    }

    #[test]
    fn unreferenced_in_flight_above_watermark_is_exempt() {
        let referenced = BTreeSet::new();
        let live: LiveTokens = [(WriterToken::new(1), 10)].into_iter().collect();
        // s=11 > W=10: a long PUT's not-yet-committed blob — must NOT be reclaimed.
        assert!(!is_orphan(blob(1, 11), &referenced, &live));
    }

    #[test]
    fn unreferenced_dead_token_blob_is_an_orphan() {
        let referenced = BTreeSet::new();
        let live = LiveTokens::new(); // token 9 is dead (absent + grace elapsed)
        assert!(
            is_orphan(blob(9, 999), &referenced, &live),
            "a dead token's unreferenced blob is a true orphan at any seq"
        );
    }

    #[tokio::test]
    async fn subtask_tombstones_only_the_orphans() {
        struct Fake {
            blobs: Vec<BlobId>,
            tombstoned: Mutex<Vec<BlobId>>,
        }
        #[async_trait]
        impl GcBackend for Fake {
            async fn list_live_blobs(&self, _e: ExtentId) -> Result<Vec<BlobId>, SubtaskError> {
                Ok(self.blobs.clone())
            }
            async fn tombstone(&self, _e: ExtentId, blob: BlobId) -> Result<(), SubtaskError> {
                self.tombstoned.lock().unwrap().push(blob);
                Ok(())
            }
        }

        // blob(1,5) referenced (keep); blob(1,7) unref ≤ W=8 (orphan);
        // blob(1,9) unref > W (exempt); blob(2,1) dead-token unref (orphan).
        let backend = Arc::new(Fake {
            blobs: vec![blob(1, 5), blob(1, 7), blob(1, 9), blob(2, 1)],
            tombstoned: Mutex::new(Vec::new()),
        });
        let referenced: Arc<BTreeSet<u64>> = Arc::new([blob(1, 5).as_u64()].into_iter().collect());
        let live: Arc<LiveTokens> = Arc::new([(WriterToken::new(1), 8)].into_iter().collect());
        let extent = ExtentId::new(epoch_proto::ShardId::from_raw(1), 1);
        let task = GcSubtask::new(Arc::clone(&backend), extent, referenced, live);

        assert_eq!(task.run().await.expect("gc"), 2, "two orphans reclaimed");
        let mut got = backend.tombstoned.lock().unwrap().clone();
        got.sort_by_key(|b| b.as_u64());
        assert_eq!(got, vec![blob(1, 7), blob(2, 1)]);
    }
}
