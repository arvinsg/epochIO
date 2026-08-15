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

//! Single-shard repair execution (01 §6.3 / 04 §5): rebuild one shard slot in
//! place from its stripe survivors and commit the completion-receipt rebind.
//!
//! Unlike [`RepairSubtask`](crate::repair::RepairSubtask) (a RepairDisk Job
//! subtask driven by the `run_job` coordinator), a ShardRepair has **no Job** —
//! PD dispatches it directly to the node hosting the target shard, which pulls
//! the ticket, rebuilds the shard, and reports completion (01 §6.3). The rebind
//! is authorized by the pending ShardRepair ticket, not a Job (01 §6.4
//! invariant 2, [`RepairBackend::commit_shard_repair`]).
//!
//! The rebuild reuses the same primitive and backend I/O as RepairDisk: read the
//! surviving `data + parity` shard bodies of each blob, rebuild the missing one
//! ([`rebuild_shard_body`]), write it to a `Rebuilding` extent, promote, then
//! rebind. It is idempotent end to end — a re-dispatch after a crash finds the
//! already-rebound slot (ticket cleared) and reports it done.

use std::sync::Arc;

use epoch_proto::BlobId;

use crate::repair::rebuild::rebuild_shard_body;
use crate::repair::subtask::{ChunkLayout, CommitOutcome, RepairBackend};
use crate::subtask::SubtaskError;

/// One in-place single-shard repair: read survivors → rebuild body → write to a
/// fresh extent → promote → ticket-authorized rebind. Idempotent end to end.
pub struct ShardRepairTask<B: RepairBackend> {
    backend: Arc<B>,
    /// The chunk this shard belongs to, with its full placement.
    layout: ChunkLayout,
    /// The shard index within the chunk to rebuild.
    index: u8,
    /// The slot epoch PD dispatched (the ticket's + rebind's staleness fence).
    expected_epoch: u32,
    /// The blob ids the target shard held (rebuilt one at a time). Enumerated
    /// from a surviving shard of the same chunk (EC stripe families share ids).
    blobs: Vec<BlobId>,
}

impl<B: RepairBackend> ShardRepairTask<B> {
    /// Builds a single-shard repair task for shard `index` of `layout`.
    #[must_use]
    pub fn new(
        backend: Arc<B>,
        layout: ChunkLayout,
        index: u8,
        expected_epoch: u32,
        blobs: Vec<BlobId>,
    ) -> Self {
        Self {
            backend,
            layout,
            index,
            expected_epoch,
            blobs,
        }
    }

    /// The shard id being rebuilt (at its pre-rebind epoch).
    fn shard_id(&self) -> epoch_proto::ShardId {
        self.layout.shards[usize::from(self.index)].0
    }

    /// Runs the repair to completion (idempotent). Rebuilds every blob of the
    /// target shard, writes to a `Rebuilding` extent, promotes it, then commits
    /// the ticket-authorized rebind.
    ///
    /// # Errors
    ///
    /// - [`SubtaskError::NotReady`] if a stripe lacks enough survivors this pass
    ///   (a later re-dispatch retries);
    /// - [`SubtaskError::Failed`] on an unrecoverable rebuild/backend error.
    pub async fn run(&self) -> Result<CommitOutcome, SubtaskError> {
        // Seal first (01 §6.3 不变量 4): fence the chunk before reading any
        // survivor, so the blob set this task rebuilds is closed against new
        // writes (a write landing mid-rebuild would otherwise be missing from
        // the rebuilt shard after the rebind's epoch bump — silent redundancy
        // loss). Idempotent, so a retry re-seals harmlessly.
        self.backend.seal_chunk(self.layout.chunk_id).await?;
        let ec = epoch_ec::Erasure::new(self.layout.data, self.layout.parity)
            .map_err(|e| SubtaskError::Failed(format!("erasure: {e}")))?;
        let extent = self.backend.ensure_rebuild_extent(self.shard_id()).await?;

        for &blob_id in &self.blobs {
            // Gather the surviving shard bodies (skip our own index).
            let mut bodies: Vec<Option<Vec<u8>>> = vec![None; self.layout.total()];
            for (j, &(shard, node)) in self.layout.shards.iter().enumerate() {
                if j == usize::from(self.index) {
                    continue;
                }
                bodies[j] = self
                    .backend
                    .read_shard(node, shard, blob_id)
                    .await
                    .unwrap_or(None);
            }
            let rebuilt = rebuild_shard_body(
                &ec,
                self.layout.stripe_size,
                usize::from(self.index),
                &bodies,
            )?;
            self.backend.write_rebuilt(extent, blob_id, rebuilt).await?;
        }

        self.backend.promote(extent).await?;
        self.backend
            .commit_shard_repair(
                self.layout.chunk_id,
                self.index,
                self.expected_epoch,
                extent,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use epoch_ec::{Erasure, frame, layout};
    use epoch_proto::{ChunkId, DiskId, ExtentId, NodeId, ShardId, WriterToken};

    const STRIPE: usize = 128;

    /// Reference encode (mirrors the write path) to produce per-shard survivor
    /// bodies the fake backend serves.
    fn encode_bodies(ec: &Erasure, blob: &[u8]) -> Vec<Vec<u8>> {
        let n = ec.data_shards();
        let total = ec.total_shards();
        let mut bodies: Vec<Vec<u8>> = (0..total).map(|_| Vec::new()).collect();
        for s in 0..layout::stripe_count(blob.len(), STRIPE) {
            let start = s * STRIPE;
            let end = (start + STRIPE).min(blob.len());
            let stripe = &blob[start..end];
            let unit = layout::unit_size(stripe.len(), n);
            let mut data_units: Vec<Vec<u8>> = Vec::with_capacity(n);
            for i in 0..n {
                let mut u = vec![0u8; unit];
                let ds = i * unit;
                if ds < stripe.len() {
                    let de = (ds + unit).min(stripe.len());
                    u[..de - ds].copy_from_slice(&stripe[ds..de]);
                }
                data_units.push(u);
            }
            let parity = ec.encode(&data_units).unwrap();
            for (i, u) in data_units.iter().enumerate() {
                frame::write_frame(u, &mut bodies[i]);
            }
            for (k, p) in parity.iter().enumerate() {
                frame::write_frame(p, &mut bodies[n + k]);
            }
        }
        bodies
    }

    /// A fake backend: serves survivor bodies per index, captures writes, and
    /// tracks promote/commit + a rebound flag (mirrors the RepairDisk double).
    struct FakeBackend {
        bodies: Vec<Vec<u8>>,
        missing: u8,
        writes: Mutex<HashMap<(ExtentId, BlobId), Vec<u8>>>,
        promotes: AtomicU32,
        repair_commits: AtomicU32,
        rebound: Mutex<bool>,
        extent: ExtentId,
    }

    #[async_trait]
    impl RepairBackend for FakeBackend {
        async fn seal_chunk(&self, _chunk_id: ChunkId) -> Result<(), SubtaskError> {
            Ok(())
        }
        async fn read_shard(
            &self,
            _node: NodeId,
            shard: ShardId,
            _blob_id: BlobId,
        ) -> Result<Option<Vec<u8>>, SubtaskError> {
            let j = shard.index();
            if j == self.missing {
                return Ok(None);
            }
            Ok(Some(self.bodies[usize::from(j)].clone()))
        }
        async fn ensure_rebuild_extent(&self, _shard: ShardId) -> Result<ExtentId, SubtaskError> {
            Ok(self.extent)
        }
        async fn write_rebuilt(
            &self,
            extent: ExtentId,
            blob_id: BlobId,
            body: Vec<u8>,
        ) -> Result<(), SubtaskError> {
            self.writes.lock().unwrap().insert((extent, blob_id), body);
            Ok(())
        }
        async fn promote(&self, _extent: ExtentId) -> Result<(), SubtaskError> {
            self.promotes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn commit_mapping(
            &self,
            _chunk_id: ChunkId,
            _index: u8,
            _expected_epoch: u32,
            _extent: ExtentId,
        ) -> Result<CommitOutcome, SubtaskError> {
            unreachable!("ShardRepairTask uses commit_shard_repair, not commit_mapping")
        }
        async fn commit_shard_repair(
            &self,
            _chunk_id: ChunkId,
            _index: u8,
            _expected_epoch: u32,
            _extent: ExtentId,
        ) -> Result<CommitOutcome, SubtaskError> {
            self.repair_commits.fetch_add(1, Ordering::SeqCst);
            let mut rebound = self.rebound.lock().unwrap();
            if *rebound {
                return Ok(CommitOutcome::AlreadyRebound);
            }
            *rebound = true;
            Ok(CommitOutcome::Rebound)
        }
        async fn slot_rebound(
            &self,
            _chunk_id: ChunkId,
            _index: u8,
            _broken_disk: DiskId,
        ) -> Result<bool, SubtaskError> {
            Ok(*self.rebound.lock().unwrap())
        }
    }

    fn shard(index: u8, epoch: u32) -> ShardId {
        ShardId::new(ChunkId::new(1), index, epoch)
    }

    fn layout() -> ChunkLayout {
        ChunkLayout {
            chunk_id: ChunkId::new(1),
            data: 2,
            parity: 1,
            stripe_size: STRIPE,
            shards: vec![
                (shard(0, 0), NodeId::new(1)),
                (shard(1, 0), NodeId::new(2)),
                (shard(2, 0), NodeId::new(3)),
            ],
        }
    }

    #[tokio::test]
    async fn rebuilds_writes_promotes_and_commits_the_repair() {
        let ec = Erasure::new(2, 1).unwrap();
        let blob_bytes = (0..300u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let bodies = encode_bodies(&ec, &blob_bytes);
        let target = 0u8; // rebuild data shard 0 in place
        let expected = bodies[usize::from(target)].clone();

        let extent = ExtentId::new(shard(target, 1), 1_700_000_000_001);
        let backend = Arc::new(FakeBackend {
            bodies,
            missing: target,
            writes: Mutex::new(HashMap::new()),
            promotes: AtomicU32::new(0),
            repair_commits: AtomicU32::new(0),
            rebound: Mutex::new(false),
            extent,
        });
        let blob_id = BlobId::new(WriterToken::new(1), 1);
        let task = ShardRepairTask::new(Arc::clone(&backend), layout(), target, 0, vec![blob_id]);

        assert_eq!(task.run().await.expect("repair"), CommitOutcome::Rebound);
        let written = backend
            .writes
            .lock()
            .unwrap()
            .get(&(extent, blob_id))
            .cloned()
            .expect("rebuilt body written");
        assert_eq!(written, expected, "rebuilt shard body matches original");
        assert_eq!(backend.promotes.load(Ordering::SeqCst), 1);
        assert_eq!(backend.repair_commits.load(Ordering::SeqCst), 1);

        // A re-run (crash + re-dispatch) is idempotent: already rebound.
        assert_eq!(
            task.run().await.expect("re-run"),
            CommitOutcome::AlreadyRebound,
            "re-dispatch after completion is idempotent"
        );
    }

    #[tokio::test]
    async fn rebuilds_a_missing_parity_shard() {
        let ec = Erasure::new(2, 1).unwrap();
        let blob_bytes = (0..300u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let bodies = encode_bodies(&ec, &blob_bytes);
        let target = 2u8; // parity index (n=2)
        let expected = bodies[usize::from(target)].clone();
        let extent = ExtentId::new(shard(target, 1), 1_700_000_000_002);
        let backend = Arc::new(FakeBackend {
            bodies,
            missing: target,
            writes: Mutex::new(HashMap::new()),
            promotes: AtomicU32::new(0),
            repair_commits: AtomicU32::new(0),
            rebound: Mutex::new(false),
            extent,
        });
        let blob_id = BlobId::new(WriterToken::new(1), 1);
        let task = ShardRepairTask::new(Arc::clone(&backend), layout(), target, 0, vec![blob_id]);
        assert_eq!(task.run().await.expect("repair"), CommitOutcome::Rebound);
        let written = backend
            .writes
            .lock()
            .unwrap()
            .get(&(extent, blob_id))
            .cloned()
            .expect("rebuilt parity written");
        assert_eq!(written, expected, "rebuilt parity body matches original");
    }
}
