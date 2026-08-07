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

//! The RepairDisk subtask + expander (01 §6.3 / 02 §3.2): rebuild every shard a
//! broken disk held and rebind each to a fresh extent on a healthy disk.
//!
//! A RepairDisk Job's coordinator expands it into one [`RepairSubtask`] per
//! shard slot the broken disk carried. Each subtask, per blob of its chunk:
//!
//! 1. reads the surviving `data + parity` shards of the blob from their nodes;
//! 2. rebuilds the missing shard's framed body ([`rebuild_shard_body`]);
//! 3. writes it to a `Rebuilding` extent on a chosen healthy disk;
//!
//! then promotes the extent and commits the shard-mapping rebind to PD
//! (epoch+1, 01 §6.4). Every step is idempotent: a re-run after a coordinator
//! crash finds already-rebuilt blobs and an already-rebound slot, and skips.
//!
//! The subtask is written against a [`RepairBackend`] seam so the orchestration
//! is unit-testable without a live cluster; the production backend (gRPC to PD +
//! data-plane RPC to DataNodes) is assembled in the node's coordinator wiring.
//!
//! Design: docs/design/01-pd.md §6.3/§6.4; docs/design/02-datanode.md §3.2;
//! docs/design/04-ec-io.md §5

use std::sync::Arc;

use async_trait::async_trait;
use epoch_proto::{BlobId, ChunkId, DiskId, ExtentId, NodeId, ShardId};

use crate::repair::rebuild::rebuild_shard_body;
use crate::subtask::{Subtask, SubtaskError};

/// The EC + placement view of a chunk the repair subtask needs (from PD
/// `GetChunk`): its code mode and, per shard index, the node currently holding
/// it. Index `i` of `shards` is shard index `i`.
#[derive(Debug, Clone)]
pub struct ChunkLayout {
    /// The chunk id.
    pub chunk_id: ChunkId,
    /// EC data-shard count.
    pub data: usize,
    /// EC parity-shard count.
    pub parity: usize,
    /// Coding-unit size in bytes.
    pub stripe_size: usize,
    /// Per shard index: `(shard_id, node)` currently holding it. A shard on the
    /// broken disk is still listed (its read will simply miss).
    pub shards: Vec<(ShardId, NodeId)>,
}

impl ChunkLayout {
    pub(crate) fn total(&self) -> usize {
        self.data + self.parity
    }
}

/// The injected cluster seam the repair subtask drives. The production impl maps
/// each method to a PD control RPC or a DataNode data-plane RPC; tests use an
/// in-memory double. Every method is idempotent-friendly.
#[async_trait]
pub trait RepairBackend: Send + Sync {
    /// Reads one shard's framed body for `blob_id` from `node`. `Ok(None)` = the
    /// shard is missing there (the broken/absent one); `Err` = a transient read
    /// failure (treated as missing for reconstruction, but surfaced for retry
    /// accounting).
    async fn read_shard(
        &self,
        node: NodeId,
        shard: ShardId,
        blob_id: BlobId,
    ) -> Result<Option<Vec<u8>>, SubtaskError>;

    /// Ensures a `Rebuilding` extent exists on the local (healthy-target) disk
    /// for the rebuilt shard slot and returns its id (idempotent per shard id).
    async fn ensure_rebuild_extent(&self, shard: ShardId) -> Result<ExtentId, SubtaskError>;

    /// Writes a rebuilt shard body to the rebuild extent (idempotent per blob).
    async fn write_rebuilt(
        &self,
        extent: ExtentId,
        blob_id: BlobId,
        body: Vec<u8>,
    ) -> Result<(), SubtaskError>;

    /// Promotes the rebuild extent `Rebuilding → Writable` (idempotent).
    async fn promote(&self, extent: ExtentId) -> Result<(), SubtaskError>;

    /// Commits the shard-mapping rebind to PD raft (01 §6.4): rebinds the slot to
    /// the rebuild `extent` (whose disk the backend knows, having created it) and
    /// bumps its epoch, gated by the Job. Idempotent — a slot already rebound
    /// (epoch advanced past `expected_epoch`) reports
    /// [`CommitOutcome::AlreadyRebound`].
    async fn commit_mapping(
        &self,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        extent: ExtentId,
    ) -> Result<CommitOutcome, SubtaskError>;

    /// Commits a single-shard-repair rebind to PD raft (01 §6.3): the
    /// completion-receipt rebind of an in-place ShardRepair, authorized by the
    /// pending ShardRepair *ticket* rather than a Job (01 §6.4 invariant 2). The
    /// backend supplies the rebuilt `extent`'s disk (equal to the slot's current
    /// disk for an in-place rebuild). Idempotent — an already-rebound slot (ticket
    /// cleared / epoch advanced) reports [`CommitOutcome::AlreadyRebound`]. Used
    /// by [`ShardRepairTask`](crate::repair::ShardRepairTask), not the Job path.
    async fn commit_shard_repair(
        &self,
        chunk_id: ChunkId,
        index: u8,
        expected_epoch: u32,
        extent: ExtentId,
    ) -> Result<CommitOutcome, SubtaskError>;

    /// Whether the slot at `(chunk_id, index)` is already rebound off the broken
    /// disk (its current disk differs from `broken_disk`), letting `is_done`
    /// short-circuit a finished subtask after a coordinator restart.
    async fn slot_rebound(
        &self,
        chunk_id: ChunkId,
        index: u8,
        broken_disk: DiskId,
    ) -> Result<bool, SubtaskError>;
}

/// The outcome of a rebind commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The rebind applied and the slot's epoch advanced.
    Rebound,
    /// The slot was already rebound (a duplicate/replayed commit) — idempotent.
    AlreadyRebound,
}

/// One shard-slot rebuild: read survivors → rebuild body → write to a fresh
/// extent → promote → rebind. Idempotent end to end.
pub struct RepairSubtask<B: RepairBackend> {
    backend: Arc<B>,
    /// The chunk this shard belongs to, with its full placement.
    layout: ChunkLayout,
    /// The shard index within the chunk to rebuild.
    index: u8,
    /// The slot epoch as PD reported it (the rebind's staleness fence).
    expected_epoch: u32,
    /// The disk that broke (this slot's current, unreadable home).
    broken_disk: DiskId,
    /// The blob ids the broken shard held (rebuilt one at a time). Enumerated
    /// from a surviving shard of the same chunk (EC stripe families share ids).
    blobs: Vec<BlobId>,
}

impl<B: RepairBackend> RepairSubtask<B> {
    /// Builds a repair subtask for one shard slot.
    #[must_use]
    pub fn new(
        backend: Arc<B>,
        layout: ChunkLayout,
        index: u8,
        expected_epoch: u32,
        broken_disk: DiskId,
        blobs: Vec<BlobId>,
    ) -> Self {
        Self {
            backend,
            layout,
            index,
            expected_epoch,
            broken_disk,
            blobs,
        }
    }

    /// The shard id being rebuilt (at its pre-rebind epoch).
    fn shard_id(&self) -> ShardId {
        self.layout.shards[usize::from(self.index)].0
    }
}

#[async_trait]
impl<B: RepairBackend> Subtask for RepairSubtask<B> {
    fn id(&self) -> u64 {
        // Stable within the Job: the epoch-zeroed shard prefix identifies the
        // slot regardless of rebind.
        self.shard_id().shard_prefix()
    }

    async fn is_done(&self) -> Result<bool, SubtaskError> {
        self.backend
            .slot_rebound(self.layout.chunk_id, self.index, self.broken_disk)
            .await
    }

    async fn execute(&self) -> Result<(), SubtaskError> {
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
        match self
            .backend
            .commit_mapping(
                self.layout.chunk_id,
                self.index,
                self.expected_epoch,
                extent,
            )
            .await?
        {
            CommitOutcome::Rebound | CommitOutcome::AlreadyRebound => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use epoch_ec::{Erasure, frame, layout};

    const STRIPE: usize = 128;

    /// Reference encode (mirrors the gateway/write path) to produce per-shard
    /// bodies the fake backend can serve as survivors.
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

    /// A fake backend: serves survivor bodies per (index, blob), captures writes,
    /// and tracks promote/commit calls + a rebound flag.
    struct FakeBackend {
        /// Reference bodies per shard index (`bodies[j]` = shard j's full body).
        bodies: Vec<Vec<u8>>,
        /// The index whose reads should miss (the broken shard).
        missing: u8,
        writes: Mutex<HashMap<(ExtentId, BlobId), Vec<u8>>>,
        promotes: AtomicU32,
        commits: AtomicU32,
        rebound: Mutex<bool>,
        extent: ExtentId,
    }

    #[async_trait]
    impl RepairBackend for FakeBackend {
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
            self.commits.fetch_add(1, Ordering::SeqCst);
            *self.rebound.lock().unwrap() = true;
            Ok(CommitOutcome::Rebound)
        }
        async fn commit_shard_repair(
            &self,
            _chunk_id: ChunkId,
            _index: u8,
            _expected_epoch: u32,
            _extent: ExtentId,
        ) -> Result<CommitOutcome, SubtaskError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            *self.rebound.lock().unwrap() = true;
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

    #[tokio::test]
    async fn rebuilds_writes_promotes_and_commits_the_missing_shard() {
        let ec = Erasure::new(2, 1).unwrap();
        let blob_bytes = (0..300u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let bodies = encode_bodies(&ec, &blob_bytes);
        let target = 0u8; // rebuild data shard 0
        let expected = bodies[usize::from(target)].clone();

        let extent = ExtentId::new(shard(target, 1), 1_700_000_000_001);
        let backend = Arc::new(FakeBackend {
            bodies,
            missing: target,
            writes: Mutex::new(HashMap::new()),
            promotes: AtomicU32::new(0),
            commits: AtomicU32::new(0),
            rebound: Mutex::new(false),
            extent,
        });
        let layout = ChunkLayout {
            chunk_id: ChunkId::new(1),
            data: 2,
            parity: 1,
            stripe_size: STRIPE,
            shards: vec![
                (shard(0, 0), NodeId::new(1)),
                (shard(1, 0), NodeId::new(2)),
                (shard(2, 0), NodeId::new(3)),
            ],
        };
        let blob_id = BlobId::new(epoch_proto::WriterToken::new(1), 1);
        let subtask = RepairSubtask::new(
            Arc::clone(&backend),
            layout,
            target,
            0,
            DiskId::new(9),
            vec![blob_id],
        );

        assert!(!subtask.is_done().await.unwrap(), "not rebound yet");
        subtask.execute().await.expect("repair");

        // The rebuilt body was written byte-identically, then promoted + committed.
        let written = backend
            .writes
            .lock()
            .unwrap()
            .get(&(extent, blob_id))
            .cloned()
            .expect("rebuilt body written");
        assert_eq!(written, expected, "rebuilt shard body matches original");
        assert_eq!(backend.promotes.load(Ordering::SeqCst), 1);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
        // After the rebind, the subtask reports done (idempotent re-run skips).
        assert!(subtask.is_done().await.unwrap());
    }
}
