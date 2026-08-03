//! The persistent delete-queue consumer (03 §8 单阶段, MetaNode 直驱; 06 §9
//! deleter.rs).
//!
//! Each partition's *leader* sweeps its `delq` segment in routing-key order:
//! entries whose `enqueue_ts` is older than the safety delay (default 2h —
//! the operator intervention window for an accidental delete) are expanded to
//! `(chunk_id, shard, blob_id)` references and tombstoned through the
//! injected [`DeleteSink`]; once every reference of one delete event
//! succeeds, the event's entries are dequeued in one raft propose
//! (攒批出队). Failures keep the entries and back off exponentially
//! (leader-local state — a new leader simply rescans, 03 §8).
//!
//! Correctness: the queue is persistent and applied atomically with the
//! metadata change, tombstoning is idempotent, and delivery is at-least-once
//! — so deletion never leaks (03 §8 正确性).
//!
//! Design: docs/design/03-metanode.md §8; §12.2 (DeleteSink 通用化缝)

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use epoch_proto::{BlobId, ChunkId};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::MetaError;
use crate::partition::PartitionRange;
use crate::raft::{MetaEntry, MetaRaft};
use crate::ref_extractor::{PendingDelete, Slice};
use crate::store::keys::MetaCf;
use crate::store::{MetaStore, StoreOp};

/// The default safety delay: only entries enqueued at least this long ago are
/// tombstoned (03 §8: 安全延迟 = 误删干预窗口).
pub const DEFAULT_SAFETY_DELAY: Duration = Duration::from_secs(2 * 60 * 60);

/// The default DeleteBlob concurrency per sweep (03 §8: 并发 DeleteBlob).
const DELETE_CONCURRENCY: usize = 32;

/// One delq scan page (entries; a page boundary never splits an event — see
/// [`Deleter::sweep`]).
const SCAN_PAGE: usize = 256;

/// The tombstone downstream (03 §12.2 通用化缝): injected by the assembly —
/// epoch-node wires `DeleteBlob` → DataNode here; another product wires its
/// own worker path. epoch-meta deliberately does not depend on the DataNode
/// client (06 §9 v0.10).
#[async_trait]
pub trait DeleteSink: Send + Sync {
    /// Tombstones one blob of a chunk. The sink fans out to the chunk's
    /// shards internally (02 §1: one blob is striped across the chunk's
    /// shards under one id).
    ///
    /// Idempotency is provided downstream: the DataNode `DeleteBlob` handler
    /// counts a missing / already-tombstoned blob as success (02 §1.6), so a
    /// replayed or duplicate tombstone returns `Ok`.
    ///
    /// A sink returns [`DeleteSinkError::Retryable`] for any lookup or
    /// transport failure — including a shard whose disk is currently
    /// unresolvable — and the deleter keeps the event queued and backs off
    /// (03 §8). At M5a every such failure is transient (topology refreshes,
    /// nodes rejoin; chunks are never removed), so a later sweep resolves it.
    ///
    /// INVARIANT(design 03 §8): a *permanently* broken-disk shard is skipped
    /// and re-tombstoned by repair during rebuild — but that skip depends on
    /// disk-health signalling and the repair path, so it lands with **M7**
    /// (RepairDisk / heal-on-read). Until then the sink surfaces an
    /// unresolvable shard as retryable rather than dropping it silently.
    ///
    /// # Errors
    ///
    /// Returns [`DeleteSinkError::Retryable`] for any failure the next sweep
    /// should retry.
    async fn delete_blob(&self, chunk_id: ChunkId, blob_id: BlobId) -> Result<(), DeleteSinkError>;
}

/// The failure vocabulary of a [`DeleteSink`] (everything is retryable: the
/// entry stays queued, 03 §8).
#[derive(Debug, thiserror::Error)]
pub enum DeleteSinkError {
    /// A retryable failure (network, node down, shard migrating, …).
    #[error("{0}")]
    Retryable(String),
}

/// One delete event: every delq entry sharing `(routing_key, seq)` (03 §8:
/// seq 标识一次删除事件, 大对象分 seg 段).
struct DeleteEvent {
    /// The shared delq key prefix `q | bucket | routing | seq`.
    prefix: Vec<u8>,
    /// `(delq key, pending value)` of every segment entry of the event.
    entries: Vec<(Vec<u8>, PendingDelete)>,
}

/// The per-partition delq consumer. One per MetaNode process; sweeps are
/// driven per partition by the ticker (leader-only).
pub struct Deleter {
    sink: Arc<dyn DeleteSink>,
    safety_delay: Duration,
    /// Leader-local retry state: event prefix → (consecutive failures,
    /// next-eligible instant). Resets on leader change — harmless, the new
    /// leader simply rescans (03 §8).
    backoff: Mutex<BTreeMap<Vec<u8>, (u32, Instant)>>,
}

impl Deleter {
    /// A deleter with the production safety delay (2h, 03 §8).
    #[must_use]
    pub fn new(sink: Arc<dyn DeleteSink>) -> Self {
        Self::with_safety_delay(sink, DEFAULT_SAFETY_DELAY)
    }

    /// A deleter with an explicit safety delay (tests shrink it to zero).
    #[must_use]
    pub fn with_safety_delay(sink: Arc<dyn DeleteSink>, safety_delay: Duration) -> Self {
        Self {
            sink,
            safety_delay,
            backoff: Mutex::new(BTreeMap::new()),
        }
    }

    fn backoff(&self) -> MutexGuard<'_, BTreeMap<Vec<u8>, (u32, Instant)>> {
        self.backoff.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// One sweep over the partition's delq range. No-op on a follower.
    /// Returns how many entries were dequeued.
    ///
    /// `now_millis` is the caller's wall clock — the deleter never reads a
    /// clock itself, keeping its decision input explicit (same discipline as
    /// the state machine's timestamps).
    ///
    /// # Errors
    ///
    /// Returns [`MetaError`] on engine failures (a raft write failure just
    /// ends the sweep early — leadership moved on).
    pub async fn sweep(
        &self,
        store: &Arc<dyn MetaStore>,
        raft: &MetaRaft,
        range: &PartitionRange,
        now_millis: i64,
    ) -> Result<usize, MetaError> {
        if !raft.metrics().borrow().state.is_leader() {
            return Ok(0);
        }
        let Some(delq_bounds) = range
            .key_ranges()
            .into_iter()
            .find(|r| r.cf == MetaCf::Delq)
        else {
            return Ok(0);
        };
        let expire_before = now_millis - self.safety_delay.as_millis() as i64;

        // 1. Collect eligible events in routing-key order. Keys sort as
        //    `q | bucket | routing | seq | seg`, so one event's entries are
        //    always contiguous; a page boundary only delays the tail event to
        //    the next page iteration (we carry it forward).
        let mut events: Vec<DeleteEvent> = Vec::new();
        let mut cursor = delq_bounds.start.clone();
        'scan: loop {
            let page = store.scan(MetaCf::Delq, &cursor, &delq_bounds.end, SCAN_PAGE)?;
            if page.is_empty() {
                break;
            }
            cursor = page
                .last()
                .map(|(k, _)| {
                    let mut next = k.clone();
                    next.push(0);
                    next
                })
                .ok_or_else(|| MetaError::Raft("delq scan page unexpectedly empty".to_string()))?;
            for (key, value) in page {
                let pending: PendingDelete = serde_json::from_slice(&value)
                    .map_err(|e| MetaError::Raft(format!("malformed delq entry: {e}")))?;
                if pending.enqueue_ts > expire_before {
                    // Entries are time-clustered only by insertion; keep
                    // scanning (later events may still be old enough).
                    continue;
                }
                if self.is_backing_off(&key) {
                    continue;
                }
                let prefix = event_prefix(&key)?;
                match events.last_mut() {
                    Some(event) if event.prefix == prefix => {
                        event.entries.push((key, pending));
                    }
                    _ => events.push(DeleteEvent {
                        prefix,
                        entries: vec![(key, pending)],
                    }),
                }
            }
            if events.len() >= SCAN_PAGE {
                break 'scan;
            }
        }

        // 2. Tombstone event by event; dequeue the fully-successful ones.
        let mut dequeued = 0usize;
        for event in events {
            match self.tombstone_event(&event).await {
                Ok(()) => {
                    self.clear_backoff(&event.prefix);
                    let ops: Vec<StoreOp> = event
                        .entries
                        .iter()
                        .map(|(key, _)| StoreOp::delete(MetaCf::Delq, key.clone()))
                        .collect();
                    // 攒批出队: one propose removes the whole event (03 §8).
                    if raft.client_write(MetaEntry::StoreOps(ops)).await.is_err() {
                        debug!(
                            partition = event.prefix.len(),
                            "dequeue propose failed; leadership likely moved, ending sweep"
                        );
                        break;
                    }
                    dequeued += event.entries.len();
                }
                Err(failed) => {
                    warn!(
                        event = event.prefix.len(),
                        failed, "delete sweep: event tombstone incomplete; backing off"
                    );
                    self.register_backoff(&event.prefix, failed);
                }
            }
        }
        Ok(dequeued)
    }

    /// Tombstones every reference of one event (bounded concurrency), or
    /// reports how many failed.
    async fn tombstone_event(&self, event: &DeleteEvent) -> Result<(), usize> {
        let slices: Vec<Slice> = event
            .entries
            .iter()
            .flat_map(|(_, pending)| pending.slices.iter().cloned())
            .collect();
        let mut pending: std::collections::VecDeque<Slice> = slices.into();
        let mut failed = 0usize;
        let mut running: JoinSet<Result<(), DeleteSinkError>> = JoinSet::new();
        while !pending.is_empty() || !running.is_empty() {
            while running.len() < DELETE_CONCURRENCY
                && let Some(slice) = pending.pop_front()
            {
                let sink = Arc::clone(&self.sink);
                running.spawn(async move {
                    for blob_id in &slice.blob_ids {
                        sink.delete_blob(slice.chunk_id, *blob_id).await?;
                    }
                    Ok(())
                });
            }
            match running.join_next().await {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(_))) | Some(Err(_)) => failed += 1,
                None => break,
            }
        }
        if failed == 0 { Ok(()) } else { Err(failed) }
    }

    /// Whether this event key is inside its backoff window.
    fn is_backing_off(&self, key: &[u8]) -> bool {
        let Ok(prefix) = event_prefix(key) else {
            return false;
        };
        self.backoff()
            .get(&prefix)
            .is_some_and(|(_, next)| *next > Instant::now())
    }

    /// Exponential backoff per consecutive failure: 1s, 2s, 4s, … capped at
    /// 5 minutes (03 §8: 部分失败 → 指数退避).
    fn register_backoff(&self, prefix: &[u8], _failed: usize) {
        let mut backoff = self.backoff();
        let (failures, _) = backoff.get(prefix).copied().unwrap_or((0, Instant::now()));
        let failures = failures.saturating_add(1).min(9);
        let delay = Duration::from_secs(1 << failures).min(Duration::from_secs(300));
        backoff.insert(prefix.to_vec(), (failures, Instant::now() + delay));
    }

    fn clear_backoff(&self, prefix: &[u8]) {
        self.backoff().remove(prefix);
    }
}

/// The event prefix of a delq key: `q | bucket | routing | seq` (drops the
/// 4-byte seg_no tail).
fn event_prefix(key: &[u8]) -> Result<Vec<u8>, MetaError> {
    if key.len() < 1 + 8 + 8 + 4 {
        return Err(MetaError::Raft(format!(
            "delq key too short: {}",
            key.len()
        )));
    }
    Ok(key[..key.len() - 4].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Namespace;
    use crate::raft::GroupManager;
    use crate::ref_extractor::Slice;
    use crate::store::keys::{flat_key, suffix};
    use crate::store::rocks::RocksEngine;

    use epoch_proto::{BlobId, BucketId};
    use openraft::{BasicNode, Config};

    /// A sink that records every call and fails blobs on its denylist.
    #[derive(Default)]
    struct FakeSink {
        calls: Mutex<Vec<(u32, u64)>>,
        deny: Vec<u64>,
    }

    #[async_trait]
    impl DeleteSink for FakeSink {
        async fn delete_blob(
            &self,
            chunk_id: ChunkId,
            blob_id: BlobId,
        ) -> Result<(), DeleteSinkError> {
            self.record(chunk_id.get(), blob_id.as_u64());
            if self.deny.contains(&blob_id.as_u64()) {
                return Err(DeleteSinkError::Retryable("denied".to_string()));
            }
            Ok(())
        }
    }

    impl FakeSink {
        fn record(&self, chunk: u32, blob: u64) {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((chunk, blob));
        }

        fn calls(&self) -> Vec<(u32, u64)> {
            self.calls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    fn slice(chunk: u32, blobs: &[u64]) -> Slice {
        Slice {
            chunk_id: ChunkId::new(chunk),
            blob_ids: blobs.iter().map(|&b| BlobId::from_raw(b)).collect(),
            blob_size: 32 << 20,
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        store: Arc<dyn MetaStore>,
        raft: MetaRaft,
        range: PartitionRange,
    }

    /// A single-node group (leader immediately) over a fresh engine.
    async fn fixture() -> Fixture {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(RocksEngine::open(&dir.path().join("sm")).expect("engine"));
        let manager = GroupManager::open(
            &dir.path().join("raft-log"),
            1,
            engine.clone(),
            Arc::new(crate::ref_extractor::EpochRefExtractor),
            Config {
                cluster_name: "deleter-test".to_string(),
                ..Default::default()
            },
        )
        .expect("manager");
        let raft = manager
            .create_group(1, PartitionRange::full(Namespace::Flat))
            .await
            .expect("create group");
        raft.initialize(BTreeMap::from([(1u64, BasicNode::new("a:1"))]))
            .await
            .expect("initialize");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !raft.metrics().borrow().state.is_leader() {
            assert!(Instant::now() < deadline, "no leader");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Fixture {
            _dir: dir,
            store: engine,
            raft,
            range: PartitionRange::full(Namespace::Flat),
        }
    }

    /// Enqueues a delq entry directly (bypassing apply — the deleter consumes
    /// whatever is in the queue).
    fn enqueue(
        store: &Arc<dyn MetaStore>,
        bucket: BucketId,
        key: &[u8],
        seq: u64,
        seg: u32,
        slices: Vec<Slice>,
        ts: i64,
    ) {
        let pending = PendingDelete {
            slices,
            enqueue_ts: ts,
        };
        store
            .apply(&[StoreOp::put(
                MetaCf::Delq,
                flat_key(MetaCf::Delq, bucket, key, &suffix::delq_seq(seq, seg)),
                serde_json::to_vec(&pending).expect("encode"),
            )])
            .expect("enqueue");
    }

    fn delq_len(store: &Arc<dyn MetaStore>, bucket: BucketId) -> usize {
        store
            .scan(
                MetaCf::Delq,
                &flat_key(MetaCf::Delq, bucket, b"", &[]),
                &[MetaCf::Delq.tag() + 1],
                1024,
            )
            .expect("scan")
            .len()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_entries_are_tombstoned_and_dequeued() {
        let fx = fixture().await;
        let bucket = BucketId::new(1);
        enqueue(
            &fx.store,
            bucket,
            b"a",
            1,
            0,
            vec![slice(7, &[100, 101])],
            1_000,
        );
        // A second segment of the same event (seq 1, seg 1) + a fresh entry
        // that must survive the sweep (safety delay).
        enqueue(&fx.store, bucket, b"a", 1, 1, vec![slice(8, &[102])], 1_000);
        enqueue(
            &fx.store,
            bucket,
            b"b",
            2,
            0,
            vec![slice(9, &[200])],
            i64::MAX - 10,
        );

        let sink = Arc::new(FakeSink::default());
        let deleter = Deleter::with_safety_delay(sink.clone(), Duration::from_secs(60));
        let dequeued = deleter
            .sweep(&fx.store, &fx.raft, &fx.range, 1_000_000)
            .await
            .expect("sweep");
        assert_eq!(dequeued, 2, "both segments of the old event dequeued");

        let mut calls = sink.calls();
        calls.sort_unstable();
        assert_eq!(
            calls,
            vec![(7, 100), (7, 101), (8, 102)],
            "every blob of the event tombstoned"
        );
        // The fresh entry survives; the old event is gone.
        assert_eq!(delq_len(&fx.store, bucket), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_blob_keeps_the_event_and_backs_off() {
        let fx = fixture().await;
        let bucket = BucketId::new(1);
        enqueue(
            &fx.store,
            bucket,
            b"c",
            1,
            0,
            vec![slice(7, &[100, 101])],
            1_000,
        );

        let sink = Arc::new(FakeSink {
            calls: Mutex::new(Vec::new()),
            deny: vec![101],
        });
        let deleter = Deleter::with_safety_delay(sink.clone(), Duration::ZERO);
        let dequeued = deleter
            .sweep(&fx.store, &fx.raft, &fx.range, 1_000_000)
            .await
            .expect("sweep");
        assert_eq!(dequeued, 0, "a failed blob keeps the whole event");
        assert_eq!(delq_len(&fx.store, bucket), 1);

        // The immediate second sweep skips the event entirely (backoff), so
        // no new calls land.
        let before = sink.calls().len();
        let again = deleter
            .sweep(&fx.store, &fx.raft, &fx.range, 1_000_000)
            .await
            .expect("sweep");
        assert_eq!(again, 0);
        assert_eq!(sink.calls().len(), before, "backoff suppresses retries");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follower_sweep_is_a_noop() {
        let fx = fixture().await;
        let bucket = BucketId::new(1);
        enqueue(&fx.store, bucket, b"a", 1, 0, vec![slice(7, &[100])], 1_000);
        // Shut the leader down: the group no longer leads, so the sweep must
        // not touch the queue.
        fx.raft.shutdown().await.expect("shutdown");
        let sink = Arc::new(FakeSink::default());
        let deleter = Deleter::with_safety_delay(sink.clone(), Duration::ZERO);
        let dequeued = deleter
            .sweep(&fx.store, &fx.raft, &fx.range, 1_000_000)
            .await
            .expect("sweep");
        assert_eq!(dequeued, 0);
        assert!(sink.calls().is_empty());
    }
}
