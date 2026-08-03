//! GET orchestration: for each blob in an [`ObjectLayout`], read its
//! `data + parity` shards in parallel and reconstruct the blob's bytes,
//! tolerating missing or bitrot-corrupted shards as long as `data` survive
//! (04 §4).
//!
//! M3 reads all `data + parity` shards per blob and reconstructs from whatever
//! returns; the topology-ordered "seed `data`, backfill from parity" reader that
//! avoids parity read-amplification is a later optimization, not a correctness
//! requirement. Design: docs/design/04-ec-io.md §4.

use std::sync::Arc;

use epoch_proto::{BlobId, ChunkId};
use epoch_rpc::{ReadShardReq, ShardTransport};
use tokio::task::JoinSet;

use crate::code::{ChunkPlacement, ObjectLayout};
use crate::error::GatewayError;
use crate::pipeline;

/// A shard that a GET had to reconstruct around (missing/bitrot): the
/// heal-on-read signal to report to PD for a ShardRepair (04 §4). Identifies the
/// slot by `(chunk_id, index)`; PD resolves the target node + epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HealReport {
    /// The chunk whose shard was bad.
    pub chunk_id: ChunkId,
    /// The bad shard's index within the chunk (`0..data+parity`).
    pub index: u8,
}

/// A read-back object plus the shards that had to be healed to serve it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadObject {
    /// The reconstructed object bytes.
    pub bytes: Vec<u8>,
    /// Deduplicated heal-on-read reports (empty when every shard was clean).
    pub heals: Vec<HealReport>,
}

/// Reads and reconstructs a whole object from its layout, collecting the
/// heal-on-read reports for any missing/corrupt shards (04 §4). The bytes are
/// returned regardless of heals — a bad shard never blocks the read as long as
/// `data` survive.
///
/// # Errors
///
/// - [`GatewayError::ShardCount`] if the layout's placement is not
///   `data + parity` shards;
/// - [`GatewayError::Ec`] if any blob has fewer than `data` surviving shards.
pub async fn get_object(
    transport: Arc<dyn ShardTransport>,
    layout: &ObjectLayout,
) -> Result<ReadObject, GatewayError> {
    let code = &layout.code;
    let ec = code.erasure()?;
    let total = code.total();

    let mut out = Vec::with_capacity(layout.size as usize);
    // Dedup heal reports across blobs: one chunk's shard index is bad once per
    // GET even if several blobs of the object hit it.
    let mut heals: std::collections::BTreeSet<(u32, u8)> = std::collections::BTreeSet::new();
    for blob in &layout.blobs {
        if blob.chunk.shards.len() != total {
            return Err(GatewayError::ShardCount {
                need: total,
                got: blob.chunk.shards.len(),
            });
        }
        let slots = read_blob_shards(&transport, &blob.chunk, blob.blob_id, total).await;
        let decoded = pipeline::decode_blob(&ec, code.stripe_size, blob.len, &slots)?;
        out.extend_from_slice(&decoded.bytes);
        for index in decoded.healed {
            heals.insert((blob.chunk.chunk_id.get(), index as u8));
        }
    }

    // INVARIANT(design 04 §4): the reconstructed length must equal what the
    // layout claims. A real check, not `debug_assert` — release builds compile
    // those out, and a truncated slice list would then be served as a *success*
    // with a short body under the metadata's `content_length`.
    if out.len() as u64 != layout.size {
        return Err(GatewayError::LengthMismatch {
            expected: layout.size,
            got: out.len() as u64,
        });
    }
    let heals = heals
        .into_iter()
        .map(|(chunk, index)| HealReport {
            chunk_id: ChunkId::new(chunk),
            index,
        })
        .collect();
    Ok(ReadObject { bytes: out, heals })
}

/// Reads all `total` shards of `blob_id` in parallel into index-ordered slots;
/// a read that errors or misses leaves its slot `None` for reconstruction.
async fn read_blob_shards(
    transport: &Arc<dyn ShardTransport>,
    chunk: &ChunkPlacement,
    blob_id: BlobId,
    total: usize,
) -> Vec<Option<Vec<u8>>> {
    let mut set = JoinSet::new();
    for (j, &(shard, node)) in chunk.shards.iter().enumerate() {
        let transport = Arc::clone(transport);
        set.spawn(async move {
            let req = ReadShardReq {
                shard_id: shard,
                blob_id,
            };
            match transport.read_shard(node, req).await {
                Ok(opt) => (j, opt),
                Err(err) => {
                    tracing::debug!(error = %err, "shard read failed; treating as missing");
                    (j, None)
                }
            }
        });
    }

    let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
    while let Some(res) = set.join_next().await {
        match res {
            Ok((j, Some(bytes))) => slots[j] = Some(bytes.to_vec()),
            Ok((_, None)) => {}
            Err(join) => tracing::warn!(error = %join, "shard read task failed"),
        }
    }
    slots
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::WriterToken;

    use crate::code::CodeMode;
    use crate::put::Writer;
    use crate::testutil::{self, Fault};

    const STRIPE: usize = 128;
    const BLOB: usize = 1024;

    fn writer() -> Writer {
        Writer::new(WriterToken::new(1))
    }

    /// PUTs `object` with `writer` against `cluster` and reads it back.
    async fn round_trip(
        writer: &Writer,
        cluster: &testutil::Cluster,
        object: &[u8],
    ) -> Result<ReadObject, GatewayError> {
        let layout = writer
            .put_object(
                cluster.transport.clone(),
                &cluster.code,
                &cluster.chunk,
                object,
            )
            .await
            .expect("put");
        get_object(cluster.transport.clone(), &layout).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn round_trip_across_sizes_all_shards_healthy() {
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster = testutil::cluster(code, &[Fault::None; 3]).await;
        // One writer across all objects: blob ids stay monotonic, so repeated
        // PUTs reuse each shard's extent rather than colliding on a blob id.
        let writer = writer();
        for &len in &[1usize, 100, 128, 300, BLOB, BLOB + 1, 2500] {
            let object = testutil::object_of(len);
            let got = round_trip(&writer, &cluster, &object).await.unwrap();
            assert_eq!(got.bytes, object, "len={len}");
            // Heals may or may not be empty here: a PUT acks at quorum (2/3) and
            // the third shard commits detached, so an immediate read can race it
            // and legitimately report the straggler (the Q5 fast path). The bytes
            // are always correct; heal *content* is asserted by the fault tests.
        }
        cluster.shutdown();
    }

    /// INVARIANT(design 04 §4): a ranged read must return exactly the requested
    /// bytes. Exercised through real EC (encode → shard writes → decode) over an
    /// object spanning several blobs, so the blob-selection math is validated
    /// against actual reconstructed bytes rather than a stub.
    ///
    /// Every window is checked, including the ones a naive implementation gets
    /// wrong: blob boundaries, single bytes, suffixes, and the whole object.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ranged_read_returns_exactly_the_window_across_blob_boundaries() {
        use crate::range::{self, ByteRange};

        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster = testutil::cluster(code, &[Fault::None; 3]).await;
        let writer = writer();
        // 3.5 blobs, so windows can straddle boundaries and land in a short tail.
        let len = BLOB * 3 + BLOB / 2;
        let object = testutil::object_of(len);
        let layout = writer
            .put_object(
                cluster.transport.clone(),
                &cluster.code,
                &cluster.chunk,
                &object,
            )
            .await
            .expect("put");
        assert!(layout.blobs.len() > 1, "object must span multiple blobs");

        let total = len as u64;
        let windows = [
            (0u64, 0u64),                               // first byte
            (total - 1, total - 1),                     // last byte
            (0, total - 1),                             // whole object
            (BLOB as u64 - 1, BLOB as u64),             // straddles blob 0/1
            (BLOB as u64, BLOB as u64 * 2 - 1),         // exactly blob 1
            (BLOB as u64 - 5, BLOB as u64 + 5),         // spans a boundary
            (BLOB as u64 * 3, total - 1),               // the short tail blob
            (10, 20),                                   // inside blob 0
            (BLOB as u64 * 2 + 7, BLOB as u64 * 3 + 9), // spans blob 2/3
        ];
        for (start, end) in windows {
            let window = ByteRange { start, end };
            let plan =
                range::plan(&layout, window).unwrap_or_else(|| panic!("plan for {start}..={end}"));
            // Only the overlapping blobs are fetched — a range must not decode
            // the whole object.
            let narrowed = crate::code::ObjectLayout {
                size: plan.blobs.iter().map(|b| b.len as u64).sum(),
                code: layout.code,
                blobs: plan.blobs.clone(),
            };
            let read = get_object(cluster.transport.clone(), &narrowed)
                .await
                .unwrap_or_else(|e| panic!("read {start}..={end}: {e}"));
            let got = &read.bytes[plan.skip..plan.skip + plan.take];
            let want = &object[start as usize..=end as usize];
            assert_eq!(
                got,
                want,
                "window {start}..={end} (len {}) mismatched",
                window.byte_count()
            );
        }

        // A window inside one blob must not read the others.
        let single = range::plan(&layout, ByteRange { start: 10, end: 20 }).expect("plan");
        assert_eq!(
            single.blobs.len(),
            1,
            "a small window reads one blob, not the whole object"
        );
        cluster.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_tolerates_one_failed_shard_and_get_reconstructs() {
        // Shard 0's write path is down: PUT still reaches quorum (2/3), and at
        // GET shard 0 has no data so the object reconstructs from 1 + parity.
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster = testutil::cluster(code, &[Fault::FailWrite, Fault::None, Fault::None]).await;
        let object = testutil::object_of(300);
        let got = round_trip(&writer(), &cluster, &object).await.unwrap();
        assert_eq!(got.bytes, object);
        // The never-written shard 0 is reported for heal-on-read.
        assert!(
            got.heals.iter().any(|h| h.index == 0),
            "failed-write shard 0 reported for heal: {:?}",
            got.heals
        );
        cluster.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_reconstructs_when_a_data_shard_read_misses() {
        // All shards written; shard 0's read path returns misses at GET.
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster = testutil::cluster(code, &[Fault::DropRead, Fault::None, Fault::None]).await;
        let object = testutil::object_of(2500); // multi-blob
        let got = round_trip(&writer(), &cluster, &object).await.unwrap();
        assert_eq!(got.bytes, object);
        // The missing shard 0 is always reported; a just-committed straggler on
        // another shard may also appear (quorum-write race), so assert 0 ∈ heals.
        assert!(
            got.heals.iter().any(|h| h.index == 0),
            "missing shard 0 reported for heal: {:?}",
            got.heals
        );
        cluster.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_reconstructs_when_a_shard_is_bitrot_corrupted() {
        // Shard 0 returns a corrupted body; the bitrot frame fails to verify and
        // the stripe reconstructs from the survivors.
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster =
            testutil::cluster(code, &[Fault::CorruptRead, Fault::None, Fault::None]).await;
        let object = testutil::object_of(300);
        let w = writer();
        let layout = w
            .put_object(
                cluster.transport.clone(),
                &cluster.code,
                &cluster.chunk,
                &object,
            )
            .await
            .unwrap();
        // The PUT acks at quorum (2 of 3, 04 §3.3); the detached third shard
        // may still be committing. With shard 0 corrupt, this read needs both
        // survivors — bounded poll until the straggler lands (never a fixed
        // sleep; converges in microseconds on a healthy node).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let got = loop {
            match get_object(cluster.transport.clone(), &layout).await {
                Ok(got) => break got,
                Err(err) if std::time::Instant::now() < deadline => {
                    tracing::debug!(error = %err, "straggler not yet committed, retrying");
                    tokio::task::yield_now().await;
                }
                Err(err) => panic!("read never converged: {err:?}"),
            }
        };
        assert_eq!(got.bytes, object);
        // The corrupt shard 0 is reported for heal-on-read.
        assert!(
            got.heals.iter().any(|h| h.index == 0),
            "bitrot shard 0 reported for heal: {:?}",
            got.heals
        );
        cluster.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_fails_when_quorum_is_unreachable() {
        // Two of three shards are down: only one commits (< quorum 2).
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster =
            testutil::cluster(code, &[Fault::FailWrite, Fault::FailWrite, Fault::None]).await;
        let err = writer()
            .put_object(
                cluster.transport.clone(),
                &cluster.code,
                &cluster.chunk,
                &testutil::object_of(200),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, GatewayError::QuorumNotMet { need: 2, got: 1 }),
            "unexpected error: {err:?}"
        );
        cluster.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_rejects_mismatched_placement() {
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster = testutil::cluster(code, &[Fault::None; 3]).await;
        let mut chunk = cluster.chunk.clone();
        chunk.shards.pop(); // 2 shards for a 3-shard code
        let err = writer()
            .put_object(
                cluster.transport.clone(),
                &cluster.code,
                &chunk,
                &testutil::object_of(50),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::ShardCount { need: 3, got: 2 }));
        cluster.shutdown();
    }

    /// C5 regression (04 §3.3): a wedged (not crashed) shard must not stall the
    /// blob — the PUT acks at quorum within the fast shards' time and reports
    /// the slow shard as a miss.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_acks_at_quorum_past_a_wedged_shard() {
        let code = CodeMode::new(2, 1, STRIPE, BLOB).unwrap();
        let cluster = testutil::cluster(code, &[Fault::None, Fault::None, Fault::SlowCommit]).await;
        let started = std::time::Instant::now();
        let layout = writer()
            .put_object(
                cluster.transport.clone(),
                &cluster.code,
                &cluster.chunk,
                &testutil::object_of(300),
            )
            .await
            .expect("put must ack at quorum without the wedged shard");
        // Quorum(2) met by the two healthy shards; the wedged one timed out —
        // well under the hour it would otherwise stall.
        assert!(
            started.elapsed() < std::time::Duration::from_secs(20),
            "put stalled on the wedged shard: {:?}",
            started.elapsed()
        );
        // The object reads back from the surviving shards (EC 2+1 tolerates 1).
        let got = get_object(cluster.transport.clone(), &layout)
            .await
            .expect("read");
        assert_eq!(got.bytes, testutil::object_of(300));
        cluster.shutdown();
    }
}
