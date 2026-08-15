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

//! PUT orchestration: cut an object into blobs, EC-encode each blob, and stream
//! its `data + parity` shards to their nodes in parallel, succeeding once
//! [`CodeMode::write_quorum`] shards commit (sticky slow-shard tolerance,
//! 04 §3.3). A blob's id is constructed locally from the writer's token and a
//! monotonic sequence, with no allocation round-trip (01 §4.3).
//!
//! The gateway writes to *already-provisioned* shards: it OPENs the writable
//! extent each shard's node created at startup (the M3 stand-in for PD-driven
//! chunk provisioning, 02 §2.1) and never creates extents itself, so repeated
//! PUTs to a chunk reuse one extent per shard rather than orphaning the last.
//!
//! The returned [`ObjectLayout`] is the M3 stand-in for the MetaNode object
//! record a GET later replays. Design: draft/design/04-ec-io.md §3.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use epoch_proto::{BlobId, EpochError, NodeId, ShardId, WriterToken};
use epoch_rpc::{EndReq, OpenReq, ShardTransport};
use std::time::Duration;

use tokio::task::JoinSet;

use crate::code::{BlobDesc, ChunkPlacement, CodeMode, ObjectLayout};
use crate::error::GatewayError;
use crate::pipeline;

/// A gateway writer: a stable [`WriterToken`] plus the monotonic blob sequence
/// it stamps onto every blob it writes (01 §4.3). One per registration; M3 uses
/// a statically-assigned token.
#[derive(Debug)]
pub struct Writer {
    token: WriterToken,
    next_seq: AtomicU32,
}

impl Writer {
    /// Creates a writer stamping blobs under `token`, starting at sequence 0.
    #[must_use]
    pub fn new(token: WriterToken) -> Self {
        Self {
            token,
            next_seq: AtomicU32::new(0),
        }
    }

    /// The writer's token.
    #[must_use]
    pub fn token(&self) -> WriterToken {
        self.token
    }

    /// Allocates the next monotonic blob id under this writer's token.
    fn next_blob(&self) -> BlobId {
        BlobId::new(self.token, self.next_seq.fetch_add(1, Ordering::Relaxed))
    }

    /// Writes `data` as an erasure-coded object across `chunk`'s shards and
    /// returns its layout.
    ///
    /// The object is cut into `code.blob_size` blobs; each blob is EC-encoded
    /// and its shards are streamed in parallel to the shards' pre-provisioned
    /// writable extents. A blob succeeds once [`CodeMode::write_quorum`] shards
    /// commit; otherwise the PUT fails and the partially-written blobs are left
    /// for GC to reclaim (02 §1.6).
    ///
    /// # Errors
    ///
    /// - [`GatewayError::ShardCount`] if `chunk` does not have `data + parity`
    ///   shards;
    /// - [`GatewayError::Ec`] on an encoding failure;
    /// - [`GatewayError::QuorumNotMet`] if a blob fails to reach write quorum.
    pub async fn put_object(
        &self,
        transport: Arc<dyn ShardTransport>,
        code: &CodeMode,
        chunk: &ChunkPlacement,
        data: &[u8],
    ) -> Result<ObjectLayout, GatewayError> {
        let ec = code.erasure()?;
        let total = code.total();
        if chunk.shards.len() != total {
            return Err(GatewayError::ShardCount {
                need: total,
                got: chunk.shards.len(),
            });
        }

        let quorum = code.write_quorum();
        let mut blobs = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + code.blob_size).min(data.len());
            let slice = &data[offset..end];
            let blob_id = self.next_blob();

            let (committed, _errors) =
                write_blob(&transport, code, chunk, &ec, blob_id, slice).await?;
            if committed < quorum {
                return Err(GatewayError::QuorumNotMet {
                    need: quorum,
                    got: committed,
                });
            }
            blobs.push(BlobDesc {
                blob_id,
                len: slice.len(),
                chunk: chunk.clone(),
                code: *code,
            });
            offset = end;
        }

        Ok(ObjectLayout {
            size: data.len() as u64,
            code: *code,
            blobs,
        })
    }
}

/// Encodes one blob and streams its shards in parallel, returning how many
/// committed and the per-shard errors observed (the gateway's rewrite trigger
/// distinguishes `Sealed`/`ChunkFull`/`DiskBroken` evictions from transient
/// faults, 04 §3.3).
/// Per-shard stream deadline: a shard slower than this is treated as a sticky
/// failure (04 §3.3) — the blob acks at quorum without it, and the stream's
/// drop sends an Abort so the server frees its state (02 §5).
#[cfg(not(test))]
const SHARD_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Shortened under this crate's own tests so the slow-shard case runs fast.
#[cfg(test)]
const SHARD_WRITE_TIMEOUT: Duration = Duration::from_millis(500);

pub(crate) async fn write_blob(
    transport: &Arc<dyn ShardTransport>,
    code: &CodeMode,
    chunk: &ChunkPlacement,
    ec: &epoch_ec::Erasure,
    blob_id: BlobId,
    slice: &[u8],
) -> Result<(usize, Vec<EpochError>), GatewayError> {
    let bodies = pipeline::encode_blob(ec, code.stripe_size, slice)?;
    let frame_lens = pipeline::stripe_frame_lens(slice.len(), code.stripe_size, code.data);
    let quorum = code.write_quorum();

    let mut set = JoinSet::new();
    for (j, body) in bodies.into_iter().enumerate() {
        let (shard, node) = chunk.shards[j];
        let transport = Arc::clone(transport);
        let frame_lens = frame_lens.clone();
        set.spawn(async move {
            // The timeout makes a wedged (not crashed) node a sticky failure
            // instead of an indefinite stall; the aborted future drops its
            // WriteStream, whose Drop sends the server an Abort.
            match tokio::time::timeout(
                SHARD_WRITE_TIMEOUT,
                write_shard(transport, node, shard, blob_id, body, frame_lens),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    // A wedged shard (04 §3.3): count it so slow-node patterns
                    // are visible on /metrics, not just in logs.
                    crate::metrics::record_shard_timeout();
                    Err(EpochError::Busy)
                }
            }
        });
    }

    // INVARIANT(design 04 §3.3): ack at quorum — do not wait for stragglers.
    // Once `committed >= quorum` the outcome cannot regress (failures after
    // quorum only widen the repair set), so the remaining streams are detached:
    // they either commit harmlessly (their shard is simply present) or drop
    // and abort. Missing shards are healed by InspectRound (the mandatory
    // backstop) and the future repair-report fast path.
    let mut committed = 0;
    let mut errors = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(())) => committed += 1,
            Ok(Err(err)) => {
                tracing::debug!(error = %err, "shard write dropped");
                errors.push(err);
            }
            Err(join) => tracing::warn!(error = %join, "shard write task failed"),
        }
        if committed >= quorum {
            crate::metrics::record_quorum_ack();
            // TODO(repair-report M7): report the shards still in flight /
            // failed as repair candidates once the ShardRepair intake exists.
            set.detach_all();
            break;
        }
    }
    Ok((committed, errors))
}

/// Streams one shard body as a sequence of per-stripe frames, then closes the
/// stream and waits for the peer's commit.
async fn write_shard(
    transport: Arc<dyn ShardTransport>,
    node: NodeId,
    shard: ShardId,
    blob_id: BlobId,
    body: Vec<u8>,
    frame_lens: Vec<usize>,
) -> Result<(), EpochError> {
    let blob_crc = crc32c::crc32c(&body);
    let frame_count = frame_lens.len() as u32;
    let bytes = Bytes::from(body);

    let mut stream = transport
        .open_write(
            node,
            OpenReq {
                blob_id,
                shard_id: shard,
            },
        )
        .await?;

    let mut off = 0;
    for flen in frame_lens {
        stream.send_frame(bytes.slice(off..off + flen)).await?;
        off += flen;
    }
    stream
        .finish(EndReq {
            frame_count,
            blob_crc,
        })
        .await
}
