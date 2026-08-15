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

//! Per-extent writer thread: the single owner of one [`ExtentFile`]'s append
//! cursor (02 §1.4). Because `append_record` needs `&mut ExtentFile`, each
//! writable extent is served by exactly one dedicated OS thread that drains a
//! bounded queue of [`WriteRequest`]s; the async data plane hands work to it
//! over a channel and awaits the reply, never touching the file directly.
//!
//! Group commit: the loop blocks for one request, then opportunistically
//! coalesces the ready backlog into a bounded batch and commits it with a single
//! fsync followed by a single synced index batch ([`DiskIndex::commit_blobs`]).
//! This amortizes the two durability barriers across concurrent writers to the
//! same shard while preserving the per-blob append→fsync→commit ordering the
//! persistence contract requires (02 §1.7).

use std::collections::HashSet;
use std::sync::Arc;
use std::thread::JoinHandle;

use bytes::Bytes;
use epoch_proto::{BlobId, EpochError, ExtentId};
use tokio::sync::{mpsc, oneshot};

use crate::error::StoreError;
use crate::extent::file::ExtentFile;
use crate::index::{BlobIndex, DiskIndex};
use crate::write::WriteOutcome;

/// Bounded depth of one extent's write queue. A full queue backpressures the
/// async caller's `send().await` rather than growing without bound (§5).
pub(crate) const WRITER_QUEUE_DEPTH: usize = 256;

/// Max records fused into one group commit (bounds one batch's fsync scope).
const MAX_BATCH_RECORDS: usize = 64;

/// Soft byte budget for one group commit: coalescing stops once the accumulated
/// bodies reach this, bounding a batch's memory and fsync latency to roughly
/// this plus one in-flight body.
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// A single blob-shard write handed to a per-extent writer thread.
///
/// The reply is the cross-component [`EpochError`] result (not [`StoreError`])
/// so a group-commit failure can fan out one cloned error to every request in
/// the batch — `StoreError` is not `Clone` (it wraps `io::Error`).
pub(crate) struct WriteRequest {
    /// Blob this shard belongs to.
    pub blob_id: BlobId,
    /// Framed shard body to append (owned; moved zero-copy from the RPC accumulator).
    pub body: Bytes,
    /// One-shot channel the writer replies on.
    pub reply: oneshot::Sender<Result<WriteOutcome, EpochError>>,
}

/// Spawns the dedicated OS thread that owns `extent` and drains its write queue,
/// returning the bounded sender and the thread handle. The thread exits once the
/// sender is dropped (after draining any queued requests), releasing the file
/// handle — this is how [`crate::service`] stops or hands off a writer.
pub(crate) fn spawn(
    index: Arc<DiskIndex>,
    extent: ExtentFile,
) -> (mpsc::Sender<WriteRequest>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(WRITER_QUEUE_DEPTH);
    let handle = std::thread::spawn(move || writer_loop(extent, &index, rx));
    (tx, handle)
}

/// Blocking drain loop: block for one request, coalesce the ready backlog into a
/// bounded batch, group-commit it, repeat until the channel closes.
fn writer_loop(mut extent: ExtentFile, index: &DiskIndex, mut rx: mpsc::Receiver<WriteRequest>) {
    while let Some(first) = rx.blocking_recv() {
        let mut batch_bytes = first.body.len();
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH_RECORDS && batch_bytes < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(req) => {
                    batch_bytes += req.body.len();
                    batch.push(req);
                }
                // Empty (nothing more ready) or disconnected: commit what we have.
                Err(_) => break,
            }
        }
        process_batch(&mut extent, index, batch);
    }
}

/// Appends and group-commits one batch, replying to each request. Idempotent
/// retries and per-request rejections reply immediately; newly appended records
/// share one fsync + one synced index batch and reply with its outcome.
fn process_batch(extent: &mut ExtentFile, index: &DiskIndex, batch: Vec<WriteRequest>) {
    let extent_id = extent.extent_id();

    // One meta read + status gate covers the whole batch (02 §1.4).
    if let Err(err) = load_writable_meta(index, extent_id) {
        let epoch = err.to_epoch();
        for req in batch {
            let _ = req.reply.send(Err(epoch.clone()));
        }
        return;
    }

    let mut appended: Vec<(BlobId, BlobIndex)> = Vec::new();
    let mut appended_replies: Vec<oneshot::Sender<Result<WriteOutcome, EpochError>>> = Vec::new();
    let mut seen_in_batch: HashSet<BlobId> = HashSet::new();
    // Set once a raw append fails (disk evidence): remaining requests fail too
    // and the group commit is skipped.
    let mut fatal: Option<EpochError> = None;

    for req in batch {
        if let Some(err) = &fatal {
            let _ = req.reply.send(Err(err.clone()));
            continue;
        }
        // Idempotent retry: an already-committed blob needs no rewrite (02 §1.4).
        match index.get_blob(extent_id, req.blob_id) {
            Ok(Some(_)) => {
                let _ = req.reply.send(Ok(WriteOutcome::AlreadyExists));
                continue;
            }
            Ok(None) => {}
            Err(err) => {
                let _ = req.reply.send(Err(StoreError::from(err).to_epoch()));
                continue;
            }
        }
        // A blob id repeated within one batch commits once.
        if !seen_in_batch.insert(req.blob_id) {
            let _ = req.reply.send(Ok(WriteOutcome::AlreadyExists));
            continue;
        }
        let size = match u32::try_from(req.body.len()) {
            Ok(size) => size,
            Err(_) => {
                let _ = req
                    .reply
                    .send(Err(StoreError::BodyTooLarge(req.body.len()).to_epoch()));
                continue;
            }
        };
        match extent.append_record(req.blob_id, extent_id.shard_id(), &req.body) {
            Ok((offset, crc)) => {
                appended.push((req.blob_id, BlobIndex::new(offset, size, crc)));
                appended_replies.push(req.reply);
            }
            Err(err) => {
                let epoch = StoreError::from(err).to_epoch();
                let _ = req.reply.send(Err(epoch.clone()));
                fatal = Some(epoch);
            }
        }
    }

    if appended.is_empty() {
        return;
    }

    let result = commit_group(extent, index, extent_id, &appended);
    for reply in appended_replies {
        let _ = match &result {
            Ok(()) => reply.send(Ok(WriteOutcome::Written)),
            Err(epoch) => reply.send(Err(epoch.clone())),
        };
    }
}

/// Reads the extent's metadata and enforces the write-path status gate
/// (shares the gate with [`crate::write::write_blob`]). A seal landing after
/// this gate but before the batch commit is the Q17 drain window — the data
/// commits and the status stays Sealed (field-scoped merges, crate::meta_merge).
fn load_writable_meta(index: &DiskIndex, extent_id: ExtentId) -> Result<(), StoreError> {
    let meta = index
        .get_extent_meta(extent_id)?
        .ok_or(StoreError::ExtentNotFound(extent_id))?;
    crate::write::writable_gate(meta.status)?;
    Ok(())
}

/// The single durability barrier for a batch: one fsync, then one synced index
/// batch recording every appended blob and merging the advanced extent size
/// (02 §1.7). The size merge is field-scoped (crate::meta_merge), so a delete
/// or seal landing between the batch's gate read and this commit is never
/// rolled back — the Q17 drain semantics: queued data commits, status stays
/// Sealed.
fn commit_group(
    extent: &mut ExtentFile,
    index: &DiskIndex,
    extent_id: ExtentId,
    appended: &[(BlobId, BlobIndex)],
) -> Result<(), EpochError> {
    extent
        .sync()
        .map_err(|err| StoreError::from(err).to_epoch())?;
    index
        .commit_blobs(extent_id, appended, extent.write_offset())
        .map_err(|err| StoreError::from(err).to_epoch())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extent::state::ExtentStatus;
    use crate::read::read_blob;
    use crate::testutil::{blob, writable_extent};

    fn request(
        seq: u32,
        body: &[u8],
    ) -> (
        WriteRequest,
        oneshot::Receiver<Result<WriteOutcome, EpochError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        (
            WriteRequest {
                blob_id: blob(seq),
                body: Bytes::copy_from_slice(body),
                reply: tx,
            },
            rx,
        )
    }

    #[test]
    fn group_commit_writes_every_blob_and_advances_size() {
        let mut fx = writable_extent();
        let (r0, mut rx0) = request(0, b"alpha");
        let (r1, mut rx1) = request(1, &[7u8; 3000]);
        let (r2, mut rx2) = request(2, b"gamma");

        process_batch(&mut fx.extent, &fx.index, vec![r0, r1, r2]);

        assert_eq!(rx0.try_recv().expect("r0"), Ok(WriteOutcome::Written));
        assert_eq!(rx1.try_recv().expect("r1"), Ok(WriteOutcome::Written));
        assert_eq!(rx2.try_recv().expect("r2"), Ok(WriteOutcome::Written));

        for (seq, body) in [(0u32, &b"alpha"[..]), (2, &b"gamma"[..])] {
            assert_eq!(
                read_blob(&fx.index, &fx.extent, blob(seq))
                    .expect("read")
                    .as_deref(),
                Some(body)
            );
        }
        // The single committed meta reflects the final append cursor.
        assert_eq!(
            fx.index
                .get_extent_meta(fx.extent_id)
                .expect("meta")
                .map(|m| m.size),
            Some(fx.extent.write_offset())
        );
    }

    #[test]
    fn duplicate_blob_ids_within_a_batch_commit_once() {
        let mut fx = writable_extent();
        let (r0, mut rx0) = request(5, b"first");
        let (dup, mut rx_dup) = request(5, b"second");

        process_batch(&mut fx.extent, &fx.index, vec![r0, dup]);

        assert_eq!(rx0.try_recv().expect("r0"), Ok(WriteOutcome::Written));
        assert_eq!(
            rx_dup.try_recv().expect("dup"),
            Ok(WriteOutcome::AlreadyExists)
        );
        // The first write's body is the one that persisted.
        assert_eq!(
            read_blob(&fx.index, &fx.extent, blob(5))
                .expect("read")
                .as_deref(),
            Some(&b"first"[..])
        );
    }

    #[test]
    fn already_committed_blob_replies_already_exists() {
        let mut fx = writable_extent();
        let (r0, mut rx0) = request(0, b"once");
        process_batch(&mut fx.extent, &fx.index, vec![r0]);
        assert_eq!(rx0.try_recv().expect("r0"), Ok(WriteOutcome::Written));

        let (retry, mut rx_retry) = request(0, b"again");
        process_batch(&mut fx.extent, &fx.index, vec![retry]);
        assert_eq!(
            rx_retry.try_recv().expect("retry"),
            Ok(WriteOutcome::AlreadyExists)
        );
    }

    #[test]
    fn non_writable_extent_fails_the_whole_batch() {
        let mut fx = writable_extent();
        let mut meta = fx
            .index
            .get_extent_meta(fx.extent_id)
            .expect("meta")
            .expect("present");
        meta.status = ExtentStatus::Sealed;
        fx.index.put_extent_meta(fx.extent_id, &meta).expect("seal");

        let (r0, mut rx0) = request(0, b"x");
        let (r1, mut rx1) = request(1, b"y");
        process_batch(&mut fx.extent, &fx.index, vec![r0, r1]);

        assert_eq!(rx0.try_recv().expect("r0"), Err(EpochError::Sealed));
        assert_eq!(rx1.try_recv().expect("r1"), Err(EpochError::Sealed));
    }
}
