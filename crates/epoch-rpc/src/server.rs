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

//! The data-plane server: accepts connections and dispatches decoded frames to
//! a [`ShardHandler`], reassembling blob write streams and replying per stream.
//!
//! Each connection is served by one task that processes frames in arrival
//! order. A write stream accumulates its data frames in memory; at END the
//! server verifies the frame count and whole-body CRC32C (the integrity gate,
//! 02 §5) before committing and replying `CommitAck`.
//!
//! Abandoned-stream hygiene (02 §5): a client aborts a stream it gives up on
//! (`Abort` pcode, best-effort); the server additionally bounds each stream's
//! accumulated bytes and the number of concurrently open streams per
//! connection, and reaps streams idle past a timeout — so a peer that vanishes
//! mid-stream can never strand unbounded memory on a long-lived connection.
//!
//! Design: docs/design/02-datanode.md §5; docs/design/04-ec-io.md §3.1

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use epoch_proto::{BlobId, EpochError, ExtentId};
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};

use crate::codec::{
    CreateExtentReq, CreateExtentResp, DeleteBlobReq, DeleteBlobResp, EndReq, ErrorResp,
    FLAG_READ_HIT, ListBlobsReq, ListBlobsResp, OpenReq, Pcode, ReadShardReq, SealReq, SealResp,
};
use crate::conn::{read_body, read_header, send_frame};
use crate::frame::FrameHeader;
use crate::transport::ShardHandler;

/// Cap on one stream's accumulated bytes: a full 32 MiB blob's largest shard
/// body (data + parity framing overhead) fits well under this; anything larger
/// is a protocol violation or a runaway peer (02 §5).
const MAX_STREAM_BYTES: usize = 48 * 1024 * 1024;

/// Cap on concurrently open write streams per connection: a gateway opens at
/// most a handful of blobs' worth of shards toward one node at a time; far
/// beyond that is a leak or abuse (02 §5).
const MAX_OPEN_STREAMS: usize = 128;

/// A stream idle longer than this (no DATA/END) is reaped: its writer is gone
/// (crashed before sending Abort) or wedged past any sane shard timeout.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The in-memory reassembly state of one in-flight blob write stream.
struct WriteAccum {
    extent: ExtentId,
    blob: BlobId,
    buf: BytesMut,
    frames: u32,
    /// Last frame arrival, for idle reaping.
    last_activity: Instant,
}

/// A data-plane server bound to a listening socket, dispatching to `H`.
pub struct Server<H> {
    listener: TcpListener,
    handler: Arc<H>,
}

impl<H: ShardHandler + 'static> Server<H> {
    /// Binds a server on `addr`.
    ///
    /// # Errors
    ///
    /// Propagates the bind I/O error.
    pub async fn bind(addr: SocketAddr, handler: Arc<H>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener, handler })
    }

    /// The address the server is actually listening on (useful when bound to
    /// port 0).
    ///
    /// # Errors
    ///
    /// Propagates the underlying socket error.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves connections until `shutdown` resolves. In-flight connection tasks
    /// continue after `shutdown`; the caller sequences engine teardown.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, _peer)) => {
                        let handler = self.handler.clone();
                        tokio::spawn(handle_conn(handler, stream));
                    }
                    Err(err) => tracing::warn!(error = %err, "data-plane accept failed"),
                },
            }
        }
    }
}

/// Serves one connection: read frame, dispatch, repeat until the peer closes or
/// a write fails.
async fn handle_conn<H: ShardHandler + 'static>(handler: Arc<H>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let mut streams: HashMap<u32, WriteAccum> = HashMap::new();
    let mut last_reap = Instant::now();
    loop {
        let Ok(header) = read_header(&mut rd).await else {
            break;
        };
        let Ok(body) = read_body(&mut rd, header.payload_len).await else {
            break;
        };
        if dispatch(&handler, &mut streams, &mut wr, &header, body)
            .await
            .is_err()
        {
            break;
        }
        // Piggybacked idle reap (no timer task needed): frames keep arriving on
        // a live connection, so abandoned sibling streams are swept promptly;
        // a fully idle connection holds no accumulators worth of traffic to
        // begin with once its streams age out on the next frame.
        if last_reap.elapsed() >= STREAM_IDLE_TIMEOUT {
            reap_idle(&mut streams);
            last_reap = Instant::now();
        }
    }
}

/// Drops streams idle past [`STREAM_IDLE_TIMEOUT`] (their writers are gone).
fn reap_idle(streams: &mut HashMap<u32, WriteAccum>) {
    streams.retain(|sid, acc| {
        let keep = acc.last_activity.elapsed() < STREAM_IDLE_TIMEOUT;
        if !keep {
            tracing::warn!(
                stream = *sid,
                blob = ?acc.blob,
                buffered = acc.buf.len(),
                "reaping idle write stream"
            );
        }
        keep
    });
}

/// Dispatches one decoded frame. Returns `Err` only when a reply write fails
/// (the connection is then closed by the caller).
async fn dispatch<H, W>(
    handler: &Arc<H>,
    streams: &mut HashMap<u32, WriteAccum>,
    w: &mut W,
    header: &FrameHeader,
    body: Bytes,
) -> io::Result<()>
where
    H: ShardHandler,
    W: AsyncWrite + Unpin,
{
    let sid = header.stream_id;
    match Pcode::from_u16(header.pcode) {
        Some(Pcode::CreateExtent) => match CreateExtentReq::decode(&body) {
            Ok(req) => match handler.create_extent(req).await {
                Ok(extent) => {
                    reply(
                        w,
                        Pcode::CreateExtentResp,
                        0,
                        sid,
                        &CreateExtentResp { extent_id: extent }.encode(),
                    )
                    .await
                }
                Err(err) => reply_error(w, sid, err).await,
            },
            Err(_) => reply_error(w, sid, EpochError::Internal).await,
        },
        Some(Pcode::Open) => match OpenReq::decode(&body) {
            Ok(req) => {
                if streams.len() >= MAX_OPEN_STREAMS {
                    // Per-connection stream cap (02 §5): reject rather than
                    // strand yet another accumulator.
                    tracing::warn!(stream = sid, "open rejected: stream cap reached");
                    return reply_error(w, sid, EpochError::Busy).await;
                }
                match handler.open_blob(req).await {
                    Ok(extent) => {
                        streams.insert(
                            sid,
                            WriteAccum {
                                extent,
                                blob: req.blob_id,
                                buf: BytesMut::new(),
                                frames: 0,
                                last_activity: Instant::now(),
                            },
                        );
                        reply(w, Pcode::OpenAck, 0, sid, &[]).await
                    }
                    Err(err) => reply_error(w, sid, err).await,
                }
            }
            Err(_) => reply_error(w, sid, EpochError::Internal).await,
        },
        Some(Pcode::Data) => {
            if let Some(acc) = streams.get_mut(&sid) {
                if acc.buf.len() + body.len() > MAX_STREAM_BYTES {
                    // Per-stream byte cap (02 §5): drop the accumulator and
                    // fail the stream — a compliant writer never gets here.
                    tracing::warn!(
                        stream = sid,
                        blob = ?acc.blob,
                        "stream exceeded byte cap, aborting"
                    );
                    streams.remove(&sid);
                    return reply_error(w, sid, EpochError::Internal).await;
                }
                acc.buf.extend_from_slice(&body);
                acc.frames += 1;
                acc.last_activity = Instant::now();
            } else {
                tracing::warn!(stream = sid, "data frame for unknown stream, dropping");
            }
            Ok(())
        }
        Some(Pcode::Abort) => {
            // Best-effort client cleanup of an abandoned stream (04 §3.3):
            // drop the accumulator, no reply (the client is not waiting).
            if streams.remove(&sid).is_some() {
                tracing::debug!(stream = sid, "write stream aborted by client");
            }
            Ok(())
        }
        Some(Pcode::End) => {
            let Ok(end) = EndReq::decode(&body) else {
                return reply_error(w, sid, EpochError::Internal).await;
            };
            match streams.remove(&sid) {
                Some(acc) => commit_stream(handler, w, sid, acc, end).await,
                None => {
                    tracing::warn!(stream = sid, "end frame for unknown stream, dropping");
                    Ok(())
                }
            }
        }
        Some(Pcode::ReadShard) => match ReadShardReq::decode(&body) {
            Ok(req) => match handler.read_shard(req).await {
                Ok(Some(bytes)) => reply(w, Pcode::ReadShardResp, FLAG_READ_HIT, sid, &bytes).await,
                Ok(None) => reply(w, Pcode::ReadShardResp, 0, sid, &[]).await,
                Err(err) => reply_error(w, sid, err).await,
            },
            Err(_) => reply_error(w, sid, EpochError::Internal).await,
        },
        Some(Pcode::Seal) => match SealReq::decode(&body) {
            Ok(req) => match handler.seal_extent(req).await {
                Ok(()) => reply(w, Pcode::SealResp, 0, sid, &SealResp.encode()).await,
                Err(err) => reply_error(w, sid, err).await,
            },
            Err(_) => reply_error(w, sid, EpochError::Internal).await,
        },
        Some(Pcode::DeleteBlob) => match DeleteBlobReq::decode(&body) {
            Ok(req) => match handler.delete_blob(req).await {
                Ok(()) => reply(w, Pcode::DeleteBlobResp, 0, sid, &DeleteBlobResp.encode()).await,
                Err(err) => reply_error(w, sid, err).await,
            },
            Err(_) => reply_error(w, sid, EpochError::Internal).await,
        },
        Some(Pcode::ListBlobs) => match ListBlobsReq::decode(&body) {
            Ok(req) => match handler.list_blobs(req).await {
                Ok(blob_ids) => {
                    reply(
                        w,
                        Pcode::ListBlobsResp,
                        0,
                        sid,
                        &ListBlobsResp { blob_ids }.encode(),
                    )
                    .await
                }
                Err(err) => reply_error(w, sid, err).await,
            },
            Err(_) => reply_error(w, sid, EpochError::Internal).await,
        },
        // Response opcodes misdirected at a server are dropped silently; a
        // genuinely unknown opcode gets an Error reply so a newer peer's
        // request never hangs waiting for a response that will never come.
        None => {
            tracing::warn!(
                pcode = header.pcode,
                "unknown pcode on server, replying error"
            );
            reply_error(w, sid, EpochError::Internal).await
        }
        Some(_) => {
            tracing::warn!(
                pcode = header.pcode,
                "unexpected response opcode on server, dropping"
            );
            Ok(())
        }
    }
}

/// Commits a finished write stream; the integrity gate lives in the handler
/// (shared with the in-process path, 04 §3.1).
async fn commit_stream<H, W>(
    handler: &Arc<H>,
    w: &mut W,
    sid: u32,
    acc: WriteAccum,
    end: EndReq,
) -> io::Result<()>
where
    H: ShardHandler,
    W: AsyncWrite + Unpin,
{
    match handler
        .commit_blob(acc.extent, acc.blob, acc.frames, end, acc.buf.freeze())
        .await
    {
        Ok(()) => reply(w, Pcode::CommitAck, 0, sid, &[]).await,
        Err(err) => reply_error(w, sid, err).await,
    }
}

/// Writes a reply frame with the given opcode, flags and payload.
async fn reply<W: AsyncWrite + Unpin>(
    w: &mut W,
    pcode: Pcode,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> io::Result<()> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "reply payload too large"))?;
    let header = FrameHeader {
        pcode: pcode.to_u16(),
        flags,
        stream_id,
        seq: 0,
        session: 0,
        payload_len,
        trace_id: 0,
        deadline_ms: 0,
    };
    send_frame(w, &header.encode(), payload).await
}

/// Writes an `Error` reply carrying `err`'s wire code.
async fn reply_error<W: AsyncWrite + Unpin>(
    w: &mut W,
    stream_id: u32,
    err: EpochError,
) -> io::Result<()> {
    reply(
        w,
        Pcode::Error,
        0,
        stream_id,
        &ErrorResp::from_error(&err).encode(),
    )
    .await
}
