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

use std::collections::HashMap;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, MutexGuard, PoisonError};

use bytes::{Bytes, BytesMut};
use epoch_proto::EpochError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex as TokioMutex, oneshot};
use tokio::task::JoinHandle;

use crate::codec::Pcode;
use crate::frame::{FRAME_HEADER_LEN, FrameHeader};

/// Upper bound on a single frame's payload, guarding against a bogus
/// `payload_len` triggering a huge allocation. A shard body for a 32 MiB blob
/// stays well under this even at low data-shard counts.
pub(crate) const MAX_FRAME_PAYLOAD: usize = 64 * 1024 * 1024;

type WaiterMap = Arc<StdMutex<Waiters>>;

/// Pending response waiters for a connection, plus a `closed` flag.
///
/// The flag lives under the same lock as the map so a request that races the
/// reader's shutdown either registers before close (and is then dropped by the
/// reader, observing an error) or sees `closed` and fails fast — it can never
/// register a waiter that no reader will ever complete (which would hang).
#[derive(Default)]
struct Waiters {
    closed: bool,
    map: HashMap<u32, oneshot::Sender<Inbound>>,
}

/// A response frame routed back to the caller that owns its stream.
pub(crate) struct Inbound {
    /// Opcode of the response.
    pub pcode: u16,
    /// Response flag bits (e.g. [`FLAG_READ_HIT`](crate::codec::FLAG_READ_HIT)).
    pub flags: u8,
    /// Response payload (control struct or body), a zero-copy slice.
    pub payload: Bytes,
}

/// Locks a std mutex, recovering the guard if a prior holder panicked. The
/// critical sections here are tiny map ops that cannot themselves panic.
fn lock<T>(m: &StdMutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Writes one frame as a single logical unit: header then body, via a vectored
/// write so the body is not copied into a combined buffer. Partial vectored
/// writes fall back to writing the remainder.
pub(crate) async fn send_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    header: &[u8; FRAME_HEADER_LEN],
    body: &[u8],
) -> io::Result<()> {
    if body.is_empty() {
        return w.write_all(header).await;
    }
    let bufs = [IoSlice::new(header), IoSlice::new(body)];
    let n = w.write_vectored(&bufs).await?;
    if n < FRAME_HEADER_LEN {
        w.write_all(&header[n..]).await?;
        w.write_all(body).await
    } else {
        let body_off = n - FRAME_HEADER_LEN;
        if body_off < body.len() {
            w.write_all(&body[body_off..]).await
        } else {
            Ok(())
        }
    }
}

/// Reads and validates one frame header.
pub(crate) async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<FrameHeader> {
    let mut buf = [0u8; FRAME_HEADER_LEN];
    r.read_exact(&mut buf).await?;
    FrameHeader::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Reads exactly `len` payload bytes into a fresh buffer (a zero-copy `Bytes`).
pub(crate) async fn read_body<R: AsyncRead + Unpin>(r: &mut R, len: u32) -> io::Result<Bytes> {
    let len = len as usize;
    if len > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame payload exceeds maximum",
        ));
    }
    if len == 0 {
        return Ok(Bytes::new());
    }
    let mut buf = BytesMut::zeroed(len);
    r.read_exact(&mut buf).await?;
    Ok(buf.freeze())
}

/// A client-side multiplexed connection to one data node.
pub(crate) struct Connection {
    write: TokioMutex<OwnedWriteHalf>,
    waiters: WaiterMap,
    next_stream: AtomicU32,
    session: u64,
    reader: JoinHandle<()>,
}

impl Connection {
    /// Connects to `addr` and starts the response reader task.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] if the TCP connection cannot be established.
    pub(crate) async fn connect(addr: SocketAddr) -> Result<Self, EpochError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|_| EpochError::Internal)?;
        let _ = stream.set_nodelay(true);
        let (rd, wr) = stream.into_split();
        let waiters: WaiterMap = Arc::new(StdMutex::new(Waiters::default()));
        let reader = tokio::spawn(reader_loop(rd, waiters.clone()));
        Ok(Self {
            write: TokioMutex::new(wr),
            waiters,
            next_stream: AtomicU32::new(1),
            session: 0,
            reader,
        })
    }

    /// Allocates the next stream id on this connection.
    pub(crate) fn alloc_stream(&self) -> u32 {
        self.next_stream.fetch_add(1, Ordering::Relaxed)
    }

    /// Registers a response waiter for `stream_id`, or `None` if the connection
    /// has already closed (so the caller fails fast instead of hanging).
    pub(crate) fn register(&self, stream_id: u32) -> Option<oneshot::Receiver<Inbound>> {
        let (tx, rx) = oneshot::channel();
        let mut waiters = lock(&self.waiters);
        if waiters.closed {
            return None;
        }
        waiters.map.insert(stream_id, tx);
        Some(rx)
    }

    /// Drops a previously registered waiter (e.g. after a send failed).
    pub(crate) fn deregister(&self, stream_id: u32) {
        lock(&self.waiters).map.remove(&stream_id);
    }

    /// Whether the reader task has observed the peer closing (or failing) and
    /// marked the connection closed. A closed connection fails all future ops
    /// fast and must be evicted by the pool rather than reused.
    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.waiters).closed
    }

    /// Builds a frame header for this connection's session. `payload_len` is
    /// filled in by [`send`](Self::send) from the actual body length.
    pub(crate) fn header(&self, pcode: Pcode, flags: u8, stream_id: u32, seq: u32) -> FrameHeader {
        FrameHeader {
            pcode: pcode.to_u16(),
            flags,
            stream_id,
            seq,
            session: self.session,
            payload_len: 0,
            trace_id: 0,
            deadline_ms: 0,
        }
    }

    /// Sends one frame (header + body), serialized against other senders. Sets
    /// the header's `payload_len` from `body` so the two cannot disagree.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] if the body is too large or the socket write
    /// fails.
    pub(crate) async fn send(
        &self,
        mut header: FrameHeader,
        body: Bytes,
    ) -> Result<(), EpochError> {
        header.payload_len = u32::try_from(body.len()).map_err(|_| EpochError::Internal)?;
        let bytes = header.encode();
        let mut w = self.write.lock().await;
        send_frame(&mut *w, &bytes, &body)
            .await
            .map_err(|_| EpochError::Internal)
    }

    /// Sends a one-shot request and awaits its single response frame.
    ///
    /// # Errors
    ///
    /// [`EpochError::Internal`] if the send fails or the connection closes
    /// before a response arrives.
    pub(crate) async fn request(
        &self,
        pcode: Pcode,
        flags: u8,
        payload: Bytes,
    ) -> Result<Inbound, EpochError> {
        let stream_id = self.alloc_stream();
        let rx = self.register(stream_id).ok_or(EpochError::Internal)?;
        let header = self.header(pcode, flags, stream_id, 0);
        if let Err(err) = self.send(header, payload).await {
            self.deregister(stream_id);
            return Err(err);
        }
        rx.await.map_err(|_| EpochError::Internal)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// The reader task: routes each inbound frame to its stream's waiter; on close,
/// marks the connection closed and drops every pending waiter so both in-flight
/// and future requests observe an error rather than hang.
async fn reader_loop(mut rd: OwnedReadHalf, waiters: WaiterMap) {
    loop {
        let Ok(header) = read_header(&mut rd).await else {
            break;
        };
        let Ok(payload) = read_body(&mut rd, header.payload_len).await else {
            break;
        };
        let waiter = lock(&waiters).map.remove(&header.stream_id);
        if let Some(tx) = waiter {
            let _ = tx.send(Inbound {
                pcode: header.pcode,
                flags: header.flags,
                payload,
            });
        }
    }
    let mut waiters = lock(&waiters);
    waiters.closed = true;
    waiters.map.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tokio::net::TcpListener;

    /// A request on a connection whose peer has closed must fail fast, never
    /// hang — the regression guard for a killed node's cached connection
    /// (a subsequent request would otherwise register a waiter no reader ever
    /// completes). Design: docs/design/04-ec-io.md §3.3.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_fails_fast_after_peer_closes() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        // Accept one connection then immediately drop it (a dead peer).
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            drop(stream);
        });

        let conn = Connection::connect(addr).await.expect("connect");
        accept.await.expect("accept task");

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            conn.request(Pcode::ReadShard, 0, Bytes::new()),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(EpochError::Internal))),
            "expected a fast Err(Internal), got a timeout or an unexpected reply"
        );
    }
}
