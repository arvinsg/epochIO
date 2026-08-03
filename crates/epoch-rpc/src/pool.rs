//! The client connection pool and remote [`ShardTransport`].
//!
//! [`RemoteTransport`] maps each [`NodeId`] to a static address and caches one
//! multiplexed [`Connection`] per node (a killed node's cached connection
//! fails its in-flight and future ops, which the gateway treats as a missing
//! shard and reconstructs around). Per-`IoClass` connection pools are deferred
//! until repair/background data-plane traffic exists (M4+); M3 has foreground
//! traffic only.
//!
//! Design: docs/design/02-datanode.md §5; docs/design/04-ec-io.md §3.3

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use epoch_proto::{EpochError, ExtentId, NodeId};

use crate::codec::{
    CreateExtentReq, CreateExtentResp, DeleteBlobReq, EndReq, ErrorResp, FLAG_READ_HIT,
    ListBlobsReq, ListBlobsResp, OpenReq, Pcode, ReadShardReq, SealReq,
};
use crate::conn::{Connection, Inbound};
use crate::transport::{ShardTransport, WriteStream};

/// Locks a std mutex, recovering the guard if a prior holder panicked.
fn lock<T>(m: &StdMutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A [`ShardTransport`] over the network, with a per-node connection cache.
pub struct RemoteTransport {
    endpoints: HashMap<NodeId, SocketAddr>,
    conns: StdMutex<HashMap<NodeId, Arc<Connection>>>,
}

impl RemoteTransport {
    /// Builds a transport over a static node→address map (M3 has no PD, so the
    /// topology is configured up front).
    #[must_use]
    pub fn new(endpoints: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            endpoints,
            conns: StdMutex::new(HashMap::new()),
        }
    }

    /// Returns a live connection to `node`, evicting a cached dead one and
    /// reconnecting as needed. A killed node fails in-flight ops fast (the
    /// gateway treats it as a missing shard), but a *transient* drop must not
    /// blacklist the node for this transport's lifetime.
    async fn conn(&self, node: NodeId) -> Result<Arc<Connection>, EpochError> {
        {
            let mut conns = lock(&self.conns);
            if let Some(conn) = conns.get(&node) {
                if !conn.is_closed() {
                    return Ok(conn.clone());
                }
                // Dead cached connection: evict so this call reconnects below.
                conns.remove(&node);
            }
        }
        let addr = *self.endpoints.get(&node).ok_or(EpochError::Internal)?;
        let conn = Arc::new(Connection::connect(addr).await?);
        // Another task may have connected concurrently; keep whichever landed.
        Ok(lock(&self.conns).entry(node).or_insert(conn).clone())
    }
}

#[async_trait]
impl ShardTransport for RemoteTransport {
    async fn create_extent(
        &self,
        node: NodeId,
        req: CreateExtentReq,
    ) -> Result<ExtentId, EpochError> {
        let conn = self.conn(node).await?;
        let payload = Bytes::copy_from_slice(&req.encode());
        let inbound = conn.request(Pcode::CreateExtent, 0, payload).await?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::CreateExtentResp) => CreateExtentResp::decode(&inbound.payload)
                .map(|resp| resp.extent_id)
                .map_err(|_| EpochError::Internal),
            Some(Pcode::Error) => Err(decode_error(&inbound)),
            _ => Err(EpochError::Internal),
        }
    }

    async fn open_write(
        &self,
        node: NodeId,
        req: OpenReq,
    ) -> Result<Box<dyn WriteStream>, EpochError> {
        let conn = self.conn(node).await?;
        let stream_id = conn.alloc_stream();
        let open_rx = conn.register(stream_id).ok_or(EpochError::Internal)?;
        let header = conn.header(Pcode::Open, 0, stream_id, 0);
        if let Err(err) = conn
            .send(header, Bytes::copy_from_slice(&req.encode()))
            .await
        {
            conn.deregister(stream_id);
            return Err(err);
        }
        // OPEN is acknowledged before any data flows (04 §3.1): a rejection
        // (Sealed/ChunkFull/ShardNotFound) fails the stream here, not after
        // the whole body has been streamed.
        let inbound = open_rx.await.map_err(|_| EpochError::Internal)?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::OpenAck) => {}
            Some(Pcode::Error) => return Err(decode_error(&inbound)),
            _ => return Err(EpochError::Internal),
        }
        let resp = conn.register(stream_id).ok_or(EpochError::Internal)?;
        Ok(Box::new(RemoteWriteStream {
            conn,
            stream_id,
            seq: 1,
            resp,
            finished: false,
        }))
    }

    async fn read_shard(
        &self,
        node: NodeId,
        req: ReadShardReq,
    ) -> Result<Option<Bytes>, EpochError> {
        let conn = self.conn(node).await?;
        let payload = Bytes::copy_from_slice(&req.encode());
        let inbound = conn.request(Pcode::ReadShard, 0, payload).await?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::ReadShardResp) => {
                Ok((inbound.flags & FLAG_READ_HIT != 0).then_some(inbound.payload))
            }
            Some(Pcode::Error) => Err(decode_error(&inbound)),
            _ => Err(EpochError::Internal),
        }
    }

    async fn seal(&self, node: NodeId, req: SealReq) -> Result<(), EpochError> {
        let conn = self.conn(node).await?;
        let payload = Bytes::copy_from_slice(&req.encode());
        let inbound = conn.request(Pcode::Seal, 0, payload).await?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::SealResp) => Ok(()),
            Some(Pcode::Error) => Err(decode_error(&inbound)),
            _ => Err(EpochError::Internal),
        }
    }

    async fn delete_blob(&self, node: NodeId, req: DeleteBlobReq) -> Result<(), EpochError> {
        let conn = self.conn(node).await?;
        let payload = Bytes::copy_from_slice(&req.encode());
        let inbound = conn.request(Pcode::DeleteBlob, 0, payload).await?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::DeleteBlobResp) => Ok(()),
            Some(Pcode::Error) => Err(decode_error(&inbound)),
            _ => Err(EpochError::Internal),
        }
    }

    async fn list_blobs(
        &self,
        node: NodeId,
        req: ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        let conn = self.conn(node).await?;
        let payload = Bytes::copy_from_slice(&req.encode());
        let inbound = conn.request(Pcode::ListBlobs, 0, payload).await?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::ListBlobsResp) => ListBlobsResp::decode(&inbound.payload)
                .map(|resp| resp.blob_ids)
                .map_err(|_| EpochError::Internal),
            Some(Pcode::Error) => Err(decode_error(&inbound)),
            _ => Err(EpochError::Internal),
        }
    }
}

/// A remote blob write stream: data frames stream out over the connection; the
/// single response (at END) resolves the finish.
struct RemoteWriteStream {
    conn: Arc<Connection>,
    stream_id: u32,
    seq: u32,
    resp: tokio::sync::oneshot::Receiver<Inbound>,
    /// Cleared by `finish`; a stream dropped before finishing sends a
    /// best-effort `Abort` so the server frees its reassembly state
    /// (02 §5 abandoned-stream hygiene; sticky failure is a routine path,
    /// 04 §3.3).
    finished: bool,
}

#[async_trait]
impl WriteStream for RemoteWriteStream {
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), EpochError> {
        let header = self.conn.header(Pcode::Data, 0, self.stream_id, self.seq);
        self.conn.send(header, frame).await?;
        self.seq = self.seq.wrapping_add(1);
        Ok(())
    }

    async fn finish(mut self: Box<Self>, end: EndReq) -> Result<(), EpochError> {
        self.finished = true; // no Abort on drop past this point
        let header = self.conn.header(Pcode::End, 0, self.stream_id, self.seq);
        self.conn
            .send(header, Bytes::copy_from_slice(&end.encode()))
            .await?;
        let inbound = (&mut self.resp).await.map_err(|_| EpochError::Internal)?;
        match Pcode::from_u16(inbound.pcode) {
            Some(Pcode::CommitAck) => Ok(()),
            Some(Pcode::Error) => Err(decode_error(&inbound)),
            _ => Err(EpochError::Internal),
        }
    }
}

impl Drop for RemoteWriteStream {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Best-effort abandoned-stream cleanup (02 §5): fire an Abort frame on
        // a short-lived task; failure is fine — the server's idle reaper is
        // the backstop.
        let conn = Arc::clone(&self.conn);
        let header = conn.header(Pcode::Abort, 0, self.stream_id, self.seq);
        tokio::spawn(async move {
            let _ = conn.send(header, Bytes::new()).await;
        });
    }
}

/// Decodes an `Error` reply's payload into an [`EpochError`] (a malformed
/// payload degrades to [`EpochError::Internal`]).
fn decode_error(inbound: &Inbound) -> EpochError {
    ErrorResp::decode(&inbound.payload)
        .map(|resp| resp.error())
        .unwrap_or(EpochError::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A cached dead connection is evicted and replaced, never reused — a
    /// transient drop must not blacklist the node for this transport's life.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conn_evicts_a_closed_connection_and_reconnects() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let node = NodeId::new(1);
        let transport = RemoteTransport::new(HashMap::from([(node, addr)]));

        // Accept the next two connections as they arrive; drop the first (the
        // transient failure) and hold the second (so the reconnect stays live).
        let acceptor = tokio::spawn(async move {
            let (s1, _) = listener.accept().await.expect("accept 1");
            drop(s1);
            let (s2, _) = listener.accept().await.expect("accept 2");
            s2
        });

        let first = transport.conn(node).await.expect("conn 1");
        for _ in 0..100 {
            if first.is_closed() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(first.is_closed(), "connection observed the peer close");

        let second = transport.conn(node).await.expect("conn 2");
        let _s2 = acceptor.await.expect("acceptor");
        assert!(
            !Arc::ptr_eq(&first, &second),
            "dead connection was replaced"
        );
        assert!(!second.is_closed());
    }
}
