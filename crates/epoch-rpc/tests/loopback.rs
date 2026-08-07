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

//! Loopback round-trip tests for the data-plane transport: a real `Server` over
//! TCP driven by `RemoteTransport`, plus the in-process `LocalTransport`, both
//! against the same mock handler.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use epoch_proto::{BlobId, ChunkId, EpochError, ExtentId, NodeId, ShardId, WriterToken};
use epoch_rpc::{
    CreateExtentReq, DeleteBlobReq, EndReq, ErrorResp, FRAME_HEADER_LEN, FrameHeader,
    LocalTransport, OpenReq, Pcode, ReadShardReq, SealReq, Server, ShardHandler, ShardTransport,
};

/// An in-memory handler: `open_blob` resolves a deterministic extent per shard,
/// `commit_blob` stores the body, `read_shard` returns it. `commit_fault` /
/// `open_fault` / `seal_fault` force mapped errors to exercise error propagation.
#[derive(Default)]
struct MockHandler {
    store: Mutex<HashMap<(u64, u64), Bytes>>,
    sealed: Mutex<Vec<ExtentId>>,
    deleted: Mutex<Vec<(ExtentId, epoch_proto::BlobId)>>,
    commit_fault: Option<EpochError>,
    open_fault: Option<EpochError>,
    seal_fault: Option<EpochError>,
}

impl MockHandler {
    fn with_commit_fault(err: EpochError) -> Self {
        Self {
            commit_fault: Some(err),
            ..Default::default()
        }
    }

    fn with_open_fault(err: EpochError) -> Self {
        Self {
            open_fault: Some(err),
            ..Default::default()
        }
    }

    fn with_seal_fault(err: EpochError) -> Self {
        Self {
            seal_fault: Some(err),
            ..Default::default()
        }
    }
}

#[async_trait]
impl ShardHandler for MockHandler {
    async fn create_extent(&self, req: CreateExtentReq) -> Result<ExtentId, EpochError> {
        Ok(ExtentId::new(req.shard_id, 0))
    }

    async fn open_blob(&self, req: OpenReq) -> Result<ExtentId, EpochError> {
        if let Some(err) = self.open_fault.clone() {
            return Err(err);
        }
        Ok(ExtentId::new(req.shard_id, 0))
    }

    async fn commit_blob(
        &self,
        extent: ExtentId,
        blob: BlobId,
        _frames: u32,
        _end: EndReq,
        body: Bytes,
    ) -> Result<(), EpochError> {
        if let Some(err) = self.commit_fault.clone() {
            return Err(err);
        }
        self.store
            .lock()
            .unwrap()
            .insert((extent.shard_id().as_u64(), blob.as_u64()), body);
        Ok(())
    }

    async fn read_shard(&self, req: ReadShardReq) -> Result<Option<Bytes>, EpochError> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .get(&(req.shard_id.as_u64(), req.blob_id.as_u64()))
            .cloned())
    }

    async fn seal_extent(&self, req: SealReq) -> Result<(), EpochError> {
        if let Some(err) = self.seal_fault.clone() {
            return Err(err);
        }
        self.sealed.lock().unwrap().push(req.extent_id);
        Ok(())
    }

    async fn delete_blob(&self, req: DeleteBlobReq) -> Result<(), EpochError> {
        self.deleted
            .lock()
            .unwrap()
            .push((req.extent_id, req.blob_id));
        Ok(())
    }

    async fn list_blobs(
        &self,
        _req: epoch_rpc::ListBlobsReq,
    ) -> Result<Vec<epoch_proto::BlobId>, EpochError> {
        Ok(Vec::new())
    }
}

/// Spawns a server on an ephemeral port; returns its address and a shutdown
/// trigger (send `()` to stop accepting).
async fn spawn_server(handler: Arc<MockHandler>) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = Server::bind(addr, handler).await.unwrap();
    let local = server.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(server.serve(async move {
        let _ = rx.await;
    }));
    (local, tx)
}

fn shard() -> ShardId {
    ShardId::new(ChunkId::new(1), 0, 0)
}

fn blob(seq: u32) -> BlobId {
    BlobId::new(WriterToken::new(1), seq)
}

#[tokio::test]
async fn remote_write_then_read_round_trip() {
    let handler = Arc::new(MockHandler::default());
    let (addr, _stop) = spawn_server(handler).await;

    let node = NodeId::new(1);
    let transport = remote(node, addr);

    let ext = transport
        .create_extent(
            node,
            CreateExtentReq {
                shard_id: shard(),
                create_ts: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(ext, ExtentId::new(shard(), 0));

    let body = Bytes::from(vec![0x42u8; 4096]);
    write_blob(&transport, node, blob(0), &body).await.unwrap();

    let got = transport
        .read_shard(
            node,
            ReadShardReq {
                shard_id: shard(),
                blob_id: blob(0),
            },
        )
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some(&body[..]));

    let miss = transport
        .read_shard(
            node,
            ReadShardReq {
                shard_id: shard(),
                blob_id: blob(99),
            },
        )
        .await
        .unwrap();
    assert_eq!(miss, None);
}

#[tokio::test]
async fn remote_multi_frame_body_reassembles() {
    let handler = Arc::new(MockHandler::default());
    let (addr, _stop) = spawn_server(handler).await;
    let node = NodeId::new(1);
    let transport = remote(node, addr);

    // Send the body as three separate data frames; the server must reassemble.
    let parts = [
        Bytes::from(vec![1u8; 1000]),
        Bytes::from(vec![2u8; 2000]),
        Bytes::from(vec![3u8; 500]),
    ];
    let mut whole = Vec::new();
    for p in &parts {
        whole.extend_from_slice(p);
    }

    let mut stream = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob(7),
                shard_id: shard(),
            },
        )
        .await
        .unwrap();
    for p in &parts {
        stream.send_frame(p.clone()).await.unwrap();
    }
    stream
        .finish(EndReq {
            frame_count: 3,
            blob_crc: crc32c::crc32c(&whole),
        })
        .await
        .unwrap();

    let got = transport
        .read_shard(
            node,
            ReadShardReq {
                shard_id: shard(),
                blob_id: blob(7),
            },
        )
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some(&whole[..]));
}

#[tokio::test]
async fn remote_commit_error_propagates() {
    let handler = Arc::new(MockHandler::with_commit_fault(EpochError::ChunkFull));
    let (addr, _stop) = spawn_server(handler).await;
    let node = NodeId::new(1);
    let transport = remote(node, addr);

    let body = Bytes::from(vec![0u8; 128]);
    let err = write_blob(&transport, node, blob(0), &body)
        .await
        .expect_err("commit fault should surface");
    assert_eq!(err, EpochError::ChunkFull);
}

#[tokio::test]
async fn local_transport_round_trip() {
    let handler = Arc::new(MockHandler::default());
    let transport = LocalTransport::new(handler);
    let node = NodeId::new(1);

    transport
        .create_extent(
            node,
            CreateExtentReq {
                shard_id: shard(),
                create_ts: 0,
            },
        )
        .await
        .unwrap();
    let body = Bytes::from(vec![9u8; 2048]);
    write_blob(&transport, node, blob(0), &body).await.unwrap();

    let got = transport
        .read_shard(
            node,
            ReadShardReq {
                shard_id: shard(),
                blob_id: blob(0),
            },
        )
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some(&body[..]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_seal_round_trip() {
    let handler = Arc::new(MockHandler::default());
    let (addr, _stop) = spawn_server(handler.clone()).await;
    let node = NodeId::new(1);
    let transport = remote(node, addr);

    let extent = ExtentId::new(shard(), 7);
    transport
        .seal(node, SealReq { extent_id: extent })
        .await
        .unwrap();
    assert_eq!(handler.sealed.lock().unwrap().as_slice(), &[extent]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_seal_error_propagates() {
    let handler = Arc::new(MockHandler::with_seal_fault(EpochError::ShardNotFound));
    let (addr, _stop) = spawn_server(handler).await;
    let node = NodeId::new(1);
    let transport = remote(node, addr);

    let err = transport
        .seal(
            node,
            SealReq {
                extent_id: ExtentId::new(shard(), 0),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err, EpochError::ShardNotFound);
}

#[tokio::test]
async fn local_seal_reaches_handler() {
    let handler = Arc::new(MockHandler::default());
    let transport = LocalTransport::new(handler.clone());
    let node = NodeId::new(1);
    let extent = ExtentId::new(shard(), 3);
    transport
        .seal(node, SealReq { extent_id: extent })
        .await
        .unwrap();
    assert_eq!(handler.sealed.lock().unwrap().as_slice(), &[extent]);
}
async fn write_blob<T: ShardTransport + ?Sized>(
    transport: &T,
    node: NodeId,
    blob_id: BlobId,
    body: &Bytes,
) -> Result<(), EpochError> {
    let mut stream = transport
        .open_write(
            node,
            OpenReq {
                blob_id,
                shard_id: shard(),
            },
        )
        .await?;
    stream.send_frame(body.clone()).await?;
    stream
        .finish(EndReq {
            frame_count: 1,
            blob_crc: crc32c::crc32c(body),
        })
        .await
}

/// Builds a single-node remote transport.
fn remote(node: NodeId, addr: SocketAddr) -> epoch_rpc::RemoteTransport {
    epoch_rpc::RemoteTransport::new(HashMap::from([(node, addr)]))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_open_failure_surfaces_at_open() {
    let handler = Arc::new(MockHandler::with_open_fault(EpochError::Sealed));
    let (addr, _stop) = spawn_server(handler).await;
    let node = NodeId::new(1);
    let transport = remote(node, addr);

    // An OPEN rejection must surface at open_write — fail fast, not after the
    // whole shard body has been streamed (04 §3.1).
    let err = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob(0),
                shard_id: shard(),
            },
        )
        .await
        .err()
        .expect("sealed open must fail at open_write");
    assert_eq!(err, EpochError::Sealed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_multiplexed_streams_interleave_frames() {
    let handler = Arc::new(MockHandler::default());
    let (addr, _stop) = spawn_server(handler).await;
    let node = NodeId::new(1);
    let transport = remote(node, addr);

    // Two blobs over one connection with deliberately interleaved frames: the
    // streams must demux by stream_id with no cross-contamination.
    let body_a = Bytes::from(vec![0xAAu8; 1500]);
    let body_b = Bytes::from(vec![0xBBu8; 1500]);

    let mut a = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob(1),
                shard_id: shard(),
            },
        )
        .await
        .unwrap();
    let mut b = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob(2),
                shard_id: shard(),
            },
        )
        .await
        .unwrap();

    a.send_frame(body_a.slice(0..800)).await.unwrap();
    b.send_frame(body_b.slice(0..800)).await.unwrap();
    a.send_frame(body_a.slice(800..)).await.unwrap();
    b.send_frame(body_b.slice(800..)).await.unwrap();
    a.finish(EndReq {
        frame_count: 2,
        blob_crc: crc32c::crc32c(&body_a),
    })
    .await
    .unwrap();
    b.finish(EndReq {
        frame_count: 2,
        blob_crc: crc32c::crc32c(&body_b),
    })
    .await
    .unwrap();

    for (id, want) in [(blob(1), body_a), (blob(2), body_b)] {
        let got = transport
            .read_shard(
                node,
                ReadShardReq {
                    shard_id: shard(),
                    blob_id: id,
                },
            )
            .await
            .unwrap();
        assert_eq!(got.as_deref(), Some(&want[..]));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_unknown_pcode_gets_error_reply() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let handler = Arc::new(MockHandler::default());
    let (addr, _stop) = spawn_server(handler).await;

    // Send a frame with an unrecognized opcode: the server must reply Error
    // (never silently drop it and hang the peer).
    let mut sock = TcpStream::connect(addr).await.unwrap();
    let header = FrameHeader {
        pcode: 99,
        flags: 0,
        stream_id: 42,
        seq: 0,
        session: 0,
        payload_len: 0,
        trace_id: 0,
        deadline_ms: 0,
    };
    sock.write_all(&header.encode()).await.unwrap();

    let mut hbuf = [0u8; FRAME_HEADER_LEN];
    sock.read_exact(&mut hbuf).await.unwrap();
    let resp = FrameHeader::decode(&hbuf).expect("reply header");
    assert_eq!(resp.pcode, Pcode::Error.to_u16());
    let mut payload = vec![0u8; resp.payload_len as usize];
    sock.read_exact(&mut payload).await.unwrap();
    assert_eq!(
        ErrorResp::decode(&payload).expect("error payload").error(),
        EpochError::Internal
    );
}

/// C4 regression (02 §5): a write stream abandoned mid-body must not strand its
/// server-side accumulator — the client's Drop sends an Abort, after which the
/// same stream id is reusable and a fresh write commits cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_stream_is_aborted_and_connection_stays_usable() {
    let handler = Arc::new(MockHandler::default());
    let (addr, _stop) = spawn_server(Arc::clone(&handler)).await;
    let node = NodeId::new(1);
    let transport = epoch_rpc::RemoteTransport::new(HashMap::from([(node, addr)]));

    let shard = ShardId::new(ChunkId::new(9), 0, 0);
    let blob = BlobId::new(WriterToken::new(9), 1);

    // Open a stream, push a frame, then abandon it (drop without finish).
    {
        let mut stream = transport
            .open_write(
                node,
                OpenReq {
                    blob_id: blob,
                    shard_id: shard,
                },
            )
            .await
            .expect("open");
        stream
            .send_frame(Bytes::from_static(b"half-written"))
            .await
            .expect("frame");
        // Dropped here: Drop fires the best-effort Abort.
    }

    // The connection stays healthy: a fresh full write on the same node
    // commits and reads back — the abandoned accumulator did not wedge it.
    let body = Bytes::from_static(b"complete-body");
    let blob2 = BlobId::new(WriterToken::new(9), 2);
    let mut stream = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob2,
                shard_id: shard,
            },
        )
        .await
        .expect("open 2");
    stream.send_frame(body.clone()).await.expect("frame 2");
    stream
        .finish(EndReq {
            frame_count: 1,
            blob_crc: crc32c::crc32c(&body),
        })
        .await
        .expect("commit");

    // Only the completed blob landed; the aborted one never committed.
    let stored = handler.store.lock().unwrap();
    assert!(stored.contains_key(&(shard.as_u64(), blob2.as_u64())));
    assert!(!stored.contains_key(&(shard.as_u64(), blob.as_u64())));
}
