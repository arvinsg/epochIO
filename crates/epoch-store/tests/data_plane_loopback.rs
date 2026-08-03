//! End-to-end data-plane test: two real [`StorageEngine`]s, each behind an
//! [`EngineHandler`] served over a TCP [`Server`], driven by the network
//! [`RemoteTransport`] client — the same path a gateway uses to write and read
//! a shard on a data node (02 §5). Exercises create → open → data → end → ack
//! and read-back over the wire, plus a read miss.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use epoch_proto::{BlobId, ChunkId, DiskId, EpochError, ExtentId, NodeId, ShardId, WriterToken};
use epoch_rpc::{
    CreateExtentReq, EndReq, OpenReq, ReadShardReq, RemoteTransport, SealReq, Server,
    ShardTransport,
};
use epoch_store::{Disk, EngineHandler, StorageEngine, Superblock};
use tempfile::TempDir;

const CLUSTER: u128 = 0x00C0_FFEE;
// Small so the per-extent fallocate reservation stays cheap on Linux CI.
const TEST_EXTENT_SIZE: u64 = 8 * 1024 * 1024;

fn fresh_engine() -> (TempDir, StorageEngine) {
    let dir = tempfile::tempdir().expect("tempdir");
    Disk::format(
        dir.path(),
        Superblock {
            disk_id: DiskId::new(1),
            cluster_id: CLUSTER,
            created_at: 0,
            flags: 0,
            extent_size: TEST_EXTENT_SIZE,
        },
    )
    .expect("format");
    let engine = StorageEngine::open(dir.path(), CLUSTER).expect("open");
    (dir, engine)
}

async fn start_server(engine: StorageEngine) -> SocketAddr {
    let handler = Arc::new(EngineHandler::new(engine));
    let server = Server::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), handler)
        .await
        .expect("bind");
    let addr = server.local_addr().expect("local_addr");
    // Serve until the test's runtime drops the task.
    tokio::spawn(server.serve(std::future::pending::<()>()));
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_node_write_read_round_trip_over_tcp() {
    let (_dir1, engine1) = fresh_engine();
    let (_dir2, engine2) = fresh_engine();
    let addr1 = start_server(engine1.clone()).await;
    let addr2 = start_server(engine2.clone()).await;

    let node1 = NodeId::new(1);
    let node2 = NodeId::new(2);
    let transport = RemoteTransport::new(HashMap::from([(node1, addr1), (node2, addr2)]));

    let blob = BlobId::new(WriterToken::new(1), 0);
    let missing = BlobId::new(WriterToken::new(1), 999);

    for (node, index, body) in [
        (node1, 0u8, Bytes::from_static(b"shard-body-on-node-1")),
        (node2, 1u8, Bytes::from_static(b"shard-body-on-node-2")),
    ] {
        let shard = ShardId::new(ChunkId::new(1), index, 0);

        // CREATE_EXTENT: ensure a writable extent backs the shard.
        transport
            .create_extent(
                node,
                CreateExtentReq {
                    shard_id: shard,
                    create_ts: 0,
                },
            )
            .await
            .expect("create_extent");

        // OPEN → DATA → END: stream the shard body as one framed chunk.
        let mut stream = transport
            .open_write(
                node,
                OpenReq {
                    blob_id: blob,
                    shard_id: shard,
                },
            )
            .await
            .expect("open_write");
        stream.send_frame(body.clone()).await.expect("send_frame");
        stream
            .finish(EndReq {
                frame_count: 1,
                blob_crc: crc32c::crc32c(&body),
            })
            .await
            .expect("finish");

        // READ_SHARD: the committed body round-trips over the wire.
        let got = transport
            .read_shard(
                node,
                ReadShardReq {
                    shard_id: shard,
                    blob_id: blob,
                },
            )
            .await
            .expect("read_shard");
        assert_eq!(got.as_deref(), Some(&body[..]));

        // A blob that was never written is a miss (empty-payload response).
        let none = transport
            .read_shard(
                node,
                ReadShardReq {
                    shard_id: shard,
                    blob_id: missing,
                },
            )
            .await
            .expect("read miss");
        assert_eq!(none, None);
    }

    engine1.shutdown();
    engine2.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_over_tcp_rejects_further_writes_but_reads_persist() {
    let (_dir, engine) = fresh_engine();
    let addr = start_server(engine.clone()).await;
    let node = NodeId::new(1);
    let transport = RemoteTransport::new(HashMap::from([(node, addr)]));

    let shard = ShardId::new(ChunkId::new(1), 0, 0);
    let blob = BlobId::new(WriterToken::new(1), 0);
    let body = Bytes::from_static(b"pre-seal-body");

    let extent: ExtentId = transport
        .create_extent(
            node,
            CreateExtentReq {
                shard_id: shard,
                create_ts: 0,
            },
        )
        .await
        .expect("create");
    let mut stream = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob,
                shard_id: shard,
            },
        )
        .await
        .expect("open_write");
    stream.send_frame(body.clone()).await.expect("send_frame");
    stream
        .finish(EndReq {
            frame_count: 1,
            blob_crc: crc32c::crc32c(&body),
        })
        .await
        .expect("finish");

    // Seal the extent over the wire.
    transport
        .seal(node, SealReq { extent_id: extent })
        .await
        .expect("seal");

    // A subsequent OPEN fails fast with Sealed (04 §3.1 OPEN gate).
    let reopen = transport
        .open_write(
            node,
            OpenReq {
                blob_id: BlobId::new(WriterToken::new(1), 1),
                shard_id: shard,
            },
        )
        .await;
    match reopen {
        Err(err) => assert_eq!(err, EpochError::Sealed),
        Ok(_) => panic!("open after seal must be rejected"),
    }

    // Reads of the committed blob still succeed.
    let got = transport
        .read_shard(
            node,
            ReadShardReq {
                shard_id: shard,
                blob_id: blob,
            },
        )
        .await
        .expect("read_shard");
    assert_eq!(got.as_deref(), Some(&body[..]));

    // Sealing again is idempotent.
    transport
        .seal(node, SealReq { extent_id: extent })
        .await
        .expect("re-seal");

    engine.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_integrity_mismatch_is_rejected_and_not_committed() {
    let (_dir, engine) = fresh_engine();
    let addr = start_server(engine.clone()).await;

    let node = NodeId::new(1);
    let transport = RemoteTransport::new(HashMap::from([(node, addr)]));
    let shard = ShardId::new(ChunkId::new(1), 0, 0);
    transport
        .create_extent(
            node,
            CreateExtentReq {
                shard_id: shard,
                create_ts: 0,
            },
        )
        .await
        .expect("create_extent");

    let blob = BlobId::new(WriterToken::new(1), 0);
    let body = Bytes::from_static(b"integrity-victim");
    let mut stream = transport
        .open_write(
            node,
            OpenReq {
                blob_id: blob,
                shard_id: shard,
            },
        )
        .await
        .expect("open_write");
    stream.send_frame(body.clone()).await.expect("send_frame");
    // Declare a CRC that does not match the body: the integrity gate must
    // reject the commit, and the blob must remain invisible (04 §3.1).
    let err = stream
        .finish(EndReq {
            frame_count: 1,
            blob_crc: crc32c::crc32c(b"a-different-body"),
        })
        .await
        .expect_err("mismatched end must be rejected");
    assert_eq!(err, EpochError::Internal);
    let got = transport
        .read_shard(
            node,
            ReadShardReq {
                shard_id: shard,
                blob_id: blob,
            },
        )
        .await
        .expect("read");
    assert_eq!(got, None, "rejected blob was not committed");

    engine.shutdown();
}
