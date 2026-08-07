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

//! M3 acceptance: real multi-process EC(2+1).
//!
//! Spawns three `epochio --role data` processes on temp disks, drives them with
//! the `epoch-gateway` library over the network [`RemoteTransport`] (the same
//! path a co-located gateway uses, minus the S3 layer), then kills one process
//! and reconstructs the object from the survivors.
//!
//! The single blueprint TOML is written once and read by both the spawned nodes
//! (each with `--node <id>`) and this driver, so placement and code mode cannot
//! drift between them. Design: docs/design/07-iteration-plan.md (M3);
//! docs/design/04-ec-io.md §3/§4.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use epoch_gateway::{ChunkPlacement, CodeMode, Writer, get_object};
use epoch_node::ClusterConfig;
use epoch_proto::{ChunkId, NodeId, WriterToken};
use epoch_rpc::{RemoteTransport, ShardTransport};
use tempfile::TempDir;
use tokio::time::timeout;

const CLUSTER: u128 = 0x00c0_ffee;
const OP_TIMEOUT: Duration = Duration::from_secs(60);

/// A spawned data-node process, killed and reaped on drop.
struct NodeProc {
    child: Child,
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl NodeProc {
    /// Kills and reaps the process now (so a later GET sees it as down).
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Grabs a currently-free ephemeral port by binding then dropping a listener.
fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// A deterministic pseudo-random object body of `len` bytes.
fn object_of(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u64).wrapping_mul(2_654_435_761) as u8)
        .collect()
}

/// Renders the cluster blueprint TOML.
fn config_text(
    extent_size: u64,
    nodes: &[(u32, SocketAddr, &Path)],
    stripe_size: usize,
    blob_size: usize,
) -> String {
    let mut s = String::new();
    s.push_str(&format!("cluster_id = \"{CLUSTER:x}\"\n"));
    s.push_str(&format!("extent_size = {extent_size}\n"));
    for (id, addr, disk) in nodes {
        s.push_str(&format!(
            "\n[[node]]\nid = {id}\naddr = \"{addr}\"\ndisk = {:?}\n",
            disk.display().to_string()
        ));
    }
    s.push_str(&format!(
        "\n[code]\ndata = 2\nparity = 1\nstripe_size = {stripe_size}\nblob_size = {blob_size}\n"
    ));
    s.push_str("\n[writer]\ntoken = 7\n");
    let shard_ids: Vec<u32> = nodes.iter().map(|(id, _, _)| *id).collect();
    s.push_str(&format!("\n[[chunk]]\nid = 1\nshards = {shard_ids:?}\n"));
    s
}

/// Spawns `epochio --role data --config <path> --node <id>`.
fn spawn_node(config_path: &Path, node_id: u32) -> NodeProc {
    let child = Command::new(env!("CARGO_BIN_EXE_epochio"))
        .arg("--role")
        .arg("data")
        .arg("--config")
        .arg(config_path)
        .arg("--node")
        .arg(node_id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn epochio");
    NodeProc { child }
}

/// Polls until `addr` accepts a connection, or panics after ~10s.
async fn wait_ready(addr: SocketAddr) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node at {addr} did not become ready in time");
}

/// End-to-end: write an object across a real 3-process EC(2+1) cluster, kill the
/// node at `kill_index`, then reconstruct and verify the object.
async fn write_kill_reconstruct(
    object_len: usize,
    stripe_size: usize,
    blob_size: usize,
    extent_size: u64,
    kill_index: usize,
) {
    let dirs: Vec<TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let addrs: Vec<SocketAddr> = (0..3)
        .map(|_| format!("127.0.0.1:{}", free_port()).parse().expect("addr"))
        .collect();
    let nodes: Vec<(u32, SocketAddr, &Path)> = (0..3)
        .map(|i| (i as u32, addrs[i], dirs[i].path()))
        .collect();

    let text = config_text(extent_size, &nodes, stripe_size, blob_size);
    let cfg_dir = tempfile::tempdir().expect("cfg tempdir");
    let cfg_path = cfg_dir.path().join("cluster.toml");
    std::fs::write(&cfg_path, &text).expect("write config");

    let mut procs: Vec<NodeProc> = (0..3).map(|i| spawn_node(&cfg_path, i as u32)).collect();
    for &addr in &addrs {
        wait_ready(addr).await;
    }

    // Driver reads the same blueprint the nodes did.
    let config = ClusterConfig::parse(&text).expect("parse config");
    let endpoints: HashMap<NodeId, SocketAddr> =
        config.node_addrs().expect("addrs").into_iter().collect();
    let transport: Arc<dyn ShardTransport> = Arc::new(RemoteTransport::new(endpoints));
    let code = CodeMode::new(
        config.code.data,
        config.code.parity,
        config.code.stripe_size,
        config.code.blob_size,
    )
    .expect("code mode");
    let chunk = ChunkPlacement {
        chunk_id: ChunkId::new(config.chunks[0].id),
        shards: config.placement(&config.chunks[0]),
    };
    let writer = Writer::new(WriterToken::new(config.writer.token));

    let object = object_of(object_len);
    let layout = timeout(
        OP_TIMEOUT,
        writer.put_object(transport.clone(), &code, &chunk, &object),
    )
    .await
    .expect("put did not time out")
    .expect("put succeeded");

    // Kill one node; its shard becomes unreadable and must be reconstructed.
    procs[kill_index].kill();

    let got = timeout(OP_TIMEOUT, get_object(transport.clone(), &layout))
        .await
        .expect("get did not time out")
        .expect("get succeeded");

    assert_eq!(
        got.bytes.len(),
        object.len(),
        "reconstructed length mismatch"
    );
    assert!(got.bytes == object, "reconstructed object bytes mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_2plus1_survives_killed_data_shard() {
    // 10 MiB → 3 blobs of 4 MiB (last 2 MiB), each 1 MiB-striped.
    write_kill_reconstruct(
        10 * 1024 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
        32 * 1024 * 1024,
        0,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ec_2plus1_survives_killed_parity_shard() {
    write_kill_reconstruct(
        10 * 1024 * 1024,
        1024 * 1024,
        4 * 1024 * 1024,
        32 * 1024 * 1024,
        2,
    )
    .await;
}

/// Heavy variant (run with `--ignored`): a 1 GiB object over 32 MiB blobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "1 GiB EC write/reconstruct; run manually"]
async fn ec_2plus1_one_gib_reconstruct() {
    write_kill_reconstruct(
        1024 * 1024 * 1024,
        1024 * 1024,
        32 * 1024 * 1024,
        768 * 1024 * 1024,
        0,
    )
    .await;
}
