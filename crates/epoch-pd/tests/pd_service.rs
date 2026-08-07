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

//! PD control-plane gRPC service over a single-node raft group: heartbeat intake
//! plus the writable-set / chunk / disk-shard reads served with leader ReadIndex
//! (design 01 §7). The service methods are exercised directly (no transport
//! socket); full multi-replica redirect lands with the raft gRPC network.

use std::sync::Arc;
use std::time::Duration;

use epoch_pd::{
    ApplyResult, DiskStatus, Journal, PdControlService, RoleSet, SlotPlan, SystemClock,
};
use epoch_proto::grpc::pd;
use epoch_proto::grpc::pd::pd_control_server::PdControl;
use epoch_proto::{ChunkId, CodeMode, CodeModeId, DiskId};
use openraft::ServerState;
use tonic::{Code, Request};

const RAFT_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7301";
const TIMEOUT: Duration = Duration::from_secs(10);
const CREATE_TS: i64 = 1_700_000_000_000;

fn code_mode() -> CodeMode {
    CodeMode {
        id: CodeModeId::new(1),
        data: 2,
        parity: 1,
        stripe_size: 1 << 20,
        blob_size: 32 << 20,
    }
}

async fn become_leader(journal: &Journal) {
    journal
        .raft()
        .wait(Some(TIMEOUT))
        .state(ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");
}

/// Registers three single-disk nodes and returns their disk ids in order.
async fn register_topology(journal: &Journal) -> Vec<DiskId> {
    let mut disks = Vec::new();
    for i in 0..3 {
        let node_id = journal
            .register_node(
                format!("10.0.0.{i}:9000"),
                "az1",
                format!("r{i}"),
                RoleSet::DATA,
            )
            .await
            .expect("register node");
        match journal
            .register_disk(
                node_id,
                "az1",
                format!("r{i}"),
                format!("/data/{i}"),
                1 << 40,
            )
            .await
            .expect("register disk")
        {
            ApplyResult::DiskRegistered { disk_id } => disks.push(disk_id),
            other => panic!("unexpected register_disk result: {other:?}"),
        }
    }
    disks
}

/// Stages and commits one `Writable` chunk over the given disks, one shard per
/// disk in index order; returns the committed chunk id.
async fn commit_chunk(journal: &Journal, disks: &[DiskId]) -> ChunkId {
    let slots: Vec<SlotPlan> = disks
        .iter()
        .enumerate()
        .map(|(index, &disk_id)| SlotPlan {
            index: u8::try_from(index).expect("small index"),
            disk_id,
        })
        .collect();
    let chunk_id = match journal
        .create_chunk_staging(code_mode(), slots, CREATE_TS, 0)
        .await
        .expect("stage")
    {
        ApplyResult::ChunkStaged { chunk_id } => chunk_id,
        other => panic!("unexpected stage result: {other:?}"),
    };
    assert_eq!(
        journal.commit_chunk(chunk_id).await.expect("commit"),
        ApplyResult::Applied
    );
    chunk_id
}

fn service(journal: Arc<Journal>) -> PdControlService {
    PdControlService::new(journal, Arc::new(SystemClock))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_reads_and_heartbeat() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Arc::new(
        Journal::open_single_node(dir.path(), RAFT_NODE_ID, ADDR)
            .await
            .expect("open"),
    );
    become_leader(&journal).await;

    let disks = register_topology(&journal).await;
    let chunk_id = commit_chunk(&journal, &disks).await;
    let svc = service(journal.clone());

    // Heartbeat is accepted on the leader.
    svc.heartbeat(Request::new(pd::HeartbeatRequest {
        node_id: 1,
        disks: vec![pd::DiskStats {
            disk_id: disks[0].get(),
            free: 1 << 40,
            used: 100,
            writable_extents: 4,
            broken: false,
        }],
    }))
    .await
    .expect("heartbeat accepted");

    // GetWritableChunks returns the committed chunk with faithful parameters.
    let writable = svc
        .get_writable_chunks(Request::new(pd::GetWritableChunksRequest {
            code_mode_id: 1,
        }))
        .await
        .expect("get writable")
        .into_inner()
        .chunks;
    assert_eq!(writable.len(), 1);
    let view = &writable[0];
    assert_eq!(view.chunk_id, chunk_id.get());
    assert_eq!(view.status, pd::ChunkStatus::Writable as i32);
    assert_eq!(view.shards.len(), 3);
    let mode = view.code_mode.as_ref().expect("code_mode present");
    assert_eq!((mode.id, mode.data, mode.parity), (1, 2, 1));

    // A different code mode has no writable chunks.
    let empty = svc
        .get_writable_chunks(Request::new(pd::GetWritableChunksRequest {
            code_mode_id: 2,
        }))
        .await
        .expect("get writable (other mode)")
        .into_inner()
        .chunks;
    assert!(empty.is_empty());

    // GetChunk resolves the id and reports NOT_FOUND for an unknown one.
    let got = svc
        .get_chunk(Request::new(pd::GetChunkRequest {
            chunk_id: chunk_id.get(),
        }))
        .await
        .expect("get chunk")
        .into_inner()
        .chunk
        .expect("chunk present");
    assert_eq!(got.chunk_id, chunk_id.get());
    let missing = svc
        .get_chunk(Request::new(pd::GetChunkRequest { chunk_id: 9999 }))
        .await
        .expect_err("unknown chunk is not found");
    assert_eq!(missing.code(), Code::NotFound);

    // ListDiskShards returns the slot bound to a disk, carrying its extent id.
    let shards = svc
        .list_disk_shards(Request::new(pd::ListDiskShardsRequest {
            disk_id: disks[0].get(),
        }))
        .await
        .expect("list disk shards")
        .into_inner()
        .shards;
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].disk_id, disks[0].get());
    assert_eq!(shards[0].extent_id.len(), 16);

    // A code_mode_id that overflows the 16-bit registry key is rejected.
    let bad = svc
        .get_writable_chunks(Request::new(pd::GetWritableChunksRequest {
            code_mode_id: u32::from(u16::MAX) + 1,
        }))
        .await
        .expect_err("oversized code_mode_id rejected");
    assert_eq!(bad.code(), Code::InvalidArgument);

    journal.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_rejected_when_leadership_cannot_be_confirmed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Arc::new(
        Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7302")
            .await
            .expect("open"),
    );
    become_leader(&journal).await;
    let svc = service(journal.clone());

    // Stopping the raft core removes the leader ReadIndex guarantee; the read
    // surfaces as a redirect-worthy FAILED_PRECONDITION rather than stale data.
    journal.shutdown().await.expect("shutdown");
    let err = svc
        .get_chunk(Request::new(pd::GetChunkRequest { chunk_id: 1 }))
        .await
        .expect_err("read rejected without confirmed leadership");
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writable_set_evicts_chunk_on_disk_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Arc::new(
        Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7303")
            .await
            .expect("open"),
    );
    become_leader(&journal).await;
    let disks = register_topology(&journal).await;
    commit_chunk(&journal, &disks).await;
    let svc = service(journal.clone());

    // All shard disks are Normal (no heartbeat yet -> kept optimistically), so
    // the chunk is published.
    let before = svc
        .get_writable_chunks(Request::new(pd::GetWritableChunksRequest {
            code_mode_id: 1,
        }))
        .await
        .expect("get writable")
        .into_inner()
        .chunks;
    assert_eq!(before.len(), 1);

    // One shard's disk fails -> the chunk drops out of the writable set (01 §4.2).
    assert_eq!(
        journal
            .update_disk_status(disks[1], DiskStatus::Broken)
            .await
            .expect("mark broken"),
        ApplyResult::Applied
    );
    let after = svc
        .get_writable_chunks(Request::new(pd::GetWritableChunksRequest {
            code_mode_id: 1,
        }))
        .await
        .expect("get writable")
        .into_inner()
        .chunks;
    assert!(
        after.is_empty(),
        "a chunk with a broken shard disk must be evicted"
    );

    journal.shutdown().await.expect("shutdown");
}
