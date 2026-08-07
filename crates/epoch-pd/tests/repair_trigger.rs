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

//! RepairDisk trigger: a heartbeat that flags a disk Broken drives the PD
//! leader to transition the disk to Broken → Repairing and start exactly one
//! `RepairDisk` job for it (02 §1.8, 01 §6.3). Re-flagging the same disk never
//! spawns a second job (idempotent).

use std::time::Duration;

use epoch_pd::{DiskHeartbeat, DiskStatus, HeartbeatReport, JobKind, Journal, RoleSet};
use epoch_proto::{DiskId, NodeId};

const RAFT_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7401";

/// A heartbeat for `node_id`'s single disk, flagging it broken or not.
fn heartbeat(node_id: NodeId, disk_id: DiskId, broken: bool) -> HeartbeatReport {
    HeartbeatReport {
        node_id,
        disks: vec![DiskHeartbeat {
            disk_id,
            free: 900,
            used: 100,
            writable_extents: 4,
            broken,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_heartbeat_triggers_one_repair_disk_job() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, ADDR)
        .await
        .expect("open");
    journal
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");

    let node_id = journal
        .register_node(ADDR, "az1", "r1", RoleSet::DATA)
        .await
        .expect("register node");
    let disk_id = match journal
        .register_disk(node_id, "az1", "r1", "/data/0", 1 << 40)
        .await
        .expect("register disk")
    {
        epoch_pd::ApplyResult::DiskRegistered { disk_id } => disk_id,
        other => panic!("unexpected register_disk result: {other:?}"),
    };
    assert_eq!(
        journal.state().disks().get(disk_id).unwrap().status,
        DiskStatus::Normal
    );

    // A healthy heartbeat starts no repair.
    journal.record_heartbeat(&heartbeat(node_id, disk_id, false), 1_000);
    assert_eq!(journal.sweep_broken_disks().await.expect("sweep"), 0);
    assert!(journal.state().jobs().list().is_empty());

    // A broken heartbeat starts exactly one RepairDisk job and moves the disk
    // to Repairing.
    journal.record_heartbeat(&heartbeat(node_id, disk_id, true), 2_000);
    assert_eq!(journal.sweep_broken_disks().await.expect("sweep"), 1);

    let jobs = journal.state().jobs().list();
    assert_eq!(jobs.len(), 1, "exactly one repair job");
    assert_eq!(jobs[0].kind, JobKind::RepairDisk { disk_id });
    assert_eq!(
        journal.state().disks().get(disk_id).unwrap().status,
        DiskStatus::Repairing,
        "disk advanced Normal -> Broken -> Repairing"
    );

    // Re-flagging the same (now Repairing) disk spawns no second job.
    journal.record_heartbeat(&heartbeat(node_id, disk_id, true), 3_000);
    assert_eq!(
        journal.sweep_broken_disks().await.expect("sweep"),
        0,
        "a disk already past Normal is not re-triggered"
    );
    assert_eq!(journal.state().jobs().list().len(), 1);

    journal.shutdown().await.expect("shutdown");
}

/// INVARIANT(design 01 §6.1): a created Job must actually reach a coordinator.
///
/// The whole chain: broken heartbeat → `RepairDisk` job created (`Created`,
/// unassigned) → dispatch sweep assigns a coordinator → the job surfaces on that
/// node's pull. Gating dispatch on lease expiry alone left every `Created` job
/// unassigned forever, silently disabling RepairDisk / DropDisk / Balance /
/// InspectRound: the job existed, never ran, and pinned the disk in `Repairing`.
///
/// Also checks the failing node is excluded as coordinator (01 §6.3).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_job_is_assigned_and_reaches_its_coordinator() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7403")
        .await
        .expect("open");
    journal
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");

    // Two DATA nodes: the first owns the disk that breaks, so it must not be
    // chosen to coordinate its own repair.
    let mut nodes = Vec::new();
    for i in 0..2u32 {
        let addr = format!("10.0.9.{i}:9000");
        let node_id = journal
            .register_node(addr, "az1", "r1", RoleSet::DATA)
            .await
            .expect("register node");
        journal
            .update_node_status(node_id, epoch_pd::NodeStatus::Live)
            .await
            .expect("mark live");
        nodes.push(node_id);
    }
    let (failing_node, healthy_node) = (nodes[0], nodes[1]);
    let disk_id = match journal
        .register_disk(failing_node, "az1", "r1", "/data/0", 1 << 40)
        .await
        .expect("register disk")
    {
        epoch_pd::ApplyResult::DiskRegistered { disk_id } => disk_id,
        other => panic!("unexpected register_disk result: {other:?}"),
    };

    // Break it: a job is created, but nothing has assigned it yet.
    journal.record_heartbeat(&heartbeat(failing_node, disk_id, true), 1_000);
    assert_eq!(journal.sweep_broken_disks().await.expect("sweep"), 1);
    let job = journal.state().jobs().list().remove(0);
    assert_eq!(job.state, epoch_pd::JobState::Created);
    assert_eq!(job.coordinator, None);
    assert!(
        journal
            .state()
            .jobs()
            .jobs_for_coordinator(healthy_node)
            .is_empty(),
        "an unassigned job reaches nobody"
    );

    // The dispatch sweep assigns it.
    epoch_pd::sweep_job_assignments(&journal, 5_000)
        .await
        .expect("dispatch sweep");

    let job = journal.state().jobs().get(job.id).expect("job");
    assert_eq!(
        job.state,
        epoch_pd::JobState::Running,
        "a created job must be assigned, not left pending forever"
    );
    assert_eq!(
        job.coordinator,
        Some(healthy_node),
        "the failing node must not coordinate its own disk's repair (01 §6.3)"
    );
    assert!(
        job.lease_expiry_millis > 5_000,
        "assignment grants a fresh lease"
    );
    // And it now reaches that coordinator's pull.
    let pulled = journal.state().jobs().jobs_for_coordinator(healthy_node);
    assert_eq!(
        pulled.len(),
        1,
        "the job surfaces on its coordinator's pull"
    );
    assert_eq!(pulled[0].id, job.id);

    // A second sweep before expiry is a no-op (no lease churn).
    epoch_pd::sweep_job_assignments(&journal, 6_000)
        .await
        .expect("second sweep");
    let after = journal.state().jobs().get(job.id).expect("job");
    assert_eq!(
        after.lease_expiry_millis, job.lease_expiry_millis,
        "a live lease is not reassigned"
    );

    journal.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_repair_advances_disk_to_repaired() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7402")
        .await
        .expect("open");
    journal
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");

    let node_id = journal
        .register_node("127.0.0.1:7402", "az1", "r1", RoleSet::DATA)
        .await
        .expect("register node");
    let disk_id = match journal
        .register_disk(node_id, "az1", "r1", "/data/0", 1 << 40)
        .await
        .expect("register disk")
    {
        epoch_pd::ApplyResult::DiskRegistered { disk_id } => disk_id,
        other => panic!("unexpected register_disk result: {other:?}"),
    };

    // Break the disk → RepairDisk job + Repairing.
    journal.record_heartbeat(&heartbeat(node_id, disk_id, true), 1_000);
    assert_eq!(journal.sweep_broken_disks().await.expect("sweep"), 1);
    let job_id = journal.state().jobs().list()[0].id;

    // With the repair job still Running, the disk stays Repairing.
    assert_eq!(
        journal.sweep_completed_repairs().await.expect("sweep"),
        0,
        "a Running repair does not advance the disk"
    );
    assert_eq!(
        journal.state().disks().get(disk_id).unwrap().status,
        DiskStatus::Repairing
    );

    // Complete the job. The disk held no shards (none were ever created), so the
    // rebound-shard invariant (shard_slots_on_disk empty) holds immediately.
    journal.complete_job(job_id).await.expect("complete");
    assert_eq!(
        journal.sweep_completed_repairs().await.expect("sweep"),
        1,
        "a Done repair with all shards rebound advances the disk"
    );
    assert_eq!(
        journal.state().disks().get(disk_id).unwrap().status,
        DiskStatus::Repaired,
        "disk advanced Repairing -> Repaired"
    );

    // Idempotent: a second sweep does not re-advance (disk no longer Repairing).
    assert_eq!(journal.sweep_completed_repairs().await.expect("sweep"), 0);

    journal.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_a_disk_starts_dropdisk_then_retires_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let journal = Journal::open_single_node(dir.path(), RAFT_NODE_ID, "127.0.0.1:7403")
        .await
        .expect("open");
    journal
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .state(openraft::ServerState::Leader, "single node becomes leader")
        .await
        .expect("become leader");

    let node_id = journal
        .register_node("127.0.0.1:7403", "az1", "r1", RoleSet::DATA)
        .await
        .expect("register node");
    let disk_id = match journal
        .register_disk(node_id, "az1", "r1", "/data/0", 1 << 40)
        .await
        .expect("register disk")
    {
        epoch_pd::ApplyResult::DiskRegistered { disk_id } => disk_id,
        other => panic!("unexpected register_disk result: {other:?}"),
    };

    // Operator marks the disk draining: Normal -> Draining (excluded from
    // placement, still readable).
    assert!(journal.mark_disk_draining(disk_id).await.expect("mark"));
    assert_eq!(
        journal.state().disks().get(disk_id).unwrap().status,
        DiskStatus::Draining
    );
    // Marking again is a no-op (already past Normal).
    assert!(!journal.mark_disk_draining(disk_id).await.expect("re-mark"));

    // The sweep starts exactly one DropDisk job.
    assert_eq!(journal.sweep_draining_disks().await.expect("sweep"), 1);
    let jobs = journal.state().jobs().list();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].kind, JobKind::DropDisk { disk_id });
    // A second sweep starts no duplicate (job still active).
    assert_eq!(journal.sweep_draining_disks().await.expect("sweep"), 0);

    // Complete the job. The disk held no shards, so the copied-off invariant
    // (shard_slots_on_disk empty) holds and the disk retires to Dropped.
    journal.complete_job(jobs[0].id).await.expect("complete");
    assert_eq!(journal.sweep_draining_disks().await.expect("sweep"), 0);
    assert_eq!(
        journal.state().disks().get(disk_id).unwrap().status,
        DiskStatus::Dropped,
        "drained disk retired Draining -> Dropped"
    );

    journal.shutdown().await.expect("shutdown");
}
