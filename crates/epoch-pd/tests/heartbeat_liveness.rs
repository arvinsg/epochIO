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

//! End-to-end node liveness: the background sweep drives a node through
//! `Starting → Live → Offline → Lost` and back to `Live` on recovery, entirely
//! from heartbeat staleness against an injected clock (design 01 §2, Q18).
//!
//! The clock is manual so the test controls the perceived passage of time
//! without sleeping: only the sweep interval uses real time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use epoch_pd::{
    Clock, DiskHeartbeat, HeartbeatReport, Journal, LivenessConfig, LivenessHandle, NodeStatus,
    RoleSet,
};
use epoch_proto::NodeId;

const RAFT_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7101";

/// A [`Clock`] whose current time the test sets explicitly.
#[derive(Clone, Default)]
struct ManualClock {
    now: Arc<AtomicU64>,
}

impl ManualClock {
    fn set(&self, millis: u64) {
        self.now.store(millis, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
}

fn heartbeat(node_id: NodeId) -> HeartbeatReport {
    HeartbeatReport {
        node_id,
        disks: vec![DiskHeartbeat {
            disk_id: epoch_proto::DiskId::new(1),
            free: 900,
            used: 100,
            writable_extents: 4,
            broken: false,
        }],
    }
}

/// Polls the node's status until it reaches `want` or the deadline elapses.
async fn wait_for_status(journal: &Journal, node_id: NodeId, want: NodeStatus) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = journal
            .state()
            .nodes()
            .get(node_id)
            .expect("node present")
            .status;
        if status == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {want:?}, still {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn liveness_sweep_drives_status_from_heartbeat_staleness() {
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
    assert_eq!(node_id, NodeId::new(1));

    let clock = ManualClock::default();
    clock.set(1_000);
    journal.record_heartbeat(&heartbeat(node_id), 1_000);

    let config = LivenessConfig {
        sweep_interval: Duration::from_millis(20),
        offline_after_millis: 100,
        lost_after_millis: 500,
    };
    let handle: LivenessHandle = journal.start_liveness(config, Arc::new(clock.clone()));

    // Fresh heartbeat promotes Starting -> Live.
    wait_for_status(&journal, node_id, NodeStatus::Live).await;

    // Advance past offline_after with no new heartbeat: Live -> Offline.
    clock.set(1_200);
    wait_for_status(&journal, node_id, NodeStatus::Offline).await;

    // Advance past lost_after: Offline -> Lost.
    clock.set(1_600);
    wait_for_status(&journal, node_id, NodeStatus::Lost).await;

    // A fresh heartbeat recovers Lost -> Live.
    journal.record_heartbeat(&heartbeat(node_id), 1_600);
    wait_for_status(&journal, node_id, NodeStatus::Live).await;

    handle.stop();
    journal.shutdown().await.expect("shutdown");
}
