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

//! GcRound round bookkeeping (01 §6.3: PD 只发 Round 号). GcRound is the only
//! self-driven Job kind — each DataNode scans its own disk (02 §3.2) — so PD's
//! role is purely to maintain a replicated, monotonic round number. The sweep
//! keeps exactly one open round, auto-completes it after a full interval, and
//! opens the next.

use std::time::Duration;

use epoch_pd::{JobKind, JobState, Journal};

const RAFT_NODE_ID: u64 = 1;
const ADDR: &str = "127.0.0.1:7409";

/// The current open (non-Done) GcRound job, if any.
fn open_round(journal: &Journal) -> Option<epoch_pd::Job> {
    journal
        .state()
        .jobs()
        .list()
        .into_iter()
        .find(|j| matches!(j.kind, JobKind::GcRound) && j.state != JobState::Done)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_round_sweep_opens_one_round_and_rolls_it_after_the_interval() {
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

    let interval = 3_600_000u64; // 1h in millis
    let t0 = 1_700_000_000_000u64;

    // First sweep opens round 1.
    assert!(
        journal.sweep_gc_round(interval, t0).await.expect("sweep"),
        "the first sweep opens a round"
    );
    let round1 = open_round(&journal).expect("an open round exists");
    assert_eq!(round1.state, JobState::Created);
    // The opening wall-clock rides the (opaque, per-Kind) watermark.
    assert_eq!(round1.progress_watermark, t0);

    // A sweep inside the same interval keeps round 1 open — no churn.
    assert!(
        !journal
            .sweep_gc_round(interval, t0 + 1_000)
            .await
            .expect("sweep"),
        "a round younger than the interval stays open"
    );
    assert_eq!(open_round(&journal).unwrap().id, round1.id);

    // After a full interval the sweep completes round 1 and opens round 2.
    assert!(
        journal
            .sweep_gc_round(interval, t0 + interval)
            .await
            .expect("sweep"),
        "a new round opens once the previous ran its interval"
    );
    let round2 = open_round(&journal).expect("a new open round");
    assert_ne!(round2.id, round1.id, "round numbers are monotonic");
    assert_eq!(
        journal.state().jobs().get(round1.id).map(|j| j.state),
        Some(JobState::Done),
        "the previous round is completed, not leaked open"
    );
}
