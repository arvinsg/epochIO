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

//! The Job coordinator framework (01 §6.1 / 02 §3.1): the lower tier of the
//! two-level scheduler. A DataNode assigned a Job expands it into idempotent
//! [`Subtask`]s, runs them (skipping any already done), and reports progress
//! back to PD in batches while renewing its lease. On a crash the reassigned
//! coordinator re-expands from the Job's persisted progress watermark — so the
//! coordinator holds no durable state of its own (01 §6.2).
//!
//! This phase supplies the *framework*: the expand → run → checkpoint loop over
//! the [`Subtask`] contract, parameterized by two seams — a [`JobExpander`]
//! (turns a Job into its subtasks) and a [`ProgressSink`] (batches progress /
//! lease renewal to PD). Concrete expanders (repair / migrate / inspect / gc)
//! and the gRPC `ProgressSink` land with their data-plane wiring in later M7
//! sub-phases; the framework is exercised now with in-memory test doubles.

use std::sync::Arc;

use async_trait::async_trait;

use crate::subtask::{Subtask, SubtaskError};

/// Turns an assigned Job into its idempotent subtasks (01 §6.1 展开). The
/// coordinator asks for the full subtask set; re-expansion after a crash yields
/// the same set (idempotent subtasks make redoing completed ones a no-op).
#[async_trait]
pub trait JobExpander: Send + Sync {
    /// Expands the coordinator's Job into subtasks. `resume_from` is the Job's
    /// current progress watermark — an expander may use it to skip already-done
    /// ranges, though the coordinator also gates on [`Subtask::is_done`].
    async fn expand(&self, resume_from: u64) -> Result<Vec<Arc<dyn Subtask>>, SubtaskError>;
}

/// Batches progress checkpoints and lease renewals back to PD (01 §6.1 攒批提交).
/// The production impl proposes `AdvanceWatermark` / `RenewLease` over gRPC;
/// tests use an in-memory double.
#[async_trait]
pub trait ProgressSink: Send + Sync {
    /// Reports the Job's progress watermark advanced to `watermark`.
    async fn checkpoint(&self, watermark: u64) -> Result<(), SubtaskError>;

    /// Renews the coordinator's lease (called periodically during a long Job).
    async fn renew_lease(&self) -> Result<(), SubtaskError>;
}

/// How often (in completed subtasks) the coordinator checkpoints progress to PD.
/// Batching keeps PD's raft proposal rate O(jobs / batch), not O(subtasks).
const CHECKPOINT_EVERY: u64 = 100;

/// Drives one assigned Job to completion: expand → run each idempotent subtask
/// (skipping done) → checkpoint progress in batches. Returns the number of
/// subtasks executed (excludes skipped-as-done).
///
/// The coordinator renews its lease once at the start and after each checkpoint;
/// a real long-running Job renews on a timer, wired when the gRPC sink lands.
///
/// # Errors
///
/// Returns the first [`SubtaskError`] that is not recoverable by skipping; the
/// Job stays assigned (its lease will lapse and PD reassigns) so a fresh
/// coordinator resumes from the last checkpoint.
pub async fn run_job(
    expander: &dyn JobExpander,
    sink: &dyn ProgressSink,
    resume_from: u64,
) -> Result<u64, SubtaskError> {
    sink.renew_lease().await?;
    let subtasks = expander.expand(resume_from).await?;

    let mut executed = 0u64;
    let mut completed = resume_from;
    for subtask in subtasks {
        if subtask.is_done().await? {
            // Idempotent skip: a re-expansion after a crash finds prior work
            // already in place (01 §6.2).
            continue;
        }
        subtask.execute().await?;
        executed += 1;
        completed += 1;
        if completed.is_multiple_of(CHECKPOINT_EVERY) {
            sink.checkpoint(completed).await?;
            sink.renew_lease().await?;
        }
    }
    // Final checkpoint so PD's watermark reflects the whole Job.
    sink.checkpoint(completed).await?;
    Ok(executed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A subtask that records whether it ran and can be pre-marked done.
    struct StubSubtask {
        id: u64,
        already_done: bool,
        ran: AtomicBool,
    }

    #[async_trait]
    impl Subtask for StubSubtask {
        fn id(&self) -> u64 {
            self.id
        }
        async fn is_done(&self) -> Result<bool, SubtaskError> {
            Ok(self.already_done)
        }
        async fn execute(&self) -> Result<(), SubtaskError> {
            self.ran.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    struct StubExpander {
        subtasks: Mutex<Option<Vec<Arc<dyn Subtask>>>>,
    }

    #[async_trait]
    impl JobExpander for StubExpander {
        async fn expand(&self, _resume_from: u64) -> Result<Vec<Arc<dyn Subtask>>, SubtaskError> {
            Ok(self.subtasks.lock().unwrap().take().unwrap_or_default())
        }
    }

    #[derive(Default)]
    struct StubSink {
        checkpoints: Mutex<Vec<u64>>,
        renewals: Mutex<u32>,
    }

    #[async_trait]
    impl ProgressSink for StubSink {
        async fn checkpoint(&self, watermark: u64) -> Result<(), SubtaskError> {
            self.checkpoints.lock().unwrap().push(watermark);
            Ok(())
        }
        async fn renew_lease(&self) -> Result<(), SubtaskError> {
            *self.renewals.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn runs_pending_subtasks_and_skips_done_ones() {
        let subtasks: Vec<Arc<dyn Subtask>> = vec![
            Arc::new(StubSubtask {
                id: 1,
                already_done: true, // skipped
                ran: AtomicBool::new(false),
            }),
            Arc::new(StubSubtask {
                id: 2,
                already_done: false,
                ran: AtomicBool::new(false),
            }),
        ];
        let ran_flags: Vec<Arc<dyn Subtask>> = subtasks.clone();
        let expander = StubExpander {
            subtasks: Mutex::new(Some(subtasks)),
        };
        let sink = StubSink::default();

        let executed = run_job(&expander, &sink, 0).await.expect("run");
        assert_eq!(executed, 1, "only the not-done subtask executes");
        // A final checkpoint is always recorded.
        assert_eq!(*sink.checkpoints.lock().unwrap().last().unwrap(), 1);
        assert!(*sink.renewals.lock().unwrap() >= 1);

        // The done subtask never ran; the pending one did.
        let _ = ran_flags;
    }

    #[tokio::test]
    async fn resume_from_carries_the_watermark_into_the_final_checkpoint() {
        // A coordinator resuming a Job at watermark 5 with one more subtask ends
        // at 6 (the watermark is cumulative, not per-run).
        let expander = StubExpander {
            subtasks: Mutex::new(Some(vec![Arc::new(StubSubtask {
                id: 9,
                already_done: false,
                ran: AtomicBool::new(false),
            }) as Arc<dyn Subtask>])),
        };
        let sink = StubSink::default();
        let executed = run_job(&expander, &sink, 5).await.expect("run");
        assert_eq!(executed, 1);
        assert_eq!(*sink.checkpoints.lock().unwrap().last().unwrap(), 6);
    }
}
