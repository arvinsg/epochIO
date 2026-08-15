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

//! Job manager (01 §6): owns the replicated `job` column family and the PD-side
//! Job state machine. Mirrors [`WriterManager`](crate::writer::WriterManager) —
//! raft applies replicated Job commands (create / assign / renew / advance /
//! complete) while an in-memory index serves reads for the lease ticker and the
//! coordinator-facing RPCs.
//!
//! Determinism (08): `apply` reads no clock. Lease expiry and coordinator choice
//! arrive in the command (computed before proposal, §6.1). Reassignment is a
//! fresh `AssignJob` proposed by the leader ticker once a lease lapses.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use epoch_proto::NodeId;
use rocksdb::{DB, WriteBatch};

use crate::cluster::RejectReason;
use crate::cluster::id_record;
use crate::job::types::{Job, JobKind, JobState};
use crate::raft::SmError;

pub mod commit;
pub mod driver;
pub mod types;

pub use commit::{CommitRejection, authorize_commit};
pub use driver::{
    JOB_LEASE_MILLIS, JobLeaseHandle, pick_coordinator, spawn_gc_trigger, spawn_inspect_trigger,
    spawn_job_lease, spawn_repair_trigger, sweep_job_assignments,
};

/// The `job` column family (01 §6).
pub(crate) const JOB_CF: &str = "job";

/// Replicated Job commands (the `apply` inputs). Each is deterministic — every
/// clock/choice-derived value is a field, chosen before proposal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JobCommand {
    /// Record a new Job (allocates the next id, state `Created`).
    Create {
        /// What the job does.
        kind: JobKind,
    },
    /// Assign (or reassign) a Job to a coordinator with a lease expiry. Moves
    /// `Created`/`Running` → `Running`; a reassignment simply overwrites the
    /// coordinator + lease (01 §6.2).
    Assign {
        /// The job to assign.
        job_id: u32,
        /// The chosen coordinator DataNode.
        coordinator: NodeId,
        /// Lease expiry in epoch millis (computed before proposal).
        lease_expiry_millis: u64,
    },
    /// Renew a running Job's lease (the coordinator's periodic keep-alive).
    RenewLease {
        /// The job whose lease renews.
        job_id: u32,
        /// The new lease expiry in epoch millis.
        lease_expiry_millis: u64,
    },
    /// Advance a running Job's progress watermark (batch checkpoint, 01 §6.1).
    /// Monotonic — a lower watermark than recorded is ignored (a stale/duplicate
    /// commit from a superseded coordinator, 01 §6.2).
    AdvanceWatermark {
        /// The job whose progress advances.
        job_id: u32,
        /// The new progress watermark.
        watermark: u64,
        /// Fresh lease expiry in epoch millis, computed by the leader *before*
        /// proposal (apply must stay deterministic — no clock reads in apply,
        /// AGENTS §8). A checkpoint renews the lease, which is what keeps a
        /// long-running job from being reassigned mid-flight.
        lease_expiry_millis: u64,
    },
    /// Mark a Job `Done` (terminal, idempotent).
    Complete {
        /// The job to complete.
        job_id: u32,
    },
}

/// The result of applying a [`JobCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    /// A Job was created; carries its assigned id.
    Created(u32),
    /// The command applied with no returned value.
    Applied,
    /// The command was rejected deterministically (unknown job, etc.).
    Rejected(RejectReason),
}

/// In-memory Job index: records + the next id to allocate.
#[derive(Debug)]
struct JobIndex {
    jobs: BTreeMap<u32, Job>,
    next_id: u32,
}

/// Owns the `job` column family and the Job index (cloneable; clones share the
/// database and index).
#[derive(Clone)]
pub struct JobManager {
    db: Arc<DB>,
    index: Arc<RwLock<JobIndex>>,
}

impl JobManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted jobs.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(JobIndex {
                jobs: BTreeMap::new(),
                next_id: id_record::FIRST_ID,
            })),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, JobIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, JobIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Rebuilds the in-memory index from the `job` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the family is missing or a record
    /// cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let (records, next_id) = id_record::load_records::<Job>(&self.db, JOB_CF)?;
        let mut index = self.write_index();
        index.jobs = records.into_iter().map(|j| (j.id, j)).collect();
        index.next_id = next_id;
        Ok(())
    }

    /// Applies one Job command, staging durable writes into `batch` (the caller
    /// flushes). Deterministic — no clock/random.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if the id counter overflows or a record
    /// cannot be serialized.
    pub(crate) fn apply(
        &self,
        batch: &mut WriteBatch,
        cmd: &JobCommand,
    ) -> Result<JobOutcome, SmError> {
        match cmd {
            JobCommand::Create { kind } => self.apply_create(batch, kind.clone()),
            JobCommand::Assign {
                job_id,
                coordinator,
                lease_expiry_millis,
            } => self.apply_mutate(batch, *job_id, |job| {
                job.state = JobState::Running;
                job.coordinator = Some(*coordinator);
                job.lease_expiry_millis = *lease_expiry_millis;
            }),
            JobCommand::RenewLease {
                job_id,
                lease_expiry_millis,
            } => self.apply_mutate(batch, *job_id, |job| {
                // Only a running job's lease renews; a Done job ignores it.
                if job.state == JobState::Running {
                    job.lease_expiry_millis = *lease_expiry_millis;
                }
            }),
            JobCommand::AdvanceWatermark {
                job_id,
                watermark,
                lease_expiry_millis,
            } => self.apply_mutate(batch, *job_id, |job| {
                // Monotonic: a superseded coordinator's stale commit can't
                // rewind progress (01 §6.2).
                if *watermark > job.progress_watermark {
                    job.progress_watermark = *watermark;
                }
                // INVARIANT(design 01 §6.4): a progress checkpoint renews the
                // lease. Without this the lease could only ever lapse, so any job
                // outliving one lease period (a full-disk repair takes hours) was
                // reassigned mid-flight, repeatedly — subtask idempotence made it
                // survivable but it never finished. Monotonic for the same reason
                // as the watermark: a stale coordinator must not extend the lease.
                if *lease_expiry_millis > job.lease_expiry_millis {
                    job.lease_expiry_millis = *lease_expiry_millis;
                }
            }),
            JobCommand::Complete { job_id } => self.apply_mutate(batch, *job_id, |job| {
                job.state = JobState::Done;
            }),
        }
    }

    /// Records a new Job with the next id, state `Created`.
    fn apply_create(&self, batch: &mut WriteBatch, kind: JobKind) -> Result<JobOutcome, SmError> {
        let cf = id_record::open_cf(&self.db, JOB_CF)?;
        let mut index = self.write_index();
        let id = index.next_id;
        let next_id = id_record::advance_counter(index.next_id)?;
        let job = Job {
            id,
            kind,
            state: JobState::Created,
            coordinator: None,
            lease_expiry_millis: 0,
            progress_watermark: 0,
        };
        id_record::stage_put_record(batch, cf, id, &job)?;
        id_record::stage_put_counter(batch, cf, next_id);
        index.jobs.insert(id, job);
        index.next_id = next_id;
        Ok(JobOutcome::Created(id))
    }

    /// Applies an in-place mutation to a job record, re-persisting it. Returns
    /// `Rejected(NotFound)` for an unknown job (a deterministic result, not an
    /// error).
    fn apply_mutate(
        &self,
        batch: &mut WriteBatch,
        job_id: u32,
        mutate: impl FnOnce(&mut Job),
    ) -> Result<JobOutcome, SmError> {
        let cf = id_record::open_cf(&self.db, JOB_CF)?;
        let mut index = self.write_index();
        let Some(job) = index.jobs.get_mut(&job_id) else {
            return Ok(JobOutcome::Rejected(RejectReason::NotFound));
        };
        mutate(job);
        let snapshot = job.clone();
        id_record::stage_put_record(batch, cf, job_id, &snapshot)?;
        Ok(JobOutcome::Applied)
    }

    /// The job with `id`, if present.
    #[must_use]
    pub fn get(&self, id: u32) -> Option<Job> {
        self.read_index().jobs.get(&id).cloned()
    }

    /// Every recorded job, in id order.
    #[must_use]
    pub fn list(&self) -> Vec<Job> {
        self.read_index().jobs.values().cloned().collect()
    }

    /// Running jobs whose lease has expired relative to `now_millis` — the
    /// reassignment candidates the leader ticker proposes fresh `Assign`s for
    /// (01 §6.2/§6.4).
    #[must_use]
    pub fn lease_expired(&self, now_millis: u64) -> Vec<Job> {
        self.read_index()
            .jobs
            .values()
            .filter(|j| j.lease_expired(now_millis))
            .cloned()
            .collect()
    }

    /// Jobs awaiting a coordinator: never-assigned (`Created`) plus those whose
    /// lease lapsed. This is what the leader's dispatch ticker consumes — see
    /// [`Job::needs_coordinator`] for why both cases must be here.
    ///
    /// In id order, so the oldest pending job is dispatched first.
    #[must_use]
    pub fn needing_coordinator(&self, now_millis: u64) -> Vec<Job> {
        let mut jobs: Vec<Job> = self
            .read_index()
            .jobs
            .values()
            .filter(|j| j.needs_coordinator(now_millis))
            .cloned()
            .collect();
        jobs.sort_by_key(|j| j.id);
        jobs
    }

    /// Running jobs currently assigned to `coordinator` — the pull-based
    /// delivery a DataNode polls (01 §6.1). In id order.
    #[must_use]
    pub fn jobs_for_coordinator(&self, coordinator: NodeId) -> Vec<Job> {
        self.read_index()
            .jobs
            .values()
            .filter(|j| j.state == JobState::Running && j.coordinator == Some(coordinator))
            .cloned()
            .collect()
    }

    /// The route-table contents for snapshots: jobs + the next id counter.
    pub(crate) fn snapshot_view(&self) -> (Vec<Job>, u32) {
        let index = self.read_index();
        (index.jobs.values().cloned().collect(), index.next_id)
    }

    /// Replaces the full job table from an installed snapshot: stages the record
    /// rewrite into `batch` and rebuilds the index. The caller flushes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if a record cannot be serialized.
    pub(crate) fn stage_and_apply_snapshot(
        &self,
        batch: &mut WriteBatch,
        jobs: Vec<Job>,
        next_id: u32,
    ) -> Result<(), SmError> {
        // A pre-Job snapshot carries no counter (serde default 0); start id
        // allocation at FIRST_ID so the first job is never id 0.
        let next_id = next_id.max(id_record::FIRST_ID);
        let cf = id_record::open_cf(&self.db, JOB_CF)?;
        id_record::stage_clear(batch, &self.db, cf)?;
        for job in &jobs {
            id_record::stage_put_record(batch, cf, job.id, job)?;
        }
        id_record::stage_put_counter(batch, cf, next_id);

        let mut index = self.write_index();
        index.jobs = jobs.into_iter().map(|j| (j.id, j)).collect();
        index.next_id = next_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::DiskId;

    fn manager() -> (JobManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(
            epoch_rocks::open_cfs(dir.path(), &epoch_rocks::state_machine_options(), &[JOB_CF])
                .expect("open db"),
        );
        (JobManager::new(db), dir)
    }

    /// Applies `cmd` in its own write batch and flushes (test convenience).
    fn apply(mgr: &JobManager, cmd: &JobCommand) -> JobOutcome {
        let mut batch = WriteBatch::default();
        let outcome = mgr.apply(&mut batch, cmd).expect("apply");
        mgr.db.write(batch).expect("flush");
        outcome
    }

    #[test]
    fn create_assign_advance_complete_lifecycle() {
        let (mgr, _dir) = manager();
        let JobOutcome::Created(id) = apply(
            &mgr,
            &JobCommand::Create {
                kind: JobKind::RepairDisk {
                    disk_id: DiskId::new(7),
                },
            },
        ) else {
            panic!("expected Created");
        };
        assert_eq!(id, 1, "first job id");
        let job = mgr.get(id).expect("job");
        assert_eq!(job.state, JobState::Created);
        assert!(job.coordinator.is_none());

        assert_eq!(
            apply(
                &mgr,
                &JobCommand::Assign {
                    job_id: id,
                    coordinator: NodeId::new(3),
                    lease_expiry_millis: 1000,
                },
            ),
            JobOutcome::Applied
        );
        let job = mgr.get(id).expect("job");
        assert_eq!(job.state, JobState::Running);
        assert_eq!(job.coordinator, Some(NodeId::new(3)));
        assert_eq!(job.lease_expiry_millis, 1000);

        apply(
            &mgr,
            &JobCommand::AdvanceWatermark {
                job_id: id,
                watermark: 42,
                lease_expiry_millis: 2000,
            },
        );
        assert_eq!(mgr.get(id).unwrap().progress_watermark, 42);
        // A lower watermark is ignored (stale coordinator, 01 §6.2).
        apply(
            &mgr,
            &JobCommand::AdvanceWatermark {
                job_id: id,
                watermark: 10,
                lease_expiry_millis: 1500,
            },
        );
        assert_eq!(mgr.get(id).unwrap().progress_watermark, 42);

        apply(&mgr, &JobCommand::Complete { job_id: id });
        assert_eq!(mgr.get(id).unwrap().state, JobState::Done);
    }

    #[test]
    fn mutating_an_unknown_job_is_rejected() {
        let (mgr, _dir) = manager();
        assert_eq!(
            apply(
                &mgr,
                &JobCommand::Assign {
                    job_id: 99,
                    coordinator: NodeId::new(1),
                    lease_expiry_millis: 1,
                },
            ),
            JobOutcome::Rejected(RejectReason::NotFound)
        );
    }

    #[test]
    fn lease_expired_lists_only_running_lapsed_jobs() {
        let (mgr, _dir) = manager();
        // Created (unassigned) job never counts as expired.
        let JobOutcome::Created(a) = apply(
            &mgr,
            &JobCommand::Create {
                kind: JobKind::GcRound,
            },
        ) else {
            panic!();
        };
        // Running with a lease in the past.
        let JobOutcome::Created(b) = apply(
            &mgr,
            &JobCommand::Create {
                kind: JobKind::InspectRound,
            },
        ) else {
            panic!();
        };
        apply(
            &mgr,
            &JobCommand::Assign {
                job_id: b,
                coordinator: NodeId::new(2),
                lease_expiry_millis: 500,
            },
        );
        let expired = mgr.lease_expired(1000);
        assert_eq!(expired.len(), 1, "only the running lapsed job");
        assert_eq!(expired[0].id, b);
        // Before expiry, nothing is due.
        assert!(mgr.lease_expired(400).is_empty());
        // The Created job is never expired.
        assert!(mgr.get(a).unwrap().lease_expiry_millis == 0);

        // ...but it DOES need a coordinator: `needing_coordinator` is the dispatch
        // ticker's input and must cover never-assigned jobs too, or a created job
        // is never assigned and its JobKind silently never runs (01 §6.1).
        let pending = mgr.needing_coordinator(1000);
        let ids: Vec<u32> = pending.iter().map(|j| j.id).collect();
        assert_eq!(
            ids,
            vec![a, b],
            "both the never-assigned and the lapsed job await a coordinator"
        );
        // A freshly-leased job is not pending; the Created one still is.
        let pending = mgr.needing_coordinator(400);
        assert_eq!(
            pending.iter().map(|j| j.id).collect::<Vec<_>>(),
            vec![a],
            "a live lease is not pending, an unassigned job always is"
        );
    }

    #[test]
    fn a_completed_job_never_needs_a_coordinator() {
        let (mgr, _dir) = manager();
        let JobOutcome::Created(id) = apply(
            &mgr,
            &JobCommand::Create {
                kind: JobKind::GcRound,
            },
        ) else {
            panic!("create");
        };
        apply(
            &mgr,
            &JobCommand::Assign {
                job_id: id,
                coordinator: NodeId::new(1),
                lease_expiry_millis: 100,
            },
        );
        apply(&mgr, &JobCommand::Complete { job_id: id });
        assert!(
            mgr.needing_coordinator(u64::MAX).is_empty(),
            "a Done job is terminal — never reassigned"
        );
    }

    #[test]
    fn snapshot_round_trips_jobs_and_counter() {
        let (mgr, _dir) = manager();
        apply(
            &mgr,
            &JobCommand::Create {
                kind: JobKind::DropDisk {
                    disk_id: DiskId::new(2),
                },
            },
        );
        apply(
            &mgr,
            &JobCommand::Create {
                kind: JobKind::Balance {
                    disk_id: DiskId::new(4),
                },
            },
        );
        let (jobs, next_id) = mgr.snapshot_view();
        assert_eq!(jobs.len(), 2);
        assert_eq!(next_id, 3);

        // Install into a fresh manager over the same db and verify equality.
        let db = mgr.db.clone();
        let restored = JobManager::new(db);
        let mut batch = WriteBatch::default();
        restored
            .stage_and_apply_snapshot(&mut batch, jobs, next_id)
            .expect("install");
        restored.db.write(batch).expect("flush");
        assert_eq!(restored.list().len(), 2);
        // A fresh create continues the counter (never reuses an id).
        let JobOutcome::Created(id) = apply(
            &restored,
            &JobCommand::Create {
                kind: JobKind::GcRound,
            },
        ) else {
            panic!();
        };
        assert_eq!(id, 3);
    }
}
