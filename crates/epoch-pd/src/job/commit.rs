//! Job-commit authorization (01 §6.4 invariant 2): a coordinator's shard-mapping
//! rebind (and any Job-scoped state change) is only accepted when the committer
//! holds a *valid, current* Job — it is the job's assigned coordinator, the job
//! is `Running`, and the caller's view epoch matches. This is the guard that
//! makes the dual-coordinator window (01 §6.2) safe: a superseded coordinator's
//! stale commit is rejected here, deterministically, before it can rebind a
//! shard.
//!
//! The actual shard-mapping rebind apply is a data-plane concern that lands with
//! the repair/migrate subtask executors (the consumers that produce these
//! commits); this module supplies the authorization predicate they will gate on,
//! and is exercised directly by unit tests now.
//!
//! Design: docs/design/01-pd.md §6.4

use epoch_proto::NodeId;

use crate::job::types::{Job, JobState};

/// Why a Job-scoped commit was refused (01 §6.4). Distinct reasons so the
/// coordinator can tell "I lost the lease" (stop) from "epoch moved" (refetch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitRejection {
    /// No Job with the claimed id exists (already completed and GC'd, or bogus).
    UnknownJob,
    /// The Job is not `Running` (already `Done`, or never assigned).
    NotRunning,
    /// The committer is not the Job's currently-assigned coordinator (a
    /// superseded coordinator after a lease reassignment, 01 §6.2).
    NotCoordinator,
    /// The committer's view epoch does not match the expected epoch (some other
    /// change landed; refetch and retry — the ConfVerChanged idea).
    EpochMismatch,
}

/// Authorizes a Job-scoped commit from `committer` carrying `commit_epoch`
/// against the Job's current record and `expected_epoch` (e.g. the chunk's
/// current mapping epoch). `Ok(())` means the commit may apply; `Err` names the
/// deterministic rejection. Pure — safe to call inside `apply`.
///
/// # Errors
///
/// Returns the [`CommitRejection`] describing why the commit is not authorized.
pub fn authorize_commit(
    job: Option<&Job>,
    committer: NodeId,
    commit_epoch: u64,
    expected_epoch: u64,
) -> Result<(), CommitRejection> {
    let job = job.ok_or(CommitRejection::UnknownJob)?;
    if job.state != JobState::Running {
        return Err(CommitRejection::NotRunning);
    }
    if job.coordinator != Some(committer) {
        return Err(CommitRejection::NotCoordinator);
    }
    if commit_epoch != expected_epoch {
        return Err(CommitRejection::EpochMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::types::JobKind;

    fn running_job(coordinator: u32) -> Job {
        Job {
            id: 1,
            kind: JobKind::RepairDisk {
                disk_id: epoch_proto::DiskId::new(1),
            },
            state: JobState::Running,
            coordinator: Some(NodeId::new(coordinator)),
            lease_expiry_millis: 1000,
            progress_watermark: 0,
        }
    }

    #[test]
    fn authorizes_the_current_coordinator_at_matching_epoch() {
        let job = running_job(3);
        assert_eq!(authorize_commit(Some(&job), NodeId::new(3), 5, 5), Ok(()));
    }

    #[test]
    fn rejects_unknown_job() {
        assert_eq!(
            authorize_commit(None, NodeId::new(3), 5, 5),
            Err(CommitRejection::UnknownJob)
        );
    }

    #[test]
    fn rejects_a_non_running_job() {
        let mut job = running_job(3);
        job.state = JobState::Done;
        assert_eq!(
            authorize_commit(Some(&job), NodeId::new(3), 5, 5),
            Err(CommitRejection::NotRunning)
        );
    }

    #[test]
    fn rejects_a_superseded_coordinator() {
        let job = running_job(3);
        // Node 9 is not the assigned coordinator (it was reassigned to 3).
        assert_eq!(
            authorize_commit(Some(&job), NodeId::new(9), 5, 5),
            Err(CommitRejection::NotCoordinator)
        );
    }

    #[test]
    fn rejects_a_stale_epoch() {
        let job = running_job(3);
        assert_eq!(
            authorize_commit(Some(&job), NodeId::new(3), 4, 5),
            Err(CommitRejection::EpochMismatch)
        );
    }
}
