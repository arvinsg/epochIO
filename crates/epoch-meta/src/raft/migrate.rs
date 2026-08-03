//! Partition migration (03 §2 迁移才动数据; 01 §5): moving a partition replica
//! from one MetaNode to another by openraft membership change. Unlike a split
//! (pure routing, zero IO), a migration *does* move data — but the movement
//! rides openraft's existing learner-replication + snapshot-install path
//! (already wired in [`network`](crate::raft::network)), chunked by
//! `Config::snapshot_max_chunk_size`. This module only sequences the
//! membership steps a leader drives:
//!
//! ```text
//! AddLearner(target) → wait catch-up → Promote(target as voter) → RemovePeer(old)
//! ```
//!
//! The target node must already host the (un-initialized) group locally so its
//! transport can accept replication — the assembly ensures that via
//! [`GroupManager`](crate::raft::GroupManager) before the leader adds it.
//!
//! Design: docs/design/03-metanode.md §2; docs/design/01-pd.md §5

use std::collections::BTreeSet;

use openraft::BasicNode;

use crate::MetaError;
use crate::raft::{MetaRaft, NodeId};

/// Adds `target` as a learner of the group and blocks until it has caught up
/// (03 §2 迁移: AddLearner 按 range 导出 snapshot 流式传输 → 追平). openraft
/// replicates the log and installs a snapshot if the target is far behind; the
/// call returns once the learner is line-rate.
///
/// Leader-only; a follower call maps to a `ForwardToLeader` error.
///
/// # Errors
///
/// Returns [`MetaError::Raft`] if the membership change or catch-up fails.
pub async fn add_learner(raft: &MetaRaft, target: NodeId, addr: String) -> Result<(), MetaError> {
    raft.add_learner(target, BasicNode::new(addr), true)
        .await
        .map(|_| ())
        .map_err(|e| MetaError::Raft(format!("add_learner {target}: {e}")))
}

/// Promotes the caught-up voters + removes the departing peer in one joint
/// membership change (03 §2 迁移: Promote → RemovePeer). `voters` is the new
/// voter set (the target included, the old peer excluded); openraft enters and
/// leaves the joint config atomically, so there is no window without a
/// quorum-capable membership.
///
/// Leader-only.
///
/// # Errors
///
/// Returns [`MetaError::Raft`] if the membership change fails (e.g. a target
/// that has not caught up, or loss of quorum).
pub async fn set_voters(raft: &MetaRaft, voters: BTreeSet<NodeId>) -> Result<(), MetaError> {
    raft.change_membership(voters, false)
        .await
        .map(|_| ())
        .map_err(|e| MetaError::Raft(format!("change_membership: {e}")))
}

/// The full single-peer swap `from → to` for a group (03 §2 迁移全流程): add
/// `to` as a learner, wait for catch-up, then atomically promote it and drop
/// `from` from the voter set. `current_voters` is the group's present voter set
/// (used to compute the post-swap set).
///
/// Leader-only, idempotent-friendly: re-running after a mid-flight failure
/// re-adds the learner (a no-op if already present) and re-issues the joint
/// change (a no-op if already applied).
///
/// # Errors
///
/// Returns [`MetaError::Raft`] on any membership-change or catch-up failure —
/// the caller (PD-driven) retries, and openraft leaves the group in its last
/// committed membership (never a torn state).
pub async fn migrate_replica(
    raft: &MetaRaft,
    from: NodeId,
    to: NodeId,
    to_addr: String,
    current_voters: &BTreeSet<NodeId>,
) -> Result<BTreeSet<NodeId>, MetaError> {
    add_learner(raft, to, to_addr).await?;
    let mut voters = current_voters.clone();
    voters.insert(to);
    voters.remove(&from);
    if voters.is_empty() {
        return Err(MetaError::Raft(
            "refusing migration that would empty the voter set".to_string(),
        ));
    }
    set_voters(raft, voters.clone()).await?;
    Ok(voters)
}
