//! Snapshot builder handle for the PD state machine.
//!
//! openraft hands out a [`RaftSnapshotBuilder`] via
//! [`RaftStateMachine::get_snapshot_builder`](openraft::storage::RaftStateMachine::get_snapshot_builder)
//! to capture a snapshot off the critical path. The snapshot content and the
//! current-snapshot record are owned by [`super::state_machine`]; this builder
//! only allocates a unique snapshot index and delegates to
//! [`build_snapshot`](super::state_machine::build_snapshot).
//!
//! Design: docs/design/01-pd.md §2; docs/design/06-code-layout.md §8

use std::sync::atomic::Ordering;

use openraft::{RaftSnapshotBuilder, Snapshot};

use crate::raft::{PdTypeConfig, SmError, state_machine};
use crate::state::PdState;

/// Builds snapshots of the PD state machine (shares the [`PdState`] handle, so
/// it reads the same in-memory indexes and snapshot counter).
pub struct PdSnapshotBuilder {
    state: PdState,
}

impl PdSnapshotBuilder {
    pub(crate) fn new(state: PdState) -> Self {
        Self { state }
    }
}

impl RaftSnapshotBuilder<PdTypeConfig> for PdSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<PdTypeConfig>, SmError> {
        let index = self.state.snapshot_index.fetch_add(1, Ordering::Relaxed);
        state_machine::build_snapshot(&self.state, index)
    }
}
