//! Validates the PD raft stores against openraft's storage contract suite.
//!
//! The suite exercises append / truncate / purge / vote / apply / snapshot
//! build+install across freshly built stores, covering the RaftLogStorage and
//! RaftStateMachine implementations directly (design 01 §2).

// The builder and test return openraft's intentionally-large `StorageError`.
#![allow(clippy::result_large_err)]

use epoch_pd::raft::PdTypeConfig;
use epoch_pd::raft::log_store::PdLogStore;
use epoch_pd::raft::state_machine::PdStateMachine;
use openraft::testing::{StoreBuilder, Suite};
use openraft::{AnyError, StorageError, StorageIOError};
use tempfile::TempDir;

/// Builds a fresh pair of stores under a temp directory kept alive by the guard.
struct PdStoreBuilder;

impl StoreBuilder<PdTypeConfig, PdLogStore, PdStateMachine, TempDir> for PdStoreBuilder {
    async fn build(&self) -> Result<(TempDir, PdLogStore, PdStateMachine), StorageError<u64>> {
        let dir = tempfile::tempdir().map_err(|e| StorageIOError::write(AnyError::new(&e)))?;
        let log = PdLogStore::open(&dir.path().join("raft-log"))
            .map_err(|e| StorageIOError::read(AnyError::new(&e)))?;
        let sm = PdStateMachine::open(&dir.path().join("state-machine"))
            .map_err(|e| StorageIOError::read(AnyError::new(&e)))?;
        Ok((dir, log, sm))
    }
}

#[test]
fn openraft_storage_suite() -> Result<(), StorageError<u64>> {
    Suite::<PdTypeConfig, PdLogStore, PdStateMachine, PdStoreBuilder, TempDir>::test_all(
        PdStoreBuilder,
    )
}
