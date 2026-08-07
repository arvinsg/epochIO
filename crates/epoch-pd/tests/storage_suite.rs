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
