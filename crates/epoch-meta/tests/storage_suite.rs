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

//! Validates the MetaNode raft stores against openraft's storage contract suite.
//!
//! The suite exercises append / truncate / purge / vote / apply / snapshot
//! build+install across freshly built stores, covering the RaftLogStorage and
//! RaftStateMachine implementations directly (design 03 §8). Group-id keyspace
//! disjointness of the shared instances is covered separately in
//! `raft/log_store.rs` unit tests; multi-group replication over the batched
//! transport in `tests/meta_raft_cluster.rs`.

// The builder and test return openraft's intentionally-large `StorageError`.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use epoch_meta::partition::{Namespace, PartitionRange};
use epoch_meta::raft::log_store::{MetaLogDb, MetaLogStore};
use epoch_meta::raft::state_machine::{MetaStateMachine, MetaTypeConfig};
use epoch_meta::store::MetaStore;
use epoch_meta::store::rocks::RocksEngine;
use openraft::testing::{StoreBuilder, Suite};
use openraft::{AnyError, StorageError, StorageIOError};
use tempfile::TempDir;

/// Builds a fresh pair of stores under a temp directory kept alive by the guard.
struct MetaStoreBuilder;

impl StoreBuilder<MetaTypeConfig, MetaLogStore, MetaStateMachine, TempDir> for MetaStoreBuilder {
    async fn build(&self) -> Result<(TempDir, MetaLogStore, MetaStateMachine), StorageError<u64>> {
        let dir = tempfile::tempdir().map_err(|e| StorageIOError::write(AnyError::new(&e)))?;
        let log_db = MetaLogDb::open(&dir.path().join("raft-log"))
            .map_err(|e| StorageIOError::read(AnyError::new(&e)))?;
        let engine: Arc<dyn MetaStore> = Arc::new(
            RocksEngine::open(&dir.path().join("state-machine"))
                .map_err(|e| StorageIOError::read(AnyError::new(&e)))?,
        );
        let sm = MetaStateMachine::new(
            engine,
            Arc::new(epoch_meta::ref_extractor::EpochRefExtractor),
            1,
            PartitionRange::full(Namespace::Flat),
            Arc::new(epoch_meta::guard::PartitionGuard::new()),
        )
        .map_err(|e| StorageIOError::read(AnyError::new(&e)))?;
        Ok((dir, log_db.group_store(1), sm))
    }
}

#[test]
fn openraft_storage_suite() -> Result<(), StorageError<u64>> {
    Suite::<MetaTypeConfig, MetaLogStore, MetaStateMachine, MetaStoreBuilder, TempDir>::test_all(
        MetaStoreBuilder,
    )
}
