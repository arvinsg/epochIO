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

//! The crate-level error type (AGENTS §9.1a standard per-crate files).

use crate::store::MetaStoreError;

/// Errors of the MetaNode service crate.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    /// A storage-engine failure.
    #[error(transparent)]
    Store(#[from] MetaStoreError),
    /// A RocksDB failure outside the store abstraction (raft-log instance).
    #[error(transparent)]
    Rocks(#[from] epoch_rocks::RocksError),
    /// The raft engine failed to start, initialize, or shut down.
    #[error("raft: {0}")]
    Raft(String),
    /// A partition raft group already exists on this node.
    #[error("group {0} already exists")]
    GroupExists(u64),
    /// A partition raft group does not exist on this node.
    #[error("unknown group {0}")]
    UnknownGroup(u64),
}
