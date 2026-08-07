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

//! Crate error type for the Placement Driver.
//!
//! Storage-trait methods return openraft's [`StorageError`](openraft::StorageError)
//! directly, as required by the raft engine; [`PdError`] covers the surrounding
//! service layer — opening the raft stores, bootstrapping the raft node, and
//! proposing through the journal.
//!
//! Design: docs/design/01-pd.md §2

use openraft::StorageError;
use openraft::error::{ClientWriteError, RaftError};

use crate::raft::NodeId;

/// Errors surfaced by the PD service layer (bootstrap + journal propose).
#[derive(Debug, thiserror::Error)]
pub enum PdError {
    /// Opening or accessing one of the raft RocksDB instances failed.
    #[error("rocksdb: {0}")]
    Rocks(#[from] epoch_rocks::RocksError),

    /// The raft storage layer failed while opening or recovering the state
    /// machine (e.g. corrupt persisted PD state). The openraft `StorageError`
    /// is rendered to text at this service boundary.
    #[error("raft storage: {0}")]
    Storage(String),

    /// The raft engine reported a runtime failure (bootstrap, initialize, or a
    /// non-redirect client-write error). The openraft error is rendered to text
    /// at this boundary; structured cases callers act on have their own variant.
    #[error("raft runtime: {0}")]
    Raft(String),

    /// A mutation was proposed to a node that is not the current raft leader.
    /// Callers should redirect to the leader and retry.
    #[error("this PD node is not the raft leader")]
    NotLeader,
}

impl From<StorageError<NodeId>> for PdError {
    fn from(e: StorageError<NodeId>) -> Self {
        PdError::Storage(e.to_string())
    }
}

impl From<RaftError<NodeId>> for PdError {
    fn from(e: RaftError<NodeId>) -> Self {
        PdError::Raft(e.to_string())
    }
}

impl From<RaftError<NodeId, ClientWriteError<NodeId, openraft::BasicNode>>> for PdError {
    fn from(e: RaftError<NodeId, ClientWriteError<NodeId, openraft::BasicNode>>) -> Self {
        match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => PdError::NotLeader,
            other => PdError::Raft(other.to_string()),
        }
    }
}
