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
