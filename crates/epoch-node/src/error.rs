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

//! Node assembly errors: configuration ([`ConfigError`]) and role startup
//! ([`NodeError`]).

/// A configuration parse or validation failure.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The TOML failed to parse or did not match the schema.
    #[error("parse config: {0}")]
    Parse(#[from] toml::de::Error),

    /// `cluster_id` is not a hexadecimal `u128`.
    #[error("invalid cluster_id {0:?}: expected a hex u128")]
    ClusterId(String),

    /// A node `addr` is not a valid socket address.
    #[error("invalid node address {0:?}")]
    Address(String),

    /// The config lists no nodes.
    #[error("config has no nodes")]
    NoNodes,

    /// Two nodes share an id.
    #[error("duplicate node id {0}")]
    DuplicateNode(u32),

    /// The code mode is out of range or badly sized.
    #[error("invalid code mode: {0}")]
    Code(&'static str),

    /// A chunk lists the wrong number of shards for the code mode.
    #[error("chunk {chunk} lists {got} shards, code mode needs {need}")]
    ChunkShardCount {
        /// The offending chunk id.
        chunk: u32,
        /// Shards the code mode requires.
        need: usize,
        /// Shards the chunk listed.
        got: usize,
    },

    /// A chunk references a node id that is not defined.
    #[error("chunk {chunk} references unknown node {node}")]
    UnknownChunkNode {
        /// The offending chunk id.
        chunk: u32,
        /// The undefined node id.
        node: u32,
    },
}

/// A role startup or runtime failure.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The configuration was invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// A local storage-engine error.
    #[error("storage engine: {0}")]
    Store(#[from] epoch_store::StoreError),

    /// A disk lifecycle error (format/load).
    #[error("disk: {0}")]
    Disk(#[from] epoch_store::DiskError),

    /// A data-plane error surfaced while provisioning shards.
    #[error("data plane: {0}")]
    Epoch(#[from] epoch_proto::EpochError),

    /// A filesystem or socket I/O error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// `--node <id>` selected a node not present in the config.
    #[error("this node id {0} is not defined in the config")]
    UnknownNode(u32),

    /// A PD control-plane call failed (registration, heartbeat).
    #[error("pd: {0}")]
    Pd(String),

    /// A gateway/S3 assembly error (code mode, cache priming, serving).
    #[error("gateway: {0}")]
    Gateway(String),

    /// The persisted superblock disk id and the PD-assigned id disagree (the
    /// disk was formatted under a different cluster registration).
    #[error("disk id mismatch: superblock has {persisted}, PD assigned {assigned}")]
    DiskIdMismatch {
        /// Disk id persisted in the superblock.
        persisted: u32,
        /// Disk id PD returned at registration.
        assigned: u32,
    },
}
