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

//! epoch-client (L2): clients and caches for PD / MetaNode / DataNode.
//!
//! Holds the writable-chunk set, chunk→shards map, metadata routing table,
//! bucket/account cache, and the `writer_token` lifecycle (the sole entry
//! point for constructing `blob_id`s).

pub mod bucket_cache;
pub mod chunk_map;
pub mod credential_cache;
pub mod error;
pub mod meta;
pub mod mux;
pub mod pd;
pub mod topology;
pub mod writable_set;
pub mod writer_token;

pub use bucket_cache::{BucketCache, BucketInfo};
pub use chunk_map::{ChunkMap, ChunkSlots, ResolvedShard};
pub use credential_cache::{CachedCredential, CredentialCache};
pub use error::ClientError;
pub use meta::{HierWrite, HttpMeta, MetaClient, PutObject};
pub use mux::MuxTransport;
pub use pd::PdClient;
pub use topology::{NodeTopo, Topology};
pub use writable_set::{WritableChunk, WritableSet};
pub use writer_token::{TokenSource, WriterSession};
