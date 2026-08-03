//! epoch-client (L2): clients and caches for PD / MetaNode / DataNode.
//!
//! Holds the writable-chunk set, chunk→shards map, metadata routing table,
//! bucket/account cache, and the `writer_token` lifecycle (the sole entry
//! point for constructing `blob_id`s).
//!
//! Design: docs/design/06-code-layout.md §7; docs/design/01-pd.md §4.3

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
