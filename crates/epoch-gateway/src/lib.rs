//! epoch-gateway (L3): the read/write orchestration role (enabled by default on
//! data nodes).
//!
//! M3 delivers the core object pipeline as a library, with no S3/HTTP layer: a
//! [`Writer`] cuts an object into blobs, EC-encodes each blob, and streams its
//! `data + parity` shards to their nodes with quorum commit ([`Writer::put_object`]);
//! [`get_object`] reads the shards back and reconstructs the object. Both drive
//! nodes through the [`epoch_rpc::ShardTransport`] seam, so a co-located store
//! (via `LocalTransport`) or a remote one (via `RemoteTransport`) work
//! identically.
//!
//! The [`ObjectLayout`] a PUT returns and a GET consumes stands in for the
//! MetaNode object record until M5. S3 protocol, SigV4 auth, inline objects,
//! ETag, listing and multipart arrive in M6.
//!
//! Design: docs/design/02-datanode.md §2; docs/design/04-ec-io.md §3/§4;
//! docs/design/06-code-layout.md §10.

pub mod admission;
pub mod auth;
pub mod code;
pub mod error;
pub mod etag;
pub mod gateway;
pub mod get;
pub mod http;
pub mod list;
pub mod object;
pub mod pipeline;
pub mod put;
pub mod range;
pub mod s3compat;
pub mod slices;

#[cfg(test)]
mod testutil;

pub use admission::{Admission, AdmissionPermit};
pub use auth::CredentialAuth;
pub use code::{BlobDesc, ChunkPlacement, CodeMode, ObjectLayout};
pub use error::{GatewayError, to_s3_error};
pub use gateway::Gateway;
pub use get::{HealReport, ReadObject, get_object};
pub use http::S3Backend;
pub use object::{FetchedObject, ObjectService, PutResult};
pub use put::Writer;
