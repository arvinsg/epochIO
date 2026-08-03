//! Object-browse seam (08 §4/§7): the console lists objects and reads their
//! head for the browser. The listing/detail data lives in the MetaNodes, not in
//! PD — but `epoch-pd` (L3) must not depend on `epoch-client` (L2) directly, and
//! more importantly PD must not become a MetaNode client on its own.
//!
//! So the console defines this trait and `epoch-node` (L4, which already owns
//! `MetaClient`) injects the implementation — the same trait-injection pattern
//! the partition scheduler uses for [`MetaAdmin`](crate::meta_sched::MetaAdmin).
//! When no browser is injected (e.g. a PD started without the object-browse
//! wiring), the object endpoints answer `501 Not Implemented`.
//!
//! Download is deliberately **not** here: 08 §4 routes object bytes via a 302 to
//! a gateway with a short-lived caller credential, and the console session holds
//! no secret key — that credential mechanism is undecided (99: presigned URL
//! pending M7 verification), so the download button stays disabled until it
//! lands. This trait covers only the metadata reads (list + head).
//!
//! Design: docs/design/08-web-console.md §4, §7

use async_trait::async_trait;

/// One object (or common prefix) in a listing. A common prefix (directory-like
/// rollup for `Hier` buckets) carries only `key` with `is_prefix = true`.
#[derive(Debug, Clone)]
pub struct BrowseEntry {
    /// The object key, or the common-prefix string.
    pub key: Vec<u8>,
    /// Whether this row is a common prefix (a "folder") rather than an object.
    pub is_prefix: bool,
    /// Object size in bytes (0 for a prefix).
    pub size: u64,
    /// Whether the object is stored inline (small-object fast path, 03 §4.3).
    pub inline: bool,
    /// Last-modified time in epoch millis (0 for a prefix).
    pub mtime_millis: i64,
}

/// One object's head (detail view): the same fields a listing row carries plus
/// the ETag. `None` from [`ObjectBrowser::head`] means the object is absent.
#[derive(Debug, Clone)]
pub struct BrowseHead {
    /// Object size in bytes.
    pub size: u64,
    /// The 16-byte object ETag.
    pub etag: Vec<u8>,
    /// Last-modified time in epoch millis.
    pub mtime_millis: i64,
    /// Whether the object is stored inline.
    pub inline: bool,
}

/// The console's read-only view of object metadata, injected by `epoch-node`.
/// All calls are made on the PD leader (the console gates on leadership before
/// dispatch); an error is surfaced to the browser as `502`.
#[async_trait]
pub trait ObjectBrowser: Send + Sync {
    /// Lists up to `limit` objects in `bucket_id` under `prefix`, starting after
    /// `start_after` (empty = from the prefix start). Returns the entries in key
    /// order; the caller derives the next cursor from the last key.
    async fn list(
        &self,
        bucket_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> Result<Vec<BrowseEntry>, ObjectBrowseError>;

    /// Reads one object's head, or `None` if it does not exist.
    async fn head(
        &self,
        bucket_id: u64,
        key: &[u8],
    ) -> Result<Option<BrowseHead>, ObjectBrowseError>;
}

/// A failure browsing objects (a MetaNode RPC error, rendered for the console).
#[derive(Debug, thiserror::Error)]
#[error("object browse failed: {0}")]
pub struct ObjectBrowseError(pub String);
