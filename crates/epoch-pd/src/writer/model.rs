//! Writer token model: the record PD replicates for each issued token, its
//! session liveness, and the two writer commands.
//!
//! A writer token is the high 32 bits of every `blob_id` a gateway mints
//! (00 §4); PD issues it from a monotonic counter and **never reuses it**
//! (01 §4.3). The gateway's node heartbeat keeps the token's session
//! [`Live`](WriterStatus::Live); once the heartbeat lapses PD marks it
//! [`Dead`](WriterStatus::Dead), and a Dead token **never revives** — the
//! gateway must register a fresh token. This one-way lifecycle is what makes a
//! Dead token's blobs safe to garbage-collect (01 §6.3 GC watermark).
//!
//! Design: docs/design/01-pd.md §4.3 (writer_token issuance / session)

use epoch_proto::{NodeId, WriterToken};
use serde::{Deserialize, Serialize};

/// Session liveness of an issued writer token (design 01 §4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriterStatus {
    /// The owning gateway is heartbeating; the token may still mint blob ids.
    Live,
    /// The heartbeat lapsed. Terminal — the token never revives, so its blobs
    /// become GC-eligible and the gateway must register a fresh token to write.
    Dead,
}

/// A replicated writer-token record (design 01 §4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterRecord {
    /// The issued token (high 32 bits of every blob id it mints). Never reused.
    pub token: WriterToken,
    /// The owning gateway node, whose heartbeat drives this token's liveness.
    pub node_id: NodeId,
    /// Current session liveness.
    pub status: WriterStatus,
}

/// Register a fresh writer token for a gateway node (design 01 §4.3).
///
/// Not idempotent: each call mints a new, never-reused token (a gateway rotating
/// its token gets a distinct one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterWriter {
    /// The gateway node requesting a token.
    pub node_id: NodeId,
}

/// Retire a writer token whose session heartbeat has lapsed (design 01 §4.3).
///
/// Idempotent, and one-way: a token already `Dead` stays `Dead`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkWriterDead {
    /// The token to retire.
    pub token: WriterToken,
}
