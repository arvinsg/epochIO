//! Crate error type for the epochIO clients.
//!
//! [`ClientError`] spans the two things this crate does: talking to the PD
//! control plane over gRPC (leader discovery, redirect, and terminal status
//! mapping) and running the local `writer_token` session (its liveness gate).
//! gRPC [`Status`](tonic::Status) values are mapped to semantic variants at the
//! client boundary so callers act on meaning, not transport codes.
//!
//! Design: docs/design/01-pd.md §7 (control plane); §4.3 (writer session)

use tonic::{Code, Status};

/// Errors surfaced by the PD client and the writer-token session.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Every configured PD endpoint was tried and none could serve the call as
    /// leader (each answered `FAILED_PRECONDITION` or was unreachable). The
    /// caller should back off and retry — a new leader may still be settling.
    #[error("no reachable PD leader after trying {attempts} endpoint(s): {last_error}")]
    NoLeader {
        /// Number of distinct endpoints attempted this call.
        attempts: usize,
        /// Rendered last retriable status/transport message, for diagnosis.
        last_error: String,
    },

    /// The leader served the call but reported the target absent
    /// (`NOT_FOUND`) — e.g. registering a writer for an unknown gateway node,
    /// or looking up a missing chunk. Terminal; not retried.
    #[error("not found: {0}")]
    NotFound(String),

    /// The request was malformed for the leader (`INVALID_ARGUMENT`). Terminal.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Any other terminal gRPC failure from the leader (rendered code + text).
    #[error("pd rpc failed ({code}): {message}")]
    Rpc {
        /// Debug rendering of the [`tonic::Code`].
        code: String,
        /// The status message.
        message: String,
    },

    /// A PD endpoint URI could not be turned into a channel (bad address).
    #[error("invalid PD endpoint {endpoint:?}: {source}")]
    Endpoint {
        /// The offending endpoint string.
        endpoint: String,
        /// The underlying transport error.
        #[source]
        source: tonic::transport::Error,
    },

    /// The writer session refused to mint a new `blob_id`: its gateway's PD
    /// heartbeat has lapsed past the stale threshold, so PD may already have
    /// retired the token (design 01 §4.3). The session must re-establish before
    /// writing again.
    #[error("writer session is stale: no successful heartbeat for {since_millis} ms")]
    WriterStale {
        /// Milliseconds since the last recorded successful heartbeat.
        since_millis: u64,
    },

    /// A client-side consistency failure (e.g. a chunk referencing a disk that
    /// is absent from the cached topology, or a malformed server projection).
    /// Distinct from wire errors — the server answered, but the answer does not
    /// line up with what the client knows.
    #[error("internal: {0}")]
    Internal(String),
}

impl ClientError {
    /// Maps a terminal gRPC [`Status`] to a semantic [`ClientError`]. Retriable
    /// codes (`FAILED_PRECONDITION` / `UNAVAILABLE`) are handled by the redirect
    /// loop and never reach here.
    pub(crate) fn from_status(status: &Status) -> Self {
        match status.code() {
            Code::NotFound => ClientError::NotFound(status.message().to_string()),
            Code::InvalidArgument => ClientError::InvalidArgument(status.message().to_string()),
            code => ClientError::Rpc {
                code: format!("{code:?}"),
                message: status.message().to_string(),
            },
        }
    }
}
