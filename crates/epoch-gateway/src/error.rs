//! [`GatewayError`]: the failures the M3 read/write orchestration surfaces to
//! its driver.
//!
//! The gateway tolerates individual shard faults by design (sticky slow-shard
//! failure, 04 §3.3): a write that still reaches [`crate::CodeMode::write_quorum`]
//! shards succeeds, and a read that still has `data` shards reconstructs. Only
//! when tolerance is exhausted does a call fail with one of these.
//!
//! Design: docs/design/04-ec-io.md §3.3/§4

use epoch_ec::EcError;

/// A read/write orchestration failure.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// Erasure configuration, encoding, or reconstruction failed. A GET whose
    /// surviving shards fall below `data` surfaces here as
    /// [`EcError::TooFewShards`].
    #[error("erasure coding: {0}")]
    Ec(#[from] EcError),

    /// Stripe/blob sizing is invalid (`stripe_size == 0` or
    /// `blob_size < stripe_size`). Data/parity counts are validated separately
    /// through [`EcError::InvalidCodeMode`].
    #[error("invalid sizing: stripe_size={stripe_size}, blob_size={blob_size}")]
    InvalidSizing {
        /// Requested stripe size (coding unit).
        stripe_size: usize,
        /// Requested blob size (object cut).
        blob_size: usize,
    },

    /// The chunk placement does not describe exactly `data + parity` shards.
    #[error("placement has {got} shards, code mode needs {need}")]
    ShardCount {
        /// Shards the code mode requires (`data + parity`).
        need: usize,
        /// Shards the placement supplied.
        got: usize,
    },

    /// A blob write reached fewer than [`crate::CodeMode::write_quorum`] shards,
    /// so the blob is not durable.
    #[error("write quorum not met: {got}/{need} shards committed")]
    QuorumNotMet {
        /// Shards required for a durable write.
        need: usize,
        /// Shards that committed.
        got: usize,
    },

    /// The writable set is empty and no refresh produced a candidate (PD
    /// unreachable, or no writable chunks exist yet).
    #[error("no writable chunk available")]
    NoWritableChunk,

    /// The writer session could not mint a blob id (heartbeat-stale or a
    /// token-source failure, 01 §4.3).
    #[error("writer: {0}")]
    Writer(String),

    /// A PD control-plane call failed while resolving placement.
    #[error("pd: {0}")]
    Pd(String),

    /// A MetaNode metadata call failed (route/leader/commit), rendered from the
    /// client error.
    #[error("meta: {0}")]
    Meta(String),

    /// A hierarchical-bucket path component conflicts with an existing file
    /// (03 §6.4: dir/file 冲突 → 400 InvalidObjectName).
    #[error("path component conflicts with an existing file (dir/file conflict)")]
    DirFileConflict,

    /// A single object's write buffer exceeds the whole EC-admission pool, so
    /// it could never be admitted (a configuration error, not backpressure).
    #[error("object buffer {bytes} B exceeds the EC admission pool of {pool_bytes} B")]
    AdmissionTooLarge {
        /// The object's buffer requirement.
        bytes: usize,
        /// The configured pool size.
        pool_bytes: usize,
    },

    /// Every candidate chunk reported the disk out of space (02 §1.7). Distinct
    /// from [`NoWritableChunk`](GatewayError::NoWritableChunk) (none published)
    /// and from a hardware fault: the cluster needs capacity, not repair.
    #[error("cluster is out of space")]
    OutOfSpace,

    /// A ranged GET's window lies outside the object (RFC 9110 §14.1.2 → 416).
    /// Carries the object size for the `Content-Range: bytes * /size` reply.
    #[error("requested range not satisfiable for an object of {0} bytes")]
    RangeNotSatisfiable(u64),

    /// A read reconstructed fewer (or more) bytes than the object's recorded
    /// size — a truncated or inconsistent slice list. Fails the read rather than
    /// serving a short body as a success.
    #[error("reconstructed {got} bytes but the object records {expected}")]
    LengthMismatch {
        /// The size the metadata records.
        expected: u64,
        /// The size actually reconstructed.
        got: u64,
    },
}

/// Maps a [`GatewayError`] to the S3 error the HTTP layer returns (M6 错误码
/// 映射表, 07 §M6). `s3s` serializes the resulting `S3Error` to the S3 XML body.
///
/// The mapping favors the S3 code a client acts on: quorum/EC failures and
/// unexpected metadata faults are `InternalError` (a 5xx the client retries);
/// admission-too-large is `EntityTooLarge`; a missing writable set is
/// `ServiceUnavailable` (transient, back off). NotFound-shaped metadata errors
/// surface via the per-operation `NoSuchKey`/`NoSuchBucket` at the call site,
/// so this catch-all never needs them.
#[must_use]
pub fn to_s3_error(err: GatewayError) -> s3s::S3Error {
    use s3s::S3ErrorCode;
    match err {
        GatewayError::AdmissionTooLarge { .. } => {
            s3s::S3Error::with_message(S3ErrorCode::EntityTooLarge, err.to_string())
        }
        GatewayError::DirFileConflict => {
            s3s::S3Error::with_message(S3ErrorCode::InvalidArgument, err.to_string())
        }
        GatewayError::NoWritableChunk => {
            s3s::S3Error::with_message(S3ErrorCode::ServiceUnavailable, err.to_string())
        }
        // 507 Insufficient Storage: the request is well-formed and authorized,
        // the cluster simply has no room. A 5xx so clients back off rather than
        // treating it as their own bad request.
        GatewayError::OutOfSpace => s3s::S3Error::with_message(
            S3ErrorCode::from_bytes(b"InsufficientStorage")
                .unwrap_or(S3ErrorCode::ServiceUnavailable),
            err.to_string(),
        ),
        GatewayError::RangeNotSatisfiable(_) => {
            s3s::S3Error::with_message(S3ErrorCode::InvalidRange, err.to_string())
        }
        GatewayError::Meta(msg) => {
            // A metadata routing/leader flap is transient; surface as 503 so
            // the client retries (the object layer already exhausted its own
            // bounded redirect budget).
            s3s::S3Error::with_message(S3ErrorCode::ServiceUnavailable, msg)
        }
        // EC, sizing, shard-count, quorum, writer, PD → an internal fault the
        // client retries; the detailed cause rides the message.
        other => s3s::S3Error::with_message(S3ErrorCode::InternalError, other.to_string()),
    }
}
