//! Writer registry: PD-issued writer tokens and their session liveness.
//!
//! [`WriterManager`] owns the replicated token records; [`model`] holds the
//! record / status / commands; [`liveness`] derives session retirement from
//! gateway heartbeat staleness. A token is the high 32 bits of every `blob_id`
//! (00 §4), issued from a monotonic counter, never reused, and one-way
//! `Live → Dead` (01 §4.3).
//!
//! Design: docs/design/01-pd.md §4.3 (writer_token issuance / session)

pub mod liveness;
pub mod manager;
pub mod model;

pub use liveness::DEFAULT_WRITER_DEAD_AFTER_MILLIS;
pub use manager::WriterManager;
pub use model::{MarkWriterDead, RegisterWriter, WriterRecord, WriterStatus};

pub(crate) use liveness::dead_writer_plan;
