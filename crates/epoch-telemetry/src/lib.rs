//! epoch-telemetry (L0): logging, metrics and distributed-trace init/propagation.
//!
//! All service crates reach observability facilities only through this crate,
//! guaranteeing consistent field names and label conventions (AGENTS.md §9.3).
//! Direct initialization of `tracing`/`prometheus` elsewhere is disallowed.
//!
//! Design: docs/design/06-code-layout.md §2; docs/design/05-ai-roadmap.md §1.4
//!
//! M0 delivers logging + metrics init; `trace`/`runtime` land with the data
//! plane (docs/design/07-iteration-plan.md).

pub mod logging;
pub mod metrics;

/// Errors raised while initializing observability facilities.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    /// The supplied log-filter directives could not be parsed.
    #[error("invalid log filter directives: {0}")]
    Filter(String),
    /// A global `tracing` subscriber was already installed, or install failed.
    #[error("failed to install tracing subscriber: {0}")]
    Init(String),
}
