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

//! epoch-telemetry (L0): logging, metrics and distributed-trace init/propagation.
//!
//! All service crates reach observability facilities only through this crate,
//! guaranteeing consistent field names and label conventions (AGENTS.md §9.3).
//! Direct initialization of `tracing`/`prometheus` elsewhere is disallowed.
//!
//! M0 delivers logging + metrics init; `trace`/`runtime` land with the data
//! plane (draft/design/07-iteration-plan.md).

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
