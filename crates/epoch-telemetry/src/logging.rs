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

//! Structured logging initialization built on `tracing` + `tracing-subscriber`.
//!
//! Two output modes (pretty for humans, JSON for aggregation) with a level
//! filter that also honors the `RUST_LOG` environment variable.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;

use crate::TelemetryError;

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable, multi-line output for development.
    Pretty,
    /// One JSON object per line for machine ingestion.
    Json,
}

/// Installs the global `tracing` subscriber.
///
/// `directives` is an [`EnvFilter`] specification (e.g. `"info"` or
/// `"info,epoch_meta=debug"`); the `RUST_LOG` environment variable, when set,
/// overrides it.
///
/// Returns an error if the directives are invalid or a global subscriber was
/// already installed. Intended to be called once during process startup.
pub fn init(format: LogFormat, directives: &str) -> Result<(), TelemetryError> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(directives))
        .map_err(|e| TelemetryError::Filter(e.to_string()))?;

    let builder = fmt().with_env_filter(filter);
    let result = match format {
        LogFormat::Pretty => builder.pretty().try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
    result.map_err(|e| TelemetryError::Init(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_is_copy_and_comparable() {
        let a = LogFormat::Json;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(LogFormat::Json, LogFormat::Pretty);
    }
}
