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

//! PD metrics (08 §5.1): the raft propose-latency histogram the `/metrics`
//! endpoint exposes. Registered once against the process-wide
//! [`epoch_telemetry::metrics::registry`]; recorded in [`Journal::propose`], the
//! single choke point every PD mutation flows through.
//!
//! The histogram is unlabeled — the propose path is one code path regardless of
//! entry kind, and an entry-kind label would balloon cardinality for no
//! operational gain (AGENTS §9.3 / 08 §5.1).
//!
//! [`Journal::propose`]: crate::journal::Journal::propose

use std::sync::OnceLock;

use prometheus::{Histogram, HistogramOpts, Registry};

static METRICS: OnceLock<Histogram> = OnceLock::new();

fn propose_duration() -> &'static Histogram {
    METRICS.get_or_init(|| register(epoch_telemetry::metrics::registry()))
}

/// Registers the propose-latency histogram against `registry` (idempotent per
/// process via the [`OnceLock`]; AlreadyRegistered is ignored so a second PD on
/// one process registry is harmless).
fn register(registry: &Registry) -> Histogram {
    use epoch_telemetry::metrics::{LATENCY_BUCKETS_SECONDS, metric_name};
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            metric_name("pd", "raft", "propose_duration_seconds"),
            "Raft propose (client_write) latency.",
        )
        .buckets(LATENCY_BUCKETS_SECONDS.to_vec()),
    )
    .expect("valid propose_duration metric");
    let _ = registry.register(Box::new(histogram.clone()));
    histogram
}

/// Records one raft propose's latency in seconds.
pub(crate) fn record_propose(seconds: f64) {
    propose_duration().observe(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_the_propose_histogram() {
        let registry = Registry::new();
        let h = register(&registry);
        h.observe(0.005);
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "epochio_pd_raft_propose_duration_seconds")
        );
    }
}
