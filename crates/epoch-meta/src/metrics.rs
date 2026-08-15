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

//! MetaNode metrics (08 §5.1): the apply-latency histogram and the per-partition
//! delete-queue depth gauge the `/metrics` endpoint exposes. Registered once
//! against the process-wide [`epoch_telemetry::metrics::registry`]; recorded on
//! the raft apply path.
//!
//! Label cardinality is bounded (AGENTS §9.3 / 08 §5.1): `ns` is `flat`/`hier`;
//! `partition` is the partition id (cluster-scale cardinality, 08 §5.1 注).
//!
//! Design: docs/design/08-web-console.md §5.1; docs/design/03-metanode.md §8

use std::sync::OnceLock;

use prometheus::{HistogramOpts, HistogramVec, IntGaugeVec, Opts, Registry};

/// The registered meta metric handles (process-wide, initialized once).
struct MetaMetrics {
    /// `epochio_meta_apply_duration_seconds{ns}` — raft apply latency per
    /// namespace.
    apply_duration: HistogramVec,
    /// `epochio_meta_delq_depth{partition}` — pending delete-queue depth.
    delq_depth: IntGaugeVec,
}

static METRICS: OnceLock<MetaMetrics> = OnceLock::new();

fn metrics() -> &'static MetaMetrics {
    METRICS.get_or_init(|| register(epoch_telemetry::metrics::registry()))
}

/// Registers the meta metric families against `registry`. Idempotent per
/// process via the [`OnceLock`]; AlreadyRegistered is ignored so the many
/// per-partition state machines on one process share the same series.
fn register(registry: &Registry) -> MetaMetrics {
    use epoch_telemetry::metrics::{LATENCY_BUCKETS_SECONDS, metric_name};
    let apply_duration = HistogramVec::new(
        HistogramOpts::new(
            metric_name("meta", "apply", "duration_seconds"),
            "Raft apply latency, by namespace.",
        )
        .buckets(LATENCY_BUCKETS_SECONDS.to_vec()),
        &["ns"],
    )
    .expect("valid apply_duration metric");
    let delq_depth = IntGaugeVec::new(
        Opts::new(
            metric_name("meta", "delq", "depth"),
            "Pending delete-queue depth, by partition.",
        ),
        &["partition"],
    )
    .expect("valid delq_depth metric");
    let _ = registry.register(Box::new(apply_duration.clone()));
    let _ = registry.register(Box::new(delq_depth.clone()));
    MetaMetrics {
        apply_duration,
        delq_depth,
    }
}

/// Records one partition apply's latency for `ns` (`"flat"` / `"hier"`).
pub(crate) fn record_apply(ns: &str, seconds: f64) {
    metrics()
        .apply_duration
        .with_label_values(&[ns])
        .observe(seconds);
}

/// Sets the delete-queue depth gauge for `partition` to its current value.
/// Called after each apply that mutates the queue, so the gauge tracks the
/// authoritative in-memory counter.
pub(crate) fn set_delq_depth(partition: u64, depth: u64) {
    metrics()
        .delq_depth
        .with_label_values(&[&partition.to_string()])
        .set(depth as i64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_both_families() {
        let registry = Registry::new();
        let m = register(&registry);
        m.apply_duration.with_label_values(&["flat"]).observe(0.002);
        m.delq_depth.with_label_values(&["7"]).set(3);
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "epochio_meta_apply_duration_seconds")
        );
        assert!(names.iter().any(|n| n == "epochio_meta_delq_depth"));
    }
}
