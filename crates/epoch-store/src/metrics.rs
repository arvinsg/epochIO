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

//! DataNode store metrics (08 §5.1): the IO byte-rate and throttle-wait counters
//! the `/metrics` endpoint exposes. Registered once against the process-wide
//! [`epoch_telemetry::metrics::registry`]; incremented on the store's IO and QoS
//! paths (02 §1.7).
//!
//! Label cardinality is bounded (AGENTS §9.3 / 08 §5.1): the only labels are
//! `class` (foreground/background/repair) and `op` (read/write) — never a
//! bucket, key, path, or node address.

use std::sync::OnceLock;

use prometheus::{CounterVec, Opts, Registry};

use crate::qos::IoClass;

/// The registered store metric handles (process-wide, initialized once).
struct StoreMetrics {
    /// `epochio_store_io_bytes_total{class, op}` — bytes moved per IO class/op.
    io_bytes: CounterVec,
    /// `epochio_store_throttle_wait_seconds_total{class}` — cumulative time the
    /// QoS limiter blocked, per class.
    throttle_wait: CounterVec,
    /// `epochio_store_io_duration_seconds{op}` — read/write latency.
    io_duration: prometheus::HistogramVec,
}

static METRICS: OnceLock<StoreMetrics> = OnceLock::new();

fn metrics() -> &'static StoreMetrics {
    METRICS.get_or_init(|| register(epoch_telemetry::metrics::registry()))
}

/// Registers the store metric families against `registry` (idempotent per
/// process via the [`OnceLock`]). A double registration would only happen across
/// two registries in tests; there the second `register` call simply builds fresh
/// (unregistered-elsewhere) handles.
fn register(registry: &Registry) -> StoreMetrics {
    let io_bytes = CounterVec::new(
        Opts::new(
            epoch_telemetry::metrics::metric_name("store", "io", "bytes_total"),
            "Bytes moved by the store, by IO class and operation.",
        ),
        &["class", "op"],
    )
    .expect("valid io_bytes metric");
    let throttle_wait = CounterVec::new(
        Opts::new(
            epoch_telemetry::metrics::metric_name("store", "throttle", "wait_seconds_total"),
            "Cumulative seconds the QoS limiter blocked IO, by class.",
        ),
        &["class"],
    )
    .expect("valid throttle_wait metric");
    // Ignore an AlreadyRegistered error so a second engine on the same process
    // registry is harmless (the first registration's handles stay authoritative,
    // and these clones still increment the same underlying series).
    let io_duration = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            epoch_telemetry::metrics::metric_name("store", "io", "duration_seconds"),
            "Store IO latency, by operation.",
        )
        .buckets(epoch_telemetry::metrics::LATENCY_BUCKETS_SECONDS.to_vec()),
        &["op"],
    )
    .expect("valid io_duration metric");
    let _ = registry.register(Box::new(io_bytes.clone()));
    let _ = registry.register(Box::new(throttle_wait.clone()));
    let _ = registry.register(Box::new(io_duration.clone()));
    StoreMetrics {
        io_bytes,
        throttle_wait,
        io_duration,
    }
}

/// The metric label for an [`IoClass`].
fn class_label(class: IoClass) -> &'static str {
    match class {
        IoClass::Foreground => "foreground",
        IoClass::Background => "background",
        IoClass::Repair => "repair",
    }
}

/// Records `bytes` moved for `class` doing `op` (`"read"` / `"write"`).
pub(crate) fn record_io(class: IoClass, op: &str, bytes: u64) {
    metrics()
        .io_bytes
        .with_label_values(&[class_label(class), op])
        .inc_by(bytes as f64);
}

/// Records one IO operation's latency for `op` (`"read"` / `"write"`).
pub(crate) fn record_io_duration(op: &str, seconds: f64) {
    metrics()
        .io_duration
        .with_label_values(&[op])
        .observe(seconds);
}

/// Records `seconds` spent blocked in the `class` QoS limiter.
pub(crate) fn record_throttle_wait(class: IoClass, seconds: f64) {
    if seconds > 0.0 {
        metrics()
            .throttle_wait
            .with_label_values(&[class_label(class)])
            .inc_by(seconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_both_families_with_bounded_labels() {
        let registry = Registry::new();
        let m = register(&registry);
        m.io_bytes
            .with_label_values(&["repair", "write"])
            .inc_by(10.0);
        m.throttle_wait.with_label_values(&["repair"]).inc_by(0.5);
        let families = registry.gather();
        let names: Vec<String> = families.iter().map(|f| f.get_name().to_string()).collect();
        assert!(names.iter().any(|n| n == "epochio_store_io_bytes_total"));
        assert!(
            names
                .iter()
                .any(|n| n == "epochio_store_throttle_wait_seconds_total")
        );
        m.io_duration.with_label_values(&["read"]).observe(0.001);
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        assert!(
            names
                .iter()
                .any(|n| n == "epochio_store_io_duration_seconds")
        );
    }
}
