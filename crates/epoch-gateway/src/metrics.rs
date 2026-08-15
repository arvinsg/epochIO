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

//! Gateway metrics (08 §5.1): the request latency / object-size histograms and
//! the shard-timeout / quorum counters the `/metrics` endpoint exposes.
//! Registered once against the process-wide
//! [`epoch_telemetry::metrics::registry`]; recorded on the S3 verb paths.
//!
//! Label cardinality is bounded (AGENTS §9.3 / 08 §5.1): the only label is `op`
//! (`put`/`get`/`list`/`delete`/…), never a bucket, key, or node address.

use std::sync::OnceLock;

use prometheus::{Counter, HistogramOpts, HistogramVec, Registry};

/// The registered gateway metric handles (process-wide, initialized once).
struct GatewayMetrics {
    /// `epochio_gateway_request_duration_seconds{op}` — S3 verb latency.
    request_duration: HistogramVec,
    /// `epochio_gateway_object_bytes{op}` — object body size per verb.
    object_bytes: HistogramVec,
    /// `epochio_gateway_shard_timeout_total` — shard writes that hit the
    /// slow-shard timeout (04 §3.3 sticky-failure signal).
    shard_timeout: Counter,
    /// `epochio_gateway_quorum_ack_total` — blob writes that reached quorum.
    quorum_ack: Counter,
}

static METRICS: OnceLock<GatewayMetrics> = OnceLock::new();

fn metrics() -> &'static GatewayMetrics {
    METRICS.get_or_init(|| register(epoch_telemetry::metrics::registry()))
}

/// Registers the gateway metric families against `registry` (idempotent per
/// process via the [`OnceLock`]; a double registration in tests simply builds
/// fresh, unregistered-elsewhere handles).
fn register(registry: &Registry) -> GatewayMetrics {
    use epoch_telemetry::metrics::{LATENCY_BUCKETS_SECONDS, SIZE_BUCKETS_BYTES, metric_name};
    let request_duration = HistogramVec::new(
        HistogramOpts::new(
            metric_name("gateway", "request", "duration_seconds"),
            "S3 request latency, by verb.",
        )
        .buckets(LATENCY_BUCKETS_SECONDS.to_vec()),
        &["op"],
    )
    .expect("valid request_duration metric");
    let object_bytes = HistogramVec::new(
        HistogramOpts::new(
            metric_name("gateway", "object", "bytes"),
            "Object body size served, by verb.",
        )
        .buckets(SIZE_BUCKETS_BYTES.to_vec()),
        &["op"],
    )
    .expect("valid object_bytes metric");
    let shard_timeout = Counter::new(
        metric_name("gateway", "shard", "timeout_total"),
        "Shard writes that hit the slow-shard timeout.",
    )
    .expect("valid shard_timeout metric");
    let quorum_ack = Counter::new(
        metric_name("gateway", "quorum", "ack_total"),
        "Blob writes that reached write quorum.",
    )
    .expect("valid quorum_ack metric");
    // Ignore AlreadyRegistered so a second registration on the same process
    // registry is harmless (the first handles stay authoritative).
    let _ = registry.register(Box::new(request_duration.clone()));
    let _ = registry.register(Box::new(object_bytes.clone()));
    let _ = registry.register(Box::new(shard_timeout.clone()));
    let _ = registry.register(Box::new(quorum_ack.clone()));
    GatewayMetrics {
        request_duration,
        object_bytes,
        shard_timeout,
        quorum_ack,
    }
}

/// A request-latency timer: records `op`'s elapsed seconds on drop. Construct at
/// a verb's entry; the measurement then covers the whole handler regardless of
/// which early-return path it takes.
pub(crate) struct RequestTimer {
    op: &'static str,
    start: std::time::Instant,
}

impl RequestTimer {
    /// Starts timing `op`.
    #[must_use]
    pub(crate) fn start(op: &'static str) -> Self {
        Self {
            op,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        record_request(self.op, self.start.elapsed().as_secs_f64());
    }
}

/// Records one S3 request's latency for `op` (`"put"`/`"get"`/`"list"`/…).
pub(crate) fn record_request(op: &str, seconds: f64) {
    metrics()
        .request_duration
        .with_label_values(&[op])
        .observe(seconds);
}

/// Records an object body's size for `op` (`"put"`/`"get"`).
pub(crate) fn record_object_bytes(op: &str, bytes: u64) {
    metrics()
        .object_bytes
        .with_label_values(&[op])
        .observe(bytes as f64);
}

/// Records a shard write that hit the slow-shard timeout (04 §3.3).
pub(crate) fn record_shard_timeout() {
    metrics().shard_timeout.inc();
}

/// Records a blob write that reached quorum.
pub(crate) fn record_quorum_ack() {
    metrics().quorum_ack.inc();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_all_families_with_bounded_labels() {
        let registry = Registry::new();
        let m = register(&registry);
        m.request_duration.with_label_values(&["put"]).observe(0.01);
        m.object_bytes.with_label_values(&["get"]).observe(4096.0);
        m.shard_timeout.inc();
        m.quorum_ack.inc();
        let names: Vec<String> = registry
            .gather()
            .iter()
            .map(|f| f.get_name().to_string())
            .collect();
        for want in [
            "epochio_gateway_request_duration_seconds",
            "epochio_gateway_object_bytes",
            "epochio_gateway_shard_timeout_total",
            "epochio_gateway_quorum_ack_total",
        ] {
            assert!(names.iter().any(|n| n == want), "missing {want}");
        }
    }
}
