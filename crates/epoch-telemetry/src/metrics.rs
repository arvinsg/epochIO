//! Prometheus metrics registry and naming conventions.
//!
//! Metric names follow `epochio_<component>_<subsystem>_<name>` so that every
//! crate emits consistently namespaced series (AGENTS.md §9.3). Two shared
//! histogram bucket sets keep latency and size distributions comparable across
//! subsystems.
//!
//! Design: docs/design/06-code-layout.md §2

use std::sync::OnceLock;

use prometheus::{Registry, TextEncoder};

/// Prefix applied to every epochIO metric name.
pub const METRIC_PREFIX: &str = "epochio";

/// Standard latency histogram buckets, in seconds (sub-millisecond to 10s).
pub const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Standard size histogram buckets, in bytes (1KiB to 1GiB).
pub const SIZE_BUCKETS_BYTES: &[f64] = &[
    1024.0,
    4096.0,
    65536.0,
    1_048_576.0,
    33_554_432.0,
    134_217_728.0,
    1_073_741_824.0,
];

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Returns the process-wide metrics registry, creating it on first use.
pub fn registry() -> &'static Registry {
    REGISTRY.get_or_init(Registry::new)
}

/// Builds a fully-qualified metric name `epochio_<component>_<subsystem>_<name>`.
///
/// `component` is the owning crate (e.g. `"meta"`, `"store"`), `subsystem` the
/// area within it (e.g. `"delq"`, `"compaction"`).
pub fn metric_name(component: &str, subsystem: &str, name: &str) -> String {
    format!("{METRIC_PREFIX}_{component}_{subsystem}_{name}")
}

/// Encodes the registry's current metric families in the Prometheus text
/// exposition format — the body a `/metrics` endpoint returns (08 §5.1). Each
/// role serves this from its own lightweight HTTP endpoint (metrics are not
/// aggregated through PD).
#[must_use]
pub fn encode() -> String {
    let mut buf = String::new();
    // `encode_utf8` only fails if a metric family is malformed (an internal
    // bug), so an empty body on error is an acceptable degradation.
    let _ = TextEncoder::new().encode_utf8(&registry().gather(), &mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_name_is_fully_namespaced() {
        assert_eq!(
            metric_name("meta", "delq", "pending_total"),
            "epochio_meta_delq_pending_total"
        );
    }

    #[test]
    fn registry_is_a_singleton() {
        assert!(std::ptr::eq(registry(), registry()));
    }

    #[test]
    fn bucket_sets_are_strictly_increasing() {
        for buckets in [LATENCY_BUCKETS_SECONDS, SIZE_BUCKETS_BYTES] {
            assert!(buckets.windows(2).all(|w| w[0] < w[1]));
        }
    }
}
