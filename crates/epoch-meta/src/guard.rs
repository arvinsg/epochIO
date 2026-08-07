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

//! The inline small-object guard (03 §4.3 护栏, 防打爆元数据节点).
//!
//! Inline content lives in the metadata engine itself, so an inline-heavy
//! bucket would turn MetaNodes into the data plane. Two layers of defense:
//!
//! 1. **Threshold resolution** (03 §4.3 配置层级): cluster default (128 KB) →
//!    bucket override → hard cap (1 MB); 0 disables inline entirely.
//! 2. **Partition ratio guard**: a partition whose inline bytes exceed 50% of
//!    its total object bytes rejects *new* inline PUTs (`inline_bytes 占比 >
//!    50%`). A rejected PUT transparently downgrades to EC at the gateway
//!    (03 §4.3; the S3 mapping lands in M6).
//!
//! Accounting is per-partition, shared between the state machine (which
//! applies signed deltas and persists the counters in the same atomic batch,
//! restart/snapshot-safe like the delq counter) and the service (which checks
//! inline PUTs before proposing). Partition heartbeats carry the counters to
//! PD for observability (03 §4.3 统计).
//!
//! The node-level capacity watermark of 03 §4.3 is configuration-plumbed and
//! lands with the node budget config (registered in 99); the MemEngine
//! variant (stricter, 03 §7) is M5b.
//!
//! Design: docs/design/03-metanode.md §4.3

use std::sync::atomic::{AtomicU64, Ordering};

/// The cluster-default inline threshold (03 §4.3: 128 KB; 0 disables inline).
pub const DEFAULT_INLINE_THRESHOLD: u64 = 128 * 1024;

/// The hard cap any configuration is clamped to (03 §4.3: 硬上限 1MB).
pub const HARD_INLINE_CAP: u64 = 1024 * 1024;

/// The partition inline-bytes share above which new inline PUTs are rejected
/// (03 §4.3: inline_bytes 占比 > 50%).
pub const MAX_INLINE_RATIO: f64 = 0.5;

/// Resolves the effective inline threshold for a bucket (03 §4.3 配置层级):
/// the bucket override when set, else the cluster default, clamped to the
/// hard cap. 0 disables inline.
#[must_use]
pub fn resolve_threshold(cluster_default: u64, bucket_override: Option<u64>) -> u64 {
    bucket_override
        .unwrap_or(cluster_default)
        .min(HARD_INLINE_CAP)
}

/// Why an inline PUT was rejected (the gateway transparently downgrades the
/// write to a plain EC object, 03 §4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InlineReject {
    /// Inline is disabled for this bucket (threshold 0).
    #[error("inline disabled (threshold 0)")]
    Disabled,
    /// The payload exceeds the effective threshold.
    #[error("inline payload {0} bytes exceeds the threshold")]
    TooLarge(u64),
    /// The partition's inline share is already above 50%.
    #[error("partition inline-bytes share above 50%")]
    RatioExceeded,
}

/// The signed accounting delta of one applied op (an overwrite or delete
/// subtracts the old object's bytes; multipart ops move nothing).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GuardDelta {
    /// Change in inline bytes.
    pub inline_bytes: i64,
    /// Change in inline object count.
    pub inline_count: i64,
    /// Change in total object bytes (Σ head.size).
    pub total_bytes: i64,
}

/// Per-partition inline accounting (03 §4.3 护栏的计数面).
///
/// The state machine applies [`GuardDelta`]s and persists the counters in the
/// same apply batch; the service reads them for the inline check; the ticker
/// reports them in partition heartbeats. All updates flow through
/// [`account`](Self::account), so the counters are always the post-apply
/// truth (in-memory mirror of the persisted values).
#[derive(Debug, Default)]
pub struct PartitionGuard {
    inline_bytes: AtomicU64,
    inline_count: AtomicU64,
    total_bytes: AtomicU64,
    /// Pending delete-queue entries in this partition's range (08 §5.1 gauge).
    /// Maintained incrementally by apply and seeded by a one-time range scan at
    /// state-machine construction; the service reads it O(1) for heartbeats
    /// instead of scanning the queue. In-memory only (a derived gauge, not part
    /// of the persisted guard record) — recovery re-seeds it from the queue.
    delq_depth: AtomicU64,
}

impl PartitionGuard {
    /// A zeroed guard (a fresh partition).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads persisted counters (state-machine recovery / snapshot install).
    pub fn load(&self, inline_bytes: u64, inline_count: u64, total_bytes: u64) {
        self.inline_bytes.store(inline_bytes, Ordering::Relaxed);
        self.inline_count.store(inline_count, Ordering::Relaxed);
        self.total_bytes.store(total_bytes, Ordering::Relaxed);
    }

    /// Applies one op's delta, returning the new counters (saturating —
    /// replay can never drive a counter negative). The only writer is the
    /// partition's serial apply, so a plain read-compute-store is race-free;
    /// readers tolerate the heuristic staleness.
    pub fn account(&self, delta: GuardDelta) -> (u64, u64, u64) {
        let inline_bytes = self
            .inline_bytes
            .load(Ordering::Relaxed)
            .saturating_add_signed(delta.inline_bytes);
        let inline_count = self
            .inline_count
            .load(Ordering::Relaxed)
            .saturating_add_signed(delta.inline_count);
        let total_bytes = self
            .total_bytes
            .load(Ordering::Relaxed)
            .saturating_add_signed(delta.total_bytes);
        self.inline_bytes.store(inline_bytes, Ordering::Relaxed);
        self.inline_count.store(inline_count, Ordering::Relaxed);
        self.total_bytes.store(total_bytes, Ordering::Relaxed);
        (inline_bytes, inline_count, total_bytes)
    }

    /// The current `(inline_bytes, inline_count, total_bytes)`.
    #[must_use]
    pub fn counters(&self) -> (u64, u64, u64) {
        (
            self.inline_bytes.load(Ordering::Relaxed),
            self.inline_count.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
        )
    }

    /// Seeds the delete-queue depth gauge (state-machine construction / snapshot
    /// install re-scan). Separate from [`load`](Self::load) because the depth is
    /// derived from the queue, not carried in the persisted guard record.
    pub fn load_delq_depth(&self, depth: u64) {
        self.delq_depth.store(depth, Ordering::Relaxed);
    }

    /// Applies a signed change to the delete-queue depth (delq entries enqueued
    /// minus dequeued in one apply batch), returning the new depth. Saturating —
    /// a dequeue can never drive it below zero. The partition's serial apply is
    /// the only writer, so read-compute-store is race-free.
    pub fn account_delq(&self, delta: i64) -> u64 {
        let depth = self
            .delq_depth
            .load(Ordering::Relaxed)
            .saturating_add_signed(delta);
        self.delq_depth.store(depth, Ordering::Relaxed);
        depth
    }

    /// The current pending delete-queue depth.
    #[must_use]
    pub fn delq_depth(&self) -> u64 {
        self.delq_depth.load(Ordering::Relaxed)
    }

    /// The inline PUT admission check (03 §4.3): threshold resolution has
    /// already happened; this enforces it plus the partition ratio guard.
    pub fn check_inline(&self, payload_len: u64, threshold: u64) -> Result<(), InlineReject> {
        if threshold == 0 {
            return Err(InlineReject::Disabled);
        }
        if payload_len > threshold {
            return Err(InlineReject::TooLarge(payload_len));
        }
        let (inline_bytes, _, total_bytes) = self.counters();
        if total_bytes > 0 && (inline_bytes as f64) > MAX_INLINE_RATIO * (total_bytes as f64) {
            return Err(InlineReject::RatioExceeded);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_resolution_layers_and_caps() {
        assert_eq!(
            resolve_threshold(DEFAULT_INLINE_THRESHOLD, None),
            DEFAULT_INLINE_THRESHOLD
        );
        assert_eq!(resolve_threshold(64, Some(4096)), 4096);
        // A bucket override beyond the hard cap is clamped.
        assert_eq!(resolve_threshold(64, Some(u64::MAX)), HARD_INLINE_CAP);
        // 0 disables inline.
        assert_eq!(resolve_threshold(64, Some(0)), 0);
    }

    #[test]
    fn check_enforces_threshold_and_ratio() {
        let guard = PartitionGuard::new();
        assert_eq!(guard.check_inline(1, 0), Err(InlineReject::Disabled));
        assert_eq!(
            guard.check_inline(HARD_INLINE_CAP + 1, HARD_INLINE_CAP),
            Err(InlineReject::TooLarge(HARD_INLINE_CAP + 1))
        );

        // 60% inline share: new inline rejected; EC (non-inline) unaffected.
        guard.load(600, 6, 1_000);
        assert_eq!(
            guard.check_inline(1, DEFAULT_INLINE_THRESHOLD),
            Err(InlineReject::RatioExceeded)
        );
        // Below the ratio: admitted; an empty partition always admits.
        guard.load(400, 4, 1_000);
        assert!(guard.check_inline(1, DEFAULT_INLINE_THRESHOLD).is_ok());
        guard.load(0, 0, 0);
        assert!(guard.check_inline(1, DEFAULT_INLINE_THRESHOLD).is_ok());
    }

    #[test]
    fn account_applies_signed_deltas_saturating() {
        let guard = PartitionGuard::new();
        let (inline, count, total) = guard.account(GuardDelta {
            inline_bytes: 100,
            inline_count: 1,
            total_bytes: 500,
        });
        assert_eq!((inline, count, total), (100, 1, 500));
        let (inline, count, total) = guard.account(GuardDelta {
            inline_bytes: -150,
            inline_count: -2,
            total_bytes: -600,
        });
        assert_eq!((inline, count, total), (0, 0, 0), "never negative");
        assert_eq!(guard.counters(), (0, 0, 0));
    }
}
