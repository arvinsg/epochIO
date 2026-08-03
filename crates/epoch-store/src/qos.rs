//! IO priority classes and a token-bucket rate limiter — the QoS skeleton the
//! background / foreground / repair IO paths throttle through (02 §1.7).
//!
//! M2 wires only the `Background` class: compaction throttles its copy loop
//! through it. The `Foreground` (data plane, M3) and `Repair` (M7) classes
//! reserve their buckets so the mapping is complete. The per-disk read/write
//! thread pools and depth-based admission (Background half-full drop → 429)
//! arrive with the async data plane (M3, which has a runtime); a synchronous
//! byte-rate token bucket is the piece M2 needs now.
//!
//! Design: docs/design/02-datanode.md §1.7

use std::time::{Duration, Instant};

/// IO priority class (02 §1.7): `Repair` > `Foreground` > `Background`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoClass {
    /// User-facing reads/writes (data plane, M3).
    Foreground,
    /// Compaction, scrub, GC — yields to foreground.
    Background,
    /// Disk repair/rebuild — dedicated budget, highest priority (M7).
    Repair,
}

/// Per-class byte-rate budgets in bytes/sec (`0` = unlimited) plus a shared
/// burst capacity. Runtime-adjustable (02 §1.7). M2 defaults to unlimited and
/// leaves real budgets to be injected from PD config in a later milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QosConfig {
    /// Foreground byte-rate budget (bytes/sec; `0` = unlimited).
    pub foreground_rate: u64,
    /// Background byte-rate budget (bytes/sec; `0` = unlimited).
    pub background_rate: u64,
    /// Repair byte-rate budget (bytes/sec; `0` = unlimited).
    pub repair_rate: u64,
    /// Burst capacity (bytes) each bucket may accumulate while idle.
    pub burst: u64,
}

/// A token bucket metering a byte rate. The refill/reserve arithmetic takes an
/// explicit `Instant` so it is deterministically testable; [`throttle`] is the
/// thin real-clock wrapper that sleeps.
///
/// [`throttle`]: RateLimiter::throttle
#[derive(Debug)]
pub struct RateLimiter {
    rate_per_sec: u64,
    capacity: u64,
    tokens: u64,
    last: Instant,
}

impl RateLimiter {
    /// A limiter granting `rate_per_sec` bytes/sec with a `capacity`-byte burst,
    /// starting full. `rate_per_sec == 0` means unlimited.
    #[must_use]
    pub fn new(rate_per_sec: u64, capacity: u64) -> Self {
        Self {
            rate_per_sec,
            capacity,
            tokens: capacity,
            last: Instant::now(),
        }
    }

    /// Reserves `cost` bytes as of `now`, returning how long the caller must
    /// wait before proceeding (`Duration::ZERO` = proceed immediately).
    /// Unlimited buckets always return zero. `cost` larger than `capacity` is
    /// honored by waiting proportionally (the bucket never deadlocks).
    fn reserve(&mut self, cost: u64, now: Instant) -> Duration {
        if self.rate_per_sec == 0 {
            return Duration::ZERO;
        }
        self.refill(now);
        if self.tokens >= cost {
            self.tokens -= cost;
            return Duration::ZERO;
        }
        let deficit = cost - self.tokens;
        self.tokens = 0;
        let nanos = u128::from(deficit) * 1_000_000_000 / u128::from(self.rate_per_sec);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last);
        if elapsed.is_zero() {
            return;
        }
        self.last = now;
        let added = u128::from(self.rate_per_sec) * elapsed.as_nanos() / 1_000_000_000;
        let added = u64::try_from(added).unwrap_or(u64::MAX);
        self.tokens = self.tokens.saturating_add(added).min(self.capacity);
    }

    /// Blocks the current thread just long enough to keep within the byte rate.
    /// A no-op for unlimited buckets. For synchronous background IO loops only
    /// (M2 has no async runtime; 02 §1.7).
    pub fn throttle(&mut self, cost: u64) {
        let wait = self.reserve(cost, Instant::now());
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }
}

/// The three per-class rate limiters for one disk.
#[derive(Debug)]
pub struct Qos {
    foreground: RateLimiter,
    background: RateLimiter,
    repair: RateLimiter,
}

impl Qos {
    /// Builds the three limiters from `config`.
    #[must_use]
    pub fn new(config: &QosConfig) -> Self {
        Self {
            foreground: RateLimiter::new(config.foreground_rate, config.burst),
            background: RateLimiter::new(config.background_rate, config.burst),
            repair: RateLimiter::new(config.repair_rate, config.burst),
        }
    }

    /// The rate limiter governing `class`.
    pub fn limiter(&mut self, class: IoClass) -> &mut RateLimiter {
        match class {
            IoClass::Foreground => &mut self.foreground,
            IoClass::Background => &mut self.background,
            IoClass::Repair => &mut self.repair,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_bucket_never_waits() {
        let mut rl = RateLimiter::new(0, 0);
        let now = Instant::now();
        assert_eq!(rl.reserve(1_000_000, now), Duration::ZERO);
        assert_eq!(rl.reserve(u64::MAX, now), Duration::ZERO);
    }

    #[test]
    fn full_bucket_grants_burst_then_meters_the_rate() {
        // 1000 bytes/sec, 1000-byte burst.
        let mut rl = RateLimiter::new(1000, 1000);
        let t0 = rl.last;

        // Burst is available immediately.
        assert_eq!(rl.reserve(1000, t0), Duration::ZERO);
        // Empty now: the next 1000 bytes must wait a full second.
        assert_eq!(rl.reserve(1000, t0), Duration::from_secs(1));
    }

    #[test]
    fn refill_is_proportional_to_elapsed_time() {
        let mut rl = RateLimiter::new(1000, 1000);
        let t0 = rl.last;
        assert_eq!(rl.reserve(1000, t0), Duration::ZERO); // drain

        // After 0.5s, 500 bytes have refilled → a 500-byte request proceeds.
        assert_eq!(
            rl.reserve(500, t0 + Duration::from_millis(500)),
            Duration::ZERO
        );
    }

    #[test]
    fn refill_is_capped_at_capacity() {
        let mut rl = RateLimiter::new(1000, 1000);
        let t0 = rl.last;
        assert_eq!(rl.reserve(1000, t0), Duration::ZERO); // drain to 0

        // Idle 10s would refill 10000, but capacity caps at 1000; asking for
        // 1001 must therefore still wait for the 1 missing byte.
        let wait = rl.reserve(1001, t0 + Duration::from_secs(10));
        assert_eq!(wait, Duration::from_millis(1));
    }

    #[test]
    fn cost_larger_than_capacity_waits_proportionally() {
        let mut rl = RateLimiter::new(1000, 100);
        let t0 = rl.last;
        // Starts with 100 tokens; asking 1100 → deficit 1000 → 1s wait.
        assert_eq!(rl.reserve(1100, t0), Duration::from_secs(1));
    }

    #[test]
    fn qos_maps_each_class_to_its_bucket() {
        let mut qos = Qos::new(&QosConfig {
            foreground_rate: 0,
            background_rate: 1000,
            repair_rate: 0,
            burst: 1000,
        });
        // Background is metered; foreground/repair are unlimited.
        let t0 = qos.background.last;
        assert_eq!(
            qos.limiter(IoClass::Background).reserve(1000, t0),
            Duration::ZERO
        );
        assert_eq!(
            qos.limiter(IoClass::Background).reserve(1000, t0),
            Duration::from_secs(1)
        );
        let t = qos.foreground.last;
        assert_eq!(
            qos.limiter(IoClass::Foreground).reserve(u64::MAX, t),
            Duration::ZERO
        );
    }
}
