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

//! EC write-buffer admission (M6, N3): a global byte budget guarding gateway
//! memory against too many concurrent EC writes (00 §write-flow v0.7 — 默认
//! 4GB 全局池). A write acquires its object's buffer budget before encoding and
//! releases it on completion (RAII), so a burst of large PUTs queues rather
//! than exhausting memory; inline and small writes are cheap and unbounded.
//!
//! Backpressure, not rejection: an acquire waits for room (bounded by the
//! caller's request timeout), matching the "慢盘不阻塞、但内存有界" intent —
//! the pool smooths bursts instead of failing them, unless a single object
//! exceeds the whole pool (a configuration error surfaced as
//! [`GatewayError::AdmissionTooLarge`]).

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::GatewayError;

/// The default EC-buffer pool: 4 GiB (00 v0.7).
pub const DEFAULT_POOL_BYTES: usize = 4 << 30;

/// Semaphore permits are per-byte at a coarse granularity to stay within
/// `Semaphore`'s `usize` permit space and avoid 1-permit-per-byte overhead:
/// budget is counted in 64 KiB units (a write rounds its size up).
const UNIT_BYTES: usize = 64 << 10;

/// A global EC-write buffer budget. Cheap to clone (shares the semaphore).
#[derive(Clone)]
pub struct Admission {
    sem: Arc<Semaphore>,
    total_units: usize,
}

impl Admission {
    /// Builds a pool of `pool_bytes` (rounded to the unit).
    #[must_use]
    pub fn new(pool_bytes: usize) -> Self {
        let total_units = pool_bytes.div_ceil(UNIT_BYTES).max(1);
        Self {
            sem: Arc::new(Semaphore::new(total_units)),
            total_units,
        }
    }

    /// The default 4 GiB pool.
    #[must_use]
    pub fn with_default_pool() -> Self {
        Self::new(DEFAULT_POOL_BYTES)
    }

    /// Reserves buffer budget for a write of `bytes`, waiting for room. The
    /// returned permit releases the budget on drop (RAII).
    ///
    /// # Errors
    ///
    /// [`GatewayError::AdmissionTooLarge`] if `bytes` exceeds the whole pool
    /// (it could never be admitted — a misconfiguration, not backpressure).
    pub async fn acquire(&self, bytes: usize) -> Result<AdmissionPermit, GatewayError> {
        let units = bytes.div_ceil(UNIT_BYTES).max(1);
        let want = u32::try_from(units).unwrap_or(u32::MAX);
        if units > self.total_units {
            return Err(GatewayError::AdmissionTooLarge {
                bytes,
                pool_bytes: self.total_units * UNIT_BYTES,
            });
        }
        // The semaphore is never closed, so acquire only fails on closure —
        // treat that as an internal invariant break.
        let permit = Arc::clone(&self.sem)
            .acquire_many_owned(want)
            .await
            .map_err(|_| GatewayError::Writer("admission semaphore closed".to_string()))?;
        Ok(AdmissionPermit { _permit: permit })
    }
}

/// A held buffer reservation; releases its budget when dropped.
#[derive(Debug)]
pub struct AdmissionPermit {
    _permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_and_release_round_trips() {
        let adm = Admission::new(UNIT_BYTES * 4);
        let p1 = adm.acquire(UNIT_BYTES * 3).await.expect("acquire 3");
        // 1 unit left; a 1-unit acquire succeeds immediately.
        let p2 = adm.acquire(1).await.expect("acquire 1");
        drop(p1);
        drop(p2);
        // Full budget available again.
        let _p3 = adm.acquire(UNIT_BYTES * 4).await.expect("acquire full");
    }

    #[tokio::test]
    async fn oversize_write_is_rejected_not_queued() {
        let adm = Admission::new(UNIT_BYTES * 2);
        let err = adm
            .acquire(UNIT_BYTES * 3)
            .await
            .expect_err("too large for pool");
        assert!(matches!(err, GatewayError::AdmissionTooLarge { .. }));
    }

    #[tokio::test]
    async fn full_pool_backpressures_until_release() {
        let adm = Admission::new(UNIT_BYTES);
        let held = adm.acquire(UNIT_BYTES).await.expect("fill pool");
        // A second acquire cannot proceed while the pool is full.
        let pending = adm.acquire(UNIT_BYTES);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "acquire must block while the pool is full"
        );
        drop(held);
        pending.await.expect("acquire after release");
    }
}
