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

//! Striping math: unit sizes, shard physical sizes and frame offsets.
//!
//! All functions are pure (no I/O, no allocation). A Blob is split into stripes
//! of `stripe_size` bytes; each stripe is EC-encoded into `data + parity` shards
//! of `unit` bytes; each shard's on-disk body is a sequence of bitrot frames
//! `[BLAKE3(32B) | unit bytes]`, where the final frame may be shorter.
//!
//! Design: docs/design/04-ec-io.md §1.2

/// BLAKE3 digest length prefixed to every bitrot frame.
pub const HASH_LEN: usize = 32;

/// Shard-length alignment: 64 bytes.
///
/// reed-solomon-simd requires equal, even-length shards; a 64-byte multiple is
/// even, SIMD-friendly, and documented compatible across all crate versions.
/// Design: docs/design/04-ec-io.md §1.2; docs/design/99-open-questions.md 已决（M1）.
pub const SHARD_ALIGN: usize = 64;

/// Per-shard `unit` for a stripe of `stripe_len` bytes over `data_shards` data
/// shards: `align64(ceil(stripe_len / data_shards))`.
///
/// Returns 0 iff `stripe_len == 0`. `data_shards` must be `> 0` (a code-mode
/// invariant validated at [`crate::Erasure`] construction).
#[must_use]
pub fn unit_size(stripe_len: usize, data_shards: usize) -> usize {
    debug_assert!(data_shards > 0, "data_shards must be > 0");
    stripe_len
        .div_ceil(data_shards)
        .next_multiple_of(SHARD_ALIGN)
}

/// Number of stripes a blob of `blob_len` bytes splits into. `stripe_size > 0`.
#[must_use]
pub fn stripe_count(blob_len: usize, stripe_size: usize) -> usize {
    debug_assert!(stripe_size > 0, "stripe_size must be > 0");
    blob_len.div_ceil(stripe_size)
}

/// Byte length of a blob's final stripe: `stripe_size` for an exact multiple,
/// the remainder otherwise, and 0 for an empty blob. `stripe_size > 0`.
#[must_use]
pub fn last_stripe_len(blob_len: usize, stripe_size: usize) -> usize {
    debug_assert!(stripe_size > 0, "stripe_size must be > 0");
    if blob_len == 0 {
        return 0;
    }
    match blob_len % stripe_size {
        0 => stripe_size,
        rem => rem,
    }
}

/// On-disk physical length of one shard body holding `logical_len` data bytes
/// framed at `unit`: `frames * HASH_LEN + logical_len` with
/// `frames = ceil(logical_len / unit)`. Design: docs/design/04-ec-io.md §1.2.
#[must_use]
pub fn shard_physical_len(logical_len: usize, unit: usize) -> usize {
    if unit == 0 || logical_len == 0 {
        return 0;
    }
    logical_len.div_ceil(unit) * HASH_LEN + logical_len
}

/// Logical data bytes (including per-stripe padding) for one shard of a blob,
/// **excluding** the per-stripe `HASH_LEN` frame prefixes.
///
/// Full stripes each contribute `unit_size(stripe_size, n)`; the final stripe
/// contributes `unit_size(last_stripe_len(..), n)` (possibly shorter). For the
/// on-disk allocation size use [`shard_physical_blob_len`].
#[must_use]
pub fn shard_logical_len(blob_len: usize, stripe_size: usize, data_shards: usize) -> usize {
    let stripes = stripe_count(blob_len, stripe_size);
    if stripes == 0 {
        return 0;
    }
    let unit_full = unit_size(stripe_size, data_shards);
    let unit_last = unit_size(last_stripe_len(blob_len, stripe_size), data_shards);
    (stripes - 1) * unit_full + unit_last
}

/// On-disk physical bytes for one shard of a blob: one `HASH_LEN` frame prefix
/// per stripe on top of [`shard_logical_len`] — the preallocation size for a
/// shard body on disk. Design: docs/design/04-ec-io.md §1.2.
#[must_use]
pub fn shard_physical_blob_len(blob_len: usize, stripe_size: usize, data_shards: usize) -> usize {
    stripe_count(blob_len, stripe_size) * HASH_LEN
        + shard_logical_len(blob_len, stripe_size, data_shards)
}

/// Byte offset of stripe `stripe_idx`'s frame within a shard body, using the
/// full-stripe `unit` (every preceding stripe is full). Design: 04 §1.2.
#[must_use]
pub fn frame_offset(stripe_idx: usize, unit: usize) -> usize {
    stripe_idx * (HASH_LEN + unit)
}

/// Maps a blob byte offset to its containing stripe index. `stripe_size > 0`.
#[must_use]
pub fn offset_to_stripe(offset: usize, stripe_size: usize) -> usize {
    debug_assert!(stripe_size > 0, "stripe_size must be > 0");
    offset / stripe_size
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRIPE: usize = 1 << 20; // 1 MiB

    #[test]
    fn unit_is_64_aligned_and_covers_stripe() {
        for n in [1usize, 6, 8, 12, 15] {
            let u = unit_size(STRIPE, n);
            assert_eq!(u % SHARD_ALIGN, 0, "unit must be 64-aligned for n={n}");
            assert!(u * n >= STRIPE, "n*unit must cover the stripe for n={n}");
        }
    }

    #[test]
    fn unit_specific_values() {
        assert_eq!(unit_size(STRIPE, 12), 87424);
        assert_eq!(unit_size(STRIPE, 6), 174784);
        assert_eq!(unit_size(STRIPE, 15), 69952);
        assert_eq!(unit_size(STRIPE, 1), STRIPE);
        assert_eq!(unit_size(0, 12), 0);
        assert_eq!(unit_size(1, 12), 64);
    }

    #[test]
    fn stripe_count_and_last_len_boundaries() {
        assert_eq!(stripe_count(0, STRIPE), 0);
        assert_eq!(last_stripe_len(0, STRIPE), 0);

        assert_eq!(stripe_count(1, STRIPE), 1);
        assert_eq!(last_stripe_len(1, STRIPE), 1);

        assert_eq!(stripe_count(STRIPE, STRIPE), 1);
        assert_eq!(last_stripe_len(STRIPE, STRIPE), STRIPE);

        assert_eq!(stripe_count(STRIPE + 1, STRIPE), 2);
        assert_eq!(last_stripe_len(STRIPE + 1, STRIPE), 1);

        assert_eq!(stripe_count(STRIPE - 1, STRIPE), 1);
        assert_eq!(last_stripe_len(STRIPE - 1, STRIPE), STRIPE - 1);

        assert_eq!(stripe_count(2 * STRIPE, STRIPE), 2);
        assert_eq!(last_stripe_len(2 * STRIPE, STRIPE), STRIPE);
    }

    #[test]
    fn physical_len_matches_frame_math() {
        let unit = 64;
        assert_eq!(shard_physical_len(0, unit), 0);
        assert_eq!(shard_physical_len(64, unit), HASH_LEN + 64);
        assert_eq!(shard_physical_len(10, unit), HASH_LEN + 10);
        assert_eq!(shard_physical_len(65, unit), 2 * HASH_LEN + 65);
    }

    #[test]
    fn shard_logical_len_sums_full_and_last() {
        let n = 12;
        let unit_full = unit_size(STRIPE, n);
        assert_eq!(shard_logical_len(0, STRIPE, n), 0);
        assert_eq!(shard_logical_len(STRIPE, STRIPE, n), unit_full);
        assert_eq!(shard_logical_len(STRIPE + 1, STRIPE, n), unit_full + 64);
        assert_eq!(
            shard_logical_len(STRIPE - 1, STRIPE, n),
            unit_size(STRIPE - 1, n)
        );
    }

    #[test]
    fn shard_physical_blob_len_adds_one_hash_per_stripe() {
        let n = 12;
        let unit_full = unit_size(STRIPE, n);
        assert_eq!(shard_physical_blob_len(0, STRIPE, n), 0);
        assert_eq!(
            shard_physical_blob_len(STRIPE, STRIPE, n),
            HASH_LEN + unit_full
        );
        assert_eq!(
            shard_physical_blob_len(3 * STRIPE, STRIPE, n),
            3 * (HASH_LEN + unit_full)
        );
        // A short final stripe still costs exactly one frame prefix.
        assert_eq!(
            shard_physical_blob_len(2 * STRIPE + 1, STRIPE, n),
            3 * HASH_LEN + 2 * unit_full + unit_size(1, n)
        );
    }

    #[test]
    fn frame_offset_and_offset_to_stripe() {
        let unit = 87424;
        assert_eq!(frame_offset(0, unit), 0);
        assert_eq!(frame_offset(3, unit), 3 * (HASH_LEN + unit));
        assert_eq!(offset_to_stripe(0, STRIPE), 0);
        assert_eq!(offset_to_stripe(STRIPE - 1, STRIPE), 0);
        assert_eq!(offset_to_stripe(STRIPE, STRIPE), 1);
        assert_eq!(offset_to_stripe(3 * STRIPE + 5, STRIPE), 3);
    }
}
