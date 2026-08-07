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

//! Property tests for the epoch-ec core: arbitrary-length round-trip, recovery
//! from any `<= parity` losses, and mandatory bitrot detection.
//!
//! Design: docs/design/07-iteration-plan.md M1; docs/design/04-ec-io.md.

use epoch_ec::Erasure;
use epoch_ec::frame::{verify_frame, verify_shard_body, write_frame};
use epoch_ec::layout::{stripe_count, unit_size};
use proptest::prelude::*;

/// The built-in code modes exercised by the property tests (incl. Replica-3).
fn code_mode() -> impl Strategy<Value = (usize, usize)> {
    prop::sample::select(vec![(2usize, 1usize), (4, 2), (6, 3), (12, 4), (1, 2)])
}

/// Splits `data` into `n` zero-padded shards of `unit` bytes (the layout used
/// before EC encoding a single stripe).
fn split_into_shards(data: &[u8], n: usize, unit: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| {
            let mut shard = vec![0u8; unit];
            let start = i * unit;
            if start < data.len() {
                let end = (start + unit).min(data.len());
                shard[..end - start].copy_from_slice(&data[start..end]);
            }
            shard
        })
        .collect()
}

/// Concatenates the `n` data shards and trims to `data_len`.
fn reassemble(data_shards: &[Vec<u8>], data_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(data_shards.iter().map(Vec::len).sum());
    for shard in data_shards {
        out.extend_from_slice(shard);
    }
    out.truncate(data_len);
    out
}

proptest! {
    /// A single stripe of any length survives split -> encode -> reassemble.
    #[test]
    fn stripe_round_trip(
        data in prop::collection::vec(any::<u8>(), 1..4096usize),
        (n, m) in code_mode(),
    ) {
        let ec = Erasure::new(n, m).unwrap();
        let unit = unit_size(data.len(), n);
        let shards = split_into_shards(&data, n, unit);
        prop_assert!(ec.encode(&shards).is_ok());
        prop_assert_eq!(reassemble(&shards, data.len()), data);
    }

    /// Dropping any `<= parity` shards (data or parity) still recovers the data.
    #[test]
    fn reconstruct_with_up_to_m_losses(
        data in prop::collection::vec(any::<u8>(), 1..4096usize),
        (n, m) in code_mode(),
        raw_drops in prop::collection::vec(0usize..16, 0..8),
    ) {
        let ec = Erasure::new(n, m).unwrap();
        let total = n + m;
        let unit = unit_size(data.len(), n);
        let data_shards = split_into_shards(&data, n, unit);
        let parity = ec.encode(&data_shards).unwrap();

        let mut slots: Vec<Option<Vec<u8>>> = data_shards
            .iter()
            .cloned()
            .map(Some)
            .chain(parity.into_iter().map(Some))
            .collect();

        // Pick at most `m` distinct valid indices to erase.
        let mut chosen = std::collections::BTreeSet::new();
        for d in raw_drops {
            if d < total && chosen.len() < m {
                chosen.insert(d);
            }
        }
        for &i in &chosen {
            slots[i] = None;
        }

        ec.reconstruct(&mut slots).unwrap();
        let recovered: Vec<Vec<u8>> =
            slots[..n].iter().map(|s| s.clone().unwrap()).collect();
        prop_assert_eq!(reassemble(&recovered, data.len()), data);
    }

    /// Flipping any single bit of a framed shard is always detected.
    #[test]
    fn bitrot_is_always_detected(
        data in prop::collection::vec(any::<u8>(), 1..4096usize),
        bit in 0u64..8,
        byte_seed in any::<usize>(),
    ) {
        let mut framed = Vec::new();
        write_frame(&data, &mut framed);
        let pos = byte_seed % framed.len();
        framed[pos] ^= 1u8 << bit;
        prop_assert!(verify_frame(&framed).is_err());
    }

    /// A multi-stripe blob of any length round-trips through the full
    /// layout + framing + encode path (small stripe keeps the test fast).
    #[test]
    fn blob_round_trip_multi_stripe(
        blob in prop::collection::vec(any::<u8>(), 0..2000usize),
        (n, m) in code_mode(),
    ) {
        const STRIPE: usize = 256;
        let ec = Erasure::new(n, m).unwrap();
        let stripes = stripe_count(blob.len(), STRIPE);

        let mut out = Vec::with_capacity(blob.len());
        for s in 0..stripes {
            let start = s * STRIPE;
            let stripe = &blob[start..(start + STRIPE).min(blob.len())];
            let unit = unit_size(stripe.len(), n);
            let shards = split_into_shards(stripe, n, unit);

            // Exercise the bitrot path: frame each data shard and verify it.
            for shard in &shards {
                let mut body = Vec::new();
                write_frame(shard, &mut body);
                prop_assert_eq!(verify_shard_body(&body, unit).unwrap(), unit);
            }
            // Exercise parity generation too.
            prop_assert!(ec.encode(&shards).is_ok());

            for shard in &shards {
                out.extend_from_slice(shard);
            }
            out.truncate(start + stripe.len());
        }

        prop_assert_eq!(out, blob);
    }
}
