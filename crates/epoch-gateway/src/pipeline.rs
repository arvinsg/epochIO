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

//! The EC pipeline bridging a blob's logical bytes and its per-shard on-disk
//! bodies (04 §1.2 / §3): [`encode_blob`] stripes a blob, erasure-codes each
//! stripe, and bitrot-frames every unit into `data + parity` shard bodies;
//! [`decode_blob`] reverses it, dropping bitrot-failed or missing units per
//! stripe and reconstructing from the survivors.
//!
//! A shard body is the concatenation of one bitrot frame per stripe
//! (`[BLAKE3(32) | unit]`, the final unit possibly shorter); frame boundaries
//! are derived from the stripe `unit`, never stored (04 §1.2). This module is
//! pure computation — no transport, no I/O.
//!
//! Design: docs/design/04-ec-io.md §1.2/§3/§4.

use epoch_ec::Erasure;
use epoch_ec::frame;
use epoch_ec::layout::{self, HASH_LEN};

use crate::error::GatewayError;

/// The framed byte length of each stripe's frame in a shard body, in stripe
/// order (`HASH_LEN + unit`, the final stripe possibly shorter). The sum equals
/// [`layout::shard_physical_blob_len`], so this doubles as the send-chunk plan
/// for streaming a shard body one stripe frame at a time.
#[must_use]
pub fn stripe_frame_lens(blob_len: usize, stripe_size: usize, data_shards: usize) -> Vec<usize> {
    let stripes = layout::stripe_count(blob_len, stripe_size);
    if stripes == 0 {
        return Vec::new();
    }
    let unit_full = layout::unit_size(stripe_size, data_shards);
    (0..stripes)
        .map(|s| {
            let unit = if s == stripes - 1 {
                layout::unit_size(layout::last_stripe_len(blob_len, stripe_size), data_shards)
            } else {
                unit_full
            };
            HASH_LEN + unit
        })
        .collect()
}

/// Encodes one blob into its `data + parity` shard bodies.
///
/// Each stripe of `stripe_size` bytes is split into `data` zero-padded units,
/// erasure-coded into `parity` recovery units, and every unit is bitrot-framed
/// and appended to its shard's body. Every returned body has the same length
/// ([`layout::shard_physical_blob_len`]).
///
/// # Errors
///
/// [`GatewayError::Ec`] if the erasure backend rejects the stripe.
pub fn encode_blob(
    ec: &Erasure,
    stripe_size: usize,
    blob: &[u8],
) -> Result<Vec<Vec<u8>>, GatewayError> {
    let n = ec.data_shards();
    let total = ec.total_shards();
    let phys = layout::shard_physical_blob_len(blob.len(), stripe_size, n);
    let mut bodies: Vec<Vec<u8>> = (0..total).map(|_| Vec::with_capacity(phys)).collect();

    let stripes = layout::stripe_count(blob.len(), stripe_size);
    for s in 0..stripes {
        let start = s * stripe_size;
        let end = (start + stripe_size).min(blob.len());
        let stripe = &blob[start..end];
        let unit = layout::unit_size(stripe.len(), n);

        let mut data_units: Vec<Vec<u8>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut u = vec![0u8; unit];
            let ds = i * unit;
            if ds < stripe.len() {
                let de = (ds + unit).min(stripe.len());
                u[..de - ds].copy_from_slice(&stripe[ds..de]);
            }
            data_units.push(u);
        }

        let parity = ec.encode(&data_units)?;
        for (i, u) in data_units.iter().enumerate() {
            frame::write_frame(u, &mut bodies[i]);
        }
        for (k, p) in parity.iter().enumerate() {
            frame::write_frame(p, &mut bodies[n + k]);
        }
    }
    Ok(bodies)
}

/// A decoded blob plus the shard indices that had to be healed to decode it.
///
/// `healed` lists every shard index (`0..data+parity`) whose framed unit was
/// missing or bitrot-corrupt in at least one stripe — i.e. the shards a
/// heal-on-read report should flag to PD (04 §4). Empty when every shard was
/// clean. Distinguishes the two failure modes the outer `Option` slot conflates
/// (a missing shard vs. a present-but-corrupt one both count as healed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBlob {
    /// The reconstructed logical bytes.
    pub bytes: Vec<u8>,
    /// Shard indices that were missing/corrupt and reconstructed from survivors.
    pub healed: Vec<usize>,
}

/// Reconstructs a blob's `blob_len` logical bytes from its shard bodies, and
/// reports which shard indices had to be healed.
///
/// `bodies` holds `data + parity` slots in shard-index order: `Some` = the
/// body read from that shard, `None` = the shard is unavailable. For each
/// stripe, bodies whose frame is missing or fails its bitrot check are dropped;
/// the survivors reconstruct the data units, which are concatenated and trimmed
/// to the stripe's logical length. A shard index dropped in any stripe is
/// recorded in [`DecodedBlob::healed`] (the heal-on-read signal, 04 §4).
///
/// # Errors
///
/// - [`GatewayError::ShardCount`] if `bodies.len()` is not `data + parity`;
/// - [`GatewayError::Ec`] ([`EcError::TooFewShards`](epoch_ec::EcError::TooFewShards))
///   if a stripe has fewer than `data` clean shards.
pub fn decode_blob(
    ec: &Erasure,
    stripe_size: usize,
    blob_len: usize,
    bodies: &[Option<Vec<u8>>],
) -> Result<DecodedBlob, GatewayError> {
    let n = ec.data_shards();
    let total = ec.total_shards();
    if bodies.len() != total {
        return Err(GatewayError::ShardCount {
            need: total,
            got: bodies.len(),
        });
    }
    if blob_len == 0 {
        return Ok(DecodedBlob {
            bytes: Vec::new(),
            healed: Vec::new(),
        });
    }

    let stripes = layout::stripe_count(blob_len, stripe_size);
    let unit_full = layout::unit_size(stripe_size, n);
    let mut out = Vec::with_capacity(blob_len);
    // A shard index is "healed" if any stripe's frame for it was missing/corrupt.
    let mut bad = vec![false; total];

    for s in 0..stripes {
        let is_last = s == stripes - 1;
        let this_len = if is_last {
            layout::last_stripe_len(blob_len, stripe_size)
        } else {
            stripe_size
        };
        let unit = if is_last {
            layout::unit_size(this_len, n)
        } else {
            unit_full
        };
        // Every full stripe before this one occupies `HASH_LEN + unit_full`, so
        // the offset uses the full unit even for the shorter final stripe.
        let frame_off = layout::frame_offset(s, unit_full);
        let frame_len = HASH_LEN + unit;

        let mut units: Vec<Option<Vec<u8>>> = vec![None; total];
        for (j, body) in bodies.iter().enumerate() {
            let frame = body
                .as_ref()
                .and_then(|b| b.get(frame_off..frame_off + frame_len));
            match frame.map(frame::verify_frame) {
                // A clean, verified frame contributes its data unit.
                Some(Ok(data)) => units[j] = Some(data.to_vec()),
                // Missing frame or a failed bitrot check → this shard needs heal.
                _ => bad[j] = true,
            }
        }

        ec.reconstruct(&mut units)?;

        let mut stripe_data = Vec::with_capacity(n * unit);
        for u in &units[..n] {
            stripe_data.extend_from_slice(
                u.as_deref()
                    .expect("reconstruct fills every data unit on success"),
            );
        }
        out.extend_from_slice(&stripe_data[..this_len]);
    }

    // INVARIANT(design 04 §1.2): the decoded blob must be exactly `blob_len`.
    // A real check (not `debug_assert`, which release builds strip) so a stripe
    // math error can never surface as a silently short blob.
    if out.len() != blob_len {
        return Err(GatewayError::LengthMismatch {
            expected: blob_len as u64,
            got: out.len() as u64,
        });
    }
    let healed = (0..total).filter(|&j| bad[j]).collect();
    Ok(DecodedBlob { bytes: out, healed })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRIPE: usize = 128;

    fn ec() -> Erasure {
        Erasure::new(2, 1).unwrap()
    }

    fn blob_of(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn stripe_frame_lens_sum_matches_physical() {
        for &n in &[1usize, 2, 6] {
            for &len in &[1usize, 63, 64, 127, 128, 129, 256, 300, 1000] {
                let sum: usize = stripe_frame_lens(len, STRIPE, n).iter().sum();
                assert_eq!(
                    sum,
                    layout::shard_physical_blob_len(len, STRIPE, n),
                    "len={len}, n={n}"
                );
            }
        }
        assert!(stripe_frame_lens(0, STRIPE, 2).is_empty());
    }

    #[test]
    fn encode_bodies_are_uniform_and_physical_sized() {
        let ec = ec();
        for &len in &[10usize, 128, 200, 256, 300] {
            let bodies = encode_blob(&ec, STRIPE, &blob_of(len)).unwrap();
            assert_eq!(bodies.len(), ec.total_shards());
            let phys = layout::shard_physical_blob_len(len, STRIPE, ec.data_shards());
            for body in &bodies {
                assert_eq!(body.len(), phys, "len={len}");
            }
        }
    }

    #[test]
    fn round_trip_all_present() {
        let ec = ec();
        for &len in &[1usize, 64, 127, 128, 129, 200, 256, 300, 1000] {
            let blob = blob_of(len);
            let bodies = encode_blob(&ec, STRIPE, &blob).unwrap();
            let slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
            let got = decode_blob(&ec, STRIPE, len, &slots).unwrap();
            assert_eq!(got.bytes, blob, "len={len}");
            assert!(got.healed.is_empty(), "all shards clean → nothing healed");
        }
    }

    #[test]
    fn reconstructs_from_parity_when_a_data_shard_is_missing() {
        let ec = ec();
        let len = 300; // 3 stripes (128, 128, 44)
        let blob = blob_of(len);
        let bodies = encode_blob(&ec, STRIPE, &blob).unwrap();
        let mut slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
        slots[0] = None; // drop data shard 0
        let got = decode_blob(&ec, STRIPE, len, &slots).unwrap();
        assert_eq!(got.bytes, blob);
        assert_eq!(got.healed, vec![0], "missing shard 0 reported for heal");
    }

    #[test]
    fn drops_bitrot_corrupted_frame_and_reconstructs() {
        let ec = ec();
        let len = 300; // 3 stripes; corrupt only the final stripe of shard 0
        let blob = blob_of(len);
        let bodies = encode_blob(&ec, STRIPE, &blob).unwrap();
        let mut slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
        let body0 = slots[0].as_mut().unwrap();
        let last = body0.len() - 1;
        body0[last] ^= 0xFF; // corrupt the last stripe's frame on shard 0
        let got = decode_blob(&ec, STRIPE, len, &slots).unwrap();
        assert_eq!(got.bytes, blob);
        assert_eq!(got.healed, vec![0], "bitrot shard 0 reported for heal");
    }

    #[test]
    fn too_few_shards_is_an_error() {
        let ec = ec();
        let len = 200;
        let blob = blob_of(len);
        let bodies = encode_blob(&ec, STRIPE, &blob).unwrap();
        let mut slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
        slots[0] = None;
        slots[1] = None; // only parity left: 1 < data(2)
        assert!(matches!(
            decode_blob(&ec, STRIPE, len, &slots),
            Err(GatewayError::Ec(epoch_ec::EcError::TooFewShards {
                have: 1,
                need: 2
            }))
        ));
    }

    #[test]
    fn decode_rejects_wrong_shard_count() {
        let ec = ec();
        let slots = vec![None, None]; // need 3
        assert!(matches!(
            decode_blob(&ec, STRIPE, 10, &slots),
            Err(GatewayError::ShardCount { need: 3, got: 2 })
        ));
    }
}
