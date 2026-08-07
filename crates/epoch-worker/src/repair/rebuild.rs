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

//! Rebuilding one EC shard's on-disk body from its stripe-peers (04 §5 repair
//! reconstruction). A repair subtask reads the surviving `data + parity` shard
//! bodies of a blob, and this pure function regenerates the *framed body* of a
//! single missing/corrupt shard index — the exact bytes the DataNode wrote
//! originally — so it can be written back to a healthy disk.
//!
//! This is distinct from the gateway's `decode_blob`, which reconstructs the
//! *logical blob*. Repair needs the opposite: reconstruct one shard's framed
//! body (`[BLAKE3(32) | unit]` per stripe, 04 §1.2). It reuses the same
//! `epoch-ec` primitives (`Erasure`, `frame`, `layout`) so the rebuilt body is
//! byte-identical to the original encode, but shares no code with the gateway
//! (which is a sibling L3 crate this L3 crate may not depend on).
//!
//! Reconstruct vs re-encode: `Erasure::reconstruct` refills missing *data*
//! units from ≥`data` survivors. To rebuild a *parity* index, we first
//! reconstruct all data units, then re-`encode` and take the target parity
//! unit. Either way the resulting unit is bitrot-framed identically to the
//! original.
//!
//! Design: docs/design/04-ec-io.md §1.2/§5

use epoch_ec::layout::{self, HASH_LEN};
use epoch_ec::{Erasure, frame};

use crate::subtask::SubtaskError;

/// Rebuilds the framed shard body for index `target` of a blob, from the blob's
/// surviving shard bodies.
///
/// `bodies` holds `data + parity` slots in shard-index order: `Some` = the
/// framed body read from that shard (may itself contain bitrot — verified and
/// dropped per stripe), `None` = the shard is unavailable. `target` is the index
/// to rebuild (`0..data+parity`); its slot in `bodies` is ignored (typically
/// `None`).
///
/// The stripe framing is inferred from a surviving body's physical length +
/// `stripe_size` (all shards of a blob share the same framing), so the caller
/// needs no object metadata — the DataNode's shard bodies are self-describing
/// for repair (04 §1.2). Returns the rebuilt body: one bitrot frame per stripe,
/// byte-identical to what the original encode produced for index `target`.
///
/// # Errors
///
/// - [`SubtaskError::Failed`] if `bodies.len()` is not `data + parity`, `target`
///   is out of range, or a survivor body's length is not a valid framing;
/// - [`SubtaskError::NotReady`] if no clean survivor exists to size the framing,
///   or a stripe has fewer than `data` clean survivors (a transient shortage a
///   later sweep may resolve once more shards come back).
pub fn rebuild_shard_body(
    ec: &Erasure,
    stripe_size: usize,
    target: usize,
    bodies: &[Option<Vec<u8>>],
) -> Result<Vec<u8>, SubtaskError> {
    let n = ec.data_shards();
    let total = ec.total_shards();
    if bodies.len() != total {
        return Err(SubtaskError::Failed(format!(
            "shard count: need {total}, got {}",
            bodies.len()
        )));
    }
    if target >= total {
        return Err(SubtaskError::Failed(format!(
            "target index {target} out of range 0..{total}"
        )));
    }

    // Every shard body of a blob shares the same physical length; take it from
    // any surviving body (skipping the rebuild target). No survivor ⇒ not ready.
    let phys = bodies
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != target)
        .find_map(|(_, b)| b.as_ref().map(Vec::len))
        .ok_or_else(|| SubtaskError::NotReady("no surviving shard body to size framing".into()))?;
    if phys == 0 {
        return Ok(Vec::new()); // an empty blob has no stripes
    }

    let frame_lens = stripe_frame_lens_from_phys(phys, stripe_size, n)?;
    let mut out = Vec::with_capacity(phys);
    let mut frame_off = 0;
    for &frame_len in &frame_lens {
        // Collect this stripe's units from the survivors (bitrot-verified).
        let mut units: Vec<Option<Vec<u8>>> = vec![None; total];
        for (j, body) in bodies.iter().enumerate() {
            if j == target {
                continue; // never trust the slot we are rebuilding
            }
            let Some(body) = body else { continue };
            let Some(frame) = body.get(frame_off..frame_off + frame_len) else {
                continue;
            };
            if let Ok(data) = frame::verify_frame(frame) {
                units[j] = Some(data.to_vec());
            }
        }

        let rebuilt_unit = rebuild_stripe_unit(ec, target, &mut units)?;
        frame::write_frame(&rebuilt_unit, &mut out);
        frame_off += frame_len;
    }

    debug_assert_eq!(out.len(), phys);
    Ok(out)
}

/// Parses a shard body's per-stripe frame byte-lengths from its physical length.
///
/// Every non-final stripe frames to `HASH_LEN + unit_full`; the final stripe may
/// be shorter (`HASH_LEN + last_unit`, `last_unit` a positive multiple of
/// [`SHARD_ALIGN`](layout) ≤ `unit_full`). Consuming full frames until one frame
/// remains recovers the framing deterministically. Errors if `phys` is not a
/// valid framing for `(stripe_size, data)`.
fn stripe_frame_lens_from_phys(
    phys: usize,
    stripe_size: usize,
    data: usize,
) -> Result<Vec<usize>, SubtaskError> {
    let unit_full = layout::unit_size(stripe_size, data);
    let full_frame = HASH_LEN + unit_full;
    let mut lens = Vec::new();
    let mut remaining = phys;
    // Every stripe but the last is exactly one full frame.
    while remaining > full_frame {
        lens.push(full_frame);
        remaining -= full_frame;
    }
    // The final frame is whatever is left: `HASH_LEN + last_unit`, with
    // `last_unit` a positive multiple of the alignment and no larger than a full
    // unit. Anything else means the body length is not a valid framing.
    let bad = || SubtaskError::Failed(format!("shard body length {phys} is not a valid framing"));
    if remaining <= HASH_LEN {
        return Err(bad());
    }
    let last_unit = remaining - HASH_LEN;
    if last_unit == 0 || last_unit > unit_full || !last_unit.is_multiple_of(layout::SHARD_ALIGN) {
        return Err(bad());
    }
    lens.push(remaining);
    Ok(lens)
}

/// Rebuilds one stripe's unit for index `target` from its surviving units.
/// `units` holds `data + parity` slots (`Some` = clean survivor). For a data
/// index, `reconstruct` fills it directly; for a parity index, reconstruct all
/// data then re-encode and take the target parity unit.
fn rebuild_stripe_unit(
    ec: &Erasure,
    target: usize,
    units: &mut [Option<Vec<u8>>],
) -> Result<Vec<u8>, SubtaskError> {
    let n = ec.data_shards();
    // `reconstruct` needs ≥ data survivors; a shortfall is transient (a later
    // sweep may find more shards) rather than a hard failure.
    let present = units.iter().filter(|u| u.is_some()).count();
    if present < n {
        return Err(SubtaskError::NotReady(format!(
            "stripe has {present} clean shards, need {n}"
        )));
    }
    ec.reconstruct(units)
        .map_err(|e| SubtaskError::Failed(format!("reconstruct: {e}")))?;

    if target < n {
        // Data index: reconstruct already refilled it.
        return units[target]
            .clone()
            .ok_or_else(|| SubtaskError::Failed("data unit missing after reconstruct".into()));
    }
    // Parity index: re-encode from the (now complete) data units and take it.
    let data_units: Vec<Vec<u8>> = units[..n]
        .iter()
        .map(|u| {
            u.clone()
                .ok_or_else(|| SubtaskError::Failed("data unit missing after reconstruct".into()))
        })
        .collect::<Result<_, _>>()?;
    let parity = ec
        .encode(&data_units)
        .map_err(|e| SubtaskError::Failed(format!("re-encode parity: {e}")))?;
    parity
        .into_iter()
        .nth(target - n)
        .ok_or_else(|| SubtaskError::Failed("parity index out of range after encode".into()))
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

    /// Re-implements the encode side (mirrors the gateway's `encode_blob`) so the
    /// test can produce reference shard bodies and check the rebuilt one matches.
    fn encode_bodies(ec: &Erasure, blob: &[u8]) -> Vec<Vec<u8>> {
        let n = ec.data_shards();
        let total = ec.total_shards();
        let mut bodies: Vec<Vec<u8>> = (0..total).map(|_| Vec::new()).collect();
        let stripes = layout::stripe_count(blob.len(), STRIPE);
        for s in 0..stripes {
            let start = s * STRIPE;
            let end = (start + STRIPE).min(blob.len());
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
            let parity = ec.encode(&data_units).unwrap();
            for (i, u) in data_units.iter().enumerate() {
                frame::write_frame(u, &mut bodies[i]);
            }
            for (k, p) in parity.iter().enumerate() {
                frame::write_frame(p, &mut bodies[n + k]);
            }
        }
        bodies
    }

    #[test]
    fn rebuilds_a_missing_data_shard_byte_identically() {
        let ec = ec();
        for &len in &[1usize, 64, 128, 200, 300, 1000] {
            let blob = blob_of(len);
            let bodies = encode_bodies(&ec, &blob);
            let original = bodies[0].clone();
            // Shard 0 (data) is gone.
            let mut slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
            slots[0] = None;
            let rebuilt = rebuild_shard_body(&ec, STRIPE, 0, &slots).unwrap();
            assert_eq!(rebuilt, original, "len={len}");
        }
    }

    #[test]
    fn rebuilds_a_missing_parity_shard_byte_identically() {
        let ec = ec();
        for &len in &[1usize, 128, 300, 1000] {
            let blob = blob_of(len);
            let bodies = encode_bodies(&ec, &blob);
            let original = bodies[2].clone(); // parity index (n=2)
            let mut slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
            slots[2] = None;
            let rebuilt = rebuild_shard_body(&ec, STRIPE, 2, &slots).unwrap();
            assert_eq!(rebuilt, original, "len={len}");
        }
    }

    #[test]
    fn too_few_survivors_is_not_ready() {
        let ec = ec();
        let len = 200;
        let blob = blob_of(len);
        let bodies = encode_bodies(&ec, &blob);
        let mut slots: Vec<Option<Vec<u8>>> = bodies.into_iter().map(Some).collect();
        slots[0] = None; // target
        slots[1] = None; // only parity left: 1 < data(2)
        assert!(matches!(
            rebuild_shard_body(&ec, STRIPE, 0, &slots),
            Err(SubtaskError::NotReady(_))
        ));
    }

    #[test]
    fn no_survivor_body_is_not_ready() {
        let ec = ec();
        // Every non-target slot empty: nothing to size the framing from.
        let slots = vec![None, None, None];
        assert!(matches!(
            rebuild_shard_body(&ec, STRIPE, 0, &slots),
            Err(SubtaskError::NotReady(_))
        ));
    }

    #[test]
    fn rejects_wrong_shard_count_and_bad_target() {
        let ec = ec();
        assert!(matches!(
            rebuild_shard_body(&ec, STRIPE, 0, &[None, None]),
            Err(SubtaskError::Failed(_))
        ));
        // Target out of range is rejected before the survivor check.
        let slots = vec![Some(vec![0u8; 96]), None, None];
        assert!(matches!(
            rebuild_shard_body(&ec, STRIPE, 9, &slots),
            Err(SubtaskError::Failed(_))
        ));
    }
}
