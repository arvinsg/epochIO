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

//! Dual-engine equivalence (03 §7: trait 双实现互为验证). Feeds an identical
//! deterministic op sequence to [`RocksEngine`] and [`MemEngine`] and asserts
//! `get` / `scan` / `snapshot` agree byte-for-byte — the property that lets a
//! bucket switch engines without any behavioral change (M5b MemEngine gray
//! rollout).

use epoch_meta::store::keys::{bucket_prefix, flat_key, suffix};
use epoch_meta::store::{KeyRange, MemEngine, MetaCf, MetaStore, RocksEngine, StoreOp};
use epoch_proto::BucketId;

/// A deterministic pseudo-random op stream (no `rand` dependency; a simple LCG
/// keyed by the step index so both engines and every run see the same
/// sequence). Mixes puts and deletes across CFs, buckets, and keys, with
/// occasional exact-key overwrites/redeletes to exercise idempotency.
fn op_sequence(steps: u64) -> Vec<StoreOp> {
    let cfs = [MetaCf::Meta, MetaCf::MetaSeg, MetaCf::Delq, MetaCf::Fs];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        // xorshift64* — deterministic, decent spread.
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    let mut ops = Vec::with_capacity(steps as usize);
    for _ in 0..steps {
        let r = next();
        let cf = cfs[(r % cfs.len() as u64) as usize];
        let bucket = BucketId::new(1 + (r >> 3) % 3);
        // A small key space (0..32) so overwrites and re-deletes collide often.
        let key_id = (r >> 8) % 32;
        let key = flat_key(
            cf,
            bucket,
            format!("k{key_id:02}").as_bytes(),
            &suffix::seg_no((r >> 16) as u32 % 3),
        );
        if r % 5 == 0 {
            ops.push(StoreOp::delete(cf, key));
        } else {
            let value = format!("v{}", r % 1000).into_bytes();
            ops.push(StoreOp::put(cf, key, value));
        }
    }
    ops
}

fn assert_engines_agree(rocks: &RocksEngine, mem: &MemEngine) {
    // Full per-CF scan comparison covers get for every present key plus order.
    for cf in [MetaCf::Meta, MetaCf::MetaSeg, MetaCf::Delq, MetaCf::Fs] {
        let start = vec![cf.tag()];
        let end = vec![cf.tag() + 1];
        let r = rocks
            .scan(cf, &start, &end, usize::MAX)
            .expect("rocks scan");
        let m = mem.scan(cf, &start, &end, usize::MAX).expect("mem scan");
        assert_eq!(r, m, "cf {cf:?} scan disagreement");
    }
}

#[test]
fn engines_agree_on_a_long_op_sequence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rocks = RocksEngine::open(dir.path()).expect("open rocks");
    let mem = MemEngine::new();

    let ops = op_sequence(4000);
    // Apply in small batches (mimicking raft entries), checking agreement
    // periodically so a divergence points at the batch that caused it.
    for (i, chunk) in ops.chunks(16).enumerate() {
        rocks.apply(chunk).expect("rocks apply");
        mem.apply(chunk).expect("mem apply");
        if i % 32 == 0 {
            assert_engines_agree(&rocks, &mem);
        }
    }
    assert_engines_agree(&rocks, &mem);

    // Re-applying the whole sequence is a no-op on both (03 §7 idempotent) and
    // must keep them in agreement.
    rocks.apply(&ops).expect("rocks replay");
    mem.apply(&ops).expect("mem replay");
    assert_engines_agree(&rocks, &mem);
}

#[test]
fn engines_agree_on_get_and_limited_scan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rocks = RocksEngine::open(dir.path()).expect("open rocks");
    let mem = MemEngine::new();
    let ops = op_sequence(500);
    rocks.apply(&ops).expect("rocks");
    mem.apply(&ops).expect("mem");

    // Point lookups over the whole small key space, both CFs.
    for cf in [MetaCf::Meta, MetaCf::Fs] {
        for bucket_id in 1..=3u64 {
            let bucket = BucketId::new(bucket_id);
            for key_id in 0..32u64 {
                let key = flat_key(
                    cf,
                    bucket,
                    format!("k{key_id:02}").as_bytes(),
                    &suffix::seg_no(0),
                );
                assert_eq!(
                    rocks.get(cf, &key).expect("rocks get"),
                    mem.get(cf, &key).expect("mem get"),
                    "get disagreement cf={cf:?} bucket={bucket_id} key={key_id}"
                );
            }
        }
    }

    // A limited scan returns the same first-N page from both.
    let bucket = BucketId::new(1);
    let start = bucket_prefix(MetaCf::Meta, bucket);
    let mut end = start.clone();
    *end.last_mut().expect("non-empty") += 1;
    assert_eq!(
        rocks.scan(MetaCf::Meta, &start, &end, 5).expect("rocks"),
        mem.scan(MetaCf::Meta, &start, &end, 5).expect("mem"),
        "limited scan disagreement"
    );
}

#[test]
fn engines_agree_on_range_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rocks = RocksEngine::open(dir.path()).expect("open rocks");
    let mem = MemEngine::new();
    let ops = op_sequence(1000);
    rocks.apply(&ops).expect("rocks");
    mem.apply(&ops).expect("mem");

    let ranges: Vec<KeyRange> = [MetaCf::Meta, MetaCf::MetaSeg, MetaCf::Delq, MetaCf::Fs]
        .iter()
        .map(|&cf| KeyRange {
            cf,
            start: bucket_prefix(cf, BucketId::new(1)),
            end: bucket_prefix(cf, BucketId::new(3)),
        })
        .collect();
    let r = rocks.snapshot(&ranges).expect("rocks snapshot");
    let m = mem.snapshot(&ranges).expect("mem snapshot");
    assert_eq!(r.entries, m.entries, "range snapshot disagreement");
}

/// The D2 streaming export (`export_page`) yields the same entries as the
/// collected `snapshot`, in the same order, on both engines — and paging at any
/// small limit reassembles identically (bounded-memory migration transfer).
#[test]
fn export_page_streams_the_same_entries_as_snapshot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let rocks = RocksEngine::open(dir.path()).expect("open rocks");
    let mem = MemEngine::new();
    let ops = op_sequence(1500);
    rocks.apply(&ops).expect("rocks");
    mem.apply(&ops).expect("mem");

    let ranges: Vec<KeyRange> = [MetaCf::Meta, MetaCf::MetaSeg, MetaCf::Delq, MetaCf::Fs]
        .iter()
        .map(|&cf| KeyRange {
            cf,
            start: bucket_prefix(cf, BucketId::new(1)),
            end: bucket_prefix(cf, BucketId::new(4)),
        })
        .collect();

    // Drain the streaming export in small pages (page size 7 to force many
    // range-boundary crossings) and compare to the collected snapshot.
    let drain = |engine: &dyn MetaStore| -> Vec<(MetaCf, Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let page = engine
                .export_page(&ranges, cursor.as_ref(), 7)
                .expect("export page");
            out.extend(page.entries);
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        out
    };

    let rocks_snapshot = rocks.snapshot(&ranges).expect("rocks snapshot").entries;
    let rocks_streamed = drain(&rocks);
    assert_eq!(
        rocks_streamed, rocks_snapshot,
        "rocks: paged export != snapshot"
    );

    let mem_streamed = drain(&mem);
    assert_eq!(
        mem_streamed, rocks_snapshot,
        "mem paged export != rocks snapshot"
    );

    // A single huge page equals the collected snapshot too (no cursor path).
    let one_shot = rocks
        .export_page(&ranges, None, usize::MAX)
        .expect("one-shot export");
    assert!(one_shot.next.is_none());
    assert_eq!(one_shot.entries, rocks_snapshot);
}
