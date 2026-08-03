//! criterion benchmarks for epoch-ec: EC12+4 encode/reconstruct and BLAKE3
//! bitrot framing throughput. Design: docs/design/07-iteration-plan.md M1.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use epoch_ec::Erasure;
use epoch_ec::frame::write_frame;
use epoch_ec::layout::unit_size;
use std::hint::black_box;

const STRIPE: usize = 1 << 20; // 1 MiB
const DATA: usize = 12;
const PARITY: usize = 4;

fn data_shards(unit: usize) -> Vec<Vec<u8>> {
    (0..DATA).map(|i| vec![i as u8; unit]).collect()
}

fn bench_encode(c: &mut Criterion) {
    let unit = unit_size(STRIPE, DATA);
    let ec = Erasure::new(DATA, PARITY).unwrap();
    let data = data_shards(unit);

    let mut group = c.benchmark_group("ec12+4");
    group.throughput(Throughput::Bytes(STRIPE as u64));
    group.bench_function("encode_1MiB", |b| {
        b.iter(|| black_box(ec.encode(black_box(&data)).unwrap()));
    });
    group.finish();
}

fn bench_reconstruct(c: &mut Criterion) {
    let unit = unit_size(STRIPE, DATA);
    let ec = Erasure::new(DATA, PARITY).unwrap();
    let data = data_shards(unit);
    let parity = ec.encode(&data).unwrap();

    // Worst-case data recovery: all PARITY data shards missing.
    let mut template: Vec<Option<Vec<u8>>> = data
        .iter()
        .cloned()
        .map(Some)
        .chain(parity.iter().cloned().map(Some))
        .collect();
    for slot in template.iter_mut().take(PARITY) {
        *slot = None;
    }

    let mut group = c.benchmark_group("ec12+4");
    group.throughput(Throughput::Bytes(STRIPE as u64));
    group.bench_function("reconstruct_4_missing_1MiB", |b| {
        b.iter_batched(
            || template.clone(),
            |mut slots| {
                ec.reconstruct(&mut slots).unwrap();
                black_box(slots);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_frame(c: &mut Criterion) {
    let unit = unit_size(STRIPE, DATA); // 87424
    let data = vec![0x5Au8; unit];

    let mut group = c.benchmark_group("bitrot");
    group.throughput(Throughput::Bytes(unit as u64));
    group.bench_function("blake3_write_frame", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(unit + 32);
            write_frame(black_box(&data), &mut out);
            black_box(out);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_encode, bench_reconstruct, bench_frame);
criterion_main!(benches);
