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

//! criterion benchmark for the M2 acceptance target: single-disk sequential
//! write throughput vs. the raw-file ceiling (07 §M2, "≥ 80% of raw speed").
//!
//! Three probes over the same byte volume, so criterion reports comparable
//! throughput:
//!   - `engine_per_blob_durable`  — [`StorageEngine::write`]: append → fsync →
//!     synced index commit, per blob (the real durability contract, 02 §1.7);
//!   - `raw_per_blob_fsync`       — raw positioned writes with an fsync per blob
//!     (same durability granularity → isolates framing/index overhead);
//!   - `raw_stream_single_fsync`  — one sequential pass, single trailing fsync
//!     (the raw sequential-bandwidth ceiling = "裸速").

use std::fs::File;
use std::os::unix::fs::FileExt;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use epoch_proto::consts::DEFAULT_EXTENT_SIZE;
use epoch_proto::{BlobId, ChunkId, DiskId, ExtentId, ShardId, WriterToken};
use epoch_store::{Disk, StorageEngine, Superblock};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const CLUSTER: u128 = 0x00C0_FFEE;
const BLOB_SIZE: usize = 1 << 20; // 1 MiB
const BLOB_COUNT: u32 = 16; // 16 MiB per iteration

fn shard() -> ShardId {
    ShardId::new(ChunkId::new(1), 0, 0)
}

fn setup_engine(rt: &Runtime) -> (TempDir, StorageEngine, ExtentId) {
    let dir = tempfile::tempdir().expect("tempdir");
    Disk::format(
        dir.path(),
        Superblock {
            disk_id: DiskId::new(1),
            cluster_id: CLUSTER,
            created_at: 0,
            flags: 0,
            extent_size: DEFAULT_EXTENT_SIZE,
        },
    )
    .expect("format");
    let engine = StorageEngine::open(dir.path(), CLUSTER).expect("open");
    let extent_id = rt
        .block_on(engine.create_extent(shard()))
        .expect("create extent");
    (dir, engine, extent_id)
}

fn setup_raw() -> (TempDir, File) {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = File::create(dir.path().join("raw")).expect("create raw");
    (dir, file)
}

fn seq_write(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let body = Bytes::from(vec![0xA5u8; BLOB_SIZE]);
    let mut group = c.benchmark_group("sequential_write");
    group.throughput(Throughput::Bytes(u64::from(BLOB_COUNT) * BLOB_SIZE as u64));
    group.sample_size(10);

    group.bench_function("engine_per_blob_durable", |b| {
        b.iter_batched(
            || setup_engine(&rt),
            |(dir, engine, extent_id)| {
                rt.block_on(async {
                    for seq in 0..BLOB_COUNT {
                        engine
                            .write(
                                extent_id,
                                BlobId::new(WriterToken::new(1), seq),
                                body.clone(),
                            )
                            .await
                            .expect("write");
                    }
                });
                (dir, engine) // returned → dropped outside the timed section
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("raw_per_blob_fsync", |b| {
        b.iter_batched(
            setup_raw,
            |(dir, file)| {
                let mut offset = 0u64;
                for _ in 0..BLOB_COUNT {
                    file.write_all_at(&body, offset).expect("write");
                    file.sync_all().expect("fsync");
                    offset += BLOB_SIZE as u64;
                }
                (dir, file)
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("raw_stream_single_fsync", |b| {
        b.iter_batched(
            setup_raw,
            |(dir, file)| {
                let mut offset = 0u64;
                for _ in 0..BLOB_COUNT {
                    file.write_all_at(&body, offset).expect("write");
                    offset += BLOB_SIZE as u64;
                }
                file.sync_all().expect("fsync");
                (dir, file)
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, seq_write);
criterion_main!(benches);
