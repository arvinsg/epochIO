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

//! Default sizing constants — the single definition point.
//!
//! These are *defaults*. The value actually in effect comes from the relevant
//! `CodeMode` (stripe/blob size) or the disk `superblock` (extent size); do NOT
//! reference these constants as fixed truths at runtime.
//! Design: docs/design/06-code-layout.md §1; docs/design/04-ec-io.md §2;
//! docs/design/02-datanode.md §1.1.

/// Default blob (object cut) size: 32 MiB.
pub const DEFAULT_BLOB_SIZE: u64 = 32 * 1024 * 1024;

/// Default EC stripe (coding unit) size: 1 MiB.
pub const DEFAULT_STRIPE_SIZE: u32 = 1024 * 1024;

/// Default on-disk extent container size: 32 GiB.
pub const DEFAULT_EXTENT_SIZE: u64 = 32 * 1024 * 1024 * 1024;

/// Default inline threshold for small objects: 128 KiB (`0` disables inline).
pub const DEFAULT_INLINE_THRESHOLD: u64 = 128 * 1024;

/// Hard upper bound for the inline threshold: 1 MiB.
pub const DEFAULT_INLINE_HARD_MAX: u64 = 1024 * 1024;

/// The namespace bit of a bucket id (03 §2/§4.1 归属不变量): hierarchical
/// buckets carry bit 63, flat buckets never do. PD allocates every bucket id
/// with this convention, so a flat partition's bucket span can never contain
/// hierarchical keys (and vice versa) in the *shared* column families
/// (`meta_seg`/`upload`/`delq`/`ttl`) — which is what makes per-CF range
/// export of a partition exact rather than approximate (03 §4.1 导出连续性).
pub const HIER_BUCKET_BIT: u64 = 1 << 63;

/// The well-known root directory inode of every hierarchical bucket (03 §6.5):
/// `f | bucket | ROOT_INO | ""` is the root sentinel and the path-walk origin,
/// initialized at CreateBucket.
pub const ROOT_INO: u64 = 1;

/// Bits reserved for the partition tag in the high end of an `ino` (03 §6.5:
/// 分区内单调计数器 + 分区标识高位). An ino is `partition_tag(high
/// [`INO_PARTITION_TAG_BITS`]) | counter(low bits)`; the tag pins an ino to the
/// partition that minted it, so a split (which only subdivides one partition's
/// counter space) never lets a child mint an ino colliding with a sibling's,
/// and a directory sentinel always lands in its own partition's range.
/// `ROOT_INO` uses tag 0 (the bootstrap partition).
pub const INO_PARTITION_TAG_BITS: u32 = 16;

/// The low-bit width of the monotonic counter inside an `ino` (the complement
/// of [`INO_PARTITION_TAG_BITS`]).
pub const INO_COUNTER_BITS: u32 = 64 - INO_PARTITION_TAG_BITS;

/// Composes an `ino` from its partition tag and per-partition counter value
/// (03 §6.5). Returns `None` if either field exceeds its bit width.
#[must_use]
pub fn compose_ino(partition_tag: u32, counter: u64) -> Option<u64> {
    if u64::from(partition_tag) >= (1 << INO_PARTITION_TAG_BITS)
        || counter >= (1 << INO_COUNTER_BITS)
    {
        return None;
    }
    Some((u64::from(partition_tag) << INO_COUNTER_BITS) | counter)
}

/// The partition tag encoded in an `ino`'s high bits (03 §6.5).
#[must_use]
pub fn ino_partition_tag(ino: u64) -> u32 {
    (ino >> INO_COUNTER_BITS) as u32
}

// Compile-time invariants on the default sizing constants (statically enforced;
// a violation fails the build).
const _: () = assert!(DEFAULT_STRIPE_SIZE as u64 <= DEFAULT_BLOB_SIZE);
const _: () = assert!(DEFAULT_BLOB_SIZE <= DEFAULT_EXTENT_SIZE);
const _: () = assert!(DEFAULT_INLINE_THRESHOLD <= DEFAULT_INLINE_HARD_MAX);
const _: () = assert!(DEFAULT_INLINE_HARD_MAX <= DEFAULT_BLOB_SIZE);
const _: () = assert!(INO_PARTITION_TAG_BITS < 64);
const _: () = assert!(ROOT_INO < (1 << INO_COUNTER_BITS));
