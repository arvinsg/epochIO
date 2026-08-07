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

//! Contiguous shard buffers for one stripe.
//!
//! Holds a stripe's `N+M` shards in a single allocation (cache-friendly and
//! reusable by a pool) while exposing them as `count` disjoint `unit`-sized
//! slices. RS coding is position-dependent, so slot index equals shard index.
//!
//! Design: docs/design/04-ec-io.md §4; docs/design/06-code-layout.md §3

/// A single contiguous allocation of `count` equal-length shard slots.
#[derive(Debug)]
pub struct ShardBuffers {
    backing: Vec<u8>,
    shard_len: usize,
    count: usize,
}

impl ShardBuffers {
    /// Allocates `count` zeroed shard slots of `shard_len` bytes each.
    ///
    /// Both `count` and `shard_len` must be `> 0`.
    #[must_use]
    pub fn new(count: usize, shard_len: usize) -> Self {
        debug_assert!(
            count > 0 && shard_len > 0,
            "count and shard_len must be > 0"
        );
        Self {
            backing: vec![0u8; count * shard_len],
            shard_len,
            count,
        }
    }

    /// Number of shard slots.
    #[must_use]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Length in bytes of each shard slot.
    #[must_use]
    pub fn shard_len(&self) -> usize {
        self.shard_len
    }

    /// Borrows shard slot `i`.
    #[must_use]
    pub fn shard(&self, i: usize) -> &[u8] {
        let start = i * self.shard_len;
        &self.backing[start..start + self.shard_len]
    }

    /// Mutably borrows shard slot `i`.
    pub fn shard_mut(&mut self, i: usize) -> &mut [u8] {
        let start = i * self.shard_len;
        &mut self.backing[start..start + self.shard_len]
    }

    /// Iterates all shard slots in index order.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.backing.chunks_exact(self.shard_len)
    }

    /// Zeroes every slot so the allocation can be reused by a pool.
    pub fn clear(&mut self) {
        self.backing.fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_disjoint_and_sized() {
        let mut buffers = ShardBuffers::new(16, 64);
        assert_eq!(buffers.count(), 16);
        assert_eq!(buffers.shard_len(), 64);

        buffers.shard_mut(0).fill(0xAA);
        buffers.shard_mut(15).fill(0xBB);
        assert!(buffers.shard(0).iter().all(|&b| b == 0xAA));
        assert!(buffers.shard(15).iter().all(|&b| b == 0xBB));
        assert!(buffers.shard(1).iter().all(|&b| b == 0), "slot 1 untouched");
        assert_eq!(buffers.iter().count(), 16);
    }

    #[test]
    fn clear_zeroes_all_slots() {
        let mut buffers = ShardBuffers::new(4, 32);
        buffers.shard_mut(2).fill(0xFF);
        buffers.clear();
        assert!(buffers.iter().flatten().all(|&b| b == 0));
    }
}
