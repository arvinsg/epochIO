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

//! Byte-level helpers shared by epoch-store's on-disk format codecs
//! (superblock, extent header, blob records).
//!
//! Deliberately narrow — only fixed-layout field access lives here, not a
//! general utility bin (AGENTS §9.1a).

/// Copies a fixed-size `[u8; N]` field out of `buf` starting at `off`.
///
/// Callers pass a buffer already known to be long enough (`off + N <= buf.len()`,
/// guaranteed by a prior length check and the module's const offset table); a
/// violation is a programming error and panics on the slice index.
pub(crate) fn read_array<const N: usize>(buf: &[u8], off: usize) -> [u8; N] {
    let mut out = [0u8; N];
    out.copy_from_slice(&buf[off..off + N]);
    out
}
