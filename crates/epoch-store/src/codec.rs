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
