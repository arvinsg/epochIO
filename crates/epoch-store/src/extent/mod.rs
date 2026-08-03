//! Extent files: the append-only, 4 KiB-aligned blob record containers that
//! back one shard slot at a time.
//!
//! Design: docs/design/02-datanode.md §1.2/§1.5; docs/design/06-code-layout.md §6

pub mod file;
pub mod record;
pub mod state;
