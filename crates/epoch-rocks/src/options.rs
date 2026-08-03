//! RocksDB option profiles.
//!
//! One tuned `Options` set per usage pattern (06 §5). Only the per-disk index
//! profile is exercised by M2; the `StateMachine` and `RaftLog` profiles land
//! with their consumers in M4/M5 (no speculative code, AGENTS §12b).
//!
//! Design: docs/design/06-code-layout.md §5; docs/design/02-datanode.md §1.3

use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, MergeOperands, Options};

/// A full-value merge function: given the existing value (if any) and the
/// ordered operand list, produce the new value. Injected by the caller so this
/// crate stays ignorant of business encodings (06 §5 "no abstraction leakage").
pub type MergeFn =
    fn(new_key: &[u8], existing: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>>;

/// Options for a per-disk blob index (02 §1.3): one small RocksDB instance per
/// physical disk. A node may mount dozens of disks, so each index keeps a
/// modest memory footprint and favors a fast codec (LZ4) over ratio.
///
/// Keys share one (default) column family under the `e` / `s` / `b` prefixes
/// (02 §1.3); range scans over `b{extent_id}{blob_id BE}` list a blob's records
/// in id order. The sizing values below are conservative starting points, not
/// tuned constants — revisit under the M2 storage bench / M9 tuning.
///
/// `merge` installs an associative merge operator used for field-scoped
/// extent-meta mutations (write path merges `size`, delete path merges
/// `deleted_bytes`, seal/compaction merge `status`) so concurrent mutators
/// never round-trip read-modify-write on the whole value (02 §1.6).
#[must_use]
pub fn disk_index_options(merge: Option<MergeFn>) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_compression_type(DBCompressionType::Lz4);

    // Bounded memory: dozens of these coexist on one node.
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts.set_max_open_files(128);

    let mut block_opts = BlockBasedOptions::default();
    let cache = Cache::new_lru_cache(16 * 1024 * 1024);
    block_opts.set_block_cache(&cache);
    block_opts.set_block_size(16 * 1024);
    opts.set_block_based_table_factory(&block_opts);

    if let Some(merge_fn) = merge {
        // Full merge and partial merge share the function: the operand
        // language must be associative (asserted by the owner's tests).
        opts.set_merge_operator("epoch-index-meta-merge", merge_fn, merge_fn);
    }

    opts
}

/// Options for a PD/Meta raft state-machine database (06 §5).
///
/// A single modest instance per service (≈4.6 GB at 1 EB, 01 §2) holding several
/// column families partitioned by module (chunk / disk / node / config / bucket
/// / …, 01 §2). Favors a fast codec (LZ4) over ratio like the disk index, and
/// enables `create_missing_column_families` so [`open_cfs`](crate::db::open_cfs)
/// can materialize module CFs on first open.
#[must_use]
pub fn state_machine_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts.set_max_open_files(256);
    opts
}

/// Options for an isolated raft-log database (Q1 / 03 §8).
///
/// The log lives in its own RocksDB instance, separate from the state machine,
/// so log churn never competes with state-machine compaction. openraft drives
/// log truncation and prefix purge explicitly; this base profile keeps writes
/// cheap (LZ4, small buffers). `create_missing_column_families` covers the
/// MetaNode multi-group layout (03 §8: `log` + `meta` CFs over one shared
/// instance); the FIFO / disabled-compaction tuning (the TiKV raftdb recipe)
/// is revisited under M9 rather than hard-coded here, where a wrong policy
/// could drop not-yet-purged log entries.
#[must_use]
pub fn raft_log_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts
}
