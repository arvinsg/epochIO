//! epoch-rocks (L1): thin RocksDB wrapper shared by four call sites
//! (PD state machine, Meta state machine, Meta raft log, per-disk blob index).
//!
//! Encapsulates option profiles, column-family lifecycle, metric hooks and
//! error mapping only — callers operate on the `rocksdb::DB` directly for
//! business reads/writes (no abstraction leakage).
//!
//! M2 delivers the per-disk index path ([`open_disk_index`] +
//! [`disk_index_options`]); the state-machine / raft-log profiles and metric
//! export land with their consumers in M4/M5 (docs/design/07-iteration-plan.md).
//!
//! Design: docs/design/06-code-layout.md §5; docs/design/03-metanode.md §8

pub mod db;
pub mod error;
pub mod options;

pub use db::{open_cfs, open_disk_index};
pub use error::{Result, RocksError};
pub use options::{MergeFn, disk_index_options, raft_log_options, state_machine_options};
