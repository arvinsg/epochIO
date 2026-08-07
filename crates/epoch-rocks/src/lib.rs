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
