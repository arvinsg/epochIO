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

//! RocksDB open / lifecycle helpers.
//!
//! Thin wrappers that apply an option profile and map failures through
//! [`crate::error`]. Callers operate on the returned `rocksdb::DB` directly for
//! business reads/writes — no abstraction leakage (06 §5). [`open_cfs`] opens a
//! database with explicit column families for the PD/Meta state machine, which
//! partitions its key space by module (01 §2).
//!
//! Design: docs/design/06-code-layout.md §5; docs/design/02-datanode.md §1.3

use std::path::Path;

use rocksdb::{DB, Options};

use crate::error::Result;
use crate::options::{MergeFn, disk_index_options};

/// Opens (creating if absent) the per-disk blob index at `path` using the
/// [`disk_index_options`](crate::options::disk_index_options) profile.
///
/// `merge` installs the owner's associative merge operator for field-scoped
/// value mutations (see [`disk_index_options`]); pass `None` for plain opens.
///
/// # Errors
///
/// Returns [`RocksError`](crate::error::RocksError) if RocksDB cannot open the
/// database; an I/O or corruption failure is classified as
/// [`RocksError::DiskFailure`](crate::error::RocksError::DiskFailure).
pub fn open_disk_index(path: &Path, merge: Option<MergeFn>) -> Result<DB> {
    let opts = disk_index_options(merge);
    Ok(DB::open(&opts, path)?)
}

/// Opens (creating if absent) a database with an explicit set of column families
/// at `path`, applying `opts` as the database-level profile.
///
/// Used by the PD/Meta state machine, which partitions its key space by module
/// into column families (01 §2). `opts` must enable
/// `create_missing_column_families` — see
/// [`state_machine_options`](crate::options::state_machine_options).
///
/// # Errors
///
/// Returns [`RocksError`](crate::error::RocksError) if RocksDB cannot open the
/// database or any of its column families.
pub fn open_cfs(path: &Path, opts: &Options, cfs: &[&str]) -> Result<DB> {
    Ok(DB::open_cf(opts, path, cfs.iter().copied())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_put_get_delete_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_disk_index(dir.path(), None).expect("open");

        let key = b"b\x00\x00\x00\x01";
        db.put(key, b"idx").expect("put");
        assert_eq!(db.get(key).expect("get").as_deref(), Some(&b"idx"[..]));

        db.delete(key).expect("delete");
        assert!(db.get(key).expect("get").is_none());
    }

    #[test]
    fn data_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let db = open_disk_index(dir.path(), None).expect("open");
            db.put(b"e\x01", b"meta").expect("put");
        }
        let db = open_disk_index(dir.path(), None).expect("reopen");
        assert_eq!(
            db.get(b"e\x01").expect("get").as_deref(),
            Some(&b"meta"[..])
        );
    }

    #[test]
    fn prefix_range_scan_is_id_ordered() {
        use rocksdb::{Direction, IteratorMode};

        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_disk_index(dir.path(), None).expect("open");

        // Two `b`-prefixed records under the same extent, big-endian blob ids.
        db.put(b"b\x00\x00\x00\x02", b"two").expect("put");
        db.put(b"b\x00\x00\x00\x01", b"one").expect("put");

        let scanned: Vec<Vec<u8>> = db
            .iterator(IteratorMode::From(b"b", Direction::Forward))
            .map(|kv| kv.expect("iter").0.to_vec())
            .collect();
        assert_eq!(
            scanned,
            vec![b"b\x00\x00\x00\x01".to_vec(), b"b\x00\x00\x00\x02".to_vec()]
        );
    }

    #[test]
    fn open_cfs_isolates_column_families() {
        use crate::options::state_machine_options;

        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_cfs(dir.path(), &state_machine_options(), &["meta", "data"]).expect("open");

        let meta = db.cf_handle("meta").expect("meta cf");
        let data = db.cf_handle("data").expect("data cf");

        // The same key in two CFs holds independent values.
        db.put_cf(&meta, b"k", b"from-meta").expect("put meta");
        db.put_cf(&data, b"k", b"from-data").expect("put data");
        assert_eq!(
            db.get_cf(&meta, b"k").expect("get").as_deref(),
            Some(&b"from-meta"[..])
        );
        assert_eq!(
            db.get_cf(&data, b"k").expect("get").as_deref(),
            Some(&b"from-data"[..])
        );
    }

    #[test]
    fn cfs_survive_reopen() {
        use crate::options::state_machine_options;

        let dir = tempfile::tempdir().expect("tempdir");
        {
            let db = open_cfs(dir.path(), &state_machine_options(), &["meta"]).expect("open");
            let meta = db.cf_handle("meta").expect("cf");
            db.put_cf(&meta, b"applied", b"7").expect("put");
        }
        let db = open_cfs(dir.path(), &state_machine_options(), &["meta"]).expect("reopen");
        let meta = db.cf_handle("meta").expect("cf");
        assert_eq!(
            db.get_cf(&meta, b"applied").expect("get").as_deref(),
            Some(&b"7"[..])
        );
    }
}
