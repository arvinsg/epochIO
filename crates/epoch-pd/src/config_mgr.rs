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

//! Cluster-level configuration KV (PD 配置中心, 01 §1 职责 7).
//!
//! [`ConfigManager`] owns the `config` column family of the PD state-machine
//! database and mirrors the other managers: replicated put/delete commands are
//! applied into a shared batch, and an in-memory index serves reads. Records
//! are the source of truth; the index is rebuilt by [`restore`](ConfigManager::restore).
//!
//! Values are opaque bytes; interpretation (codemode registry, watermarks,
//! feature flags) belongs to the readers. Keys are plain strings, ordered for
//! prefix scans.
//!
//! Design: docs/design/01-pd.md §1 (配置中心); §3 (EC 配置)

// The apply / recovery methods return openraft's intentionally-large
// `StorageError` (see the `raft` module): they run inside the raft state machine,
// so boxing it is not an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use rocksdb::{DB, WriteBatch};

use crate::cluster::id_record;
use crate::raft::SmError;

/// The `config` column family.
pub(crate) const CONFIG_CF: &str = "config";

/// Replicated config write command.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PutConfig {
    /// Config key (e.g. `placement.writable_target`).
    pub key: String,
    /// Opaque value bytes (reader-interpreted).
    pub value: Vec<u8>,
}

/// Replicated config delete command.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteConfig {
    /// Config key to remove.
    pub key: String,
}

/// Owns the `config` column family and the config index (cloneable; clones
/// share the same database and index).
#[derive(Clone)]
pub struct ConfigManager {
    db: Arc<DB>,
    index: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
}

impl ConfigManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted entries.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn cf(&self) -> Result<&rocksdb::ColumnFamily, SmError> {
        id_record::open_cf(&self.db, CONFIG_CF)
    }

    /// Rebuilds the in-memory index from the `config` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the column family is missing or an
    /// entry cannot be read.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let cf = self.cf()?;
        let mut index = self.write_index();
        index.clear();
        for kv in self.db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
            let (key, value) = kv.map_err(crate::raft::sm_read_err)?;
            index.insert(String::from_utf8_lossy(&key).into_owned(), value.to_vec());
        }
        Ok(())
    }

    /// Applies a put: stage the durable write and update the index.
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the durable write cannot be staged.
    pub(crate) fn apply_put(&self, batch: &mut WriteBatch, cmd: &PutConfig) -> Result<(), SmError> {
        batch.put_cf(self.cf()?, cmd.key.as_bytes(), &cmd.value);
        self.write_index()
            .insert(cmd.key.clone(), cmd.value.clone());
        Ok(())
    }

    /// Applies a delete (idempotent — a missing key applies cleanly).
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the durable write cannot be staged.
    pub(crate) fn apply_delete(
        &self,
        batch: &mut WriteBatch,
        cmd: &DeleteConfig,
    ) -> Result<(), SmError> {
        batch.delete_cf(self.cf()?, cmd.key.as_bytes());
        self.write_index().remove(&cmd.key);
        Ok(())
    }

    /// The value for `key`, if present.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.read_index().get(key).cloned()
    }

    /// Every `(key, value)` with `key` starting with `prefix`, in key order.
    #[must_use]
    pub fn list_prefix(&self, prefix: &str) -> Vec<(String, Vec<u8>)> {
        self.read_index()
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_manager() -> (tempfile::TempDir, ConfigManager) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[CONFIG_CF],
        )
        .expect("open db");
        (dir, ConfigManager::new(Arc::new(db)))
    }

    #[test]
    fn put_get_delete_round_trip_and_prefix_scan() {
        let (_dir, manager) = open_manager();
        let mut batch = WriteBatch::default();
        manager
            .apply_put(
                &mut batch,
                &PutConfig {
                    key: "placement.writable_target".into(),
                    value: b"8".to_vec(),
                },
            )
            .expect("put 1");
        manager
            .apply_put(
                &mut batch,
                &PutConfig {
                    key: "placement.interval_ms".into(),
                    value: b"5000".to_vec(),
                },
            )
            .expect("put 2");
        manager
            .apply_put(
                &mut batch,
                &PutConfig {
                    key: "feature.lrc".into(),
                    value: b"false".to_vec(),
                },
            )
            .expect("put 3");

        assert_eq!(
            manager.get("placement.writable_target"),
            Some(b"8".to_vec())
        );
        let placement = manager.list_prefix("placement.");
        assert_eq!(placement.len(), 2);
        assert_eq!(placement[0].0, "placement.interval_ms");

        manager
            .apply_delete(
                &mut batch,
                &DeleteConfig {
                    key: "feature.lrc".into(),
                },
            )
            .expect("delete");
        assert_eq!(manager.get("feature.lrc"), None);
        // Deleting a missing key is a clean no-op.
        manager
            .apply_delete(
                &mut batch,
                &DeleteConfig {
                    key: "feature.missing".into(),
                },
            )
            .expect("idempotent delete");
    }

    #[test]
    fn restore_rebuilds_the_index() {
        let (_dir, manager) = open_manager();
        let db = manager.db.clone();
        let mut batch = WriteBatch::default();
        manager
            .apply_put(
                &mut batch,
                &PutConfig {
                    key: "k".into(),
                    value: b"v".to_vec(),
                },
            )
            .expect("put");
        db.write(batch).expect("flush");

        let restored = ConfigManager::new(db);
        restored.restore().expect("restore");
        assert_eq!(restored.get("k"), Some(b"v".to_vec()));
    }
}
