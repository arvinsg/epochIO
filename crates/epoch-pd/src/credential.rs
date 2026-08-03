//! Credential store and manager (PD, 01 §6 认证: v1 存 PD). Access-key →
//! secret-key records the gateway pulls to verify SigV4 locally (secrets never
//! leave the domain in a live request — the gateway fetches them from PD and
//! caches). Mirrors [`BucketManager`](crate::bucket::BucketManager): a `cred`
//! column family, replicated create, an in-memory index for reads.
//!
//! Scope (M6): root + static sub-account keys with an optional bucket
//! allow-list (IAM-lite). Policy engine / STS / bucket policy are out of scope
//! (01 §6 范围).
//!
//! Design: docs/design/01-pd.md §6 (Bucket 表与认证 N5)

// The apply / recovery methods return openraft's intentionally-large
// `StorageError`; boxing it is not an option inside the state machine. Scope
// the allow to this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use rocksdb::{DB, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use crate::cluster::id_record;
use crate::raft::SmError;

/// The `cred` column family (access_key → [`Credential`]).
pub(crate) const CRED_CF: &str = "cred";

/// A credential's console role (08 §4.1 IAM-lite): two tiers gate what the Web
/// Console shows and permits. `Readonly` sees the overview + usage sections;
/// `Admin` sees everything and may perform write operations. Orthogonal to the
/// S3 bucket allow-list ([`Credential::allowed_buckets`]) — the role governs the
/// console, the allow-list governs S3 data access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ConsoleRole {
    /// Read-only console access (the default, least privilege).
    #[default]
    Readonly,
    /// Full console access including write operations.
    Admin,
}

/// A stored credential: a secret key, its console role, and its bucket
/// authorization (01 §6 / 08 §4.1 IAM-lite). The access key is the record's CF
/// key, not a field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credential {
    /// The SigV4 secret key (verified at the gateway; kept in-domain).
    pub secret_key: String,
    /// Bucket authorization: `None` = all buckets (root/admin); `Some(list)` =
    /// only the named buckets are allowed (a static sub-account).
    pub allowed_buckets: Option<Vec<String>>,
    /// Console role (08 §4.1). Defaults to `Readonly` so an older persisted
    /// record (pre-role) decodes as least-privilege rather than admin.
    #[serde(default)]
    pub role: ConsoleRole,
}

impl Credential {
    /// Whether this credential may access `bucket` (01 §6 allow/deny).
    #[must_use]
    pub fn allows_bucket(&self, bucket: &str) -> bool {
        match &self.allowed_buckets {
            None => true,
            Some(list) => list.iter().any(|b| b == bucket),
        }
    }
}

/// Replicated credential-put command (create or replace by access key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutCredential {
    /// The access key (the record identity).
    pub access_key: String,
    /// The secret key.
    pub secret_key: String,
    /// Bucket allow-list (`None` = all buckets).
    pub allowed_buckets: Option<Vec<String>>,
    /// Console role (08 §4.1). `#[serde(default)]` so a command replayed from an
    /// older log entry (pre-role) decodes as `Readonly`.
    #[serde(default)]
    pub role: ConsoleRole,
}

/// Owns the `cred` column family and an access-key index (cloneable; clones
/// share the database and index).
#[derive(Clone)]
pub struct CredentialManager {
    db: Arc<DB>,
    index: Arc<RwLock<BTreeMap<String, Credential>>>,
}

impl CredentialManager {
    /// Creates a manager over `db` with an empty index; call
    /// [`restore`](Self::restore) to load persisted credentials.
    pub(crate) fn new(db: Arc<DB>) -> Self {
        Self {
            db,
            index: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn read_index(&self) -> RwLockReadGuard<'_, BTreeMap<String, Credential>> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_index(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Credential>> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn cf(&self) -> Result<&rocksdb::ColumnFamily, SmError> {
        id_record::open_cf(&self.db, CRED_CF)
    }

    /// Rebuilds the in-memory index from the `cred` column family.
    ///
    /// # Errors
    ///
    /// Returns a state-machine read error if the CF is missing or a record
    /// cannot be decoded.
    pub(crate) fn restore(&self) -> Result<(), SmError> {
        let cf = self.cf()?;
        let mut index = self.write_index();
        index.clear();
        for kv in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = kv.map_err(crate::raft::sm_read_err)?;
            let access_key = String::from_utf8(key.to_vec())
                .map_err(|_| crate::raft::sm_corrupt("bad credential key (non-utf8)"))?;
            let cred: Credential = serde_json::from_slice(&value)
                .map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
            index.insert(access_key, cred);
        }
        Ok(())
    }

    /// Applies a credential put (create or replace by access key). Not
    /// id-allocating, so a repeated apply is naturally idempotent (the record
    /// is overwritten with identical bytes).
    ///
    /// # Errors
    ///
    /// Returns a state-machine write error if the record cannot be serialized.
    pub(crate) fn apply_put(
        &self,
        batch: &mut WriteBatch,
        cmd: &PutCredential,
    ) -> Result<(), SmError> {
        let cred = Credential {
            secret_key: cmd.secret_key.clone(),
            allowed_buckets: cmd.allowed_buckets.clone(),
            role: cmd.role,
        };
        let value =
            serde_json::to_vec(&cred).map_err(|e| crate::raft::sm_corrupt(&e.to_string()))?;
        let cf = self.cf()?;
        batch.put_cf(cf, cmd.access_key.as_bytes(), value);
        self.write_index().insert(cmd.access_key.clone(), cred);
        Ok(())
    }

    /// The credential for an access key, if present (the gateway's SigV4
    /// secret-key lookup).
    #[must_use]
    pub fn get(&self, access_key: &str) -> Option<Credential> {
        self.read_index().get(access_key).cloned()
    }

    /// Every `(access_key, credential)`, in access-key order (gateway pull).
    #[must_use]
    pub fn list(&self) -> Vec<(String, Credential)> {
        self.read_index()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_manager() -> (tempfile::TempDir, CredentialManager) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = epoch_rocks::open_cfs(
            dir.path(),
            &epoch_rocks::state_machine_options(),
            &[CRED_CF],
        )
        .expect("open db");
        (dir, CredentialManager::new(Arc::new(db)))
    }

    fn put(manager: &CredentialManager, ak: &str, sk: &str, allowed: Option<Vec<String>>) {
        put_role(manager, ak, sk, allowed, ConsoleRole::Readonly);
    }

    fn put_role(
        manager: &CredentialManager,
        ak: &str,
        sk: &str,
        allowed: Option<Vec<String>>,
        role: ConsoleRole,
    ) {
        let mut batch = WriteBatch::default();
        manager
            .apply_put(
                &mut batch,
                &PutCredential {
                    access_key: ak.to_string(),
                    secret_key: sk.to_string(),
                    allowed_buckets: allowed,
                    role,
                },
            )
            .expect("put");
        manager.db.write(batch).expect("flush");
    }

    #[test]
    fn put_get_and_bucket_authorization() {
        let (_dir, m) = open_manager();
        put(&m, "AKROOT", "secret-root", None);
        put(&m, "AKSUB", "secret-sub", Some(vec!["data".to_string()]));

        let root = m.get("AKROOT").expect("root");
        assert_eq!(root.secret_key, "secret-root");
        assert!(root.allows_bucket("anything"), "root allows all buckets");

        let sub = m.get("AKSUB").expect("sub");
        assert!(sub.allows_bucket("data"));
        assert!(
            !sub.allows_bucket("logs"),
            "sub is scoped to its allow-list"
        );
        assert!(m.get("AKUNKNOWN").is_none());
    }

    #[test]
    fn put_is_idempotent_replace_and_restores() {
        let (dir, m) = open_manager();
        put(&m, "AK", "old", None);
        put(&m, "AK", "new", Some(vec!["b".to_string()]));
        assert_eq!(m.get("AK").expect("ak").secret_key, "new");
        assert_eq!(m.list().len(), 1, "replace does not duplicate");

        // Restore from the CF rebuilds the same index.
        let restored = CredentialManager::new(m.db.clone());
        restored.restore().expect("restore");
        let cred = restored.get("AK").expect("restored ak");
        assert_eq!(cred.secret_key, "new");
        assert_eq!(cred.allowed_buckets, Some(vec!["b".to_string()]));
        drop(dir);
    }

    #[test]
    fn role_defaults_readonly_and_round_trips() {
        let (dir, m) = open_manager();
        put(&m, "AKRO", "s", None); // default Readonly
        put_role(&m, "AKADM", "s", None, ConsoleRole::Admin);
        assert_eq!(m.get("AKRO").unwrap().role, ConsoleRole::Readonly);
        assert_eq!(m.get("AKADM").unwrap().role, ConsoleRole::Admin);

        // The role survives restore from the CF.
        let restored = CredentialManager::new(m.db.clone());
        restored.restore().expect("restore");
        assert_eq!(restored.get("AKADM").unwrap().role, ConsoleRole::Admin);
        drop(dir);
    }

    #[test]
    fn legacy_record_without_role_decodes_as_readonly() {
        // A record persisted before the `role` field existed (no `role` key) must
        // decode as the least-privilege default, not admin.
        let legacy = serde_json::json!({
            "secret_key": "s",
            "allowed_buckets": null
        });
        let cred: Credential = serde_json::from_value(legacy).expect("decode legacy");
        assert_eq!(cred.role, ConsoleRole::Readonly);
    }
}
