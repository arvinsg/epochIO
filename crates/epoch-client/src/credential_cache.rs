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

//! Credential cache: the gateway's access-key → secret-key lookup for local
//! SigV4 verification (01 §6 分发: gateway 周期拉取账号表, 秘钥不出域). The
//! gateway never round-trips PD per request — it pulls the credential table and
//! caches it, refreshing on a miss and periodically (like [`BucketCache`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::error::ClientError;
use crate::pd::PdClient;

/// A cached credential: the secret key and its bucket authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedCredential {
    /// The SigV4 secret key.
    pub secret_key: String,
    /// `None` = all buckets (root); `Some(list)` = only the named buckets.
    pub allowed_buckets: Option<Vec<String>>,
}

impl CachedCredential {
    /// Whether this credential may access `bucket` (01 §6 allow/deny).
    #[must_use]
    pub fn allows_bucket(&self, bucket: &str) -> bool {
        match &self.allowed_buckets {
            None => true,
            Some(list) => list.iter().any(|b| b == bucket),
        }
    }
}

/// An access-key → [`CachedCredential`] cache backed by PD `ListCredentials`.
///
/// Cheap to clone (shares the PD client and the map).
#[derive(Clone)]
pub struct CredentialCache {
    pd: PdClient,
    by_access_key: Arc<Mutex<HashMap<String, CachedCredential>>>,
}

impl CredentialCache {
    /// Builds an empty cache over a PD client.
    #[must_use]
    pub fn new(pd: PdClient) -> Self {
        Self {
            pd,
            by_access_key: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, CachedCredential>> {
        self.by_access_key
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The secret credential for an access key, refilling from PD on a miss.
    /// Returns `None` if the access key is unknown (an anonymous / invalid
    /// request — the SigV4 layer rejects it).
    ///
    /// # Errors
    /// PD leader/transport failures as [`ClientError`].
    pub async fn resolve(&self, access_key: &str) -> Result<Option<CachedCredential>, ClientError> {
        if let Some(cred) = self.map().get(access_key).cloned() {
            return Ok(Some(cred));
        }
        self.refresh().await?;
        Ok(self.map().get(access_key).cloned())
    }

    /// Refills the whole cache from PD (the ticker's periodic pull + miss path).
    ///
    /// # Errors
    /// PD leader/transport failures as [`ClientError`].
    pub async fn refresh(&self) -> Result<(), ClientError> {
        let creds = self.pd.list_credentials().await?;
        let fresh: HashMap<String, CachedCredential> = creds
            .into_iter()
            .map(|c| {
                let allowed_buckets = (!c.all_buckets).then_some(c.allowed_buckets);
                (
                    c.access_key,
                    CachedCredential {
                        secret_key: c.secret_key,
                        allowed_buckets,
                    },
                )
            })
            .collect();
        *self.map() = fresh;
        Ok(())
    }

    /// The cached entry for `access_key` without a refill (tests / observation).
    #[must_use]
    pub fn peek(&self, access_key: &str) -> Option<CachedCredential> {
        self.map().get(access_key).cloned()
    }
}
