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

//! Console authentication + session store (08 §4.1): AK/SK login → an
//! in-memory session token in an HttpOnly cookie. Sessions live **only in
//! leader memory** (08 §8: no raft, no persistence) — a failover invalidates
//! them and the browser re-logs-in.
//!
//! Login is an AK/SK equality check, not SigV4: the console runs inside PD with
//! direct [`CredentialManager`](crate::credential::CredentialManager) access, so
//! it compares the presented secret against the stored one (constant-time) and
//! reads the credential's [`ConsoleRole`]. SigV4 exists to avoid sending the
//! secret each request; a login form submits it once over TLS.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crate::credential::ConsoleRole;

/// Session lifetime: a token is valid this many milliseconds after issue.
pub const SESSION_TTL_MILLIS: u64 = 12 * 60 * 60 * 1000; // 12h

/// The cookie name carrying the session token.
pub const SESSION_COOKIE: &str = "epochio_console";

/// A logged-in console session (leader memory only). The token is the cookie
/// value; this is what it maps to.
#[derive(Debug, Clone)]
pub struct Session {
    /// The authenticated access key.
    pub access_key: String,
    /// The credential's console role (gates admin writes).
    pub role: ConsoleRole,
    /// Absolute expiry in epoch millis.
    pub expiry_millis: u64,
}

/// In-memory session table (token → session). Cloneable; clones share the map.
/// Mirrors the house pattern (`Arc<RwLock<HashMap>>`, poison-tolerant) used by
/// `HeartbeatTracker` / the PD managers.
#[derive(Clone, Default)]
pub struct SessionStore {
    inner: Arc<RwLock<HashMap<String, Session>>>,
}

impl SessionStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Issues a session for `access_key`/`role`, returning the opaque token to
    /// set as the cookie. The token is 256 bits of OS entropy, hex-encoded.
    #[must_use]
    pub fn issue(&self, access_key: String, role: ConsoleRole, now_millis: u64) -> String {
        let token = fresh_token();
        let session = Session {
            access_key,
            role,
            expiry_millis: now_millis.saturating_add(SESSION_TTL_MILLIS),
        };
        self.inner
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(token.clone(), session);
        token
    }

    /// Resolves a token to its live session, or `None` if unknown or expired.
    /// An expired token is evicted lazily on lookup (no sweep needed at v1
    /// scale; a background sweep is a follow-up if the map ever grows large).
    #[must_use]
    pub fn resolve(&self, token: &str, now_millis: u64) -> Option<Session> {
        // Fast path: read lock.
        {
            let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            match guard.get(token) {
                Some(s) if s.expiry_millis > now_millis => return Some(s.clone()),
                Some(_) => {} // expired — fall through to evict
                None => return None,
            }
        }
        // Slow path: evict the expired entry under the write lock.
        self.inner
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(token);
        None
    }

    /// Drops a session (logout), idempotent.
    pub fn revoke(&self, token: &str) {
        self.inner
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(token);
    }
}

/// A 256-bit hex session token from OS entropy. `getrandom` failure is
/// unrecoverable (no safe fallback for a security token), so it panics — the
/// process cannot mint sessions without entropy.
fn fresh_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("OS entropy for session token");
    let mut s = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Extracts the session token from a `Cookie` header value (finds the
/// [`SESSION_COOKIE`] pair among `; `-separated cookies). Returns `None` if
/// absent.
#[must_use]
pub fn token_from_cookie_header(header: &str) -> Option<&str> {
    header.split(';').map(str::trim).find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == SESSION_COOKIE).then_some(value)
    })
}

/// Whether a presented secret matches the stored one, in constant time (avoids a
/// timing side channel on the secret).
#[must_use]
pub fn secret_matches(stored: &str, presented: &str) -> bool {
    let (a, b) = (stored.as_bytes(), presented.as_bytes());
    // Length differing is itself distinguishing, but the secret length is not
    // itself secret; fold length into the result and compare all bytes.
    let mut diff = (a.len() ^ b.len()) as u8;
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_resolve_and_expire() {
        let store = SessionStore::new();
        let tok = store.issue("AK".into(), ConsoleRole::Admin, 1_000);
        let s = store.resolve(&tok, 1_000).expect("live");
        assert_eq!(s.access_key, "AK");
        assert_eq!(s.role, ConsoleRole::Admin);
        // Still live just before expiry, gone at/after it.
        assert!(
            store
                .resolve(&tok, 1_000 + SESSION_TTL_MILLIS - 1)
                .is_some()
        );
        assert!(store.resolve(&tok, 1_000 + SESSION_TTL_MILLIS).is_none());
        // Lazy eviction removed it.
        assert!(store.resolve(&tok, 1_000).is_none());
    }

    #[test]
    fn revoke_drops_the_session() {
        let store = SessionStore::new();
        let tok = store.issue("AK".into(), ConsoleRole::Readonly, 0);
        store.revoke(&tok);
        assert!(store.resolve(&tok, 0).is_none());
    }

    #[test]
    fn tokens_are_unique() {
        let store = SessionStore::new();
        let a = store.issue("AK".into(), ConsoleRole::Readonly, 0);
        let b = store.issue("AK".into(), ConsoleRole::Readonly, 0);
        assert_ne!(a, b, "each session gets a fresh token");
    }

    #[test]
    fn cookie_parsing() {
        assert_eq!(
            token_from_cookie_header("epochio_console=abc123"),
            Some("abc123")
        );
        assert_eq!(
            token_from_cookie_header("other=x; epochio_console=tok; z=1"),
            Some("tok")
        );
        assert_eq!(token_from_cookie_header("other=x"), None);
    }

    #[test]
    fn constant_time_secret_compare() {
        assert!(secret_matches("s3cr3t", "s3cr3t"));
        assert!(!secret_matches("s3cr3t", "s3cr3T"));
        assert!(!secret_matches("s3cr3t", "s3cr3t-longer"));
        assert!(!secret_matches("s3cr3t", ""));
    }
}
