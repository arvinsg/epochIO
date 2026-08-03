//! SigV4 authentication (M6): the gateway verifies request signatures locally
//! against credentials pulled from PD (01 §6 秘钥不出域). `s3s` performs the
//! full SigV4 canonicalization and signature check; this only supplies the
//! secret key for an access key ([`s3s::auth::S3Auth`]), backed by the
//! [`CredentialCache`] (name→secret, PD-backed, cached).
//!
//! This layer answers *who* (identity, for SigV4 verification). *What they may
//! touch* (IAM-lite bucket allow/deny, 01 §6) is enforced by `S3Backend::bucket`,
//! which every bucket-naming operation already calls — so authorization rides the
//! resolution every handler performs and cannot be skipped at a call site.
//!
//! Design: docs/design/01-pd.md §6

use epoch_client::CredentialCache;
use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, s3_error};

/// A [`S3Auth`] provider backed by the PD credential cache.
pub struct CredentialAuth {
    creds: CredentialCache,
}

impl CredentialAuth {
    /// Builds the auth provider over a credential cache.
    #[must_use]
    pub fn new(creds: CredentialCache) -> Self {
        Self { creds }
    }
}

#[async_trait::async_trait]
impl S3Auth for CredentialAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.creds.resolve(access_key).await {
            Ok(Some(cred)) => Ok(SecretKey::from(cred.secret_key)),
            // Unknown access key → SigV4 verification cannot proceed.
            Ok(None) => Err(s3_error!(InvalidAccessKeyId, "unknown access key")),
            // A PD lookup failure is transient; ask the client to retry.
            Err(e) => Err(s3_error!(InternalError, "credential lookup: {e}")),
        }
    }
}
