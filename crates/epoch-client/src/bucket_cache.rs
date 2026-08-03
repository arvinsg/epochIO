//! Bucket cache: the gateway's name→bucket lookup for the S3 hot path
//! (design 01 §6 分发: gateway 周期拉取 bucket 表 + 版本失效). Every S3 request
//! names a bucket; the gateway resolves it to `(bucket_id, ns_mode,
//! inline_threshold, codemode)` here rather than round-tripping PD per request.
//!
//! The cache is refilled from PD [`list_buckets`](PdClient::list_buckets) on a
//! miss (and periodically by the caller's ticker); buckets are create-only in
//! M6 (no rename/delete on the hot path), so a present entry never goes stale
//! in a way that matters for routing — only newly-created buckets need a
//! refill, which a miss triggers.
//!
//! Design: docs/design/01-pd.md §6

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use epoch_proto::grpc::pd::{self, NsMode};

use crate::error::ClientError;
use crate::pd::PdClient;

/// The resolved metadata a gateway needs for a bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketInfo {
    /// PD-assigned bucket id (carries the hier namespace bit for hier buckets).
    pub bucket_id: u64,
    /// Namespace mode (flat = S3-native, hier = FUSE/tree with S3 compat).
    pub ns_mode: NsMode,
    /// Effective inline threshold in bytes (0 = inline disabled).
    pub inline_threshold: u64,
    /// The bucket's EC code-mode id.
    pub codemode_id: u32,
}

impl BucketInfo {
    fn from_proto(b: &pd::BucketInfo) -> Self {
        Self {
            bucket_id: b.bucket_id,
            ns_mode: NsMode::try_from(b.ns_mode).unwrap_or(NsMode::Flat),
            inline_threshold: b.inline_threshold,
            codemode_id: b.codemode_id,
        }
    }
}

/// A name→[`BucketInfo`] cache backed by PD `ListBuckets`.
///
/// Cheap to clone (shares the PD client and the map).
#[derive(Clone)]
pub struct BucketCache {
    pd: PdClient,
    by_name: Arc<Mutex<HashMap<String, BucketInfo>>>,
}

impl BucketCache {
    /// Builds an empty cache over a PD client.
    #[must_use]
    pub fn new(pd: PdClient) -> Self {
        Self {
            pd,
            by_name: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, BucketInfo>> {
        self.by_name.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Resolves `name`, refilling from PD on a miss. Returns
    /// [`ClientError::NotFound`] if the bucket does not exist.
    ///
    /// # Errors
    /// [`ClientError::NotFound`] for an unknown bucket; PD leader/transport
    /// failures as [`ClientError`].
    pub async fn resolve(&self, name: &str) -> Result<BucketInfo, ClientError> {
        if let Some(info) = self.map().get(name).cloned() {
            return Ok(info);
        }
        self.refresh().await?;
        self.map()
            .get(name)
            .cloned()
            .ok_or_else(|| ClientError::NotFound(format!("bucket {name:?}")))
    }

    /// Refills the whole cache from PD (the ticker's periodic pull, and the
    /// miss path). Replaces the map so a deleted bucket eventually drops out.
    ///
    /// # Errors
    /// PD leader/transport failures as [`ClientError`].
    pub async fn refresh(&self) -> Result<(), ClientError> {
        let buckets = self.pd.list_buckets().await?;
        let fresh: HashMap<String, BucketInfo> = buckets
            .iter()
            .map(|b| (b.name.clone(), BucketInfo::from_proto(b)))
            .collect();
        *self.map() = fresh;
        Ok(())
    }

    /// The cached entry for `name` without a refill (observation / tests).
    #[must_use]
    pub fn peek(&self, name: &str) -> Option<BucketInfo> {
        self.map().get(name).cloned()
    }
}
