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

//! The production [`ObjectBrowser`] (M9 console object browse): the console's
//! bridge to the MetaNodes for the read-only object list + head views (08 §4).
//!
//! `epoch-pd` defines the [`ObjectBrowser`] trait so it need not depend on
//! `epoch-client` (L2); `epoch-node` (L4) owns the `MetaClient` and injects this
//! implementation into the console state — the same trait-injection the
//! partition scheduler uses for `MetaAdmin`.
//!
//! Download is not here: object bytes go via a 302 to a gateway (08 §4), which
//! needs a caller credential the console session does not hold; that mechanism
//! is undecided (presigned URL pending), so only metadata reads are wired.

use async_trait::async_trait;
use epoch_client::MetaClient;
use epoch_pd::console::browse::{BrowseEntry, BrowseHead, ObjectBrowseError, ObjectBrowser};

/// Serves the console's object list/head reads from the MetaNodes.
pub struct MetaObjectBrowser {
    meta: MetaClient,
}

impl MetaObjectBrowser {
    /// Wraps the shared [`MetaClient`] (routes to the owning partition leader).
    #[must_use]
    pub fn new(meta: MetaClient) -> Self {
        Self { meta }
    }
}

#[async_trait]
impl ObjectBrowser for MetaObjectBrowser {
    async fn list(
        &self,
        bucket_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> Result<Vec<BrowseEntry>, ObjectBrowseError> {
        let entries = self
            .meta
            .list_objects(bucket_id, prefix, start_after, limit)
            .await
            .map_err(|e| ObjectBrowseError(e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(|e| {
                let head = e.head.unwrap_or_default();
                BrowseEntry {
                    key: e.key,
                    // The flat listing yields objects, not directory rollups.
                    is_prefix: false,
                    size: head.size,
                    inline: head.inline,
                    mtime_millis: head.mtime,
                }
            })
            .collect())
    }

    async fn head(
        &self,
        bucket_id: u64,
        key: &[u8],
    ) -> Result<Option<BrowseHead>, ObjectBrowseError> {
        let resp = self
            .meta
            .get_object_meta(bucket_id, key)
            .await
            .map_err(|e| ObjectBrowseError(e.to_string()))?;
        Ok(resp.and_then(|r| r.head).map(|head| BrowseHead {
            size: head.size,
            etag: head.etag,
            mtime_millis: head.mtime,
            inline: head.inline,
        }))
    }
}
