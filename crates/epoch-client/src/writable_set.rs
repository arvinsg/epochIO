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

//! The writable-chunk set (02 §2.1): the gateway's periodic pull of `Writable`
//! chunks per code mode, with **error-driven eviction** (`Sealed` / `ChunkFull`
//! / `DiskBroken` evict immediately, no lease) and topology-affined picking —
//! chunks hosting more shards on the local node/rack are preferred (Q21:
//! locality first), then weighted-random to spread load.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use epoch_proto::{ChunkId, NodeId};

use crate::chunk_map::{ChunkMap, ChunkSlots};
use crate::error::ClientError;
use crate::pd::PdClient;

/// Default pull interval for the writable set (02 §2.1).
pub const DEFAULT_WRITABLE_REFRESH: Duration = Duration::from_secs(30);

/// A writable chunk picked for one blob write: its id and resolved shard
/// endpoints, freshly checked against the epoch in the set.
#[derive(Debug, Clone)]
pub struct WritableChunk {
    /// The EC group id.
    pub chunk_id: ChunkId,
    /// Resolved shard slots in shard-index order.
    pub slots: ChunkSlots,
}

/// The gateway's writable-chunk set, per code mode.
pub struct WritableSet {
    pd: PdClient,
    chunk_map: ChunkMap,
    refresh_interval: Duration,
    code_mode_id: epoch_proto::CodeModeId,
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    /// Candidate chunk ids, most recently published as writable.
    chunks: Vec<ChunkId>,
    /// Chosen-index cursor for round-robin spread (avoids a shared RNG).
    cursor: usize,
    last_refresh: Option<Instant>,
}

impl WritableSet {
    /// Builds an empty set for one code mode; fill it with [`refresh`](Self::refresh).
    #[must_use]
    pub fn new(
        pd: PdClient,
        chunk_map: ChunkMap,
        code_mode_id: epoch_proto::CodeModeId,
        refresh_interval: Duration,
    ) -> Self {
        Self {
            pd,
            chunk_map,
            refresh_interval,
            code_mode_id,
            inner: Arc::new(RwLock::new(Inner {
                chunks: Vec::new(),
                cursor: 0,
                last_refresh: None,
            })),
        }
    }

    /// Repopulates the set from PD's published writable chunks (01 §4.2).
    ///
    /// # Errors
    /// [`ClientError::NoLeader`] if PD cannot serve the read.
    pub async fn refresh(&self) -> Result<(), ClientError> {
        let views = self.pd.get_writable_chunks(self.code_mode_id).await?;
        let chunks = views
            .into_iter()
            .map(|view| ChunkId::new(view.chunk_id))
            .collect();
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.chunks = chunks;
        inner.last_refresh = Some(Instant::now());
        Ok(())
    }

    /// Whether the set needs a refresh (empty or past the refresh interval).
    #[must_use]
    pub fn is_stale(&self) -> bool {
        let inner = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.chunks.is_empty()
            || inner
                .last_refresh
                .is_none_or(|t| t.elapsed() >= self.refresh_interval)
    }

    /// Picks a writable chunk for one blob write: candidates are scored by how
    /// many shards they host on `local_node` and its rack (Q21 locality), the
    /// top score is rotated round-robin for spread. Returns `None` when the set
    /// is empty (caller refreshes or fails over to a static placement).
    ///
    /// Resolution failures (a chunk whose shards are not in topology) are
    /// evicted inline and skipped.
    pub async fn pick(&self, local_node: Option<NodeId>) -> Option<WritableChunk> {
        if self.is_stale()
            && let Err(err) = self.refresh().await
        {
            tracing::debug!(error = %err, "writable-set refresh failed");
        }
        let (candidates, cursor) = {
            let inner = self
                .inner
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (inner.chunks.clone(), inner.cursor)
        };
        if candidates.is_empty() {
            return None;
        }

        // Resolve + score: (local-node shards, same-rack shards) descending.
        let local_rack =
            local_node.and_then(|node| self.chunk_map.topology().node(node).map(|topo| topo.rack));
        let mut scored: Vec<(usize, usize, usize, WritableChunk)> = Vec::new();
        for chunk_id in candidates {
            let Ok(slots) = self.chunk_map.get(chunk_id).await else {
                self.evict(chunk_id);
                continue;
            };
            if !slots.writable {
                self.evict(chunk_id);
                continue;
            }
            let node_count = local_node.map_or(0, |node| {
                slots.shards.iter().filter(|s| s.node_id == node).count()
            });
            let rack_count = local_rack.as_ref().map_or(0, |rack| {
                slots.shards.iter().filter(|s| &s.rack == rack).count()
            });
            scored.push((
                node_count,
                rack_count,
                scored.len(),
                WritableChunk { chunk_id, slots },
            ));
        }
        if scored.is_empty() {
            return None;
        }
        scored.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then(a.2.cmp(&b.2))
        });
        let top_score = (scored[0].0, scored[0].1);
        let top: Vec<&(usize, usize, usize, WritableChunk)> = scored
            .iter()
            .take_while(|s| (s.0, s.1) == top_score)
            .collect();
        let chosen = top[cursor % top.len()].3.clone();
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cursor = cursor.wrapping_add(1);
        Some(chosen)
    }

    /// Evicts a chunk immediately (error-driven refresh, 02 §2.1): `Sealed`,
    /// `ChunkFull`, `DiskBroken`, or a resolution failure makes it ineligible
    /// until the next refresh republishes it.
    pub fn evict(&self, chunk_id: ChunkId) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .chunks
            .retain(|id| *id != chunk_id);
        self.chunk_map.invalidate(chunk_id);
    }
}
