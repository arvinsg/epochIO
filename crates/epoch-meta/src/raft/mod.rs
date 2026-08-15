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

//! Multi-raft assembly: every partition on a MetaNode is one openraft group
//! (03 §8: 每 partition 一个 group，单进程 O(百) group), and this module owns
//! their lifecycle — create, look up, and recover-on-restart.
//!
//! All groups share the node's two storage instances: the state-machine
//! engine ([`MetaStore`], 03 §7) and the isolated raft-log database
//! ([`MetaLogDb`], group-id-prefixed keyspace). Peer traffic of every group
//! flows through one node-wide [`BatcherHub`] (03 §8 心跳合并), so a node pair
//! carries a single batched RPC stream no matter how many groups replicate
//! across it.
//!
//! ## Node identity
//!
//! The raft [`NodeId`] (`u64`) is the cluster `NodeId` (`u32`, assigned by PD
//! at MetaNode registration) zero-extended — unlike PD replicas, MetaNodes
//! join an existing cluster, so they reuse the cluster id namespace.
//!
//! (raft/mod.rs)

// openraft's `StorageError` is intentionally large (its own crate allows this
// lint for the same reason); every storage-trait method here must return it, so
// boxing is not an option. Scope the allow to the raft subtree.
#![allow(clippy::result_large_err)]

pub mod log_store;
pub mod migrate;
pub mod network;
pub mod state_machine;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use openraft::{AnyError, BasicNode, Config, Raft, StorageError, StorageIOError};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::MetaError;
use crate::guard::PartitionGuard;
use crate::partition::PartitionRange;
use crate::raft::log_store::MetaLogDb;
use crate::raft::network::{BatcherHub, MetaNetworkFactory};
use crate::raft::state_machine::MetaStateMachine;
use crate::ref_extractor::RefExtractor;
use crate::store::MetaStore;

pub use state_machine::{MetaEntry, MetaTypeConfig, PendingSplit, SplitOp};

pub use crate::ns_flat::MetaResponse;

/// Raft replica id within a partition group: the cluster `NodeId` (`u32`)
/// zero-extended.
pub type NodeId = u64;

/// The state-machine storage error returned across the apply / recovery paths.
pub(crate) type SmError = StorageError<NodeId>;

/// Maps a read-side failure (engine or deserialization) to a state-machine
/// read error.
pub(crate) fn sm_read_err(e: impl std::error::Error + 'static) -> SmError {
    StorageIOError::read_state_machine(AnyError::new(&e)).into()
}

/// Maps a write-side failure (engine or serialization) to a state-machine
/// write error.
pub(crate) fn sm_write_err(e: impl std::error::Error + 'static) -> SmError {
    StorageIOError::write_state_machine(AnyError::new(&e)).into()
}

/// A state-machine read error for a malformed persisted record.
pub(crate) fn sm_corrupt(msg: &str) -> SmError {
    let io = std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    StorageIOError::read_state_machine(AnyError::new(&io)).into()
}

/// A running partition raft group.
pub type MetaRaft = Raft<MetaTypeConfig>;

/// The persisted group registry record (raft-log DB `meta` CF, `g | group`):
/// everything restart recovery needs to reopen the group.
#[derive(Debug, Serialize, Deserialize)]
pub struct GroupRecord {
    /// The partition's routing-key interval at creation (splits update the
    /// in-log descriptor, 03 §2 — this record is only the bootstrap seed).
    pub range: PartitionRange,
}

/// The multi-group owner: one per MetaNode process.
///
/// Group handles are cheap clones of the raft handle; the map is the
/// dispatch table the transport service fans envelopes out through
/// ([`MetaRaftTransportService`](network::MetaRaftTransportService)).
pub struct GroupManager {
    node_id: NodeId,
    config: Arc<Config>,
    log_db: MetaLogDb,
    store: Arc<dyn MetaStore>,
    extractor: Arc<dyn RefExtractor>,
    hub: Arc<BatcherHub>,
    groups: Mutex<BTreeMap<u64, MetaRaft>>,
    guards: Mutex<BTreeMap<u64, Arc<PartitionGuard>>>,
}

impl GroupManager {
    /// Opens the multi-group runtime over the shared raft-log database at
    /// `log_dir` and the shared state-machine `store`.
    ///
    /// No groups are started here: new ones come from
    /// [`create_group`](Self::create_group), persisted ones from
    /// [`recover`](Self::recover).
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Rocks`] if the log database cannot be opened.
    pub fn open(
        log_dir: &Path,
        node_id: NodeId,
        store: Arc<dyn MetaStore>,
        extractor: Arc<dyn RefExtractor>,
        config: Config,
    ) -> Result<Self, MetaError> {
        let log_db = MetaLogDb::open(log_dir)?;
        Ok(Self {
            node_id,
            config: Arc::new(config),
            log_db,
            store,
            extractor,
            hub: Arc::new(BatcherHub::new()),
            groups: Mutex::new(BTreeMap::new()),
            guards: Mutex::new(BTreeMap::new()),
        })
    }

    /// Creates and starts a new partition group, persisting its registry
    /// record first so a crash between start and register cannot leave an
    /// untracked group.
    ///
    /// The group starts with no cluster membership; the caller initializes it
    /// (single-replica bootstrap) or PD drives joint membership changes over
    /// the transport.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::GroupExists`] if the group is already running (or
    /// registered), [`MetaError::Rocks`] if the registry write fails, or
    /// [`MetaError::Raft`] if the raft engine fails to start.
    pub async fn create_group(
        &self,
        group: u64,
        range: PartitionRange,
    ) -> Result<MetaRaft, MetaError> {
        {
            let groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
            if groups.contains_key(&group) {
                return Err(MetaError::GroupExists(group));
            }
        }
        info!(group, "created partition raft group");
        self.register_and_start(group, range).await
    }

    /// Reopens every group persisted in the registry (restart path: the kill
    /// -9 acceptance of 07 §M5a — data and delq resume from the persisted
    /// apply position, never from scratch).
    ///
    /// Groups already running are skipped (idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Rocks`] if the registry cannot be scanned, or
    /// [`MetaError::Raft`] if a group record is malformed or fails to start.
    pub async fn recover(&self) -> Result<Vec<u64>, MetaError> {
        let mut recovered = Vec::new();
        for (group, record) in self.log_db.registered_groups()? {
            if self.raft(group).is_some() {
                continue;
            }
            let record: GroupRecord = serde_json::from_slice(&record)
                .map_err(|e| MetaError::Raft(format!("malformed group {group} record: {e}")))?;
            self.start_group(group, record.range).await?;
            recovered.push(group);
        }
        info!(
            recovered = recovered.len(),
            "recovered partition raft groups"
        );
        Ok(recovered)
    }

    /// Ensures `group` exists locally (un-initialized), starting it over the
    /// shared engine if absent — the migration target's prerequisite (03 §2):
    /// a node must host the group before the leader can `add_learner` it, so
    /// its transport can accept replication and the snapshot install. Idempotent
    /// (a running group is left as-is; membership arrives by replication, not by
    /// a local `initialize`).
    ///
    /// # Errors
    ///
    /// Returns [`MetaError`] if the registry write or raft start fails.
    pub async fn ensure_group_present(
        &self,
        group: u64,
        range: PartitionRange,
    ) -> Result<MetaRaft, MetaError> {
        if let Some(raft) = self.raft(group) {
            return Ok(raft);
        }
        self.register_and_start(group, range).await
    }

    /// The raft handle of `group`, if running on this node.
    #[must_use]
    pub fn raft(&self, group: u64) -> Option<MetaRaft> {
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&group)
            .cloned()
    }

    /// Derives any child groups a committed `Split` left pending (03 §2: 派生
    /// 子组). For every running group carrying a pending-child record, starts
    /// the child group over the shared engine (its data already lives there —
    /// keys carry no partition id), initializes the child's membership on the
    /// designated bootstrap node exactly once, then clears the record.
    ///
    /// Idempotent and crash-safe: a child already running is skipped, a child
    /// already initialized is not re-initialized, and the record persists until
    /// the child is confirmed running — so a crash mid-derivation re-drives on
    /// the next reconcile. Leader and followers all run this; each derives its
    /// own local child replica.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError`] if a record cannot be read, or a child group fails
    /// to start or initialize.
    pub async fn reconcile_splits(&self) -> Result<Vec<u64>, MetaError> {
        let mut derived = Vec::new();
        for parent in self.group_ids() {
            let Some(pending) = state_machine::read_pending_split(self.store.as_ref(), parent)?
            else {
                continue;
            };
            // Start the child replica if not already running (idempotent).
            if self.raft(pending.child_group).is_none() {
                self.register_and_start(pending.child_group, pending.child_range.clone())
                    .await?;
                derived.push(pending.child_group);
            }
            // The bootstrap node forms the child's membership exactly once
            // (03 §8); a restart replays the persisted membership instead.
            if pending.bootstrap_node == self.node_id {
                let raft = self
                    .raft(pending.child_group)
                    .ok_or_else(|| MetaError::Raft("child missing after start".to_string()))?;
                if !raft
                    .is_initialized()
                    .await
                    .map_err(|e| MetaError::Raft(e.to_string()))?
                {
                    let members: BTreeMap<u64, BasicNode> = pending
                        .child_peers
                        .iter()
                        .map(|(id, addr)| (*id, BasicNode::new(addr.clone())))
                        .collect();
                    raft.initialize(members)
                        .await
                        .map_err(|e| MetaError::Raft(format!("initialize child: {e}")))?;
                }
            }
            // Child is running (and initialized on the bootstrap node): clear
            // the pending record so the reconcile stops re-deriving it.
            state_machine::clear_pending_split(self.store.as_ref(), parent)?;
        }
        if !derived.is_empty() {
            info!(derived = derived.len(), "derived split child groups");
        }
        Ok(derived)
    }

    /// Registers a group in the log-db registry (so restart recovers it) and
    /// starts it — the shared body of [`create_group`](Self::create_group) and
    /// split child derivation.
    async fn register_and_start(
        &self,
        group: u64,
        range: PartitionRange,
    ) -> Result<MetaRaft, MetaError> {
        let record = GroupRecord {
            range: range.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|e| MetaError::Raft(format!("encode group record: {e}")))?;
        self.log_db.register_group(group, &bytes)?;
        self.start_group(group, range).await
    }

    /// The ids of every group currently running on this node (ordered).
    #[must_use]
    pub fn group_ids(&self) -> Vec<u64> {
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// The node-wide batching hub (transport assembly and diagnostics).
    #[must_use]
    pub fn hub(&self) -> Arc<BatcherHub> {
        Arc::clone(&self.hub)
    }

    /// The shared state-machine engine (read paths route through it after a
    /// leader ReadIndex, 03 §5).
    #[must_use]
    pub fn store(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.store)
    }

    /// The inline guard of `group`, if hosted (the service's inline-PUT
    /// admission check, 03 §4.3).
    #[must_use]
    pub fn guard(&self, group: u64) -> Option<Arc<PartitionGuard>> {
        self.guards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&group)
            .cloned()
    }

    /// Starts one group over the shared instances and registers its handle.
    async fn start_group(&self, group: u64, range: PartitionRange) -> Result<MetaRaft, MetaError> {
        let log_store = self.log_db.group_store(group);
        let guard = Arc::new(PartitionGuard::new());
        let state_machine = MetaStateMachine::new(
            Arc::clone(&self.store),
            Arc::clone(&self.extractor),
            group,
            range,
            Arc::clone(&guard),
        )?;
        let network = MetaNetworkFactory::new(group, Arc::clone(&self.hub));
        let raft = Raft::new(
            self.node_id,
            Arc::clone(&self.config),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| MetaError::Raft(e.to_string()))?;
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(group, raft.clone());
        self.guards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(group, guard);
        Ok(raft)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Namespace;
    use crate::store::rocks::RocksEngine;

    fn test_config() -> Config {
        Config {
            cluster_name: "meta-test".to_string(),
            ..Default::default()
        }
    }

    fn manager(dir: &tempfile::TempDir) -> GroupManager {
        let engine = Arc::new(RocksEngine::open(&dir.path().join("sm")).expect("engine"));
        GroupManager::open(
            &dir.path().join("raft-log"),
            1,
            engine,
            Arc::new(crate::ref_extractor::EpochRefExtractor),
            test_config(),
        )
        .expect("open")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_reject_duplicate_and_recover_reopens() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let manager = manager(&dir);
            manager
                .create_group(1, PartitionRange::full(Namespace::Flat))
                .await
                .expect("create 1");
            manager
                .create_group(2, PartitionRange::full(Namespace::Hier))
                .await
                .expect("create 2");
            let duplicate = manager
                .create_group(1, PartitionRange::full(Namespace::Flat))
                .await;
            assert!(
                matches!(duplicate, Err(MetaError::GroupExists(1))),
                "duplicate create must fail"
            );
            assert_eq!(manager.group_ids(), vec![1, 2]);
            // Stop the group cores before the manager drops, so the engine
            // lock is released deterministically for the reopen below (a
            // dropped handle alone lets the core race the reopen).
            for group in manager.group_ids() {
                if let Some(raft) = manager.raft(group) {
                    raft.shutdown().await.expect("shutdown");
                }
            }
        }

        // Restart: recovery reopens exactly the registered groups.
        let manager = manager(&dir);
        let mut recovered = manager.recover().await.expect("recover");
        recovered.sort_unstable();
        assert_eq!(recovered, vec![1, 2]);
        assert_eq!(manager.group_ids(), vec![1, 2]);

        // Idempotent: a second recovery finds nothing new.
        assert!(manager.recover().await.expect("recover again").is_empty());
    }

    /// A committed Split on the parent, followed by `reconcile_splits`, derives
    /// and bootstraps the child group over the shared engine; the child then
    /// serves writes on its half of the range (03 §2 派生子组).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_reconcile_derives_and_bootstraps_child() {
        use crate::raft::SplitOp;
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().expect("tempdir");
        let manager = manager(&dir);
        let parent = manager
            .create_group(1, PartitionRange::full(Namespace::Flat))
            .await
            .expect("create parent");
        parent
            .initialize(BTreeMap::from([(1u64, BasicNode::new("a:1"))]))
            .await
            .expect("initialize parent");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !parent.metrics().borrow().state.is_leader() {
            assert!(Instant::now() < deadline, "no parent leader");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Propose the split (PD would drive this; here we propose directly).
        parent
            .client_write(MetaEntry::Split(SplitOp {
                child_group: 2,
                at_bucket: 5,
                at_routing_key: b"m".to_vec(),
                child_peers: vec![(1, "a:1".to_string())],
                bootstrap_node: 1,
                child_ino_tag: 1,
            }))
            .await
            .expect("propose split");

        // Reconcile derives + bootstraps the child; a second run is a no-op.
        let derived = manager.reconcile_splits().await.expect("reconcile");
        assert_eq!(derived, vec![2], "child group derived");
        assert_eq!(manager.group_ids(), vec![1, 2]);
        assert!(
            manager
                .reconcile_splits()
                .await
                .expect("reconcile again")
                .is_empty(),
            "pending record cleared → second reconcile derives nothing"
        );

        // The child becomes leader of its own group and serves a write.
        let child = manager.raft(2).expect("child running");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !child.metrics().borrow().state.is_leader() {
            assert!(Instant::now() < deadline, "no child leader");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        child
            .client_write(MetaEntry::StoreOps(Vec::new()))
            .await
            .expect("child serves write");
    }
}
