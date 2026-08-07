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

//! Raft state machine for the PD group, backed by RocksDB column families.
//!
//! Implements openraft's [`RaftStateMachine`] over the shared [`PdState`]. The
//! `meta` column family holds the raft bookkeeping (applied log id, membership,
//! current snapshot); each domain module owns its own column family (the node
//! manager owns `node`). Applied entries persist their effect (and the applied
//! log id) synchronously before `apply` returns, so the machine recovers
//! directly from RocksDB on restart (the "persistent state machine" model).
//!
//! Apply threads each committed command through [`PdState::apply_command`],
//! which stages durable writes into one [`WriteBatch`] and updates the in-memory
//! indexes; a single `sync` flush per raft batch commits them.
//!
//! INVARIANT(design 01 §2 / Q18): entries are applied serially and application
//! never reads wall-clock time, randomness, or heartbeat statistics — the
//! replicated result is a pure function of the committed command, keeping every
//! replica deterministic. Heartbeat load/free/writable-extent statistics stay in
//! leader memory and never enter this state machine.
//!
//! Snapshot data ([`PdSnapshotData`]) and the current-snapshot record are owned
//! here; [`super::snapshot::PdSnapshotBuilder`] only generates ids and delegates
//! to [`build_snapshot`].
//!
//! Design: docs/design/01-pd.md §2; docs/design/06-code-layout.md §8

use std::io::Cursor;
use std::path::Path;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership,
};
use rocksdb::{ColumnFamily, DB, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::chunk::model::{Chunk, StagingChunk};
use crate::cluster::disk::Disk;
use crate::cluster::node::Node;
use crate::error::PdError;
use crate::journal::entry::ApplyResult;
use crate::raft::snapshot::PdSnapshotBuilder;
use crate::raft::{NodeId, PdTypeConfig, SmError, missing_cf, sm_read_err, sm_write_err};
use crate::state::PdState;
use crate::writer::WriterRecord;

/// Column family holding the raft bookkeeping (applied id / membership / snapshot).
pub(crate) const META_CF: &str = "meta";
const APPLIED_KEY: &[u8] = b"applied";
const MEMBERSHIP_KEY: &[u8] = b"membership";
const SNAPSHOT_KEY: &[u8] = b"snapshot";

/// The serialized state machine content carried inside a snapshot.
///
/// Grows one field group per module as those managers land: raft-required
/// applied position + membership, then node / disk membership, then chunk
/// records (committed + in-flight staging plans).
#[derive(Debug, Serialize, Deserialize)]
struct PdSnapshotData {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    nodes: Vec<Node>,
    next_node_id: u32,
    disks: Vec<Disk>,
    next_disk_id: u32,
    chunks: Vec<Chunk>,
    next_chunk_id: u32,
    staging_chunks: Vec<StagingChunk>,
    writers: Vec<WriterRecord>,
    next_writer_id: u32,
    partitions: Vec<crate::meta_mgr::MetaPartition>,
    next_partition_id: u64,
    /// Next split-child ino partition tag (03 §6.5); monotonic, never reused.
    next_partition_ino_tag: u32,
    #[serde(default)]
    jobs: Vec<crate::job::types::Job>,
    #[serde(default)]
    next_job_id: u32,
    #[serde(default)]
    shard_repairs: Vec<crate::shard_repair::ShardRepairTicket>,
}

/// The persisted current-snapshot record (openraft metadata + raw snapshot bytes).
#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

fn sync_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

fn meta_cf(db: &DB) -> Result<&ColumnFamily, SmError> {
    db.cf_handle(META_CF).ok_or_else(|| missing_cf(META_CF))
}

fn read_applied(db: &DB) -> Result<Option<LogId<NodeId>>, SmError> {
    let cf = meta_cf(db)?;
    match db.get_cf(&cf, APPLIED_KEY).map_err(sm_read_err)? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(sm_read_err)?)),
        None => Ok(None),
    }
}

fn read_membership(db: &DB) -> Result<StoredMembership<NodeId, BasicNode>, SmError> {
    let cf = meta_cf(db)?;
    match db.get_cf(&cf, MEMBERSHIP_KEY).map_err(sm_read_err)? {
        Some(bytes) => Ok(serde_json::from_slice(&bytes).map_err(sm_read_err)?),
        None => Ok(StoredMembership::default()),
    }
}

/// Builds a snapshot of the current applied state and persists it as the current
/// snapshot record. Called by [`PdSnapshotBuilder`].
///
/// `snapshot_index` is a local monotonic counter used only to make the snapshot
/// id unique for a single node; multi-node transfer hardening arrives with the
/// gRPC raft network.
pub(crate) fn build_snapshot(
    state: &PdState,
    snapshot_index: u64,
) -> Result<Snapshot<PdTypeConfig>, SmError> {
    let last_applied = read_applied(&state.db)?;
    let last_membership = read_membership(&state.db)?;
    let (nodes, next_node_id) = state.nodes.snapshot_view();
    let (disks, next_disk_id) = state.disks.snapshot_view();
    let (chunks, next_chunk_id, staging_chunks) = state.chunks.snapshot_view();
    let (writers, next_writer_id) = state.writers.snapshot_view();
    let (partitions, next_partition_id, next_partition_ino_tag) = state.partitions.snapshot_view();
    let (jobs, next_job_id) = state.jobs.snapshot_view();
    let shard_repairs = state.shard_repairs.snapshot_view();

    let data = PdSnapshotData {
        last_applied,
        last_membership: last_membership.clone(),
        nodes,
        next_node_id,
        disks,
        next_disk_id,
        chunks,
        next_chunk_id,
        staging_chunks,
        writers,
        next_writer_id,
        partitions,
        next_partition_id,
        next_partition_ino_tag,
        jobs,
        next_job_id,
        shard_repairs,
    };
    let bytes = serde_json::to_vec(&data).map_err(sm_write_err)?;

    let snapshot_id = format!(
        "pd-snapshot-{}-{}",
        last_applied.map(|l| l.index).unwrap_or(0),
        snapshot_index
    );
    let meta = SnapshotMeta {
        last_log_id: last_applied,
        last_membership,
        snapshot_id,
    };

    let stored = StoredSnapshot {
        meta: meta.clone(),
        data: bytes.clone(),
    };
    let cf = meta_cf(&state.db)?;
    state
        .db
        .put_cf(
            &cf,
            SNAPSHOT_KEY,
            serde_json::to_vec(&stored).map_err(sm_write_err)?,
        )
        .map_err(sm_write_err)?;

    Ok(Snapshot {
        meta,
        snapshot: Box::new(Cursor::new(bytes)),
    })
}

fn load_current_snapshot(db: &DB) -> Result<Option<Snapshot<PdTypeConfig>>, SmError> {
    let cf = meta_cf(db)?;
    match db.get_cf(&cf, SNAPSHOT_KEY).map_err(sm_read_err)? {
        Some(bytes) => {
            let stored: StoredSnapshot = serde_json::from_slice(&bytes).map_err(sm_read_err)?;
            Ok(Some(Snapshot {
                meta: stored.meta,
                snapshot: Box::new(Cursor::new(stored.data)),
            }))
        }
        None => Ok(None),
    }
}

fn install_snapshot_bytes(
    state: &PdState,
    meta: &SnapshotMeta<NodeId, BasicNode>,
    bytes: Vec<u8>,
) -> Result<(), SmError> {
    let data: PdSnapshotData = serde_json::from_slice(&bytes).map_err(sm_read_err)?;
    let cf = meta_cf(&state.db)?;

    let mut batch = WriteBatch::default();
    match data.last_applied {
        Some(log_id) => batch.put_cf(
            &cf,
            APPLIED_KEY,
            serde_json::to_vec(&log_id).map_err(sm_write_err)?,
        ),
        None => batch.delete_cf(&cf, APPLIED_KEY),
    }
    batch.put_cf(
        &cf,
        MEMBERSHIP_KEY,
        serde_json::to_vec(&data.last_membership).map_err(sm_write_err)?,
    );

    // Node and disk records live in their managers' own column families; each
    // replaces its full set and rebuilds its in-memory index from the snapshot.
    state
        .nodes
        .stage_and_apply_snapshot(&mut batch, data.nodes, data.next_node_id)?;
    state
        .disks
        .stage_and_apply_snapshot(&mut batch, data.disks, data.next_disk_id)?;
    state.chunks.stage_and_apply_snapshot(
        &mut batch,
        data.chunks,
        data.next_chunk_id,
        data.staging_chunks,
    )?;
    state
        .writers
        .stage_and_apply_snapshot(&mut batch, data.writers, data.next_writer_id)?;
    state.partitions.stage_and_apply_snapshot(
        &mut batch,
        data.partitions,
        data.next_partition_id,
        data.next_partition_ino_tag,
    )?;
    state
        .jobs
        .stage_and_apply_snapshot(&mut batch, data.jobs, data.next_job_id)?;
    state
        .shard_repairs
        .stage_and_apply_snapshot(&mut batch, data.shard_repairs)?;

    let stored = StoredSnapshot {
        meta: meta.clone(),
        data: bytes,
    };
    batch.put_cf(
        &cf,
        SNAPSHOT_KEY,
        serde_json::to_vec(&stored).map_err(sm_write_err)?,
    );
    state
        .db
        .write_opt(batch, &sync_write_options())
        .map_err(sm_write_err)?;
    Ok(())
}

/// Raft state machine over the shared [`PdState`] (cloneable; clones share the
/// same database and in-memory indexes).
#[derive(Clone)]
pub struct PdStateMachine {
    state: PdState,
}

impl PdStateMachine {
    /// Opens (creating if absent) a state machine with its own [`PdState`] at
    /// `path`. Used by the storage test suite; the running service shares one
    /// [`PdState`] via [`new`](Self::new).
    ///
    /// # Errors
    ///
    /// Returns [`PdError`] if the database cannot be opened or recovered.
    pub fn open(path: &Path) -> Result<Self, PdError> {
        Ok(Self {
            state: PdState::open(path)?,
        })
    }

    /// Wraps a shared [`PdState`] (the read side holds the same handle).
    pub(crate) fn new(state: PdState) -> Self {
        Self { state }
    }
}

impl RaftStateMachine<PdTypeConfig> for PdStateMachine {
    type SnapshotBuilder = PdSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), SmError> {
        Ok((
            read_applied(&self.state.db)?,
            read_membership(&self.state.db)?,
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, SmError>
    where
        I: IntoIterator<Item = Entry<PdTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut batch = WriteBatch::default();
        let mut results = Vec::with_capacity(entries.len());
        let mut last_log_id = None;

        for entry in &entries {
            last_log_id = Some(entry.log_id);
            let result = match &entry.payload {
                // Blank (leader-establish) changes no state; a command is
                // dispatched to its owning manager; a membership entry is stored
                // verbatim. No clock/heartbeat read here (Q18).
                EntryPayload::Blank => ApplyResult::Applied,
                EntryPayload::Normal(cmd) => self.state.apply_command(&mut batch, cmd)?,
                EntryPayload::Membership(membership) => {
                    let cf = meta_cf(&self.state.db)?;
                    let stored = StoredMembership::new(Some(entry.log_id), membership.clone());
                    batch.put_cf(
                        &cf,
                        MEMBERSHIP_KEY,
                        serde_json::to_vec(&stored).map_err(sm_write_err)?,
                    );
                    ApplyResult::Applied
                }
            };
            results.push(result);
        }

        if let Some(log_id) = last_log_id {
            let cf = meta_cf(&self.state.db)?;
            batch.put_cf(
                &cf,
                APPLIED_KEY,
                serde_json::to_vec(&log_id).map_err(sm_write_err)?,
            );
        }
        self.state
            .db
            .write_opt(batch, &sync_write_options())
            .map_err(sm_write_err)?;

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        PdSnapshotBuilder::new(self.state.clone())
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, SmError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), SmError> {
        install_snapshot_bytes(&self.state, meta, snapshot.into_inner())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<PdTypeConfig>>, SmError> {
        load_current_snapshot(&self.state.db)
    }
}
