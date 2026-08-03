//! Shared PD state: the handle threaded between the raft state machine (the
//! writer, via apply) and the journal read side (readers, after apply).
//!
//! [`PdState`] owns the state-machine RocksDB handle, the per-module managers
//! (node / disk membership; chunk / … land next), and the snapshot index. It is
//! cheap to clone (all fields are `Arc`), so the state machine and the read path
//! observe the same in-memory indexes. It is also the aggregation point for
//! cross-manager invariants (e.g. a disk registration validates its owning node).
//!
//! Design: docs/design/01-pd.md §2

// `apply_command` / `open` return openraft's intentionally-large `StorageError`
// (see the `raft` module); they feed the raft state machine, so boxing it is not
// an option. Scope the allow to this module.
#![allow(clippy::result_large_err)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use epoch_proto::{ChunkId, DiskId};
use rocksdb::{DB, WriteBatch};

use crate::bucket::{BUCKET_CF, BucketManager};
use crate::chunk::ChunkManager;
use crate::chunk::manager::{CHUNK_CF, STAGING_CF, validate_slots};
use crate::chunk::model::CreateChunkStaging;
use crate::chunk::placement::{AntiAffinity, DiskCandidate, SlotDomain, rebind_preserves_affinity};
use crate::cluster::RejectReason;
use crate::cluster::RoleSet;
use crate::cluster::disk::{DISK_CF, DiskManager, DiskStatus};
use crate::cluster::node::{NODE_CF, NodeManager};
use crate::config_mgr::{CONFIG_CF, ConfigManager};
use crate::credential::{CRED_CF, CredentialManager};
use crate::error::PdError;
use crate::job::JOB_CF;
use crate::job::JobManager;
use crate::journal::entry::{ApplyResult, PdEntry};
use crate::meta_mgr::{META_PARTITION_CF, MetaPartitionManager};
use crate::raft::SmError;
use crate::raft::state_machine::META_CF;
use crate::shard_repair::{SHARD_REPAIR_CF, ShardRepairRegistry};
use crate::writer::WriterManager;
use crate::writer::manager::WRITER_CF;

/// The shared PD state handle (cloneable; clones share the same database and
/// in-memory indexes).
#[derive(Clone)]
pub struct PdState {
    pub(crate) db: Arc<DB>,
    pub(crate) nodes: NodeManager,
    pub(crate) disks: DiskManager,
    pub(crate) chunks: ChunkManager,
    pub(crate) writers: WriterManager,
    pub(crate) buckets: BucketManager,
    pub(crate) configs: ConfigManager,
    pub(crate) partitions: MetaPartitionManager,
    pub(crate) credentials: CredentialManager,
    pub(crate) jobs: JobManager,
    pub(crate) shard_repairs: ShardRepairRegistry,
    pub(crate) snapshot_index: Arc<AtomicU64>,
}

/// The fault-domain granularity a shard rebind must preserve.
///
/// Must match the creation path's `PlacementConfig::affinity` (currently
/// `HostAware`): a stricter gate here would reject repairs of chunks that
/// creation legitimately placed, wedging them permanently in `Rebuilding`.
/// When affinity becomes configurable, both sides must read the same setting.
const REBIND_AFFINITY: AntiAffinity = AntiAffinity::HostAware;

impl PdState {
    /// Opens the state-machine database at `path` and restores the in-memory
    /// indexes from it.
    ///
    /// # Errors
    ///
    /// Returns [`PdError::Rocks`] if the database or its column families cannot
    /// be opened, or [`PdError::Storage`] if a persisted record cannot be
    /// decoded during recovery.
    pub(crate) fn open(path: &Path) -> Result<Self, PdError> {
        let db = Arc::new(epoch_rocks::open_cfs(
            path,
            &epoch_rocks::state_machine_options(),
            &[
                META_CF,
                NODE_CF,
                DISK_CF,
                CHUNK_CF,
                STAGING_CF,
                WRITER_CF,
                BUCKET_CF,
                CONFIG_CF,
                META_PARTITION_CF,
                CRED_CF,
                JOB_CF,
                SHARD_REPAIR_CF,
            ],
        )?);
        let nodes = NodeManager::new(db.clone());
        nodes.restore()?;
        let disks = DiskManager::new(db.clone());
        disks.restore()?;
        let chunks = ChunkManager::new(db.clone());
        chunks.restore()?;
        let writers = WriterManager::new(db.clone());
        writers.restore()?;
        let buckets = BucketManager::new(db.clone());
        buckets.restore()?;
        let configs = ConfigManager::new(db.clone());
        configs.restore()?;
        let partitions = MetaPartitionManager::new(db.clone());
        partitions.restore()?;
        let credentials = CredentialManager::new(db.clone());
        credentials.restore()?;
        let jobs = JobManager::new(db.clone());
        jobs.restore()?;
        let shard_repairs = ShardRepairRegistry::new(db.clone());
        shard_repairs.restore()?;
        Ok(Self {
            db,
            nodes,
            disks,
            chunks,
            writers,
            buckets,
            configs,
            partitions,
            credentials,
            jobs,
            shard_repairs,
            snapshot_index: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Borrows the node manager for the read path.
    #[must_use]
    pub fn nodes(&self) -> &NodeManager {
        &self.nodes
    }

    /// Borrows the disk manager for the read path.
    #[must_use]
    pub fn disks(&self) -> &DiskManager {
        &self.disks
    }

    /// Borrows the chunk manager for the read path.
    #[must_use]
    pub fn chunks(&self) -> &ChunkManager {
        &self.chunks
    }

    /// Borrows the bucket manager for the read path.
    #[must_use]
    pub fn buckets(&self) -> &BucketManager {
        &self.buckets
    }

    /// Borrows the config manager for the read path.
    #[must_use]
    pub fn configs(&self) -> &ConfigManager {
        &self.configs
    }

    /// Borrows the credential manager for the read path (gateway SigV4 pull).
    #[must_use]
    pub fn credentials(&self) -> &CredentialManager {
        &self.credentials
    }

    /// Borrows the writer registry for the read path.
    #[must_use]
    pub fn writers(&self) -> &WriterManager {
        &self.writers
    }

    /// Borrows the Job manager for the read path (lease ticker + coordinator RPCs).
    #[must_use]
    pub fn jobs(&self) -> &JobManager {
        &self.jobs
    }

    /// Borrows the ShardRepair ticket registry for the read path (the node-pull
    /// RPC + rebind authorization, 01 §6.3).
    #[must_use]
    pub fn shard_repairs(&self) -> &ShardRepairRegistry {
        &self.shard_repairs
    }

    /// Borrows the partition manager for the read path.
    #[must_use]
    pub fn partitions(&self) -> &MetaPartitionManager {
        &self.partitions
    }

    /// Dispatches one committed command to its owning manager, staging durable
    /// writes into `batch` (the state machine flushes once per raft batch) and
    /// updating the in-memory indexes.
    ///
    /// # Errors
    ///
    /// Returns a state-machine error if a manager cannot stage its writes.
    pub(crate) fn apply_command(
        &self,
        batch: &mut WriteBatch,
        cmd: &PdEntry,
    ) -> Result<ApplyResult, SmError> {
        match cmd {
            PdEntry::Noop => Ok(ApplyResult::Applied),
            PdEntry::RegisterNode(cmd) => Ok(ApplyResult::NodeRegistered {
                node_id: self.nodes.apply_register(batch, cmd)?,
            }),
            PdEntry::UpdateNodeStatus(cmd) => Ok(reject_to_result(
                self.nodes.apply_update_status(batch, cmd)?,
            )),
            PdEntry::RemoveNode(cmd) => Ok(reject_to_result(self.nodes.apply_remove(batch, cmd)?)),
            PdEntry::RegisterDisk(cmd) => {
                // A disk must belong to a registered node (cross-manager invariant).
                if !self.nodes.contains(cmd.node_id) {
                    return Ok(ApplyResult::Rejected(RejectReason::NodeNotFound));
                }
                Ok(ApplyResult::DiskRegistered {
                    disk_id: self.disks.apply_register(batch, cmd)?,
                })
            }
            PdEntry::UpdateDiskStatus(cmd) => Ok(reject_to_result(
                self.disks.apply_update_status(batch, cmd)?,
            )),
            PdEntry::CreateChunkStaging(cmd) => {
                // Validate the plan shape and its target disks (cross-manager
                // invariant, deterministic — reads committed disk state only).
                if let Some(reason) = self.chunk_plan_reject(cmd) {
                    return Ok(ApplyResult::Rejected(reason));
                }
                Ok(ApplyResult::ChunkStaged {
                    chunk_id: self.chunks.apply_create_staging(batch, cmd)?,
                })
            }
            PdEntry::CommitChunk(cmd) => {
                Ok(reject_to_result(self.chunks.apply_commit(batch, cmd)?))
            }
            PdEntry::RebumpStaging(cmd) => {
                Ok(reject_to_result(self.chunks.apply_rebump(batch, cmd)?))
            }
            PdEntry::SealChunk(cmd) => {
                Ok(reject_to_result(self.chunks.apply_seal_chunk(batch, cmd)?))
            }
            PdEntry::CreateBucket(cmd) => Ok(ApplyResult::BucketCreated {
                bucket_id: self.buckets.apply_create(batch, cmd)?,
            }),
            PdEntry::CreatePartition(cmd) => {
                // Every peer must be a registered node with the META role
                // (cross-manager invariant, deterministic — reads committed
                // node state only).
                for peer in &cmd.peers {
                    let Some(node) = self.nodes.get(*peer) else {
                        return Ok(ApplyResult::Rejected(RejectReason::NodeNotFound));
                    };
                    if !node.roles.contains(RoleSet::META) {
                        return Ok(ApplyResult::Rejected(RejectReason::NodeNotFound));
                    }
                }
                Ok(ApplyResult::PartitionCreated {
                    partition_id: self.partitions.apply_create(batch, cmd)?,
                })
            }
            PdEntry::SplitPartition(cmd) => match self.partitions.apply_split(batch, cmd)? {
                Some((child_id, child_ino_tag)) => Ok(ApplyResult::PartitionSplit {
                    child_id,
                    child_ino_tag,
                }),
                None => Ok(ApplyResult::Rejected(RejectReason::NotFound)),
            },
            PdEntry::MigratePartition(cmd) => match self.partitions.apply_migrate(batch, cmd)? {
                Some(peers) => Ok(ApplyResult::PartitionMigrated { peers }),
                None => Ok(ApplyResult::Rejected(RejectReason::NotFound)),
            },
            PdEntry::PutConfig(cmd) => {
                self.configs.apply_put(batch, cmd)?;
                Ok(ApplyResult::Applied)
            }
            PdEntry::PutCredential(cmd) => {
                self.credentials.apply_put(batch, cmd)?;
                Ok(ApplyResult::Applied)
            }
            PdEntry::DeleteConfig(cmd) => {
                self.configs.apply_delete(batch, cmd)?;
                Ok(ApplyResult::Applied)
            }
            PdEntry::RegisterWriter(cmd) => {
                // A writer token belongs to a registered gateway node
                // (cross-manager invariant, deterministic — reads committed node
                // state only).
                if !self.nodes.contains(cmd.node_id) {
                    return Ok(ApplyResult::Rejected(RejectReason::NodeNotFound));
                }
                Ok(ApplyResult::WriterRegistered {
                    token: self.writers.apply_register(batch, cmd)?,
                })
            }
            PdEntry::MarkWriterDead(cmd) => {
                Ok(reject_to_result(self.writers.apply_mark_dead(batch, cmd)?))
            }
            PdEntry::Job(cmd) => match self.jobs.apply(batch, cmd)? {
                crate::job::JobOutcome::Created(job_id) => Ok(ApplyResult::JobCreated { job_id }),
                crate::job::JobOutcome::Applied => Ok(ApplyResult::Applied),
                crate::job::JobOutcome::Rejected(reason) => Ok(ApplyResult::Rejected(reason)),
            },
            PdEntry::CommitShardMapping(cmd) => {
                // Invariant 2 (01 §6.4): a shard-mapping rebind is authorized only
                // for the Job's current coordinator while the Job is Running. The
                // staleness fence for the rebind itself is the *slot* epoch,
                // enforced in `apply_commit_shard_mapping`; `authorize_commit`'s
                // epoch check is therefore a no-op here (both args equal) — its job
                // is the ownership + Running check. Deterministic (committed state).
                if crate::job::authorize_commit(
                    self.jobs.get(cmd.job_id).as_ref(),
                    cmd.committer,
                    0,
                    0,
                )
                .is_err()
                {
                    return Ok(ApplyResult::Rejected(RejectReason::NotFound));
                }
                // Anti-affinity gate (01 §4.1): a repair/migration rebind must not
                // collocate two shards of this chunk in one fault domain.
                if let Some(reason) =
                    self.rebind_affinity_reject(cmd.chunk_id, cmd.index, cmd.new_disk)
                {
                    return Ok(ApplyResult::Rejected(reason));
                }
                match self.chunks.apply_commit_shard_mapping(batch, cmd)? {
                    None => Ok(ApplyResult::ShardRebound {
                        new_epoch: cmd.expected_epoch + 1,
                    }),
                    Some(reason) => Ok(ApplyResult::Rejected(reason)),
                }
            }
            PdEntry::ReportShardRepair(cmd) => {
                // Resolve the target node + observed epoch from committed state
                // (never the reporter's claim): find the shard slot in its chunk,
                // then the node owning the slot's disk. A missing chunk/slot/disk
                // is a benign rejection (the reporter's view was stale).
                let Some(chunk) = self.chunks.get(cmd.chunk_id) else {
                    return Ok(ApplyResult::Rejected(RejectReason::NotFound));
                };
                let Some(slot) = chunk.shards.iter().find(|s| s.index() == cmd.index) else {
                    return Ok(ApplyResult::Rejected(RejectReason::NotFound));
                };
                let Some(disk) = self.disks.get(slot.disk_id) else {
                    return Ok(ApplyResult::Rejected(RejectReason::NotFound));
                };
                let ticket =
                    crate::shard_repair::ticket_from_slot(cmd.chunk_id, slot, disk.node_id);
                let created = matches!(
                    self.shard_repairs.apply_report(batch, ticket)?,
                    crate::shard_repair::ReportOutcome::Created
                );
                Ok(ApplyResult::ShardRepairReported { created })
            }
            PdEntry::CommitShardRepair(cmd) => {
                // Invariant 2 (01 §6.4): a shard-mapping rebind is authorized here
                // by a matching ShardRepair *ticket* (target node + observed
                // epoch) in place of a Job — ShardRepair is 无 Job (01 §6.3). The
                // slot-epoch staleness fence is enforced again in `apply_rebind`.
                // Deterministic (reads committed ticket state only).
                let prefix = crate::shard_repair::shard_prefix(cmd.chunk_id, cmd.index);
                if crate::shard_repair::authorize_commit(
                    self.shard_repairs.get(prefix).as_ref(),
                    cmd.committer,
                    cmd.expected_epoch,
                )
                .is_err()
                {
                    return Ok(ApplyResult::Rejected(RejectReason::NotFound));
                }
                // Anti-affinity gate (01 §4.1): the ShardRepair coordinator targets
                // its *own* disk for data affinity, so without this check repeated
                // repairs would pile shards of one chunk onto one node.
                if let Some(reason) =
                    self.rebind_affinity_reject(cmd.chunk_id, cmd.index, cmd.new_disk)
                {
                    return Ok(ApplyResult::Rejected(reason));
                }
                match self.chunks.apply_rebind(
                    batch,
                    cmd.chunk_id,
                    cmd.index,
                    cmd.expected_epoch,
                    cmd.new_disk,
                    cmd.new_create_ts,
                )? {
                    None => {
                        // Rebind applied → clear the ticket (idempotent).
                        self.shard_repairs.apply_clear(batch, prefix)?;
                        Ok(ApplyResult::ShardRebound {
                            new_epoch: cmd.expected_epoch + 1,
                        })
                    }
                    Some(reason) => Ok(ApplyResult::Rejected(reason)),
                }
            }
        }
    }

    /// Rejects a shard rebind that would collocate two shards of `chunk_id` in
    /// one fault domain (01 §4.1 anti-affinity).
    ///
    /// Deterministic: every domain is resolved from committed chunk/disk state,
    /// never from the reporter's claim or heartbeat memory, so replicas reach the
    /// same verdict (AGENTS §8).
    ///
    /// INVARIANT(design 01 §4.1): anti-affinity belongs to the chunk, so **both**
    /// rebind paths (`CommitShardMapping` for Job-driven repair/migration and
    /// `CommitShardRepair` for ticket-driven single-shard repair) pass through
    /// here. `plan_placement` only covers creation; a rebind that skipped this
    /// check would silently degrade EC redundancy to a correlated risk — reads
    /// keep working, so nothing surfaces it until one node takes two shards down.
    fn rebind_affinity_reject(
        &self,
        chunk_id: ChunkId,
        index: u8,
        new_disk: DiskId,
    ) -> Option<RejectReason> {
        let Some(chunk) = self.chunks.get(chunk_id) else {
            // A missing chunk is `apply_rebind`'s NotFound to report.
            return None;
        };
        let Some(target) = self.disks.get(new_disk) else {
            return Some(RejectReason::DiskUnavailable);
        };
        // The chunk's other slots, with their domains from committed state. A slot
        // whose disk is gone contributes no constraint (it cannot be a collision).
        let others: Vec<SlotDomain> = chunk
            .shards
            .iter()
            .filter(|slot| slot.index() != index)
            .filter_map(|slot| {
                self.disks.get(slot.disk_id).map(|disk| SlotDomain {
                    index: slot.index(),
                    node_id: disk.node_id,
                    rack: disk.rack.clone(),
                })
            })
            .collect();
        let candidate = DiskCandidate {
            disk_id: new_disk,
            node_id: target.node_id,
            rack: target.rack.clone(),
            // Unused by the affinity predicate; placement weighs it, not this gate.
            writable_extents: 0,
        };
        if rebind_preserves_affinity(&others, &candidate, REBIND_AFFINITY) {
            None
        } else {
            Some(RejectReason::AffinityViolation)
        }
    }

    /// Validates a chunk creation plan: its shape (via [`validate_slots`]) and
    /// that every target disk is a registered, `Normal` disk. Returns the
    /// rejection reason, or `None` if the plan is applicable.
    fn chunk_plan_reject(&self, cmd: &CreateChunkStaging) -> Option<RejectReason> {
        if let Some(reason) = validate_slots(&cmd.code_mode, &cmd.slots) {
            return Some(reason);
        }
        for slot in &cmd.slots {
            match self.disks.get(slot.disk_id) {
                Some(disk) if disk.status == DiskStatus::Normal => {}
                _ => return Some(RejectReason::DiskUnavailable),
            }
        }
        None
    }
}

/// Maps a manager's optional rejection to an [`ApplyResult`].
fn reject_to_result(reject: Option<RejectReason>) -> ApplyResult {
    reject.map_or(ApplyResult::Applied, ApplyResult::Rejected)
}
