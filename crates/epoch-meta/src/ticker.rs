//! The node's periodic partition heartbeat reporter (06 §9 ticker.rs: 分区级
//! 后台循环 — 分区心跳统计上报; deleter / multipart TTL / 孤儿哨兵巡检 join
//! this loop in later phases, 03 §8).
//!
//! Every interval the node reports, for every locally-hosted group, the
//! leader it observes plus its applied index, and its full hosted set — the
//! input PD's route table (leader memory) and CreateRaftGroup reconciliation
//! consume (01 §5). Reports are best-effort: a failed round retries at the
//! next tick.
//!
//! Design: docs/design/01-pd.md §5 (PartitionHeartbeat)

use std::sync::Arc;
use std::time::Duration;

use epoch_proto::grpc::pd::pd_control_client::PdControlClient;
use epoch_proto::grpc::pd::{PartitionHeartbeatRequest, PartitionStat};
use tokio::task::JoinHandle;
use tonic::transport::Endpoint;

use crate::deleter::Deleter;
use crate::partition::PartitionRegistry;
use crate::raft::GroupManager;

/// The default heartbeat interval (01 §5: leader reports must refresh PD's
/// leader memory far faster than any client-side staleness window).
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// The default delq sweep interval (03 §8: 每分区 leader 后台 deleter).
pub const DEFAULT_DELETE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Spawns the partition-heartbeat loop: reports every hosted group to PD at
/// `pd_addr` every `interval`, until the returned handle is aborted.
#[must_use]
pub fn spawn_partition_heartbeat(
    manager: Arc<GroupManager>,
    node_id: u32,
    pd_addr: String,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let channel = Endpoint::from_shared(format!("http://{pd_addr}"))
            .map(|endpoint| endpoint.connect_lazy())
            .ok();
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately — report early so PD's route table
        // learns leaders right after bootstrap.
        loop {
            ticker.tick().await;
            let Some(channel) = channel.clone() else {
                tracing::warn!(pd_addr, "partition heartbeat: invalid PD address");
                return;
            };
            let mut stats = Vec::new();
            let mut hosted = Vec::new();
            for group in manager.group_ids() {
                hosted.push(group);
                let Some(raft) = manager.raft(group) else {
                    continue;
                };
                let metrics = raft.metrics().borrow().clone();
                let (inline_bytes, inline_count, total_bytes) = manager
                    .guard(group)
                    .map(|g| g.counters())
                    .unwrap_or_default();
                stats.push(PartitionStat {
                    partition_id: group,
                    leader_node_id: metrics
                        .current_leader
                        .and_then(|id| u32::try_from(id).ok())
                        .unwrap_or(0),
                    applied_index: metrics.last_applied.map(|l| l.index).unwrap_or(0),
                    inline_bytes,
                    inline_count,
                    total_bytes,
                });
            }
            let request = PartitionHeartbeatRequest {
                node_id,
                stats,
                hosted_partitions: hosted,
            };
            let mut client = PdControlClient::new(channel);
            if let Err(err) = client.partition_heartbeat(request).await {
                tracing::debug!(error = %err, "partition heartbeat failed");
            }
        }
    })
}

/// Spawns the expired-upload-session sweep (03 §5 废弃 multipart 会话清理):
/// every `interval`, every locally-hosted partition leader aborts its
/// expired sessions (the sweep itself gates on leadership).
#[must_use]
pub fn spawn_upload_ttl_sweep(
    manager: Arc<GroupManager>,
    registry: Arc<PartitionRegistry>,
    ttl: Duration,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            for group in manager.group_ids() {
                let (Some(raft), Some(info)) = (manager.raft(group), registry.info(group)) else {
                    continue;
                };
                match crate::ns_flat::multipart::sweep_expired(
                    &manager.store(),
                    &raft,
                    &info.range,
                    ttl,
                    now_millis,
                )
                .await
                {
                    Ok(aborted) if aborted > 0 => {
                        tracing::info!(
                            partition = group,
                            aborted,
                            "upload ttl sweep aborted expired sessions"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::debug!(partition = group, error = %err, "upload ttl sweep failed");
                    }
                }
            }
        }
    })
}

/// Spawns the split-reconcile loop (03 §2/§8): every `interval`, this node
/// derives the child group of any committed-but-not-yet-materialized split
/// (`reconcile_splits`). Without this loop a committed `SplitOp` narrows the
/// parent's range but the child group is never started, so the split never
/// completes in production. The reconcile is idempotent and crash-safe (it
/// drains persisted `PendingSplit` records and clears each once its child is
/// running), so a dumb periodic tick suffices; it is safe on every node (one
/// that does not host the child simply finds nothing to do).
#[must_use]
pub fn spawn_split_reconcile(manager: Arc<GroupManager>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match manager.reconcile_splits().await {
                Ok(derived) if !derived.is_empty() => {
                    tracing::info!(
                        derived = derived.len(),
                        "split reconcile derived child groups"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::debug!(error = %err, "split reconcile failed");
                }
            }
        }
    })
}

/// Spawns the orphan-sentinel sweep (03 §6.2 注: 孤儿哨兵回收): every
/// `interval`, every locally-hosted partition leader reclaims directory
/// sentinels whose parent link never committed (a crashed mkdir) once aged
/// past `ttl` (the sweep gates on leadership and the age window). Only hier
/// partitions carry sentinels; flat partitions are a cheap no-op.
#[must_use]
pub fn spawn_orphan_sentinel_sweep(
    manager: Arc<GroupManager>,
    registry: Arc<PartitionRegistry>,
    ttl: Duration,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            for group in manager.group_ids() {
                let (Some(raft), Some(info)) = (manager.raft(group), registry.info(group)) else {
                    continue;
                };
                match crate::ns_hier::sweep_orphan_sentinels(
                    &manager.store(),
                    &raft,
                    &info.range,
                    ttl,
                    now_millis,
                )
                .await
                {
                    Ok(reclaimed) if reclaimed > 0 => {
                        tracing::info!(
                            partition = group,
                            reclaimed,
                            "orphan sentinel sweep reclaimed directories"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::debug!(partition = group, error = %err, "orphan sentinel sweep failed");
                    }
                }
            }
        }
    })
}
/// Spawns the delq sweep loop (03 §8): every `interval`, one [`Deleter`]
/// sweep per locally-hosted group (the deleter itself gates on leadership and
/// the safety delay, so the loop stays dumb). Sweeps run sequentially per
/// node — tombstone fan-out happens inside each sweep's bounded concurrency.
#[must_use]
pub fn spawn_delete_sweep(
    manager: Arc<GroupManager>,
    registry: Arc<PartitionRegistry>,
    deleter: Arc<Deleter>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            for group in manager.group_ids() {
                let (Some(raft), Some(info)) = (manager.raft(group), registry.info(group)) else {
                    continue;
                };
                match deleter
                    .sweep(&manager.store(), &raft, &info.range, now_millis)
                    .await
                {
                    Ok(dequeued) if dequeued > 0 => {
                        tracing::info!(
                            partition = group,
                            dequeued,
                            "delete sweep dequeued entries"
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::debug!(partition = group, error = %err, "delete sweep failed");
                    }
                }
            }
        }
    })
}
