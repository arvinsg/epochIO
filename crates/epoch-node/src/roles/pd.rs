//! The `pd` role: run one PD replica — openraft peer transport and the
//! control-plane gRPC on one port, the leader liveness ticker, and the
//! watermark-driven placement loop (01 §4.1, M4).
//!
//! Bootstrap: the lowest-id replica initializes membership once (idempotent —
//! a restart with persisted membership is a no-op); the others join as the
//! replicated entry commits. Tickers re-check leadership every tick, so no
//! start/stop lifecycle plumbing is needed on failover.
//!
//! Design: docs/design/01-pd.md; docs/design/07-iteration-plan.md (M4)

use std::future::Future;
use std::sync::Arc;

use epoch_pd::{
    Journal, LivenessConfig, PdControlService, PdRaftPeerService, PlacementConfig, SystemClock,
    spawn_placement,
};

use crate::config::{ClusterConfig, PdSpec};
use crate::error::NodeError;

/// Runs the `pd` role for replica `node_id` (the raft node id in `[[pdnode]]`)
/// until `shutdown` resolves, then stops the raft node.
///
/// # Errors
///
/// [`NodeError`] if the replica id is not in the config, the raft stores cannot
/// be opened, bootstrap fails, or the gRPC server cannot bind.
pub async fn run(
    config: &ClusterConfig,
    node_id: u64,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), NodeError> {
    let spec = config
        .pdnode(node_id)
        .ok_or(NodeError::UnknownNode(node_id as u32))?;
    let addr = spec.socket_addr()?;

    // 1. Open the raft stores (membership is initialized later by the bootstrap
    //    replica — this node may come up before or after it).
    let journal = Arc::new(
        Journal::open_member(&spec.dir, spec.id)
            .await
            .map_err(|e| NodeError::Pd(format!("open journal: {e}")))?,
    );

    // 2. Bootstrap once from the designated (lowest-id) replica; idempotent.
    if spec.id == config.bootstrap_pd_id() {
        journal
            .initialize_cluster(config.pd_members())
            .await
            .map_err(|e| NodeError::Pd(format!("initialize cluster: {e}")))?;
    }

    // 3. gRPC: raft peer transport + control plane on one port.
    let raft_service = PdRaftPeerService::new(journal.raft().clone()).into_server();
    let control = PdControlService::new(Arc::clone(&journal), Arc::new(SystemClock)).into_server();
    let serve = tonic::transport::Server::builder()
        .add_service(raft_service)
        .add_service(control)
        .serve_with_shutdown(addr, shutdown);

    // 4. Background loops (leader-scoped at sweep time).
    let _liveness = journal.start_liveness(LivenessConfig::default(), Arc::new(SystemClock));
    // Writer-session liveness (01 §4.3): retire tokens whose gateway heartbeat
    // lapsed — the PD half of the liveness contract (the gateway half is its
    // session's stale gate). Sweep at a third of the retirement window.
    let _writer_liveness = journal.start_writer_liveness(
        std::time::Duration::from_millis(epoch_pd::DEFAULT_WRITER_DEAD_AFTER_MILLIS / 3),
        epoch_pd::DEFAULT_WRITER_DEAD_AFTER_MILLIS,
        Arc::new(SystemClock),
    );
    let _placement = spawn_placement(
        Arc::clone(&journal),
        config.code_mode()?,
        PlacementConfig::default(),
        Arc::new(SystemClock),
    );
    // Partition scheduler (M7, 01 §5): the PD-leader loop that splits oversized
    // partitions and migrates replicas off failed / overloaded MetaNodes. Its
    // MetaNode-facing side dials MetaNode admin RPCs, resolving addresses from
    // the committed node table + leader reports.
    let admin: Arc<dyn epoch_pd::meta_sched::MetaAdmin> = Arc::new(
        crate::roles::meta_admin::PdMetaAdmin::new(Arc::clone(&journal)),
    );
    let _scheduler = epoch_pd::meta_sched::spawn_meta_scheduler(
        Arc::clone(&journal),
        admin,
        config.scheduler.to_config(),
        config.scheduler.interval(),
    );
    // Job lease sweep (M7, 01 §6.2): reassign Jobs whose coordinator lease
    // lapsed. Sweep at a third of the lease window so a lapse is caught well
    // within one lease period.
    let _job_lease = epoch_pd::job::spawn_job_lease(
        Arc::clone(&journal),
        std::time::Duration::from_millis(epoch_pd::job::JOB_LEASE_MILLIS / 3),
        Arc::new(SystemClock),
    );
    // Repair trigger (M7, 02 §1.8 / 01 §6.3): start a RepairDisk job for every
    // disk a heartbeat newly flags Broken. Same cadence as the placement sweep.
    let _repair_trigger = epoch_pd::job::spawn_repair_trigger(
        Arc::clone(&journal),
        std::time::Duration::from_secs(5),
        config.scheduler.migrate_tolerant_ratio,
    );
    // Inspect trigger (M7, 01 §6.3/§6.4 correctness backstop): periodically run a
    // full-cluster stripe-presence scan so a silently-missing shard is found and
    // repaired even if no read touches it. The interval is the SLO exposure-window
    // bound; a short dev-scale cadence here (production tunes toward §6.4's ≤7d).
    let _inspect_trigger = epoch_pd::job::spawn_inspect_trigger(
        Arc::clone(&journal),
        config.scheduler.inspect_interval(),
    );

    // Observability: serve /metrics on the control port + 1000 (08 §5.1, each
    // role exposes its own endpoint) and keep the PD raft gauges fresh. Both
    // handles abort on drop, so a killed replica stops serving/updating.
    let _metrics =
        crate::metrics_server::spawn_metrics_server(crate::metrics_server::metrics_addr(addr));
    let _pd_gauges = spawn_pd_gauges(Arc::clone(&journal));

    // Web Console (08 §1/§4): PD hosts /console + /api/v1 on the control port +
    // 2000. Any replica serves the static login page; API reads are leader-gated
    // (follower → 307 to the leader's console, derived by the same offset). The
    // handle aborts on drop with the role.
    //
    // Object browse (08 §4): the console lists object metadata via a MetaClient
    // routed through PD. `epoch-pd` defines the browser trait; we own the client
    // and inject the implementation (like `MetaAdmin`) so PD keeps no dependency
    // on `epoch-client`. A bad PD endpoint list only disables object browse (the
    // endpoints answer 501) — it never blocks the PD role from serving.
    let pd_endpoints: Vec<String> = config.pd_members().into_values().collect();
    let mut console_state = epoch_pd::console::ConsoleState::new(Arc::clone(&journal));
    match epoch_client::PdClient::connect(&pd_endpoints) {
        Ok(pd_client) => {
            let browser =
                std::sync::Arc::new(crate::roles::object_browser::MetaObjectBrowser::new(
                    epoch_client::MetaClient::new(pd_client),
                ));
            console_state = console_state.with_browser(browser);
        }
        Err(err) => {
            tracing::warn!(error = %err, "console object browse disabled: PD client connect failed");
        }
    }
    let _console = epoch_pd::console::spawn_console_server_with_state(
        epoch_pd::console::console_addr(addr),
        console_state,
    );

    tracing::info!(replica = spec.id, %addr, "pd serving");
    serve
        .await
        .map_err(|e| NodeError::Pd(format!("grpc serve: {e}")))?;

    journal
        .shutdown()
        .await
        .map_err(|e| NodeError::Pd(format!("raft shutdown: {e}")))?;
    Ok(())
}

/// Spawns a leader-gated updater that refreshes the PD raft gauges (08 §5.1:
/// `epochio_pd_raft_applied_index` / `_is_leader`) every few seconds from the
/// raft metrics. Registered once against the process registry. The returned
/// handle **aborts the task on drop** so a killed replica's updater stops
/// holding its `Journal` alive (else raft shutdown / failover would stall).
fn spawn_pd_gauges(journal: Arc<Journal>) -> crate::metrics_server::MetricsHandle {
    use epoch_telemetry::metrics::{metric_name, registry};
    use prometheus::IntGauge;

    let applied = IntGauge::new(
        metric_name("pd", "raft", "applied_index"),
        "The PD raft last-applied log index.",
    )
    .expect("valid applied_index gauge");
    let is_leader = IntGauge::new(
        metric_name("pd", "raft", "is_leader"),
        "1 when this PD replica is the raft leader, else 0.",
    )
    .expect("valid is_leader gauge");
    let _ = registry().register(Box::new(applied.clone()));
    let _ = registry().register(Box::new(is_leader.clone()));

    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            ticker.tick().await;
            let m = journal.raft().metrics().borrow().clone();
            if let Some(log_id) = m.last_applied {
                applied.set(log_id.index as i64);
            }
            is_leader.set(i64::from(m.state.is_leader()));
        }
    });
    crate::metrics_server::MetricsHandle::from_task(task)
}

/// Identity of one PD replica in `[[pd]]` (raft node id, gRPC address, store dir).
impl PdSpec {
    /// Parses the gRPC listen address.
    ///
    /// # Errors
    ///
    /// [`crate::error::ConfigError::Address`] if `addr` is not a valid socket address.
    pub fn socket_addr(&self) -> Result<std::net::SocketAddr, crate::error::ConfigError> {
        self.addr
            .parse()
            .map_err(|_| crate::error::ConfigError::Address(self.addr.clone()))
    }
}
