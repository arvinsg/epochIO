//! The cluster blueprint: a single TOML file shared by every process.
//!
//! A data node selects itself with `--node <id>` and uses its own disk/addr
//! plus the chunk map to provision the shards it hosts; a driver reads the same
//! file to build its transport, code mode and placement. Fields are validated
//! at [`ClusterConfig::parse`]; the typed accessors below assume that
//! validation succeeded.
//!
//! Design: docs/design/06-code-layout.md §12; docs/design/07-iteration-plan.md (M3).

use std::net::SocketAddr;
use std::path::PathBuf;

use epoch_proto::{ChunkId, NodeId, ShardId};
use serde::Deserialize;

use crate::error::ConfigError;

/// The whole-cluster blueprint.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Cluster identity as a hex `u128` (guards against misplugged disks).
    pub cluster_id: String,
    /// Per-extent on-disk reservation; defaults to [`DEFAULT_EXTENT_SIZE`].
    ///
    /// [`DEFAULT_EXTENT_SIZE`]: epoch_proto::consts::DEFAULT_EXTENT_SIZE
    #[serde(default = "default_extent_size")]
    pub extent_size: u64,
    /// PD control-plane gRPC endpoints (`host:port` each). Empty selects the
    /// static M3 fallback (config-file chunk map); non-empty selects PD-driven
    /// mode: registration, heartbeats, and PD-driven chunk creation (M4).
    #[serde(default)]
    pub pd: Vec<String>,
    /// PD replicas (`[[pd]]` tables): the `pd` role picks itself by raft id.
    #[serde(rename = "pdnode", default)]
    pub pdnodes: Vec<PdSpec>,
    /// The nodes in the cluster (`[[node]]` tables).
    #[serde(rename = "node")]
    pub nodes: Vec<NodeSpec>,
    /// The erasure code mode.
    pub code: CodeSpec,
    /// The static writer identity (M3 stand-in for PD-issued tokens).
    pub writer: WriterSpec,
    /// The chunk → shard placement (`[[chunk]]` tables).
    #[serde(rename = "chunk", default)]
    pub chunks: Vec<ChunkSpec>,
    /// MetaNode role tuning (`[meta]` table; every field has a production
    /// default, 03 §8).
    #[serde(default)]
    pub meta: MetaSpec,
    /// Partition-scheduler tuning (`[scheduler]` table; every field has a
    /// production default, 01 §5).
    #[serde(default)]
    pub scheduler: SchedulerSpec,
    /// DataNode QoS byte-rate budgets (`[qos]` table; every field defaults to
    /// unlimited, 02 §1.7).
    #[serde(default)]
    pub qos: QosSpec,
    /// GcRound self-scan tuning (`[gc]` table; every field has a default).
    #[serde(default)]
    pub gc: GcSpec,
    /// Compaction / scrub maintenance tuning (`[maintenance]` table; every field
    /// has a default).
    #[serde(default)]
    pub maintenance: MaintenanceSpec,
}

fn default_extent_size() -> u64 {
    epoch_proto::consts::DEFAULT_EXTENT_SIZE
}

/// One node's identity, listen address, and disk directory.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
    /// Node id (also used as the disk id in M3).
    pub id: u32,
    /// Data-plane listen address (`host:port`).
    pub addr: String,
    /// MetaNode listen address (`host:port`) when this node also hosts the
    /// `meta` role. Defaults to the data-plane port + 1000 so a co-located
    /// dev node runs both roles without a port clash (they are distinct PD
    /// nodes with distinct addresses).
    #[serde(default)]
    pub meta_addr: Option<String>,
    /// Disk root directory (auto-formatted if unregistered, M3 convenience).
    pub disk: PathBuf,
    /// Availability zone (topology reported to PD on registration; dev default).
    #[serde(default = "default_az")]
    pub az: String,
    /// Rack (topology reported to PD on registration; dev default).
    #[serde(default = "default_rack")]
    pub rack: String,
}

fn default_az() -> String {
    "az1".to_string()
}

fn default_rack() -> String {
    "r1".to_string()
}

impl NodeSpec {
    /// Parses the listen address.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Address`] if `addr` is not a valid socket address.
    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        self.addr
            .parse()
            .map_err(|_| ConfigError::Address(self.addr.clone()))
    }

    /// The MetaNode listen address: the explicit `meta_addr`, else the
    /// data-plane address with its port raised by 1000 (a distinct co-located
    /// port so the `meta` and `data` roles do not clash in a dev cluster).
    ///
    /// # Errors
    ///
    /// [`ConfigError::Address`] if neither address parses.
    pub fn meta_socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        if let Some(addr) = &self.meta_addr {
            return addr.parse().map_err(|_| ConfigError::Address(addr.clone()));
        }
        let mut derived = self.socket_addr()?;
        let port = derived
            .port()
            .checked_add(1000)
            .ok_or_else(|| ConfigError::Address(self.addr.clone()))?;
        derived.set_port(port);
        Ok(derived)
    }
}

/// Erasure code parameters.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeSpec {
    /// Data shards per stripe.
    pub data: usize,
    /// Parity shards per stripe.
    pub parity: usize,
    /// Stripe (coding unit) size in bytes.
    pub stripe_size: usize,
    /// Blob (object cut) size in bytes.
    pub blob_size: usize,
}

impl CodeSpec {
    /// Total shards per stripe (`data + parity`).
    #[must_use]
    pub fn total(&self) -> usize {
        self.data + self.parity
    }
}

/// The static writer token.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterSpec {
    /// Writer token value.
    pub token: u32,
}

/// MetaNode role tuning (03 §8; production defaults, overridable for tests
/// and ops).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaSpec {
    /// Delete-queue safety delay in seconds (03 §8: 误删干预窗口, default 2h).
    #[serde(default = "default_delete_safety_delay")]
    pub delete_safety_delay_secs: u64,
    /// Delete sweep interval in seconds (03 §8: default 60).
    #[serde(default = "default_delete_sweep_interval")]
    pub delete_sweep_interval_secs: u64,
}

impl Default for MetaSpec {
    fn default() -> Self {
        Self {
            delete_safety_delay_secs: default_delete_safety_delay(),
            delete_sweep_interval_secs: default_delete_sweep_interval(),
        }
    }
}

fn default_delete_safety_delay() -> u64 {
    2 * 60 * 60
}

fn default_delete_sweep_interval() -> u64 {
    60
}

/// Partition-scheduler tuning (01 §5; production defaults, overridable for
/// tests and ops). Drives the PD-leader split/migrate loop.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerSpec {
    /// Scheduler sweep interval in seconds (default 30).
    #[serde(default = "default_scheduler_interval")]
    pub interval_secs: u64,
    /// Split a partition once its live size exceeds this many bytes
    /// (soft per-partition size cap, 01 §5; default 4 GiB).
    #[serde(default = "default_split_threshold_bytes")]
    pub split_threshold_bytes: u64,
    /// Ceiling on concurrent partition migrations (data moves; default 2).
    #[serde(default = "default_max_concurrent_migrations")]
    pub max_concurrent_migrations: usize,
    /// Balance tolerance ratio (a node may carry `quota * (1 + ratio)`
    /// partitions before it is overloaded; default 0.2).
    #[serde(default = "default_migrate_tolerant_ratio")]
    pub migrate_tolerant_ratio: f64,
    /// InspectRound interval in seconds — the SLO exposure-window bound for a
    /// silently-missing shard (01 §6.4; default 3600 = hourly at cluster scale,
    /// production tunes toward ≤7d full-round completion).
    #[serde(default = "default_inspect_interval")]
    pub inspect_interval_secs: u64,
}

impl Default for SchedulerSpec {
    fn default() -> Self {
        Self {
            interval_secs: default_scheduler_interval(),
            split_threshold_bytes: default_split_threshold_bytes(),
            max_concurrent_migrations: default_max_concurrent_migrations(),
            migrate_tolerant_ratio: default_migrate_tolerant_ratio(),
            inspect_interval_secs: default_inspect_interval(),
        }
    }
}

impl SchedulerSpec {
    /// The pure-decision config the scheduler planner consumes.
    #[must_use]
    pub fn to_config(&self) -> epoch_pd::meta_sched::SchedulerConfig {
        epoch_pd::meta_sched::SchedulerConfig {
            split_threshold_bytes: self.split_threshold_bytes,
            max_concurrent_migrations: self.max_concurrent_migrations,
            migrate_tolerant_ratio: self.migrate_tolerant_ratio,
        }
    }

    /// The sweep interval as a `Duration`.
    #[must_use]
    pub fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.interval_secs)
    }

    /// The InspectRound interval as a `Duration`.
    #[must_use]
    pub fn inspect_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.inspect_interval_secs)
    }
}

fn default_scheduler_interval() -> u64 {
    30
}

fn default_inspect_interval() -> u64 {
    3600
}

fn default_split_threshold_bytes() -> u64 {
    4 << 30
}

fn default_max_concurrent_migrations() -> usize {
    2
}

fn default_migrate_tolerant_ratio() -> f64 {
    0.2
}

/// DataNode QoS byte-rate budgets (02 §1.7). Each rate is bytes/sec; `0` (the
/// default) is unlimited. The repair rate is the MTTR knob (01 §6.3): capping it
/// bounds rebuild bandwidth so a repair storm yields to foreground IO.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QosSpec {
    /// Foreground (user data-plane) byte-rate budget; 0 = unlimited.
    #[serde(default)]
    pub foreground_rate: u64,
    /// Background (compaction / scrub / GC) byte-rate budget; 0 = unlimited.
    #[serde(default)]
    pub background_rate: u64,
    /// Repair (rebuild) byte-rate budget = MTTR knob; 0 = unlimited.
    #[serde(default)]
    pub repair_rate: u64,
    /// Burst capacity in bytes each bucket may accumulate while idle.
    #[serde(default)]
    pub burst: u64,
}

impl QosSpec {
    /// The engine-level QoS config this spec configures.
    #[must_use]
    pub fn to_config(&self) -> epoch_store::QosConfig {
        epoch_store::QosConfig {
            foreground_rate: self.foreground_rate,
            background_rate: self.background_rate,
            repair_rate: self.repair_rate,
            burst: self.burst,
        }
    }
}

/// GcRound self-scan tuning (01 §6.3 / Q27). Each DataNode reclaims orphaned
/// blobs on its own disk on this cadence.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcSpec {
    /// Seconds between GC self-scan rounds (default 3600 = hourly at cluster
    /// scale; production tunes to balance reclaim latency vs scan cost).
    #[serde(default = "default_gc_interval")]
    pub interval_secs: u64,
}

impl Default for GcSpec {
    fn default() -> Self {
        Self {
            interval_secs: default_gc_interval(),
        }
    }
}

impl GcSpec {
    /// The GC round interval as a `Duration`.
    #[must_use]
    pub fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.interval_secs)
    }
}

fn default_gc_interval() -> u64 {
    3600
}

/// Compaction / scrub maintenance tuning (`[maintenance]`, 02 §1.6/§1.9).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceSpec {
    /// Seconds between maintenance sweeps (default 300). Each sweep compacts the
    /// dirtiest extents and scrubs a rotating slice of the disk.
    #[serde(default = "default_maintenance_interval")]
    pub interval_secs: u64,
    /// Tombstoned fraction (percent) at which an extent becomes a compaction
    /// candidate (default 30, 02 §1.6).
    #[serde(default = "default_compact_ratio_percent")]
    pub compact_ratio_percent: u8,
    /// Max extents compacted per sweep (default 4). Bounds the background I/O a
    /// single sweep can cause; the rest wait for the next one.
    #[serde(default = "default_compact_budget")]
    pub compact_per_sweep: usize,
    /// Max extents scrubbed per sweep (default 2; 0 disables scrub). Scrub walks
    /// the disk round-robin so cold data is eventually verified (02 §1.9).
    #[serde(default = "default_scrub_budget")]
    pub scrub_per_sweep: usize,
}

impl Default for MaintenanceSpec {
    fn default() -> Self {
        Self {
            interval_secs: default_maintenance_interval(),
            compact_ratio_percent: default_compact_ratio_percent(),
            compact_per_sweep: default_compact_budget(),
            scrub_per_sweep: default_scrub_budget(),
        }
    }
}

impl MaintenanceSpec {
    /// The maintenance sweep interval as a `Duration`.
    #[must_use]
    pub fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.interval_secs)
    }
}

fn default_maintenance_interval() -> u64 {
    300
}

fn default_compact_ratio_percent() -> u8 {
    30
}

fn default_compact_budget() -> usize {
    4
}

fn default_scrub_budget() -> usize {
    2
}

/// One chunk's placement: `shards[i]` is the node id hosting shard index `i`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkSpec {
    /// Chunk id.
    pub id: u32,
    /// Node id per shard index (`data + parity` entries, data shards first).
    pub shards: Vec<u32>,
}

/// One PD replica (`[[pd]]` table): its raft node id, gRPC address, and the
/// directory its raft stores live in.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PdSpec {
    /// Raft node id (also the bootstrap ordering key).
    pub id: u64,
    /// gRPC listen address (`host:port`) for both raft peer and control plane.
    pub addr: String,
    /// Raft store directory (log + state machine).
    pub dir: PathBuf,
}

impl ClusterConfig {
    /// Parses and validates a config from TOML text.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] on a parse failure or any validation violation
    /// (bad cluster id, no nodes, duplicate node id, bad address, invalid code
    /// mode, chunk shard-count mismatch, or a chunk referencing an unknown node).
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: ClusterConfig = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates the whole blueprint.
    fn validate(&self) -> Result<(), ConfigError> {
        self.cluster_id()?;
        if self.nodes.is_empty() {
            return Err(ConfigError::NoNodes);
        }
        let mut seen = std::collections::HashSet::new();
        for node in &self.nodes {
            if !seen.insert(node.id) {
                return Err(ConfigError::DuplicateNode(node.id));
            }
            node.socket_addr()?;
        }
        self.validate_code()?;
        let total = self.code.total();
        for chunk in &self.chunks {
            if chunk.shards.len() != total {
                return Err(ConfigError::ChunkShardCount {
                    chunk: chunk.id,
                    need: total,
                    got: chunk.shards.len(),
                });
            }
            for &node in &chunk.shards {
                if self.node(NodeId::new(node)).is_none() {
                    return Err(ConfigError::UnknownChunkNode {
                        chunk: chunk.id,
                        node,
                    });
                }
            }
        }
        Ok(())
    }

    /// Validates the code mode (range and sizing).
    fn validate_code(&self) -> Result<(), ConfigError> {
        let c = &self.code;
        if c.data == 0 || c.parity == 0 {
            return Err(ConfigError::Code("data and parity must be >= 1"));
        }
        if c.total() > 256 {
            return Err(ConfigError::Code(
                "data + parity must be <= 256 (shard index is u8)",
            ));
        }
        if c.stripe_size == 0 || c.blob_size < c.stripe_size {
            return Err(ConfigError::Code(
                "need stripe_size > 0 and blob_size >= stripe_size",
            ));
        }
        Ok(())
    }

    /// The cluster identity.
    ///
    /// # Errors
    ///
    /// [`ConfigError::ClusterId`] if `cluster_id` is not a hex `u128`.
    pub fn cluster_id(&self) -> Result<u128, ConfigError> {
        let hex = self
            .cluster_id
            .strip_prefix("0x")
            .unwrap_or(&self.cluster_id);
        u128::from_str_radix(hex, 16).map_err(|_| ConfigError::ClusterId(self.cluster_id.clone()))
    }

    /// The node with `id`, if defined.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<&NodeSpec> {
        self.nodes.iter().find(|n| n.id == id.get())
    }

    /// The PD replica with raft `id`, if defined.
    #[must_use]
    pub fn pdnode(&self, id: u64) -> Option<&PdSpec> {
        self.pdnodes.iter().find(|n| n.id == id)
    }

    /// The replica id that bootstraps the cluster (the lowest pd id).
    ///
    /// # Panics
    ///
    /// Panics if no `[[pd]]` is defined (the caller checks before use).
    #[must_use]
    pub fn bootstrap_pd_id(&self) -> u64 {
        self.pdnodes
            .iter()
            .map(|n| n.id)
            .min()
            .expect("bootstrap_pd_id requires at least one pd replica")
    }

    /// The full membership map `replica id → gRPC address` for bootstrap.
    #[must_use]
    pub fn pd_members(&self) -> std::collections::BTreeMap<u64, String> {
        self.pdnodes
            .iter()
            .map(|n| (n.id, n.addr.clone()))
            .collect()
    }

    /// The PD-published code mode (erasure parameters + stripe/blob sizing),
    /// derived from `[code]` with the default registry id.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Code`] if the counts overflow the wire widths.
    pub fn code_mode(&self) -> Result<epoch_proto::CodeMode, ConfigError> {
        Ok(epoch_proto::CodeMode {
            id: epoch_proto::CodeModeId::new(1),
            data: u8::try_from(self.code.data).map_err(|_| ConfigError::Code("data exceeds u8"))?,
            parity: u8::try_from(self.code.parity)
                .map_err(|_| ConfigError::Code("parity exceeds u8"))?,
            stripe_size: u32::try_from(self.code.stripe_size)
                .map_err(|_| ConfigError::Code("stripe_size exceeds u32"))?,
            blob_size: u64::try_from(self.code.blob_size)
                .map_err(|_| ConfigError::Code("blob_size exceeds u64"))?,
        })
    }

    /// The shard slots this node hosts, derived from the chunk map (epoch 0).
    #[must_use]
    pub fn shards_hosted_by(&self, id: NodeId) -> Vec<ShardId> {
        let mut shards = Vec::new();
        for chunk in &self.chunks {
            for (index, &node) in chunk.shards.iter().enumerate() {
                if node == id.get() {
                    let index = u8::try_from(index).expect("shard index < 256 (validated)");
                    shards.push(ShardId::new(ChunkId::new(chunk.id), index, 0));
                }
            }
        }
        shards
    }

    /// Every node's `(id, address)`, for building a client transport map.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Address`] if any node address fails to parse.
    pub fn node_addrs(&self) -> Result<Vec<(NodeId, SocketAddr)>, ConfigError> {
        self.nodes
            .iter()
            .map(|n| Ok((NodeId::new(n.id), n.socket_addr()?)))
            .collect()
    }

    /// The `(shard slot, hosting node)` placement of `chunk`, in shard-index
    /// order (epoch 0).
    #[must_use]
    pub fn placement(&self, chunk: &ChunkSpec) -> Vec<(ShardId, NodeId)> {
        chunk
            .shards
            .iter()
            .enumerate()
            .map(|(index, &node)| {
                let index = u8::try_from(index).expect("shard index < 256 (validated)");
                (
                    ShardId::new(ChunkId::new(chunk.id), index, 0),
                    NodeId::new(node),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = r#"
cluster_id = "00c0ffee"
extent_size = 8388608

[[node]]
id = 0
addr = "127.0.0.1:9101"
disk = "/tmp/d0"

[[node]]
id = 1
addr = "127.0.0.1:9102"
disk = "/tmp/d1"

[[node]]
id = 2
addr = "127.0.0.1:9103"
disk = "/tmp/d2"

[code]
data = 2
parity = 1
stripe_size = 1048576
blob_size = 33554432

[writer]
token = 7

[[chunk]]
id = 1
shards = [0, 1, 2]
"#;

    /// The shipped example config must parse and validate.
    ///
    /// Without this, the one artifact an operator starts from is the first thing
    /// to rot: every added field, renamed table, or new validation rule can
    /// invalidate it silently, and the failure surfaces as a stranger's cluster
    /// refusing to boot.
    #[test]
    fn the_shipped_example_config_parses() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/deploy/cluster.example.toml"
        );
        let text = std::fs::read_to_string(path).expect("example config is present");
        let cfg = ClusterConfig::parse(&text).expect("example config parses and validates");

        // Spot-check that it describes a usable cluster rather than just parsing:
        // enough nodes for the code mode, and PD endpoints to dial.
        assert!(!cfg.pd.is_empty(), "example lists PD endpoints");
        assert!(
            cfg.nodes.len() >= cfg.code.total(),
            "the example's node count must satisfy its own code mode ({} nodes, \
             needs {}), else a cluster built from it can never place a chunk",
            cfg.nodes.len(),
            cfg.code.total()
        );
        // Every documented default must match the code's default, or the file
        // teaches the wrong values.
        assert_eq!(cfg.maintenance.compact_ratio_percent, 30);
        assert_eq!(cfg.gc.interval_secs, 3600);
    }

    #[test]
    fn parses_and_exposes_a_valid_blueprint() {
        let cfg = ClusterConfig::parse(GOOD).unwrap();
        assert_eq!(cfg.cluster_id().unwrap(), 0x00c0_ffee);
        assert_eq!(cfg.extent_size, 8 * 1024 * 1024);
        assert_eq!(cfg.code.total(), 3);
        assert_eq!(cfg.writer.token, 7);

        // Node 1 hosts shard index 1 of chunk 1.
        let hosted = cfg.shards_hosted_by(NodeId::new(1));
        assert_eq!(hosted.len(), 1);
        assert_eq!(hosted[0], ShardId::new(ChunkId::new(1), 1, 0));

        let addrs = cfg.node_addrs().unwrap();
        assert_eq!(addrs.len(), 3);
        assert_eq!(
            addrs[0],
            (NodeId::new(0), "127.0.0.1:9101".parse().unwrap())
        );

        let placement = cfg.placement(&cfg.chunks[0]);
        assert_eq!(placement.len(), 3);
        assert_eq!(
            placement[2],
            (ShardId::new(ChunkId::new(1), 2, 0), NodeId::new(2))
        );
    }

    #[test]
    fn default_extent_size_applies_when_omitted() {
        let text = GOOD.replace("extent_size = 8388608\n", "");
        let cfg = ClusterConfig::parse(&text).unwrap();
        assert_eq!(cfg.extent_size, epoch_proto::consts::DEFAULT_EXTENT_SIZE);
    }

    #[test]
    fn rejects_bad_cluster_id() {
        let text = GOOD.replace(r#"cluster_id = "00c0ffee""#, r#"cluster_id = "nothex""#);
        assert!(matches!(
            ClusterConfig::parse(&text),
            Err(ConfigError::ClusterId(_))
        ));
    }

    #[test]
    fn rejects_bad_address() {
        let text = GOOD.replace(r#"addr = "127.0.0.1:9101""#, r#"addr = "not-an-addr""#);
        assert!(matches!(
            ClusterConfig::parse(&text),
            Err(ConfigError::Address(_))
        ));
    }

    #[test]
    fn rejects_chunk_shard_count_mismatch() {
        let text = GOOD.replace("shards = [0, 1, 2]", "shards = [0, 1]");
        assert!(matches!(
            ClusterConfig::parse(&text),
            Err(ConfigError::ChunkShardCount {
                chunk: 1,
                need: 3,
                got: 2
            })
        ));
    }

    #[test]
    fn rejects_chunk_referencing_unknown_node() {
        let text = GOOD.replace("shards = [0, 1, 2]", "shards = [0, 1, 9]");
        assert!(matches!(
            ClusterConfig::parse(&text),
            Err(ConfigError::UnknownChunkNode { chunk: 1, node: 9 })
        ));
    }

    #[test]
    fn rejects_duplicate_node_id() {
        let text = GOOD.replace(
            "id = 2\naddr = \"127.0.0.1:9103\"",
            "id = 1\naddr = \"127.0.0.1:9103\"",
        );
        assert!(matches!(
            ClusterConfig::parse(&text),
            Err(ConfigError::DuplicateNode(1))
        ));
    }

    #[test]
    fn rejects_invalid_code_mode() {
        let text = GOOD.replace("blob_size = 33554432", "blob_size = 1024");
        assert!(matches!(
            ClusterConfig::parse(&text),
            Err(ConfigError::Code(_))
        ));
    }

    #[test]
    fn meta_addr_defaults_to_data_port_plus_1000() {
        let cfg = ClusterConfig::parse(GOOD).unwrap();
        // Node 0 has data addr 127.0.0.1:9101 and no explicit meta_addr.
        let node = cfg.node(NodeId::new(0)).unwrap();
        assert_eq!(node.meta_addr, None);
        assert_eq!(
            node.meta_socket_addr().unwrap(),
            "127.0.0.1:10101".parse().unwrap()
        );
    }

    #[test]
    fn explicit_meta_addr_overrides_the_derived_port() {
        let text = GOOD.replace(
            "id = 0\naddr = \"127.0.0.1:9101\"\ndisk = \"/tmp/d0\"",
            "id = 0\naddr = \"127.0.0.1:9101\"\nmeta_addr = \"127.0.0.1:7000\"\ndisk = \"/tmp/d0\"",
        );
        let cfg = ClusterConfig::parse(&text).unwrap();
        let node = cfg.node(NodeId::new(0)).unwrap();
        assert_eq!(
            node.meta_socket_addr().unwrap(),
            "127.0.0.1:7000".parse().unwrap()
        );
    }
}
