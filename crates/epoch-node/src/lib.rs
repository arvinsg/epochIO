//! epoch-node (L4): process assembly for the `epochio` binary.
//!
//! Wires a role's services together from a [`config::ClusterConfig`]: the
//! `data` role (disk open/format, shard provisioning, data plane), the `pd`
//! role (raft peer transport + control plane, leader tickers), and the
//! `meta` role (PD registration, multi-raft recovery, MetaNode gRPC + batched
//! raft transport, partition heartbeat + delete/upload sweeps).
//!
//! Design: docs/design/06-code-layout.md §12; docs/design/00-overview.md §3.

pub mod config;
pub mod error;
pub mod metrics_server;
pub mod roles;
pub mod shutdown;
pub mod sink;

pub use config::{
    ChunkSpec, ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec,
    WriterSpec,
};
pub use error::{ConfigError, NodeError};
