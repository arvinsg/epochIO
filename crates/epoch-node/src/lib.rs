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
