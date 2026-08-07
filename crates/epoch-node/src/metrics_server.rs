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

//! The per-role `/metrics` endpoint (08 §5.1): a lightweight hyper 1 HTTP server
//! that answers `GET /metrics` with the process registry's Prometheus text
//! exposition. Each role (PD / MetaNode / DataNode / gateway) exposes its own
//! endpoint — metrics are scraped per node, never aggregated through PD (08 §3).
//!
//! Built on the workspace's hyper 1 + hyper-util (no axum, per 08 §7). The
//! endpoint is best-effort observability: a bind failure is logged and the role
//! serves without it rather than failing startup.
//!
//! Design: docs/design/08-web-console.md §5.1/§7

use std::convert::Infallible;
use std::net::SocketAddr;

/// The `/metrics` port offset from a role's primary listen port (08 §5.1: each
/// role serves its own endpoint; +1000 mirrors the `meta_addr` convention).
pub const METRICS_PORT_OFFSET: u16 = 1000;

/// Derives an auxiliary listen address from a primary one by `offset`.
///
/// The offset applies *downward* when adding would leave the u16 range. Plain
/// wrapping maps a high OS-assigned port (e.g. 64409 + 1000) onto a low
/// privileged port that cannot be bound without root — the endpoint would then
/// silently never serve, which for `/metrics` means a blind cluster.
#[must_use]
pub fn derived_addr(primary: SocketAddr, offset: u16) -> SocketAddr {
    let mut addr = primary;
    let port = primary.port();
    addr.set_port(match port.checked_add(offset) {
        Some(p) => p,
        // The offset is small relative to u16::MAX, so a port that overflowed the
        // add is always large enough to subtract from.
        None => port - offset,
    });
    addr
}

/// The `/metrics` address for a role bound at `primary`.
#[must_use]
pub fn metrics_addr(primary: SocketAddr) -> SocketAddr {
    derived_addr(primary, METRICS_PORT_OFFSET)
}

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Spawns the `/metrics` server on `addr`, serving until the task is aborted
/// (the returned handle aborts on drop). Returns `None` if the socket cannot be
/// bound — observability is best-effort and never blocks a role from serving.
#[must_use]
pub fn spawn_metrics_server(addr: SocketAddr) -> Option<MetricsHandle> {
    let std_listener = match std::net::TcpListener::bind(addr) {
        Ok(l) => l,
        Err(err) => {
            tracing::warn!(%addr, error = %err, "metrics endpoint bind failed; serving without it");
            return None;
        }
    };
    if let Err(err) = std_listener.set_nonblocking(true) {
        tracing::warn!(%addr, error = %err, "metrics endpoint nonblocking failed");
        return None;
    }
    let task = tokio::spawn(async move {
        let listener = match TcpListener::from_std(std_listener) {
            Ok(l) => l,
            Err(err) => {
                tracing::warn!(error = %err, "metrics listener adopt failed");
                return;
            }
        };
        tracing::info!(%addr, "metrics endpoint serving /metrics");
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let io = TokioIo::new(stream);
            tokio::spawn(async move {
                let _ = ConnBuilder::new(TokioExecutor::new())
                    .serve_connection(io, service_fn(handle))
                    .await;
            });
        }
    });
    Some(MetricsHandle { task })
}

/// Answers `GET /metrics` with the registry text; anything else is 404.
async fn handle(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    if req.uri().path() == "/metrics" {
        let body = epoch_telemetry::metrics::encode();
        let resp = Response::builder()
            .status(StatusCode::OK)
            .header(
                hyper::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )
            .body(Full::new(Bytes::from(body)))
            .expect("valid metrics response");
        return Ok(resp);
    }
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from_static(b"not found")))
        .expect("valid 404 response"))
}

/// Owns the metrics server task; aborts it on drop.
#[derive(Debug)]
pub struct MetricsHandle {
    task: JoinHandle<()>,
}

impl MetricsHandle {
    /// Wraps an already-spawned task so it aborts on drop — used for auxiliary
    /// observability loops (e.g. the PD raft-gauge updater) that must not
    /// outlive their owning role.
    #[must_use]
    pub fn from_task(task: JoinHandle<()>) -> Self {
        Self { task }
    }
}

impl Drop for MetricsHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
