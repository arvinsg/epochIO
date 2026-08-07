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

//! The PD-hosted Web Console HTTP server (08 §4): a hyper 1 endpoint serving the
//! embedded frontend at `/console/*` and a read/JSON + thin-write API at
//! `/api/v1/*`. Runs inside the PD process (the console's data is the cluster
//! global view, authoritative in PD's raft state machine + leader memory, 08
//! §1). Same hyper stack as the S3 head — no axum (08 §7).
//!
//! Leader gating (08 §4): the console view is a leader read. A follower answers
//! `/api/v1/*` with **307** to the leader's console address (derived from the
//! leader's raft/gRPC address in membership by the same fixed port offset the
//! listener uses — a config-based port would not be discoverable from raft
//! membership). Static assets are served by any replica so the login page
//! always loads.
//!
//! Design: docs/design/08-web-console.md §1, §4, §4.1, §7

pub mod api;
pub mod assets;
pub mod auth;
pub mod browse;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::cluster::{Clock, SystemClock};
use crate::credential::ConsoleRole;
use crate::journal::Journal;

use auth::{SESSION_COOKIE, Session, SessionStore};

/// The fixed offset added to a PD replica's gRPC/raft port to get its console
/// HTTP port. Chosen so the console address is derivable from the raft
/// membership address (which only carries the gRPC port) for the follower→leader
/// 307 — a config-based port could not be discovered from membership. Mirrors
/// the metrics endpoint's +1000 convention (08 §4).
pub const CONSOLE_PORT_OFFSET: u16 = 2000;

/// The console address for a replica whose gRPC/raft address is `grpc`.
///
/// The offset is applied *downward* when adding it would leave the u16 range:
/// plain wrapping would land a high gRPC port (e.g. an OS-assigned 64409) on a
/// low privileged port (873) that cannot be bound without root, and the console
/// would silently fail to serve. Subtracting keeps the derivation total and the
/// result bindable, while staying a pure function of the gRPC port so a follower
/// can still compute the leader's console address from raft membership alone.
#[must_use]
pub fn console_addr(grpc: SocketAddr) -> SocketAddr {
    let mut addr = grpc;
    let port = grpc.port();
    let console_port = match port.checked_add(CONSOLE_PORT_OFFSET) {
        Some(p) => p,
        // Both operands are ≤ u16::MAX and the offset is small, so the
        // subtraction cannot underflow for any port that overflowed the add.
        None => port - CONSOLE_PORT_OFFSET,
    };
    addr.set_port(console_port);
    addr
}

/// Shared console server state: the PD journal (reads + write wrappers), the
/// in-memory session store, the clock (session TTL), and an optional injected
/// object browser (the MetaNode-facing read seam, 08 §4). Cheap to clone.
#[derive(Clone)]
pub struct ConsoleState {
    journal: Arc<Journal>,
    sessions: SessionStore,
    clock: Arc<dyn Clock>,
    browser: Option<Arc<dyn browse::ObjectBrowser>>,
}

impl ConsoleState {
    /// Assembles the state over the shared journal, with a fresh session store,
    /// the system clock, and no object browser (the object endpoints answer
    /// `501` until a browser is injected via [`with_browser`](Self::with_browser)).
    #[must_use]
    pub fn new(journal: Arc<Journal>) -> Self {
        Self {
            journal,
            sessions: SessionStore::new(),
            clock: Arc::new(SystemClock),
            browser: None,
        }
    }

    /// Attaches the object-browse implementation (injected by `epoch-node`,
    /// which owns the `MetaClient`). Enables the `/api/v1/objects*` endpoints.
    #[must_use]
    pub fn with_browser(mut self, browser: Arc<dyn browse::ObjectBrowser>) -> Self {
        self.browser = Some(browser);
        self
    }

    /// The journal (read side + write wrappers).
    #[must_use]
    pub fn journal(&self) -> &Arc<Journal> {
        &self.journal
    }

    /// The injected object browser, if any (`None` → object endpoints 501).
    #[must_use]
    pub fn browser(&self) -> Option<&Arc<dyn browse::ObjectBrowser>> {
        self.browser.as_ref()
    }

    /// Current wall-clock millis via the injected clock (heartbeat staleness).
    #[must_use]
    pub fn now_millis(&self) -> u64 {
        self.clock.now_millis()
    }

    /// Whether this replica is the current raft leader (console reads gate on it).
    fn is_leader(&self) -> bool {
        self.journal.raft().metrics().borrow().state.is_leader()
    }

    /// The current leader's **console** address, derived from its raft-membership
    /// gRPC address by [`console_addr`]. `None` if there is no known leader yet.
    fn leader_console_addr(&self) -> Option<String> {
        let m = self.journal.raft().metrics().borrow().clone();
        let leader = m.current_leader?;
        let grpc = m
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader)
            .map(|(_, node)| node.addr.clone())?;
        let parsed: SocketAddr = grpc.parse().ok()?;
        Some(console_addr(parsed).to_string())
    }
}

/// Owns the console server task; aborts it on drop (so a killed replica stops
/// serving, like the metrics/gauge handles).
#[derive(Debug)]
pub struct ConsoleHandle {
    task: JoinHandle<()>,
}

impl Drop for ConsoleHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the console HTTP server on `addr`, serving until the handle is dropped.
/// Returns `None` if the socket cannot be bound (logged; the PD role still
/// serves gRPC — the console is not on the data path). The object endpoints
/// answer `501` (no browser injected); use [`spawn_console_server_with_state`]
/// to enable object browse.
#[must_use]
pub fn spawn_console_server(addr: SocketAddr, journal: Arc<Journal>) -> Option<ConsoleHandle> {
    spawn_console_server_with_state(addr, ConsoleState::new(journal))
}

/// Spawns the console HTTP server on `addr` over a prebuilt [`ConsoleState`]
/// (e.g. one carrying an injected object browser). Returns `None` if the socket
/// cannot be bound.
#[must_use]
pub fn spawn_console_server_with_state(
    addr: SocketAddr,
    state: ConsoleState,
) -> Option<ConsoleHandle> {
    let std_listener = match std::net::TcpListener::bind(addr) {
        Ok(l) => l,
        Err(err) => {
            tracing::warn!(%addr, error = %err, "console bind failed; serving without it");
            return None;
        }
    };
    if std_listener.set_nonblocking(true).is_err() {
        return None;
    }
    let task = tokio::spawn(async move {
        let Ok(listener) = TcpListener::from_std(std_listener) else {
            return;
        };
        tracing::info!(%addr, "console serving /console");
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let io = TokioIo::new(stream);
            let state = state.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req| {
                    let state = state.clone();
                    async move { route(state, req).await }
                });
                let _ = ConnBuilder::new(TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });
    Some(ConsoleHandle { task })
}

/// Top-level request router: static `/console/*`, auth + JSON `/api/v1/*`,
/// everything else 404.
async fn route(
    state: ConsoleState,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    // Static frontend: any replica serves it (login page must always load).
    if path == "/" || path == "/console" || path.starts_with("/console/") {
        return Ok(serve_asset(&path));
    }
    if let Some(rest) = path.strip_prefix("/api/v1/") {
        return Ok(handle_api(state, rest, req).await);
    }
    Ok(text(StatusCode::NOT_FOUND, "not found"))
}

/// Serves an embedded frontend asset. `/` and `/console` → index; deep links
/// under `/console/` map to files, falling back to index for SPA client routes.
fn serve_asset(path: &str) -> Response<Full<Bytes>> {
    let rel = path
        .strip_prefix("/console/")
        .unwrap_or("")
        .trim_start_matches('/');
    let asset = assets::get(rel).unwrap_or_else(assets::index);
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, asset.content_type)
        .body(Full::new(Bytes::from_static(asset.bytes)))
        .expect("valid asset response")
}

/// `/api/v1/*` dispatch: `login` is public; everything else requires a session,
/// and reads are leader-gated (follower → 307).
async fn handle_api(
    state: ConsoleState,
    rest: &str,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    // Login is the one unauthenticated endpoint.
    if rest == "login" && req.method() == Method::POST {
        return handle_login(&state, req).await;
    }
    if rest == "logout" && req.method() == Method::POST {
        if let Some(tok) = session_token(&req) {
            state.sessions.revoke(tok);
        }
        return text(StatusCode::OK, "{}");
    }

    // All other endpoints require a session.
    let Some(session) = authenticate(&state, &req) else {
        return text(StatusCode::UNAUTHORIZED, r#"{"error":"unauthenticated"}"#);
    };

    let query = req.uri().query().unwrap_or("").to_string();

    // Reads are a leader view: a follower redirects to the leader's console
    // (preserving the query string so list cursors survive the hop).
    if !state.is_leader() {
        return match state.leader_console_addr() {
            Some(addr) => {
                let suffix = if query.is_empty() {
                    String::new()
                } else {
                    format!("?{query}")
                };
                redirect_307(&format!("http://{addr}/api/v1/{rest}{suffix}"))
            }
            None => text(
                StatusCode::SERVICE_UNAVAILABLE,
                r#"{"error":"no leader yet"}"#,
            ),
        };
    }

    api::dispatch(&state, rest, &query, &session).await
}

/// Handles `POST /api/v1/login`: AK/SK equality check against the credential
/// store, issues a session cookie on success. Body is `access_key\nsecret_key`
/// (form-simple; the frontend posts these two fields).
async fn handle_login(
    state: &ConsoleState,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return text(StatusCode::BAD_REQUEST, r#"{"error":"bad body"}"#),
    };
    let Ok(text_body) = std::str::from_utf8(&body) else {
        return text(StatusCode::BAD_REQUEST, r#"{"error":"bad body"}"#);
    };
    let (access_key, secret_key) = match parse_login(text_body) {
        Some(pair) => pair,
        None => {
            return text(
                StatusCode::BAD_REQUEST,
                r#"{"error":"missing credentials"}"#,
            );
        }
    };
    // Look up the credential and compare secrets in constant time.
    let cred = state.journal.state().credentials().get(access_key);
    let ok = cred
        .as_ref()
        .is_some_and(|c| auth::secret_matches(&c.secret_key, secret_key));
    if !ok {
        return text(
            StatusCode::UNAUTHORIZED,
            r#"{"error":"invalid credentials"}"#,
        );
    }
    let role = cred.map(|c| c.role).unwrap_or(ConsoleRole::Readonly);
    let now = self_now(state);
    let token = state.sessions.issue(access_key.to_string(), role, now);
    let role_str = match role {
        ConsoleRole::Admin => "admin",
        ConsoleRole::Readonly => "readonly",
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(
            hyper::header::SET_COOKIE,
            format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/"),
        )
        .body(Full::new(Bytes::from(format!(
            r#"{{"role":"{role_str}"}}"#
        ))))
        .expect("valid login response")
}

/// Parses the login body into `(access_key, secret_key)`. Accepts either
/// `access_key\nsecret_key` or a tiny JSON `{"access_key":..,"secret_key":..}`.
fn parse_login(body: &str) -> Option<(&str, &str)> {
    if let Some((ak, sk)) = body.split_once('\n') {
        let ak = ak.trim();
        let sk = sk.trim();
        if !ak.is_empty() && !sk.is_empty() {
            return Some((ak, sk));
        }
    }
    None
}

/// The session token from the request's cookie header, if any.
fn session_token(req: &Request<hyper::body::Incoming>) -> Option<&str> {
    let header = req.headers().get(hyper::header::COOKIE)?.to_str().ok()?;
    auth::token_from_cookie_header(header)
}

/// Resolves the request's session (cookie → live session), or `None`.
fn authenticate(state: &ConsoleState, req: &Request<hyper::body::Incoming>) -> Option<Session> {
    let token = session_token(req)?;
    state.sessions.resolve(token, self_now(state))
}

/// Current wall-clock millis via the injected clock.
fn self_now(state: &ConsoleState) -> u64 {
    state.clock.now_millis()
}

/// A JSON/text response with a status.
fn text(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("valid text response")
}

/// A 307 redirect to `location` (used for follower→leader on API reads).
fn redirect_307(location: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header(hyper::header::LOCATION, location)
        .body(Full::new(Bytes::new()))
        .expect("valid redirect response")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A high OS-assigned port must still yield a *bindable* console port.
    /// Plain wrapping mapped 64409 → 873, a privileged port the console cannot
    /// bind without root — it would simply never serve.
    #[test]
    fn console_addr_stays_bindable_for_a_high_grpc_port() {
        let grpc: SocketAddr = "127.0.0.1:64409".parse().unwrap();
        let console = console_addr(grpc).port();
        assert!(
            console > 1024,
            "console port {console} must not land in the privileged range"
        );
        assert_eq!(
            console,
            64409 - CONSOLE_PORT_OFFSET,
            "offset applies downward"
        );
        // Still a pure function of the gRPC port (a follower derives the leader's).
        assert_eq!(console_addr(grpc).port(), console);
    }

    #[test]
    fn console_addr_offsets_the_grpc_port() {
        let grpc: SocketAddr = "10.0.0.5:7300".parse().unwrap();
        assert_eq!(
            console_addr(grpc).to_string(),
            "10.0.0.5:9300",
            "console port = grpc port + 2000, so it is derivable from raft membership"
        );
    }

    #[test]
    fn parse_login_accepts_ak_newline_sk() {
        assert_eq!(parse_login("AK\nSK"), Some(("AK", "SK")));
        assert_eq!(parse_login("  AK \n SK "), Some(("AK", "SK")));
        assert_eq!(parse_login("AKonly"), None);
        assert_eq!(parse_login("AK\n"), None, "empty secret rejected");
    }
}
