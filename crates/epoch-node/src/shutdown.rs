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

//! Shutdown signalling: a future that resolves on the first termination signal,
//! handed to a role's serve loop to stop accepting and begin teardown.
//!
//! INVARIANT(design 07 §M9 运维面): **SIGTERM must be handled, not just SIGINT.**
//! Ctrl-C sends SIGINT, but every supervisor stops a process with SIGTERM —
//! `systemctl stop`, `docker stop`, a Kubernetes pod deletion. Listening only for
//! SIGINT means the default disposition applies to SIGTERM and the process dies
//! instantly: raft never steps down cleanly, writer threads are never joined, and
//! every managed restart behaves like `kill -9`. Rolling restarts then drop
//! in-flight writes on every node they touch.

use tokio::signal;

/// Resolves on the first SIGINT (Ctrl-C) or SIGTERM (supervisor stop).
///
/// If a handler cannot be installed, logs and resolves immediately so the process
/// terminates rather than hanging with no way to stop it.
pub async fn ctrl_c() {
    terminate().await;
}

/// Resolves on the first termination signal, whichever arrives.
#[cfg(unix)]
async fn terminate() {
    use signal::unix::{SignalKind, signal as unix_signal};

    let mut sigterm = match unix_signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(error = %err, "failed to install SIGTERM handler; shutting down");
            return;
        }
    };
    let mut sigint = match unix_signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(error = %err, "failed to install SIGINT handler; shutting down");
            return;
        }
    };
    // Whichever lands first wins; the role then runs its own teardown.
    let signal_name = tokio::select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
    };
    tracing::info!(
        signal = signal_name,
        "termination signal received; draining"
    );
}

/// Non-unix fallback: only Ctrl-C exists.
#[cfg(not(unix))]
async fn terminate() {
    if let Err(err) = signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to install ctrl-c handler; shutting down");
    }
}
