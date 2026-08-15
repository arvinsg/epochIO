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

//! Writer-token session: the sole entry point for constructing `blob_id`s
//! (design 00 §4, 01 §4.3).
//!
//! A gateway obtains one [`WriterToken`] from PD (the high 32 bits of every
//! `blob_id`) and mints ids locally by pairing it with a monotonic 32-bit `seq`
//! — no per-id allocation RPC. [`WriterSession`] owns that counter and enforces
//! two safety properties:
//!
//! - **Liveness gate** — if the gateway's PD heartbeat has lapsed past a
//!   threshold, PD may already have retired the token, so the session refuses
//!   to mint (returns [`ClientError::WriterStale`]) until a heartbeat resumes.
//!   INVARIANT(design 01 §4.3): a session that has lost its heartbeat must not
//!   hand out new blob ids.
//! - **Exhaustion rotation** — a token yields exactly `2^32` ids; when `seq`
//!   is spent the session transparently registers a fresh token and continues
//!   from `seq = 0`. Tokens are never reused, so old and new ids never collide.
//!
//! The heartbeat *driver* (the gateway loop that calls [`record_heartbeat_ok`])
//! and the clock live with the gateway (a later phase); this type stays
//! deterministic by taking `now_millis` explicitly.
//!
//! [`record_heartbeat_ok`]: WriterSession::record_heartbeat_ok

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use epoch_proto::{BlobId, NodeId, WriterToken};

use crate::error::ClientError;

/// A source of fresh writer tokens — implemented by the PD client, faked in
/// tests. Registration is rare (session establish + token rotation), so the
/// boxed future from `async_trait` is not a hot-path concern.
#[async_trait::async_trait]
pub trait TokenSource: Send + Sync {
    /// Registers `node_id` with PD and returns a fresh, never-reused token.
    async fn register_writer(&self, node_id: NodeId) -> Result<WriterToken, ClientError>;

    /// Heartbeats a writer session (01 §4.3): refreshes PD's last-seen for the
    /// owning node, reports the token's GC commit watermark `W(t)` (Q27), and
    /// returns whether `token` is still `Live`. `false` (Dead or unknown) tells
    /// the session to rotate — a Dead token never revives.
    async fn writer_heartbeat(
        &self,
        node_id: NodeId,
        token: WriterToken,
        commit_watermark: i64,
    ) -> Result<bool, ClientError>;
}

/// The mutable token/counter state, guarded by a synchronous mutex. The mint
/// fast path never awaits while holding it (AGENTS §5); rotation releases it
/// before calling out to the token source.
struct TokenState {
    /// The token currently minting ids.
    token: WriterToken,
    /// The next `seq` to hand out.
    next_seq: u32,
    /// Set once `seq` has wrapped (u32::MAX was handed out); the next mint
    /// rotates to a fresh token before serving.
    exhausted: bool,
    /// The seqs minted from `token` that have not yet committed or been
    /// abandoned — the *in-flight* set for the GC commit watermark (Q27). A blob
    /// is added when minted and removed by [`resolve_blob`](WriterSession::resolve_blob)
    /// once its object PUT commits (or fails). Reset on token rotation (a spent
    /// token's ids are all resolved by the time 2^32 mints exhaust it).
    in_flight: BTreeSet<u32>,
}

/// A live writer-token session for one gateway node: mints unique `blob_id`s
/// while its PD heartbeat is fresh, rotating tokens when a token is spent.
///
/// Cheap to share behind an `Arc`; [`next_blob_id`](Self::next_blob_id) takes
/// `&self` and serializes only the tiny counter update.
pub struct WriterSession<S: TokenSource> {
    source: S,
    node_id: NodeId,
    stale_after_millis: u64,
    state: Mutex<TokenState>,
    last_heartbeat_millis: AtomicU64,
}

impl<S: TokenSource> WriterSession<S> {
    /// Establishes a session by registering an initial token for `node_id`.
    ///
    /// `stale_after_millis` is the heartbeat grace after which minting is
    /// blocked; `now_millis` seeds the last-heartbeat watermark so a freshly
    /// established session is immediately live.
    ///
    /// # Errors
    /// Returns the [`ClientError`] from the initial registration (e.g.
    /// [`ClientError::NotFound`] if the gateway node is unknown to PD).
    pub async fn establish(
        source: S,
        node_id: NodeId,
        stale_after_millis: u64,
        now_millis: u64,
    ) -> Result<Self, ClientError> {
        let token = source.register_writer(node_id).await?;
        Ok(Self {
            source,
            node_id,
            stale_after_millis,
            state: Mutex::new(TokenState {
                token,
                next_seq: 0,
                exhausted: false,
                in_flight: BTreeSet::new(),
            }),
            last_heartbeat_millis: AtomicU64::new(now_millis),
        })
    }

    /// Records that a heartbeat to PD succeeded at `now_millis`, refreshing the
    /// liveness watermark that [`next_blob_id`](Self::next_blob_id) gates on.
    pub fn record_heartbeat_ok(&self, now_millis: u64) {
        self.last_heartbeat_millis
            .store(now_millis, Ordering::Relaxed);
    }

    /// The token currently minting ids (rotates on exhaustion).
    #[must_use]
    pub fn current_token(&self) -> WriterToken {
        self.lock_state().token
    }

    /// Marks a minted blob resolved (its object PUT committed to MetaNode, or was
    /// abandoned) so it no longer counts as in-flight for the GC commit watermark
    /// (Q27). Idempotent; a blob from a rotated-away token is a no-op (its seqs
    /// were cleared on rotation). The caller resolves a blob only after the
    /// MetaNode commit that references it (or on a terminal write failure).
    pub fn resolve_blob(&self, blob_id: BlobId) {
        let mut state = self.lock_state();
        if blob_id.writer_token() == state.token {
            state.in_flight.remove(&blob_id.seq());
        }
    }

    /// The GC commit watermark `W(t)` for the current token (Q27): the largest
    /// `seq` that is guaranteed to have a terminal outcome (committed or
    /// abandoned), so an unreferenced `(t, s ≤ W)` blob is a true orphan. It is
    /// `min(in-flight seq) − 1`, or the current cursor (`next_seq − 1`) when
    /// nothing is in-flight. Returned as a signed value so an empty token with no
    /// mints yet yields `-1` (nothing reclaimable). Paired with `current_token`.
    #[must_use]
    pub fn commit_watermark(&self) -> (WriterToken, i64) {
        let state = self.lock_state();
        let watermark = match state.in_flight.iter().next() {
            // Earliest in-flight seq minus one: everything below it is terminal.
            Some(&min_in_flight) => i64::from(min_in_flight) - 1,
            // No in-flight: the cursor's predecessor is the last minted seq, all
            // terminal. `next_seq == 0` (nothing minted) → -1.
            None => i64::from(state.next_seq) - 1,
        };
        (state.token, watermark)
    }

    /// Mints the next unique `blob_id`, rotating the token if the current one is
    /// spent.
    ///
    /// # Errors
    /// - [`ClientError::WriterStale`] if the heartbeat has lapsed past the
    ///   grace (the caller must re-establish before writing).
    /// - Any [`ClientError`] from registering a replacement token on rotation.
    pub async fn next_blob_id(&self, now_millis: u64) -> Result<BlobId, ClientError> {
        // Liveness gate first: a session PD may have retired must not mint.
        let idle = now_millis.saturating_sub(self.last_heartbeat_millis.load(Ordering::Relaxed));
        if idle > self.stale_after_millis {
            return Err(ClientError::WriterStale { since_millis: idle });
        }

        loop {
            // Fast path: hand out a seq under the lock, without awaiting.
            {
                let mut state = self.lock_state();
                if !state.exhausted {
                    let seq = state.next_seq;
                    match seq.checked_add(1) {
                        Some(next) => state.next_seq = next,
                        None => state.exhausted = true,
                    }
                    // Track the minted seq as in-flight until its PUT resolves
                    // (Q27 commit watermark). The token rides in the id.
                    state.in_flight.insert(seq);
                    return Ok(BlobId::new(state.token, seq));
                }
            }

            // Slow path: the token is spent. Register a replacement WITHOUT
            // holding the lock, then install it (unless another task already
            // rotated) and retry the fast path.
            let fresh = self.source.register_writer(self.node_id).await?;
            let mut state = self.lock_state();
            if state.exhausted {
                state.token = fresh;
                state.next_seq = 0;
                state.exhausted = false;
                state.in_flight.clear();
            }
        }
    }

    /// Forces rotation to a fresh token, discarding the current one. Called
    /// when PD answers a heartbeat with `live = false` — the token is Dead and
    /// never revives (01 §4.3), so the session must not mint from it again.
    /// Also refreshes the liveness watermark: the rotation round-trip itself
    /// proves PD connectivity.
    ///
    /// # Errors
    /// Any [`ClientError`] from registering the replacement token; on failure
    /// the current token is marked exhausted so no further ids are minted
    /// from it (the next mint retries the rotation).
    pub async fn force_rotate(&self, now_millis: u64) -> Result<(), ClientError> {
        // Fence the dead token first: even if registration fails, no more ids.
        {
            let mut state = self.lock_state();
            state.exhausted = true;
        }
        let fresh = self.source.register_writer(self.node_id).await?;
        let mut state = self.lock_state();
        state.token = fresh;
        state.next_seq = 0;
        state.exhausted = false;
        state.in_flight.clear();
        drop(state);
        self.record_heartbeat_ok(now_millis);
        Ok(())
    }

    /// Locks the counter state, treating poisoning as an unrecoverable invariant
    /// violation (a task panicked mid-update).
    fn lock_state(&self) -> std::sync::MutexGuard<'_, TokenState> {
        self.state
            .lock()
            .expect("writer session state mutex poisoned")
    }
}

impl<S: TokenSource + 'static> WriterSession<S> {
    /// Spawns the session's heartbeat driver (01 §4.3): every `interval` it
    /// heartbeats PD and, on success, refreshes the liveness watermark that
    /// gates minting; on `live = false` it force-rotates to a fresh token.
    ///
    /// INVARIANT(design 01 §4.3): a transport failure refreshes nothing — the
    /// stale gate then blocks minting once the grace lapses (heartbeat lapse ⇒
    /// stop minting). The returned handle owns the task (AGENTS §5: no
    /// detached task); dropping it stops the loop.
    #[must_use]
    pub fn spawn_heartbeat(
        self: &std::sync::Arc<Self>,
        interval: std::time::Duration,
    ) -> WriterHeartbeatHandle {
        let session = std::sync::Arc::clone(self);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let (token, watermark) = session.commit_watermark();
                match session
                    .source
                    .writer_heartbeat(session.node_id, token, watermark)
                    .await
                {
                    Ok(true) => session.record_heartbeat_ok(clock_millis()),
                    Ok(false) => {
                        // Dead token: rotate before the next mint (01 §4.3).
                        if let Err(err) = session.force_rotate(clock_millis()).await {
                            tracing::warn!(error = %err, "writer token rotation failed");
                        }
                    }
                    // Transport failure: leave the watermark alone — the stale
                    // gate stops minting if this persists past the grace.
                    Err(err) => tracing::debug!(error = %err, "writer heartbeat failed"),
                }
            }
        });
        WriterHeartbeatHandle { task }
    }
}

/// Owns the writer-session heartbeat task and aborts it on drop.
pub struct WriterHeartbeatHandle {
    task: tokio::task::JoinHandle<()>,
}

impl WriterHeartbeatHandle {
    /// Stops the heartbeat loop.
    pub fn stop(self) {
        self.task.abort();
    }
}

impl Drop for WriterHeartbeatHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Wall-clock milliseconds for the heartbeat watermark (the session core stays
/// deterministic by taking `now_millis` explicitly; only the driver reads the
/// clock).
fn clock_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, AtomicUsize};

    use super::*;

    const STALE_AFTER: u64 = 30_000;
    const NODE: NodeId = NodeId::new(1);

    /// A token source that hands out `base, base+1, ...`, counting calls and
    /// optionally failing the first registration.
    struct FakeTokenSource {
        next: AtomicU32,
        calls: AtomicUsize,
        fail: std::sync::atomic::AtomicBool,
        /// Heartbeat behavior: 0 = live=false, 1 = live=true, 2 = transport error.
        heartbeat_answer: AtomicU32,
    }

    impl FakeTokenSource {
        fn starting_at(base: u32) -> Self {
            Self {
                next: AtomicU32::new(base),
                calls: AtomicUsize::new(0),
                fail: std::sync::atomic::AtomicBool::new(false),
                heartbeat_answer: AtomicU32::new(1),
            }
        }

        fn failing() -> Self {
            Self {
                next: AtomicU32::new(0),
                calls: AtomicUsize::new(0),
                fail: std::sync::atomic::AtomicBool::new(true),
                heartbeat_answer: AtomicU32::new(1),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn fail_next(&self, fail: bool) {
            self.fail.store(fail, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl TokenSource for FakeTokenSource {
        async fn register_writer(&self, _node_id: NodeId) -> Result<WriterToken, ClientError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.fail.load(Ordering::Relaxed) {
                return Err(ClientError::NotFound(
                    "gateway node is not registered".into(),
                ));
            }
            Ok(WriterToken::new(self.next.fetch_add(1, Ordering::Relaxed)))
        }

        async fn writer_heartbeat(
            &self,
            _node_id: NodeId,
            _token: WriterToken,
            _commit_watermark: i64,
        ) -> Result<bool, ClientError> {
            match self.heartbeat_answer.load(Ordering::Relaxed) {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(ClientError::Internal("hb transport down".into())),
            }
        }
    }

    async fn establish(source: FakeTokenSource) -> WriterSession<FakeTokenSource> {
        WriterSession::establish(source, NODE, STALE_AFTER, 0)
            .await
            .expect("establish")
    }

    #[tokio::test]
    async fn mints_monotonic_blob_ids_under_one_token() {
        let session = establish(FakeTokenSource::starting_at(10)).await;
        assert_eq!(session.current_token(), WriterToken::new(10));

        let first = session.next_blob_id(0).await.expect("mint");
        let second = session.next_blob_id(0).await.expect("mint");
        let third = session.next_blob_id(0).await.expect("mint");

        assert_eq!(first, BlobId::new(WriterToken::new(10), 0));
        assert_eq!(second, BlobId::new(WriterToken::new(10), 1));
        assert_eq!(third, BlobId::new(WriterToken::new(10), 2));
        // No rotation: only the establishing registration happened.
        assert_eq!(session.source.calls(), 1);
    }

    #[tokio::test]
    async fn rotates_to_a_fresh_token_when_seq_is_exhausted() {
        let session = establish(FakeTokenSource::starting_at(10)).await;
        // Jump to the last seq of the first token without minting 2^32 ids.
        session.lock_state().next_seq = u32::MAX;

        // The final id of token 10, then the boundary flips to exhausted.
        let last = session.next_blob_id(0).await.expect("mint last");
        assert_eq!(last, BlobId::new(WriterToken::new(10), u32::MAX));
        assert!(session.lock_state().exhausted);

        // The next mint rotates to token 11 and restarts the sequence.
        let rotated = session.next_blob_id(0).await.expect("mint after rotate");
        assert_eq!(rotated, BlobId::new(WriterToken::new(11), 0));
        assert_eq!(session.current_token(), WriterToken::new(11));
        // Establish + one rotation registration.
        assert_eq!(session.source.calls(), 2);
    }

    #[tokio::test]
    async fn refuses_to_mint_when_heartbeat_is_stale_then_resumes() {
        let session = establish(FakeTokenSource::starting_at(10)).await;

        // Within the grace: mints fine.
        session.next_blob_id(STALE_AFTER).await.expect("still live");

        // Past the grace: the session is stale and refuses.
        let err = session
            .next_blob_id(STALE_AFTER + 1)
            .await
            .expect_err("stale");
        assert!(matches!(
            err,
            ClientError::WriterStale { since_millis } if since_millis == STALE_AFTER + 1
        ));

        // A fresh heartbeat lifts the gate; the sequence continues (no rotation).
        session.record_heartbeat_ok(STALE_AFTER + 1);
        let resumed = session
            .next_blob_id(STALE_AFTER + 1)
            .await
            .expect("resumed");
        assert_eq!(resumed, BlobId::new(WriterToken::new(10), 1));
        assert_eq!(session.source.calls(), 1);
    }

    #[tokio::test]
    async fn establish_propagates_registration_error() {
        let result =
            WriterSession::establish(FakeTokenSource::failing(), NODE, STALE_AFTER, 0).await;
        assert!(matches!(result, Err(ClientError::NotFound(_))));
    }

    /// C3: a dead-token heartbeat answer must rotate to a fresh token and the
    /// session must keep minting — with a strictly higher token (never reused).
    #[tokio::test]
    async fn force_rotate_moves_to_a_fresh_token_and_refreshes_liveness() {
        let session = establish(FakeTokenSource::starting_at(10)).await;
        let first = session.next_blob_id(0).await.expect("mint");
        assert_eq!(first.writer_token().get(), 10);

        // Simulate PD answering live=false at t=STALE_AFTER-1 (still in grace):
        session.force_rotate(STALE_AFTER - 1).await.expect("rotate");
        let after = session
            .next_blob_id(STALE_AFTER - 1)
            .await
            .expect("mint after rotate");
        assert_eq!(
            after.writer_token().get(),
            11,
            "rotated token must be fresh"
        );
        assert_eq!(after.seq(), 0, "sequence restarts on the new token");

        // The rotation refreshed the watermark: minting is live well past the
        // original establish time.
        assert!(
            session
                .next_blob_id(STALE_AFTER - 1 + STALE_AFTER)
                .await
                .is_ok(),
            "rotation must refresh the liveness watermark"
        );
    }

    /// C3: a failed rotation fences the dead token — no ids are minted from it.
    #[tokio::test]
    async fn failed_rotation_fences_the_dead_token() {
        let source = FakeTokenSource::starting_at(20);
        let session = establish(source).await;
        let _ = session.next_blob_id(0).await.expect("mint");

        // Make the replacement registration fail, then force-rotate.
        session.source.fail_next(true);
        assert!(session.force_rotate(0).await.is_err());

        // The old token is fenced: the next mint tries to rotate again (and
        // succeeds once registration recovers), never reusing token 20.
        session.source.fail_next(false);
        let id = session.next_blob_id(0).await.expect("mint after recovery");
        assert_eq!(id.writer_token().get(), 21);
    }

    /// G1 (Q27): the commit watermark is `min(in-flight) − 1`, or the cursor's
    /// predecessor when nothing is in-flight, so an in-flight blob is never below
    /// the watermark (never GC'd) while its earlier peers are.
    #[tokio::test]
    async fn commit_watermark_tracks_in_flight_minimum() {
        let session = establish(FakeTokenSource::starting_at(5)).await;
        // No mints yet: nothing reclaimable.
        assert_eq!(session.commit_watermark(), (WriterToken::new(5), -1));

        let b0 = session.next_blob_id(0).await.expect("mint"); // seq 0
        let b1 = session.next_blob_id(0).await.expect("mint"); // seq 1
        let b2 = session.next_blob_id(0).await.expect("mint"); // seq 2
        // All three in-flight: W = min(0,1,2) − 1 = −1 (none terminal yet).
        assert_eq!(session.commit_watermark().1, -1);

        // Resolve the earliest: W advances to 0 (seq 0 terminal, 1 & 2 still in
        // flight so it cannot cross them).
        session.resolve_blob(b0);
        assert_eq!(session.commit_watermark().1, 0);

        // Resolve out of order: seq 2 done but seq 1 still in flight → W stays 0.
        session.resolve_blob(b2);
        assert_eq!(session.commit_watermark().1, 0);

        // Resolve the last in-flight (seq 1): nothing in flight, cursor is 3, so
        // W = next_seq − 1 = 2 (all three minted seqs are now terminal).
        session.resolve_blob(b1);
        assert_eq!(session.commit_watermark().1, 2);
    }

    /// G1: resolving a blob from a rotated-away token is a harmless no-op (its
    /// in-flight seqs were cleared on rotation).
    #[tokio::test]
    async fn resolve_from_stale_token_is_a_noop() {
        let session = establish(FakeTokenSource::starting_at(5)).await;
        let stale = session.next_blob_id(0).await.expect("mint"); // token 5, seq 0
        session.lock_state().next_seq = u32::MAX;
        let _ = session.next_blob_id(0).await.expect("mint last"); // exhausts
        let _ = session.next_blob_id(0).await.expect("rotate"); // token 6, seq 0
        assert_eq!(session.current_token(), WriterToken::new(6));
        // Resolving the old token's blob does not touch the new token's in-flight.
        session.resolve_blob(stale);
        // Token 6 has seq 0 in flight → W = −1.
        assert_eq!(session.commit_watermark(), (WriterToken::new(6), -1));
    }
}
