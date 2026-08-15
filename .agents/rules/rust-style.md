# epochIO Rust rules

Single source of truth for Rust coding rules in this workspace. `AGENTS.md` at the
repo root carries the always-on overview and points here. Read this file before a
change that needs design alignment, or when working in an area you have not touched
this session.

Precedence between this file and the surrounding module is **scoped** — see
`AGENTS.md` §2. In short: local taste (naming, comment voice, granularity, error
type of the layer, test shape) → the module wins, say the rule looks stale.
Cross-module invariants (§8 persistence, §9 raft, §4 concurrency, §4.6 boundaries,
crate layering) → the rules win, say the module looks wrong. Never pick a side
silently.

---

## 1. Naming

One principle: **a reader should know what a thing is, and what it acts on, from its
name plus its owner.** `Runner::start` is complete; a free `start()` is not.
`fn handle(&self, msg: &Message)` on a trait is complete; the trait supplies the
object.

If you need "and", or a second qualifier, to say what something does, that is a
design signal — split the thing rather than lengthening the name.

| Kind | Budget |
| ---- | ------ |
| Local | at most 2 concepts |
| Field | at most 3 concepts |
| Function | at most 4 concepts |
| Test fn | no length limit — `subject_verb_object`, says *what* holds, not *how* |

Count concepts, not underscore segments: a multi-word domain term (`blob_index`) and
a conventional suffix (`_handler`, `_unlocked`, `_locked`) each count as one. Over
budget is a design signal — split the thing, don't lengthen the name. Test names are
the exception: `compaction_keeps_live_blobs_readable` is long and correct. Cut
`does_not_` negations (prefer `rejects`, `skips`, `ignores`) and a repeated subject;
keep a `when_` clause when it *is* the precondition being tested.

**Banned shapes**, regardless of module:

- A numeric suffix to dodge a collision (`result2`, `state_tmp`). Rename both to say
  how they differ.
- The type baked into the name: `shard_list` → `shards`, `id_string` → `id`.
- The owning type's name repeated (`chunk_manager_state` inside `ChunkManager`).
- Meaningless heads: `helper`, `util`, `*_impl`, a bare `tmp`.
- Verbs with no object (`do_work`, `process`, `execute`) outside a trait that fixes
  the object.
- Plural or singular lying about cardinality.

**Three carve-outs that matter**, because ignoring them fights this codebase:

- **Do not shorten `node_id` to `id`** in a module handling several id kinds at
  once. The qualifier is what disambiguates `node_id` / `chunk_id` / `blob_id` /
  `extent_id` / `partition_id`.
- **`data` / `info` / `item` / `value` / `key` / `entry` are correct** where they
  mirror a proto, model, or wire field. Renaming them diverges from `epoch-proto`.
- **`handle` / `run` / `start` are fine** when a trait fixes the object or the
  component name supplies it.

**The domain vocabulary is fixed by the design docs' terminology section**: Chunk = EC
group, Shard = shard slot, Blob = 32MB object segment, Extent = 32GB on-disk
container, Stripe = 1MB coding unit. Never invent a synonym for one of these, and
never import cubefs/MinIO vocabulary (volume, bid, vid, chunk-as-container, xl.meta)
— see §12. Before inventing any other noun, grep for the one already in use:
`rg -w '<candidate>' --type rust`. A second word for a thing the workspace already
names is a defect. And `node`, `server`, `instance`, `peer`, `member` name different
concepts in a distributed system, sometimes with different ID widths for what looks
like the same field. Confirm which concept you are in.

---

## 2. Comments and wording

A comment explains **why**. Code says **what**. If a comment restates the code,
delete it; if the code needs a comment to be readable, fix the code first.

There is no density budget. A line count cannot tell a good comment from a bad one,
and a percentage cap counts license headers and `///` API docs as noise, so chasing
the number means deleting documentation.

Worth writing: an invariant the types cannot express · why the obvious
implementation was rejected · an external constraint (wire format, on-disk
compatibility, an upstream bug) · lock ordering · `// SAFETY:` · operational
rationale for a tradeoff an on-call reader would otherwise re-derive under pressure.
Not a closed list — if you would want it at 2am during an incident, keep it.

### 2.1 Comment openers that signal narration

`// Step 1:` · `// Now ...` · `// We ...` · `// First,` · `// Then,` ·
`// Finally,` · `// This ensures ...` · `// Note that ...` · `// Helper to ...` ·
`// Create/Get/Set/Check the ...` when the function name already says it.

A doc comment must add a constraint the signature cannot state. The `/// Returns X`
**shape is not the problem** — whether the sentence carries information is.

```rust
// Zero information — the name already said this
/// Returns the chunk id.
fn chunk_id(&self) -> ChunkId;

// Real information: "believes" says this is a possibly-stale local view
/// Returns true when this PD node currently believes it is raft leader.
fn is_leader(&self) -> bool;

// Real information: distinguishes None = "not known here" from
// None = "no writable chunk exists"
/// Returns a writable chunk for this code mode if the local view has one.
fn pick_writable(&self, mode: CodeMode) -> Option<ChunkId>;
```

Trait definitions earn docs more readily than impls: the doc is the contract for
implementors, and there is no body to fall back on.

### 2.2 Wording — code, commits, plans, and replies

**Cut words that add emphasis without adding information.** Words that usually fail
that test: `leverage` (use *use*) · `utilize` · `robust` · `seamless` ·
`comprehensive` · `crucial` · `essential` · `powerful` · `elegant` · `gracefully` ·
`basically` · `note that` · `it's worth noting` · `in order to` (use *to*) ·
`a variety of` · `facilitate` · `ensure that` (state the concrete condition) ·
`handle` as a verb (name the action).

中文（设计文档、方案、回复）：值得注意的是 · 简而言之 · 优雅地 · 健壮 · 强大 ·
确保 · 进行处理 · 相关逻辑 · 深入探讨 · 赋能 · 一键 · 极大地.

The test is information, not the word. "The chunk just became sealed" carries
timing; "it just wraps the manager" carries nothing. `fn ensure_leader` names a
precondition check; "ensure the lock is released" should be "release the lock".

Reports must be falsifiable: state the command run and its result, or state that it
was not run. No "should work", no "looks good", no coverage claim without a test
name. See §16 and `workflow.md` §7.

---

## 3. Toolchain, formatting, dependencies

- Use the repository-pinned Rust toolchain (`rust-toolchain.toml`) and the Cargo
  workspace configuration.
- Treat `rustfmt.toml` as the formatting authority and `clippy.toml` as the Clippy
  configuration authority. Do not fight formatter output or introduce local
  formatting conventions that conflict with the configured tools.
- Keep lint suppressions narrowly scoped. Every new `#[allow(...)]` should have a
  nearby reason when the justification is not obvious. Do not add crate-wide or
  workspace-wide suppression to avoid fixing a local issue.
- Reuse dependencies declared by the Cargo workspace instead of adding independent
  crate-local versions.
- Do not add a dependency when the standard library or an existing workspace
  dependency provides a clear and maintainable solution.
- Keep dependency features minimal. Do not enable broad feature sets solely for one
  small API without checking compile-time, binary-size, platform, and
  transitive-cost impact.
- Keep conditional compilation paths consistent. A change behind `cfg` or a Cargo
  feature must preserve the behavior and buildability of relevant alternative paths.

---

## 4. Code structure and APIs

### 4.1 Imports and paths

**A reader should be able to tell where every name came from without searching.**

- Prefer top-of-module `use` declarations over long paths inside function bodies.
- At most three `::` segments after `crate::`, `super::`, or `std::`. Before writing
  a `use` with four or more module segments, check whether an intermediate `mod.rs`
  already re-exports the item; if so, import from there. A long path is usually a
  missing re-export, not a caller problem.
- Add a `pub use` only when the item is intentionally part of the owning module's
  public domain API. Do not expand the public API merely to shorten one caller.
- Avoid wildcard imports outside `#[cfg(test)] use super::*`, a curated prelude, or
  a crate-root facade.

```rust
// Bad
let x = crate::pd::config::keys::SOME_KEY_CONST;

// Good
use crate::pd::config::keys;
let x = keys::SOME_KEY_CONST;
```

### 4.2 Control flow

Simple iterator chains such as `.iter().filter().map().collect()` are fine.
**Extract a closure when its body is logic worth naming.** A closure that only
builds a value, only adapts an error, or is itself the atomic section of a
compare-and-swap has nothing to name — leave it. A closure that branches, returns
early, handles errors, or mutates is doing work a reader would want named.

Beyond that: never call the same expensive function twice across iterator stages
(hoist it above the loop); keep side effects out of boolean expressions and
combinators; precompute sort keys once; use early returns to keep the main path
shallow.

```rust
// Bad — a mutation hidden inside a combinator that yields a bool
self.index.get_mut(&key).map(|ids| { ids.remove(&shard); ids.is_empty() })
    .unwrap_or(false)

// Good — the mutation is visible
let Some(ids) = self.index.get_mut(&key) else { return false };
ids.remove(&shard);
ids.is_empty()
```

### 4.3 Phases, not streams

A public or central function with several phases should expose those phases clearly:

```rust
fn execute(&self) -> Result<()> {
    let input = self.load_input()?;
    let plan = self.build_plan(input)?;
    self.apply_plan(plan)
}
```

The signal is not length, it is whether a body reads as **named phases or a flat
stream**. Phase-shaped code legitimately runs long when each phase does; a flat
dependency-wiring sequence with no boundaries is the thing to break up.

Test: can you describe the function in one sentence without saying "and also"?

Consider extracting a helper around: lock scope boundaries · configuration lookups,
filesystem access, RPC, or other I/O · repeated predicates or transformations · a
loop body with multiple responsibilities · each of parse / validate / plan / mutate ·
code that requires a separate invariant explanation.

Do not extract when the helper only renames a trivial expression, hides important
local behavior, or would need an excessive parameter list. Rethink the data flow
first.

### 4.4 Ownership and visibility

- Prefer borrowing over cloning when ownership does not need to change.
- Do not add `Arc`, `Mutex`, `RwLock`, interior mutability, or a global singleton
  only to work around unclear ownership. Fix the ownership.
- Keep fields and functions private unless a real caller needs broader visibility.
- Prefer domain-specific types or enums over booleans, ambiguous tuples, and bare
  integers when they prevent invalid states or clarify units. Guidance for new code,
  not licence to newtype existing IDs.
- Avoid lossy `as` conversions for IDs, offsets, lengths, timestamps, and persisted
  values. Use checked arithmetic and `TryFrom` where truncation or overflow is
  possible.

### 4.5 Public contracts

Document where absence can make a caller wrong, not every `pub` item. Write rustdoc
when the item is invariant-bearing (correct use depends on call order or a prior
validation), can panic (`# Panics`), carries a unit the name does not, is persisted
or wire-visible, or returns errors whose kinds drive caller control flow
(`# Errors`). Update docs when changing public APIs. Keep examples accurate and
compilable when practical. Never restate the signature — see §2.

### 4.6 Validate at boundaries

- Validate external input, configuration, decoded data, and constructor arguments at
  the boundary where they enter a trusted domain.
- Never construct a partially valid value and rely on every later caller to remember
  the same checks. A constructor that skips validation and a `pub` accessor that
  unwraps a re-parse of the unvalidated field are the same bug seen twice.
- Fallible construction must not be named `from` — use `TryFrom`, or an inherent
  `load` / `parse` / `from_file` returning `Result`. Use `From` only for infallible
  conversions.
- A lossy conversion is acceptable only where the loss cannot reach a caller who
  could act on it. Rendering for a human-facing view qualifies; anything a caller
  branches on does not.
- Implement `Default`, `AsRef`, `Borrow`, `FromIterator`, `IntoIterator` only when
  the conventional semantics accurately describe the type. No trait impl that
  performs I/O, expensive work, lossy conversion, or hidden allocation.

---

## 5. Error handling

- No `unwrap()`, `expect()`, `panic!()`, `todo!()`, `unreachable!()` in new
  production paths for recoverable input, config, storage, RPC, or runtime errors.
- Return the error type **of the layer you are in**, and convert at the layer
  boundary rather than inside it. This workspace layers several error families —
  `epoch-proto` wire codes, store engine, service, HTTP/S3 edge. Read the module's
  signatures and match them.
- Prefer a specific error constructor over a general one carrying a formatted
  string. A crate where most errors collapse into one string-typed kind has lost its
  context regardless of how few `unwrap`s it has.
- `expect()` only for a genuine internal invariant, with a message naming it. Tests
  may `unwrap()` when the failure location stays obvious.
- Never silently discard an error. Best-effort cleanup must be visible, and logged
  when operationally useful.
- Preserve the original source across layers. Do not stringify a structured error
  before the boundary that needs text.
- A new wire error code is additive: unknown codes must degrade to `Internal` on old
  readers. State the rollout impact (§8.1).

---

## 6. Async and concurrency

- No blocking filesystem operations, FFI calls, long CPU work, or blocking waits on
  an async executor thread. Use the existing blocking-task facilities.
- Never hold a sync or `parking_lot` guard across `.await`.
- Keep lock scopes small; release before RPC, filesystem I/O, callbacks, or calls
  into independently owned components.
- Avoid nested locks. Where unavoidable, document the acquisition order and follow it
  everywhere.
- Never call unknown or external code while holding a lock unless the behavior is
  explicitly designed and reviewed.
- Channels and queues need explicit capacity and backpressure. No unbounded
  accumulation of requests, buffers, or retries.
- Consider cancellation at every `.await`: a dropped future must not leave state
  half-mutated.
- Every spawned task needs an owner and a shutdown path. No detached background task
  outliving its owning component unintentionally.
- Use timeouts wherever an indefinite wait would block shutdown, recovery, or
  request completion.
- RAII for sync cleanup; an explicit async `close`/`shutdown` when cleanup awaits.
- `Drop` must not panic, do unbounded work, wait indefinitely, or require a runtime.
- Make shutdown ordering explicit for components owning tasks, channels, files,
  sockets, or native resources. Signal handling covers **SIGTERM and SIGINT** —
  managed restarts send SIGTERM, and losing it makes every restart equivalent to
  `kill -9`.

---

## 7. Unsafe code and FFI

Keep `unsafe` and FFI surfaces minimal behind safe wrappers. Every `unsafe` block
needs an adjacent `// SAFETY:` naming the pointer, lifetime, aliasing,
initialization, bounds, or thread-safety invariant relied on; every `unsafe fn`
documents caller obligations in `# Safety`. Validate lengths, nullability, ownership
transfer, and cleanup at FFI edges; never widen an unsafe region to bypass the borrow
checker; never let a panic unwind across an FFI boundary; treat any `Send`/`Sync`
impl change as a review of all shared state.

---

## 8. Persistence, serialization, compatibility

### 8.1 Compatibility is the default

Persisted state, journal entries, snapshots, RPC payloads, metadata, config formats,
IDs, keys, and serialized enums stay compatible unless the user says otherwise.

Before changing a serialized or persisted field:

- identify every writer and reader;
- determine whether old data can be read by new code;
- determine whether new data must be read by old code;
- check defaults, missing fields, enum variants, numeric widths, ordering, and
  encoding assumptions;
- add round-trip and compatibility tests for important formats;
- document any migration, rollout ordering, or version boundary required.

**Which direction of compatibility you owe depends on the codec — check it first.**

- **Tag-based** (protobuf): both directions are achievable. Old readers skip unknown
  fields, new readers see defaults. Owe both, and never renumber or reuse a tag.
- **Positional** (bincode and similar, used for raft journal, snapshots, superblock,
  frame headers): forward compatibility is **not achievable** — an old binary meeting
  an unknown enum discriminant errors out, and no discipline in the writer changes
  that. Do not add machinery claiming otherwise. What you owe instead: new code reads
  old data, variants are appended only, and the **rollout ordering** is stated
  explicitly, because that ordering is what protects a mixed-version cluster.

Round-trip tests should fail when a variant is added without coverage. A test that
enumerates all variants is worth more than N hand-written cases.

If the user explicitly states compatibility is not required, take the simplest clean
design — no migration layer, dual format, or version negotiation — but still report
the incompatible impact and test the new format.

### 8.2 Volatile state stays volatile

- Runtime-only fields rebuilt on startup or each scheduling cycle must not be
  accidentally persisted. Use `#[serde(skip)]` / `default` and make deserialization
  produce a valid reconstructable value.
- Add a serialization test where skipping is load-bearing, and make it fail if the
  attribute is removed — which means the fixture must set a non-default value.
- Volatile statistics must not influence replicated transitions or persistent
  placement decisions unless the design makes them replicated input explicitly.

### 8.3 Shared encoding and ID ownership

- Cross-crate encoding, decoding, key layout, protocol, and stable ID rules belong
  behind named functions or domain types in `epoch-proto`.
- Logic shared only by modules within one crate stays in the owning crate unless
  there is a demonstrated cross-crate contract.
- Do not scatter bit shifts, masks, packed IDs, hashes, or byte-layout expressions
  across call sites.
- Encoding helpers must test zero values, maximum values, invalid input, overflow
  boundaries, and encode/decode round trips where applicable.

---

## 9. Raft and replicated-state correctness

- A replicated transition must be deterministic from the committed command plus
  replicated state.
- While applying a command, never read wall-clock time, randomness, process-local
  counters, thread-locals, environment variables, or volatile node statistics if they
  can affect the result.
- Timestamps, generated IDs, lease expiries, and random choices that affect
  replicated state are computed **before** proposal and carried in the command.
- Iteration order of a `HashMap`/`HashSet` must not reach a result that depends on
  order: same-key conflicts, a checksum over the sequence, assigned sequence numbers,
  externally visible ordering. Batching writes over **distinct** keys is
  order-independent and needs no sort. The question is not "is a HashMap being
  iterated" but "would a different order produce a different committed state".
- Resolve a decision domain from **committed state**, not from the reporting party's
  payload, wherever replicas must agree.
- Avoid floats in replicated decisions. If unavoidable, define NaN, equality,
  rounding, and total order explicitly, and test them.
- Keep proposal, validation, application, and side effects separate.
- No irreversible external side effect inside a replicated transition unless it is
  explicitly idempotent and replay-safe.
- Snapshot and journal changes require recovery, replay, and compatibility analysis.
- Preserve idempotency wherever retries, duplicate delivery, leader change, or replay
  can repeat an operation.

---

## 10. Module and file conventions (anti-accretion)

- `lib.rs` contains crate docs, module declarations, and intentional re-exports only
  — no logic.
- One module per domain concept, named by domain (`extent.rs`, `compaction.rs`,
  `blob_index.rs`). **Never create `utils.rs`, `helpers.rs`, `misc.rs`, `common.rs`
  grab-bag modules** — if a helper has no domain home, its data flow is wrong;
  rethink before filing it somewhere.
- Standard per-crate files where applicable: `error.rs` (crate error type),
  `config.rs` (crate config structs with serde + validation).
- Unit tests live in `#[cfg(test)] mod tests` beside the code; cross-module scenarios
  go in `tests/`. Shared test fixtures live in one `testutil` module per crate, not
  copied between test files.
- When a file grows past the §15 signals, split by responsibility (state machine vs
  I/O vs types), not by line count.
- Crate layering is a hard rule with its own enforcement — see `AGENTS.md` §4 and
  `cargo xtask layers`. If two crates need the same item, it moves DOWN (usually to
  `epoch-proto`), never gets duplicated and never creates a sideways edge.
- Before routing work through a manager, owner, or facade layer, **confirm that layer
  exists in the code.** Do not infer one from a document or from a type name that
  sounds like it should own something. Domain types and journal entries can exist
  while the manager and apply path are not written yet — adding that layer is an
  alignment stop, not an implementation detail.

---

## 11. Design-doc traceability (anti-drift)

Traceability is carried by information, not by pointers. **No `//! Design: <path>`
anchor line at the top of a file** — a path-and-section pointer restates what the
module doc and the design docs already say, goes stale silently, and 162 of them were
removed for that reason.

What carries it instead:

- **Short-form section citations inline, where they explain something.** House style
  and already pervasive: `(02 §1.6)` after the sentence stating the rule it comes from,
  `04 §1.2` beside the constant it fixes. The citation earns its place by being
  attached to a claim, not by sitting in a header.
- **`INVARIANT(design 0X §Y): ...` at the enforcing code site**, stating the invariant
  itself. Each has at least one test referencing it. This is the load-bearing form —
  it survives because deleting it deletes information.
- **The doc side records the landing point.** The open-questions record's 已决
  tables and `07-iteration-plan.md` 实际交付 carry a 落点 column naming the file and
  module. Doc → code is the direction that needs maintaining, because the design docs
  are the source of truth and the reader arrives from them.

A module doc explains what the module is for and which invariants it upholds. If a
reader needs the design section to understand a decision, cite it beside the decision.

No machine check enforces this section. One was written and removed: whether a citation
earns its place is a judgement, and a check strict enough to police comment content
produced more false positives than findings. Reviews may reject code whose behavior
cannot be traced to a design section or an approved plan, and a design path cited in code
must still resolve to a real file.

---

## 12. Design-doc synchronization and terminology

- Any change that alters behavior described by the design docs must update the
  corresponding doc section in the same change.
- Newly aligned decisions are recorded in the open-questions record (已决
  tables) with the module-doc landing point. Milestone deliverables and deviations
  are recorded in the iteration plan (实际交付). See
  `workflow.md` §6.
- Do not import cubefs/MinIO vocabulary (volume, bid, vid, chunk-as-container,
  xl.meta, ...) into epochIO code, comments, RPC fields, logs, or metrics. The only
  authority is the design docs' terminology section. Checked by review, not by a linter:
  the terms that matter most are ordinary English words elsewhere ("byte volume"), so a
  mechanical ban fires on prose that is correct.

---

## 13. Logging and metrics

- Use the repository's existing structured logging and metrics facilities
  (`epoch-telemetry`).
- Choose log levels deliberately. Expected per-request behavior is not a warning, and
  a condition that persists across every heartbeat must not warn on each one. No
  noisy logs inside hot loops without rate or volume review.
- Include stable identifiers and actionable context. Never log secrets, tokens,
  credentials, raw user data, or unnecessarily large payloads.
- No unbounded or high-cardinality metric labels: bucket names, object keys, paths,
  request IDs, error strings, addresses, user-provided values. Per-bucket metrics
  need an explicit cap.
- A metric registered in a process that exposes no endpoint is not observable.
  Confirm the role wiring, not just the registration.
- Logging and metrics must not change correctness, lock ordering, replicated state,
  or error propagation.

---

## 14. Testing

### 14.1 What to test

- New behavior requires tests. Bug fixes get a regression test.
- Core logic requires **direct** coverage. Core logic is anything that *decides*:
  state transitions, scheduling and placement decisions, routing, encode/decode and
  key layout, persistence and recovery, compatibility handling, permission checks,
  admission and validation guards. If a function chooses between outcomes, a test
  pins the choice. Pure delegation does not need its own test.
- **A regression test must enter through the production path.** The dominant defect
  class in this repository is a mechanism that is implemented, unit-tested, and
  green, while the production entry point is not wired to it or bypasses its guard.
  A test that calls the internal safe method while production calls the unsafe one
  proves nothing. Enter at the RPC handler, HTTP route, or role ticker that real
  traffic uses.
- **Falsify the test.** Re-introduce the defect and confirm the test goes red. Record
  that result in the task row (`workflow.md` §3). A guard with no recorded
  falsification is not covered.
- Watch for two independent literal lists that must agree — a "handle synchronously"
  set and a "forward" set, a CF allow-list and a reference extractor. Nothing fails
  when one is updated and the other is not, so a test comparing them beats a test of
  either alone. Prefer exclusion lists over allow-lists where a missed entry means
  data loss.
- Incidental execution through a broad integration test is not coverage of the
  decision logic underneath.
- If core logic is hard to test, separate the deterministic decision from I/O,
  clocks, randomness, and wiring — pass `now_ms` in rather than reading a clock,
  split a side-effect-free prepare stage from the stage that writes. House pattern;
  do not lower the bar instead.
- New features cover the normal path, the boundaries, and the meaningful failures.
- Persistence and protocol changes need round-trip, old-data, replay, or migration
  coverage.
- Example configuration files need a test that they parse and that their defaults
  match the code, or they rot first.

### 14.2 Keeping tests readable

- Reuse existing fixtures, builders, contexts, temporary-directory utilities, and
  assertion helpers. For PD scheduler or Job-coordination tests, use the existing
  shared test utilities instead of rebuilding the full manager context.
- Cases sharing arrange/act/assert and differing only in inputs → one table-driven
  test, each row carrying a `name` that reaches the assertion message so a failure
  names the case. Do not force unrelated scenarios into one table when their setup,
  behavior, or failure diagnosis is clearer apart.
- Test names are constrained by shape, not length: `subject_verb_object`, stating
  what holds. See §1.
- No dependence on execution order, shared global state, fixed ports, external
  services, or timing races unless the test controls them. Readiness signals,
  barriers, and bounded polling — never fixed sleeps.
- Async and concurrency tests need a bound so a hang cannot stall the suite:

  ```rust
  #[tokio::test]
  async fn rebuilds_missing_shard_on_read() {
      tokio::time::timeout(Duration::from_secs(10), async {
          // body
      })
      .await
      .expect("test exceeded its time budget");
  }
  ```

  This matters most for a test that binds a real socket or joins spawned threads —
  those hang forever instead of failing.
- `debug_assert!` is not a check. Release builds compile it out, so a truncated slice
  list can be returned as success. Use a real check on any path where the assertion
  protects returned data.

---

## 15. Size and complexity signals

Review signals, not automatic failure conditions:

- A production source file approaching or exceeding roughly 500 lines may have mixed
  responsibilities.
- A large inline test module approaching or exceeding roughly 300 lines may be easier
  to navigate as focused submodules or integration tests.
- A long function, deeply nested iterator chain, or closure with multiple side effects
  may need clearer phases or an explicit loop.
- A test name that becomes a sentence may indicate that setup or cases should be
  organized differently.
- A helper with many parameters may indicate missing state grouping or unclear
  ownership.

Do not split files, functions, tests, or names mechanically to satisfy a number.

**Hard signals requiring action** in the same change or a declared follow-up task:

- a third copy of the same logic pattern appears (two similar sites may coexist;
  three means extract the shared owner now);
- a module's `pub` surface is growing while its callers reach around it for internals
  (the abstraction boundary is wrong — fix the boundary, don't widen the surface);
- a match/if-else over the same discriminant appears in 3+ places (move the behavior
  into the type: enum method or trait dispatch);
- a function accumulates boolean/option parameters controlling its internal branches
  (split it, or introduce a request struct with a builder).

---

## 16. Validation order

Narrowest first, then widen to the affected scope:

```bash
cargo fmt --all -- --check
cargo check -p <crate> --all-targets
cargo test -p <crate> <test_name>
cargo clippy -p <crate> --all-targets --no-deps -- \
  -D warnings --allow clippy::uninlined-format-args
cargo xtask gate
```

`--no-deps` keeps `-D warnings` scoped to the crate you changed; drop it only when
deliberately auditing dependencies. Widen to workspace checks when the change spans
crates, shared types, or workspace config. Include optional or heavyweight crates
when the change affects them rather than assuming default workspace membership covers
them.

After a command that rewrites files, inspect the diff and revert unrelated churn.

If a validation command cannot be run: state exactly which command was skipped,
explain why, describe the narrower validation that was completed, and do not claim the
change is verified.

---

## 17. Not allowed by default

Each is a live temptation in this workspace rather than generic hygiene. Unless
explicitly requested, do not:

- **Upgrade a dependency or the toolchain.** Dependencies are workspace-declared, so
  any bump is repo-global.
- **Add a new framework or large abstraction.** The workspace already carries HTTP
  (`hyper 1`), RPC, raft, RocksDB, and serialization stacks, and "zero external
  dependencies" is a product property.
- **Widen a public API for convenience.** A `pub fn` with `#[allow(dead_code)]` and
  no callers is this rule being breached.
- **Add or reorder a persisted or protocol field without compatibility analysis.**
  Highest stakes; see §8.1.
- Perform broad formatting-only rewrites, or rename/move large numbers of files.
- Add migration or compatibility machinery after the user explicitly opts out of
  compatibility.
- Suppress warnings broadly.
- Delete existing behavior without a replacement or explanation.
- Leave temporary scripts, debug output, commented-out experiments, or generated junk
  in the repository. Debug output in a benchmark or stress test that reports a
  measurement is fine; debug output left in a production path is not.
- **Claim validation that was not run.** No linter catches a false claim.
