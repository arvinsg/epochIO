# epochIO Agent Project Rules

This document is the self-contained coding and execution guide for AI coding agents
working in this repository. Apply it to every analysis, implementation, test,
refactor, and review unless the user explicitly requests a different approach.

## 0. Project anchor

epochIO is a next-generation distributed object store written in Rust: three
services (PD / DataNode / MetaNode), EB-scale data, 10B+ objects of metadata, zero
external dependencies, and AI-oriented workloads.

- **Design docs are the source of truth.** `docs/design/` (reading order in
  `docs/design/README.md`): 00 overview, 01 PD, 02 DataNode, 03 MetaNode, 04 EC/IO,
  05 AI roadmap, 99 open questions. Code must follow the design docs. If code and
  docs ever conflict, one of them must change in the same task — report which and
  why.
- **Terminology authority** is `docs/design/00 §2`: **Chunk** = EC group,
  **Shard** = shard slot, **Blob** = 32MB object segment, **Extent** = 32GB on-disk
  container, **Stripe** = 1MB coding unit. Note the deliberate mismatch with cubefs
  vocabulary (epochIO Chunk ≈ cubefs volume; epochIO Extent ≈ cubefs chunk). Use
  only epochIO terms in code, comments, RPC, logs, and metrics.
- **Open-question gate.** Items listed as 「待决」 in
  `docs/design/99-open-questions.md` are NOT decided. **Check that file at task
  start** — do not rely on any cached list of open questions (they change between
  sessions). Do not implement an undecided item as if decided; if a task depends on
  one, stop and align with the user first. If the 待决 section is empty, all
  recorded decisions in the 已决 tables are binding.
- **License red line.** MinIO is AGPLv3: study its design ideas only, never copy
  code or files. CubeFS is Apache-2.0 and may be referenced. Reference repositories
  live outside this repository (`../cubefs`, `../minio`, `../seaweedfs`); do not
  copy files or code fragments from them into this repository.
- **Language.** Code, comments, identifiers, and commit messages are English.
  Design documents under `docs/` are Chinese (to be translated before
  open-sourcing).
- **Git.** Do not run `git commit`, `git push`, `git rebase`, `git reset`, or any
  other git mutation unless the user explicitly asks. Ask for confirmation each
  time, even if earlier conversations contained approvals.

## 1. Core execution principles

### 1.1 Understand before changing

- Read the target module, its callers, related types, tests, re-exports, and existing
  abstractions before editing.
- Search for similar implementations and established patterns before introducing a new
  helper, abstraction, dependency, or style.
- Identify the behavior being changed, the affected crates, and any persistence,
  protocol, concurrency, or compatibility boundaries.
- Resolve questions that can be answered from the repository before asking the user.

### 1.2 Align uncertainty before acting

- Do not guess, invent, or silently infer unspecified functional behavior.
- If any requirement, expected behavior, boundary, scope, compatibility expectation, or
  acceptance criterion remains uncertain after inspecting the repository, stop and
  align with the user before editing code or making an irreversible change.
- Present the uncertainty as concrete questions, explain why each answer affects the
  implementation, and provide a recommendation when useful.
- Do not hide an unresolved requirement behind a working assumption.
- Resume implementation only after the relevant uncertainty has been resolved or the
  user has explicitly authorized a stated assumption.

### 1.3 Plan large features before implementation

A large feature requires an agreed implementation plan before code changes begin. A
change should be treated as large when it introduces a subsystem, spans multiple
crates, changes public APIs, alters persistence or protocols, changes major ownership
boundaries, or requires coordinated rollout.

The proposal should cover, as applicable:

- goal, scope, and explicit non-goals;
- current behavior and the problem being solved;
- high-level architecture and end-to-end flow;
- affected crates, modules, APIs, data structures, and ownership boundaries;
- persistence, protocol, compatibility, migration, and rollout impact;
- concurrency, failure handling, recovery, and observability;
- testing and validation strategy;
- implementation phases and important tradeoffs.

Planning rules:

- Present the overall flow before detailed implementation steps.
- Identify decisions that require user confirmation.
- Do not start implementation until the user approves the overall approach.
- Keep the proposal proportional to the feature. Planning should prevent rework, not
  create unnecessary speculative design.

### 1.4 Make the smallest complete change

- Keep the change tied to the requested behavior.
- Avoid opportunistic refactors, dependency upgrades, broad formatting, or unrelated
  cleanup.
- Do not change a public interface unless the task requires it.
- A small change must still be complete: update all required callers, tests,
  serialization behavior, and validation paths.

### 1.5 Reuse existing ownership and abstractions

- Prefer existing helpers, fixtures, configuration constants, domain types, common
  utilities, and module exports.
- Do not duplicate logic that already has a clear owner.
- Place new logic where future maintainers would naturally look for it, not merely in
  the file that currently needs it.
- Do not create a shared abstraction for a single speculative future use.

### 1.6 Optimize for correctness and readability

- Prefer explicit control flow over clever or compressed expressions.
- Make important invariants visible in types, names, validation, and comments.
- Keep side effects, lock boundaries, I/O, and state transitions easy to identify.
- Refactor when repeated patches make the affected code harder to reason about, but
  keep the refactor scoped to the requested feature area.

## 2. Rust toolchain and formatting

- Use the repository-pinned Rust toolchain and Cargo workspace configuration once the
  workspace exists; until then use stable Rust and standard Cargo layout.
- Treat `rustfmt.toml` as the formatting authority and `clippy.toml` as the Clippy
  configuration authority.
- Do not manually fight formatter output or introduce local formatting conventions
  that conflict with the configured tools.
- Keep lint suppressions narrowly scoped. Every new `#[allow(...)]` should have a
  nearby reason when the justification is not obvious.
- Do not add crate-wide or workspace-wide lint suppression to avoid fixing a local
  issue.
- Reuse dependencies declared by the Cargo workspace instead of adding independent
  crate-local versions.
- Do not add a dependency when the standard library or an existing workspace
  dependency provides a clear and maintainable solution.
- Keep dependency features minimal. Do not enable broad feature sets solely for one
  small API without checking compile-time, binary-size, platform, and transitive-cost
  impact.
- Keep conditional compilation paths consistent. A change behind `cfg` or a Cargo
  feature must preserve the behavior and buildability of relevant alternative paths.

## 3. Rust code structure and APIs

### 3.1 Keep imports and paths readable

- Prefer top-of-module `use` declarations over long paths inside function bodies.
- If a path has more than three `::` segments after `crate::`, `super::`, or `std::`,
  consider importing the item or its owning module locally.
- Check existing module exports before adding a new import path.
- Add a public re-export only when the item is intentionally part of the owning
  module's public domain API. Do not expand the public API merely to shorten a caller's
  path.
- Avoid wildcard imports outside narrowly scoped tests or prelude-style modules where
  the imported surface is deliberate and stable.

### 3.2 Keep control flow understandable

- Simple iterator chains such as `.iter().filter().map().collect()` are fine.
- Avoid nested closures, multi-line closures with unrelated side effects, and repeated
  expensive calls across separate iterator stages.
- Prefer a named helper or explicit `for` loop when it makes ownership, error handling,
  mutation, early exit, or performance clearer.
- Use early returns to keep the main path shallow when they improve readability.
- Avoid hidden work in boolean expressions or chained combinators when the work has
  observable side effects.

### 3.3 Make central functions read like recipes

A public or central function with several phases should expose those phases clearly:

```rust
fn execute(&self) -> Result<()> {
    let input = self.load_input()?;
    let plan = self.build_plan(input)?;
    self.apply_plan(plan)
}
```

Consider extracting a helper around:

- lock scope boundaries;
- configuration lookups, filesystem access, RPC, or other I/O;
- repeated predicates or transformations;
- a loop body with multiple responsibilities;
- parsing, validation, planning, and mutation phases;
- code that requires a separate invariant explanation.

Do not extract a helper when it only renames a trivial expression, hides important
local behavior, or requires an excessive parameter list. Rethink the data flow first.

### 3.4 Keep ownership and visibility intentional

- Prefer borrowing over cloning when ownership does not need to change.
- Do not add `Arc`, `Mutex`, `RwLock`, interior mutability, or a global singleton only
  to work around unclear ownership.
- Keep fields and functions private unless a real caller needs broader visibility.
- Prefer domain-specific types or enums over booleans, ambiguous tuples, and bare
  integers when they prevent invalid states or clarify units.
- Avoid lossy `as` conversions for IDs, offsets, lengths, timestamps, and persisted
  values. Use checked arithmetic and fallible conversions where truncation or overflow
  is possible.

### 3.5 Document public contracts

- Add or update Rust documentation when changing public APIs.
- Document important invariants, error conditions, side effects, units, ownership
  expectations, and compatibility constraints.
- Keep examples accurate and compilable when practical.
- Do not use comments to repeat the code. Explain why a constraint or non-obvious
  decision exists.

### 3.6 Validate invariants at boundaries

- Validate external input, configuration, decoded data, and constructor arguments at
  the boundary where they enter a trusted domain.
- Do not create a partially valid value and rely on every later caller to remember the
  same checks.
- Prefer constructors, parsers, and domain types that make invalid states difficult to
  represent.
- Use `From` only for infallible conversions and `TryFrom` or an explicit parser for
  fallible conversions.
- Implement standard traits such as `Default`, `AsRef`, `Borrow`, `FromIterator`, or
  `IntoIterator` only when their conventional semantics accurately describe the type.
- Avoid surprising trait implementations or implicit conversions that perform I/O,
  expensive work, lossy conversion, or hidden allocation.

## 4. Error handling

- New production paths should not use `unwrap()`, `expect()`, `panic!()`, `todo!()`, or
  `unreachable!()` for recoverable input, configuration, storage, RPC, or runtime
  errors.
- Return the repository's existing error type and preserve useful context.
- Use `expect()` only for a genuine internal invariant that cannot be represented more
  safely, and provide a precise message describing the violated invariant.
- Tests may use `unwrap()` and `expect()` when failure location remains clear.
- Do not silently discard errors. If best-effort cleanup intentionally ignores an
  error, make that decision visible and log it when operationally useful.
- Preserve the original error source when mapping errors across layers.
- Avoid converting structured errors into strings before the boundary that requires a
  textual representation.

## 5. Async and concurrency

- Do not run blocking filesystem operations, FFI calls, long CPU work, or blocking
  waits directly on an async executor thread. Use the existing blocking-task runtime
  facilities when needed.
- Do not hold a synchronous or `parking_lot` lock guard across `.await`.
- Keep lock scopes small and release locks before RPC, filesystem I/O, callbacks, or
  calls into independently owned components.
- Avoid nested locks. When unavoidable, document and consistently follow the lock
  acquisition order.
- Do not call unknown or external code while holding a lock unless the behavior is
  explicitly designed and reviewed.
- Choose channels and queues with explicit capacity and backpressure behavior. Avoid
  unbounded accumulation of requests, buffers, or retry work.
- Consider cancellation and partial progress around `.await` points. State mutations
  must not be left in an invalid intermediate state if a future is dropped.
- Spawned tasks must have an ownership and shutdown strategy. Do not leave detached
  background tasks that outlive their owning component unintentionally.
- Use timeouts where waiting indefinitely would block shutdown, recovery, or request
  completion.
- Prefer RAII guards for synchronous resource cleanup, but provide an explicit async
  close or shutdown method when cleanup requires awaiting.
- `Drop` implementations must not panic, perform unbounded work, wait indefinitely, or
  rely on an async runtime being available.
- Make shutdown ordering explicit for components that own tasks, channels, files,
  sockets, or native resources.

## 6. Unsafe code and FFI

- Keep unsafe operations and FFI boundaries as small as practical and expose safe
  wrappers to normal callers.
- Every new `unsafe` block must have an adjacent `// SAFETY:` comment that explains the
  required pointer, lifetime, aliasing, initialization, bounds, or thread-safety
  invariant.
- Every `unsafe fn` must document its caller obligations in a `# Safety` section.
- Validate lengths, nullability, ownership transfer, and cleanup behavior at FFI
  boundaries.
- Do not extend an unsafe region merely to bypass borrow checking or simplify local
  code.
- Changes to `Send` or `Sync` implementations require an explicit review of all shared
  state and threading assumptions.
- Do not allow a Rust panic to unwind across an FFI boundary. Convert failures into the
  boundary's explicit error representation or contain the unwind where appropriate.

## 7. Persistence, serialization, and compatibility

### 7.1 Compatibility is the default

Changes to persisted state, journal entries, snapshots, RPC payloads, metadata,
configuration formats, IDs, keys, and serialized enums must preserve compatibility by
default.

Before changing a serialized or persisted field:

- identify every writer and reader;
- determine whether old data can be read by new code;
- determine whether new data must be read by old code;
- check default values, missing fields, enum variants, numeric widths, ordering, and
  encoding assumptions;
- add round-trip and compatibility tests for important formats;
- document any migration, rollout ordering, or version boundary required.

If the user explicitly states that compatibility is not required, prefer the simplest
clean design and do not add unnecessary migration layers, dual formats, version
negotiation, or compatibility branches. Still report the incompatible impact and test
the new format or behavior.

### 7.2 Volatile state must remain volatile

- Runtime-only fields rebuilt on startup or each scheduling cycle must not be
  accidentally persisted.
- Use an appropriate Serde skip/default strategy and ensure deserialization produces a
  valid reconstructable value.
- Add a serialization test when skipping a field is important to state correctness.
- Volatile statistics must not affect replicated transitions or persistent placement
  decisions unless the design explicitly makes them part of the replicated input.

### 7.3 Shared encoding and ID ownership

- Cross-crate encoding, decoding, key layout, protocol, and stable ID rules belong
  behind named functions or domain types in `epoch-proto`.
- Logic shared only by modules within one crate should remain in the owning crate unless
  there is a demonstrated cross-crate contract.
- Do not scatter bit shifts, masks, packed IDs, hashes, or byte-layout expressions
  across call sites.
- Encoding helpers must test zero values, maximum values, invalid input, overflow
  boundaries, and encode/decode round trips where applicable.

## 8. Raft and distributed-state correctness

- A replicated state transition must be deterministic from the committed command and
  replicated state.
- Do not read wall-clock time, random values, process-local counters, thread-local
  state, environment variables, or volatile node statistics while applying a Raft
  command if they can affect the replicated result.
- Values such as timestamps, generated IDs, or random choices that affect replicated
  state should be generated before proposal and included in the committed command.
- Do not derive replicated output, persisted order, hashes, or externally visible
  behavior from `HashMap` or `HashSet` iteration order. Sort explicitly or use an
  ordered collection when order affects the result.
- Avoid floating-point values in replicated decisions. If unavoidable, define NaN,
  equality, rounding, and total-order behavior explicitly and test it.
- Keep proposal, validation, application, and side effects clearly separated.
- Do not perform irreversible external side effects inside a replicated state-machine
  transition unless the operation is explicitly designed to be idempotent and
  replay-safe.
- Snapshot and journal changes require recovery, replay, and compatibility analysis.
- Preserve idempotency where retries, duplicate delivery, leader changes, or replay can
  repeat an operation.

## 9. epochIO project-specific rules

### 9.1 Crate layout and ownership

The workspace layout is defined in `docs/design/06-code-layout.md` (file-level
blueprint; supersedes the summary in `docs/design/02 §4`):

- `epoch-proto`: ID types, error codes, constants, gRPC definitions — shared by all.
- `epoch-telemetry`: logging / metrics / trace-id initialization and propagation.
- `epoch-ec`: EC encode/decode + bitrot frames — **pure computation, no I/O**;
  independently unit-testable and benchmarkable.
- `epoch-rpc`: data-plane custom binary RPC (incl. in-process LocalTransport).
- `epoch-rocks`: thin RocksDB wrapper (option profiles, CF mgmt, metrics, errors).
- `epoch-store`: Extent engine (superblock, extent, index, write/read, compact, QoS).
- `epoch-client`: PD / MetaNode / DataNode clients, caches, writer_token lifecycle.
- `epoch-pd`: PD service (raft, cluster, chunk manager, placement, jobs, buckets,
  writer registry).
- `epoch-meta`: MetaNode service (MetaStore engines, multi-raft, namespaces, deleter).
- `epoch-gateway`: S3 protocol layer, auth, read/write orchestration.
- `epoch-worker`: task executors + Job coordinator.
- `epoch-node`: process assembly (roles, config, shutdown, telemetry endpoints).

**Dependency direction is a hard rule.** Allowed edges only (lower → may be used by higher):

```
Layer 0: epoch-proto, epoch-telemetry       (depend on no workspace crate)
Layer 1: epoch-ec, epoch-rpc, epoch-rocks   (→ L0; epoch-ec additionally: no tokio,
                                             no I/O crates — enforced by review + xtask)
Layer 2: epoch-store, epoch-client          (→ L0, L1)
Layer 3: epoch-pd, epoch-meta,
         epoch-gateway, epoch-worker        (→ L0–L2; NO dependencies between L3 crates)
Layer 4: epoch-node                         (→ anything; nothing depends on it)
```

- No cycles; no same-layer dependencies unless listed above. `cargo xtask layers`
  enforces this in CI.
- Adding a NEW cross-crate edge is an architectural decision: state the reason in the
  change description; if it violates the layering, stop and align with the user.
- If two crates need the same item, it moves DOWN (usually to `epoch-proto`), never
  gets duplicated and never creates a sideways edge.

Rules:

- Add shared items to `epoch-proto` only when a cross-crate consumer or a stable
  shared contract exists (see §7.3).
- `epoch-ec` must stay pure computation: no I/O, no async-runtime dependency; I/O is
  injected via traits.
- Keep server-only policy and orchestration in the owning crate/module.
- Check the nearest module exports before exposing an internal implementation type.
- Keep protocol-independent utilities separate from stateful managers and I/O code.

### 9.1a Module and file conventions (anti-accretion)

- `lib.rs` contains crate docs, module declarations, and intentional re-exports only —
  no logic.
- One module per domain concept, named by domain (`extent.rs`, `compaction.rs`,
  `blob_index.rs`). **Never create `utils.rs`, `helpers.rs`, `misc.rs`, `common.rs`
  grab-bag modules** — if a helper has no domain home, its data flow is wrong;
  rethink before filing it somewhere.
- Standard per-crate files where applicable: `error.rs` (crate error type),
  `config.rs` (crate config structs with serde + validation).
- Unit tests live in `#[cfg(test)] mod tests` beside the code; cross-module scenarios
  go in `tests/`. Shared test fixtures live in one `testutil` module per crate, not
  copied between test files.
- When a file grows past the §11 signals, split by responsibility (state machine vs
  I/O vs types), not by line count.

### 9.1b Design-doc traceability (anti-drift)

- Every module that implements a mechanism specified in `docs/design/` starts its
  module doc with an anchor line:
  `//! Design: docs/design/02-datanode.md §1.4 (blob id allocator)`.
- Invariants stated in the design docs (e.g. "old slices are captured in apply",
  "split is a log op") appear as `// INVARIANT(design 03 §5): ...` comments at the
  enforcing code site, and each has at least one test referencing it.
- Reviews may reject code whose behavior cannot be traced to a design section or an
  approved plan.

### 9.2 Design-doc synchronization and terminology

- Any change that alters behavior described in `docs/design/*` must update the
  corresponding doc section in the same change.
- Newly aligned decisions are recorded in `docs/design/99-open-questions.md` (已决
  tables) with the module-doc landing point.
- Do not import cubefs/MinIO vocabulary (volume, bid, vid, chunk-as-container, xl.meta,
  ...) into epochIO code, comments, RPC fields, or metrics; the only authority is
  `docs/design/00 §2`.

### 9.3 Logging and metrics

- Use the repository's existing structured logging and metrics facilities.
- Choose log levels deliberately. Do not emit expected per-request behavior as warnings
  or errors, and do not add noisy logs inside hot loops without rate or volume review.
- Include stable identifiers and actionable context, but do not log secrets, tokens,
  credentials, raw user data, or unnecessarily large payloads.
- Avoid unbounded or high-cardinality metric labels such as file paths, request IDs,
  error strings, or user-provided values.
- Logging and metric collection must not change correctness, lock ordering, replicated
  state, or error propagation.

## 10. Testing rules

### 10.1 Test behavior and failures

- New behavior requires tests.
- Core logic must have direct automated test coverage. Core logic includes state
  transitions, scheduling and placement decisions, routing, encoding and decoding,
  persistence, recovery, compatibility, permission checks, and other correctness-critical
  domain rules.
- Do not treat incidental execution through a broad integration test as sufficient when
  the core decision logic can be tested directly.
- If core logic is difficult to test, separate deterministic decision code from I/O,
  runtime wiring, clocks, randomness, and external services rather than lowering the
  test requirement.
- Bug fixes should include a regression test when practical.
- New features should cover the normal path, important boundaries, and meaningful
  failure cases.
- Refactors must preserve existing behavior and must add tests when core behavior was
  previously unprotected.
- Persistence or protocol changes require round-trip, old-data, replay, or migration
  coverage as applicable.

### 10.2 Keep tests maintainable

- Reuse existing fixtures, builders, test contexts, temporary-directory utilities, and
  assertion helpers.
- For PD scheduler or Job-coordination tests, use the existing shared test utilities
  instead of rebuilding the full manager context in each test.
- When two or more test cases share the same arrange, act, and assert structure and
  differ mainly in inputs or expected results, consider a table-driven test.
- Do not force unrelated scenarios into one table when their setup, behavior, or failure
  diagnosis is clearer as separate tests.
- Give each table case a descriptive name and include that name in assertion failures.
- Keep each test focused on one behavior, with failure messages that identify the case.
- Avoid tests that depend on execution order, shared global state, fixed ports, external
  services, or timing races unless the test explicitly controls them.
- Prefer readiness signals, barriers, notifications, and bounded polling over fixed
  sleeps.
- Async and concurrency tests should have a timeout so failures cannot hang the suite.

### 10.3 Validate from narrow to broad

Run the smallest relevant checks first, then expand according to the affected scope.
Typical commands are:

```bash
cargo fmt --all -- --check
cargo check -p <crate> --all-targets
cargo test -p <crate> <test_name>
cargo clippy -p <crate> --all-targets -- \
  -D warnings \
  --allow clippy::uninlined-format-args
```

For changes spanning multiple crates, shared types, workspace configuration, or common
code, run the appropriate broader workspace checks. Include optional or heavyweight
crates when the change affects them rather than assuming default workspace membership
covers them.

After running a formatting command that writes files, inspect the diff and revert
unrelated formatting changes.

If a validation command cannot be run:

- state exactly which command was skipped;
- explain why;
- describe the narrower validation that was completed;
- do not claim the change is fully verified.

## 11. Size and complexity reminders

The following are review signals, not automatic failure conditions:

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

Do not split files, functions, tests, or names mechanically to satisfy a number. Change
the structure only when it improves ownership, comprehension, reuse, reviewability, or
testability.

Hard signals that require action (not just review) in the same change or a declared
follow-up task:

- a third copy of the same logic pattern appears (two similar sites may coexist;
  three means extract the shared owner now);
- a module's `pub` surface is growing while its callers reach around it for internals
  (the abstraction boundary is wrong — fix the boundary, don't widen the surface);
- a match/if-else over the same discriminant appears in 3+ places (move the behavior
  into the type: enum method or trait dispatch);
- a function accumulates boolean/option parameters controlling its internal branches
  (split it, or introduce a request struct with a builder).

## 12. Default change workflow

1. **Clarify scope**
   - Identify the behavior, crate, module, callers, and compatibility boundaries.
   - Do not guess unresolved functional requirements. Align uncertainties with the user
     before implementation.

2. **Read context**
   - Inspect similar implementations, tests, domain types, configuration constants,
     serialization, module exports, and ownership boundaries.

3. **Assess size and risk**
   - Check for public API, persistence, protocol, Raft, async, concurrency, unsafe, FFI,
     and rollout implications.
   - Decide whether the change is large enough to require an approved implementation
     plan.

4. **Design the smallest complete solution**
   - Reuse existing patterns and keep changes focused.
   - For a large feature, present the overall flow, affected boundaries, tradeoffs,
     compatibility strategy, implementation phases, and test plan, then wait for user
     approval before editing code.

5. **Implement**
   - Match surrounding style and preserve invariants.
   - Keep state transitions, errors, locks, I/O, and side effects explicit.

6. **Verify**
   - Run narrow tests first, then formatting, checks, Clippy, and broader tests according
     to the affected scope.
   - Distinguish introduced failures from pre-existing or environment-related failures.

7. **Review the diff**
   - Remove debug output, temporary code, accidental formatting, unused abstractions,
     and unrelated edits.
   - Recheck compatibility and error paths.
   - Run the §12a self-review checklist and fix findings before reporting.

8. **Report**
   - Summarize what changed and why.
   - List validation performed.
   - Mention compatibility impact, risks, skipped checks, and follow-up work.

## 12a. Self-review checklist (run before every report)

Answer each; a "no" without justification means the change is not done:

- **Placement**: is every new item in the crate/module where a maintainer would look
  for it first? Does the dependency graph still match §9.1 layers?
- **Duplication**: did I search for an existing helper/type before writing a new one?
  (Name the search performed.)
- **Deletion**: does this change make any existing code dead? Dead code is removed in
  the same change, not left behind.
- **Traceability**: does new mechanism code carry its design anchor (§9.1b)? Do new
  invariants have `INVARIANT` comments and tests?
- **Surface**: did the public API grow? Each new `pub` item needs a current caller.
- **Terminology**: no cubefs/MinIO vocabulary leaked into identifiers, RPC, logs,
  metrics (§9.2)?
- **TODO discipline**: every `TODO` left behind has an owner-readable form
  `// TODO(scope): what + why deferred`; no `todo!()` in reachable production paths.
- **Scope**: is every hunk in the diff attributable to the stated task?

## 12b. Session budget and stopping rule (anti-accretion for automated runs)

When executing autonomously (long tasks, loops, or multi-step plans):

- **One concern per change.** Land work as a sequence of small complete changes, each
  independently reviewable and passing §10.3 checks — never one giant diff.
- **Refactor debt is declared, not squeezed in.** If mid-task you discover the
  surrounding code needs restructuring, finish or checkpoint the current concern,
  then report the needed refactor as a separate proposed task. Exception: trivial
  renames/moves fully contained in files the task already touches.
- **Stop condition.** If after two attempts an approach still fights the existing
  structure (needs layering violations, wide `pub` opening, or copy-paste), stop and
  report the friction instead of forcing the third attempt — structural friction is
  a design signal, not an implementation obstacle.
- **No speculative code.** Do not write code for undecided design points (§0
  open-question gate) or "we'll need it later" functionality. Empty trait impls,
  placeholder modules, and unused config knobs count as speculative.

## 13. Actions not allowed by default

Unless explicitly requested, do not:

- perform broad formatting-only rewrites;
- upgrade dependencies or the Rust toolchain;
- add a new framework or large abstraction;
- rename or move large numbers of files or modules;
- expand public APIs solely for convenience;
- introduce new persisted or protocol fields without compatibility analysis;
- add migration or compatibility machinery after the user explicitly opts out of
  compatibility;
- suppress warnings broadly;
- delete existing behavior without a replacement or explanation;
- leave temporary scripts, debug output, commented-out experiments, or generated junk
  in the repository;
- claim validation that was not actually run.
