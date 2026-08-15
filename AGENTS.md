# epochIO Agent Project Rules

Overview and red lines for AI coding agents working in this repository. Always in
context. The detailed rules load on demand:

| File | Contents | Read it when |
| --- | --- | --- |
| [`.agents/rules/rust-style.md`](.agents/rules/rust-style.md) | Naming, comments, structure, errors, async, persistence, raft, testing, validation | Before a change that needs design alignment, or in an area you have not touched this session |
| [`.agents/rules/workflow.md`](.agents/rules/workflow.md) | PDD loop: design gate → plan gate → implement → review → commit → write-back | At the start of any work order, and before every commit |

Apply these to every analysis, implementation, test, refactor, and review unless the
user explicitly requests a different approach.

---

## 1. Project anchor

epochIO is a next-generation distributed object store written in Rust: three services
(PD / DataNode / MetaNode), EB-scale data, 10B+ objects of metadata, zero external
dependencies, and AI-oriented workloads.

- **The design docs are the source of truth.** Read them before changing behavior they
  describe; their own index states the reading order. Code must follow them. If code and
  docs ever conflict, one of them must change in the same task — report which and why.
  `workflow.md` §0 names where they live.
- **Terminology authority is the design docs**: **Chunk** = EC group, **Shard** = shard
  slot, **Blob** = 32MB object segment, **Extent** = 32GB on-disk container,
  **Stripe** = 1MB coding unit. Note the deliberate mismatch with cubefs vocabulary
  (epochIO Chunk ≈ cubefs volume; epochIO Extent ≈ cubefs chunk). Use only epochIO
  terms in code, comments, RPC, logs, and metrics.
  **Check the open-questions record at task start** — do not rely on any cached list,
  it changes between sessions. Do not implement an undecided item as if decided; if a
  task depends on one, stop and align with the user first. If the 待决 section is empty,
  all recorded 已决 decisions are binding.
- **Git.** Do not run `git commit`, `git push`, `git rebase`, `git reset`, or any other
  git mutation unless the user explicitly asks. Ask for confirmation each time, even if
  earlier conversations contained approvals.

---

## 2. Precedence

**Read the code before writing any.** Before editing a module, read it, its callers,
its tests, and the nearest `mod.rs` re-exports. **Then write in its style, not yours.**
The codebase is the style specification and is more precise than prose — 66k lines of
it, with 82 `INVARIANT` comments marking the constraints that must not be broken.

Precedence when the module and these rules disagree is **scoped**:

1. **Explicit user instruction** in the current conversation. Always wins.
2. **Cross-module invariants** — persistence and compatibility (rust-style §8), raft
   determinism (§9), concurrency and shutdown (§6), boundary validation (§4.6), crate
   layering (§4 below). A single module has no way to know these → **the rules win.**
   Say the module looks wrong.
3. **The surrounding module**, for anything local: naming, comment voice, function
   granularity, which error type this layer returns, test shape → **the module wins.**
   Say the rule looks stale.
4. The rest of the rules files — a weaker written copy of (3).
5. General Rust convention.

Either way, surface the conflict; never pick a side silently.

Resolve questions that can be answered from the repository before asking the user.

---

## 3. Alignment gate

There is one gate, and it is about **design**, not size.

**Stop and align** before writing code when any of these holds:

- a new module, subsystem, or crate;
- a new `trait`, generic parameter, or type-level abstraction;
- a change to a journal entry, snapshot, RPC payload, or any persisted layout;
- a new external dependency;
- a new cross-crate edge, and any edge violating the §4 layering;
- two or more designs are defensible and the request does not imply the choice;
- a behavior the request does not specify and the repo does not answer.

A hard stop — no "just the struct definitions" first, no proceeding on a stated
assumption. Never hide an unresolved requirement behind a working assumption. Present
uncertainty as concrete questions, say why each answer changes the implementation, and
recommend one.

Otherwise **proceed**. Report these afterwards; do not ask permission beforehand:
a function over ~60 lines or more than ~5 files touched · a changed public API
signature · a new `Mutex`/`RwLock` or interior mutability (a plain `Arc` does not
count) · an existing test modified rather than added to.

If the implementation starts feeling complicated, that is a design problem surfacing
late. Stop and align, even mid-change.

A work order larger than two commits goes through the plan gate first — see
`workflow.md` §2. **No code before the user approves the plan.**

Beyond that, four habits shape every change: make the **smallest complete** change (no
opportunistic refactor, but all callers, tests, serialization, and validation paths
updated); **reuse** the existing owner rather than duplicating logic; put new logic
where a future maintainer would look for it, not merely in the file that currently
needs it; prefer explicit control flow and visible invariants over compressed
cleverness.

---

## 4. Crate layering — a hard rule

The workspace layout is defined by the design docs' code-layout blueprint. Allowed
edges only (lower → may be used by higher):

```
Layer 0: epoch-proto, epoch-telemetry       (depend on no workspace crate)
Layer 1: epoch-ec, epoch-rpc, epoch-rocks   (→ L0; epoch-ec additionally: no tokio,
                                             no I/O crates — pure computation)
Layer 2: epoch-store, epoch-client          (→ L0, L1)
Layer 3: epoch-pd, epoch-meta,
         epoch-gateway, epoch-worker        (→ L0–L2; NO dependencies between L3 crates)
Layer 4: epoch-node                         (→ anything; nothing depends on it)
```

| Crate | Owns |
| --- | --- |
| `epoch-proto` | ID types, error codes, constants, gRPC definitions — shared by all |
| `epoch-telemetry` | logging / metrics / trace-id initialization and propagation |
| `epoch-ec` | EC encode/decode + bitrot frames — pure computation, no I/O |
| `epoch-rpc` | data-plane custom binary RPC (incl. in-process LocalTransport) |
| `epoch-rocks` | thin RocksDB wrapper (option profiles, CF mgmt, metrics, errors) |
| `epoch-store` | Extent engine (superblock, extent, index, write/read, compact, QoS) |
| `epoch-client` | PD / MetaNode / DataNode clients, caches, writer_token lifecycle |
| `epoch-pd` | PD service (raft, cluster, chunk manager, placement, jobs, buckets, writer registry, console) |
| `epoch-meta` | MetaNode service (MetaStore engines, multi-raft, namespaces, deleter) |
| `epoch-gateway` | S3 protocol layer, auth, read/write orchestration |
| `epoch-worker` | task executors + Job coordinator |
| `epoch-node` | process assembly (roles, config, shutdown, telemetry endpoints) |

- No cycles; no same-layer dependencies unless listed above. `cargo xtask layers`
  enforces this.
- Adding a NEW cross-crate edge is an architectural decision: state the reason in the
  change description; if it violates the layering, stop and align with the user.
- If two crates need the same item, it moves DOWN (usually to `epoch-proto`), never
  gets duplicated and never creates a sideways edge.
- Add shared items to `epoch-proto` only when a cross-crate consumer or a stable shared
  contract exists. Keep server-only policy and orchestration in the owning crate.
- Never infer an ownership boundary. Before routing work through a manager or facade,
  confirm that layer exists in the code — domain types and journal entries can exist
  while the manager and apply path are not written yet, and adding that layer is an
  alignment stop.

---

## 5. Validation

Narrowest first, then widen to the affected scope. Full ordering in rust-style §16.

```bash
cargo fmt --all -- --check
cargo check -p <crate> --all-targets
cargo test -p <crate> <test_name>
cargo clippy -p <crate> --all-targets --no-deps -- \
  -D warnings --allow clippy::uninlined-format-args
cargo xtask gate     # layers · docrefs · vocab · headers · registry
```

**Reports must be falsifiable**: name the command and its result, or say it was not
run. No "should work", no "looks good", no coverage claim without a test name. If a
check could not be run: name the exact command skipped, say why, describe the narrower
validation that passed, and do not claim the change is verified.

**A regression test must enter through the production path**, and every new guard needs
a recorded falsification ("re-introduce the defect → this test goes red"). The dominant
defect class in this repository is a mechanism that is implemented, unit-tested, and
green while the production entry point is not wired to it or bypasses its guard. See
rust-style §14.1 and workflow §4.

---

## 6. Not allowed by default

Unless explicitly requested, do not: upgrade a dependency or the toolchain (deps are
workspace-declared, so any bump is repo-global) · add a new framework or large
abstraction (the workspace already carries hyper 1, RPC, raft, RocksDB, serialization,
and zero external dependencies is a product property) · widen a public API for
convenience · add or reorder a persisted or protocol field without compatibility
analysis · perform broad formatting-only rewrites · rename or move large numbers of
files · add migration machinery after the user opts out of compatibility · suppress
warnings broadly · delete existing behavior without a replacement or explanation ·
leave temporary scripts, debug output, or commented-out experiments in the repository ·
**claim validation that was not run.**

Full list with rationale in rust-style §17.

---

## 7. Skills

<!-- SKILLS_TABLE_START -->
<skills_system priority="1">

<usage>
Project skills live under `.agents/skills/` (canonical), reached by other agent tools
through symlinks (`.claude/skills`, `.cursor/skills`). Read the skill file at
`.agents/skills/<skill-name>/SKILL.md` before following it.

- For epochIO workflow tasks, prefer the `ep-*` skills listed in <available_skills>
- Do not invent or invoke project skills that are not listed
- Do not invoke a skill that is already loaded in your context
- Each skill invocation is stateless
- Only `ep-*` skills are versioned in this repository
</usage>

<available_skills>

<skill>
<name>ep-commit</name>
<description>Commit one concern from an epochIO work order using the project's message convention, after the repository gate passes and the user confirms. Use when the user asks to commit, or when a verified task row is ready to land.</description>
<location>project</location>
</skill>

<skill>
<name>ep-doc-sync</name>
<description>Write a finished epochIO work order back into the design docs — the iteration plan's 实际交付 block, the open-questions 已决 tables, and the owning design sections. Use when a work order is complete, when decisions were made during implementation, or when the user asks to sync or write back the docs.</description>
<location>project</location>
</skill>

<skill>
<name>ep-implement</name>
<description>Execute one task row from an approved epochIO plan — read context, make the smallest complete change, verify narrow to broad, falsify the guard, self-review, and update the row. Use when implementing an approved task, resuming a plan part-way through, or when the user names a plan id or task number.</description>
<location>project</location>
</skill>

<skill>
<name>ep-plan</name>
<description>Produce the one-page plan file and task table for an epochIO work order, then stop for approval before any code is written. Use when starting a feature or correctness fix, when a change spans crates or touches persistence or protocols, when work decomposes into more than two commits, or when the user asks to plan, decompose, or break down work.</description>
<location>project</location>
</skill>

<skill>
<name>ep-review</name>
<description>Review an epochIO working-tree diff or commit range against the defect classes this repository has actually shipped, then present an accept/reject findings table and wait for the user's decision before changing anything. Use when the user asks to review a change or diff, after finishing a task row, or before committing on a data, metadata, or raft path.</description>
<location>project</location>
</skill>

<skill>
<name>ep-status</name>
<description>Report epochIO's real progress against what the docs claim, by reading the iteration plan, the open-questions record, the plan files, git log, and the code that is supposed to back each claim. Use when the user asks where the project stands, before starting a milestone, when a delivery claim needs verification, or when picking up work after a break.</description>
<location>project</location>
</skill>

</available_skills>

</skills_system>
<!-- SKILLS_TABLE_END -->
