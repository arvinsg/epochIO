---
name: ep-plan
description: Produce the one-page plan file and task table for an epochIO work order, then stop for approval before any code is written. Use when starting a feature or correctness fix, when a change spans crates or touches persistence or protocols, when work decomposes into more than two commits, or when the user asks to plan, decompose, or break down work.
---

# ep-plan

Turns a request into a reviewable one page: goal, non-goals, end-to-end flow, impact,
and a task table where every row is one commit carrying its own production entry point,
acceptance command, and falsification record. The page is the only thing the user has to
read before code exists, so a wrong assumption must be visible in 20 lines rather than
500.

Full loop: `.agents/rules/workflow.md` §2. Coding rules: `.agents/rules/rust-style.md`.

## When to Use

- The user asks for a feature, a correctness fix, or the next milestone segment
- The change introduces a subsystem, spans crates, changes public APIs, alters
  persistence or protocols, changes ownership boundaries, or needs coordinated rollout
- The work decomposes into more than two commits
- The user asks to plan, decompose, or break down a change
- A plan file already exists for this work order and needs updating

## Step 1: Check the open-question gate

Read the open-questions record's 待决 section **now**, not from memory — it changes
between sessions, and the session brief only reports whether anything is pending.

| Finding | Action |
| --- | --- |
| 待决 empty | proceed; every 已决 entry is binding |
| Work depends on a 待决 item | **stop.** Present the item, why it blocks, and a recommendation. No plan, no code |
| Work would decide something not recorded anywhere | that is a G1 design question — raise it before planning |

## Step 2: Decide whether a plan is needed

| Scope | Next |
| --- | --- |
| Trivial, one concern, obvious landing point | no plan file — do the work and report |
| Two commits, no persistence/protocol/API change | short plan, task table only |
| Anything larger, or any item from the When-to-Use list | full plan |

Say which row applies. Do not produce a full plan for a two-line fix.

## Step 3: Read before planning

Read the modules the plan will touch, their callers, their tests, and the nearest
`mod.rs` re-exports. Read the owning design section. Name the searches performed — a
plan that invents a helper the repo already has is worse than no plan.

Confirm every ownership boundary the plan routes through **exists in the code**. Domain
types and journal entries can exist while the manager and apply path do not; adding that
layer is an alignment stop, not a task row.

## Step 4: Write the plan file

Path: `draft/plan/<id>-<slug>.md`, where `<id>` is the milestone segment (`m7-c`,
`m8-a`) or `fix-<topic>` for correctness work outside a milestone. Use the template in
`.agents/rules/workflow.md` §2 verbatim.

Chinese, matching the design docs. Cite design sections in short form (`04 §3.1`).

If the file exists, read and update it — never start over. Record decisions made in
conversation so a later session inherits them.

## Step 5: Fill every task row

A row without these four fields is not a task, it is a wish:

| Field | Rule |
| --- | --- |
| 生产入口 | the RPC handler, HTTP route, or role ticker real traffic passes through. Not an internal method |
| 测试 | names the test, and it enters at that entry point |
| 验收命令 | a command the user can run, narrowest first |
| 反证记录 | left empty here; `ep-implement` fills it after measuring |

Each row is one commit and one concern, leaves the tree shippable, and states its
dependencies. Order rows so no row needs a later one to compile or pass.

## Step 6: Present and stop

Show the plan in the reply — the file is for the next session, the reply is what the user
reviews. Then **stop**.

**Do not write code until the user approves the plan.** Not the struct definitions, not
"just the types", not a stated assumption carried forward.

## Rules

- List every decision needing the user's answer under 待确认, each with why it changes
  the implementation and a recommendation.
- Keep it proportional. This prevents rework; it is not speculative design.
- Never hide an unresolved requirement behind a working assumption.
- **Stop and ask** on: ambiguous acceptance criteria, circular dependencies, a task that
  cannot ship without a stub or flag, an unknown test strategy, an unclear goal/non-goal
  boundary.
- No speculative rows: nothing for an undecided design point or "we'll need it later".

## Do Not

- Do not plan around a 待决 item as though it were decided.
- Do not write a row whose only test reaches an internal method.
- Do not put the plan only in the reply — a later session cannot read the chat.
- Do not carry cubefs/MinIO vocabulary into the plan; the design docs' terminology
  section is the authority.

## Related

- Execute a row → `ep-implement`
- Review the result → `ep-review`
- Write the outcome back → `ep-doc-sync`
- Where the project stands → `ep-status`

## Checklist

- [ ] 待决 section read this session, and the work does not depend on a pending item
- [ ] Scope row named; a full plan is actually warranted
- [ ] Target modules, callers, and tests read; searches named
- [ ] Every routed ownership boundary confirmed present in the code
- [ ] Plan written to `draft/plan/<id>-<slug>.md` and shown in the reply
- [ ] Every row carries 生产入口, 测试, 验收命令; 反证记录 left for implementation
- [ ] Rows are commit-sized, dependency-ordered, each shippable
- [ ] 待确认 lists every open decision with a recommendation
- [ ] Stopped for approval; no code written
