# epochIO workflow rules

Plan-driven development (PDD) for this repository. Referenced by the `ep-*` skills.
Coding rules live in [rust-style.md](rust-style.md); the always-on overview and red
lines live in `AGENTS.md`.

## 0. Artifact chain and the three gates

```
draft/design/*.md            ── G1 design gate ── human + AI decide together
      │                          99-open-questions.md 待决 is a hard gate
      ▼ ep-plan
draft/plan/<id>-<slug>.md    ── G2 plan gate ── one page, human approves before code
      ▼ ep-implement             one task row = one commit
      ▼ ep-review              ── G3 delivery gate ── findings table, human decides
      ▼ ep-commit
      ▼ ep-doc-sync              write back to 07 实际交付 / 99 已决
```

Human attention concentrates on G1 (discussion) and G2 (one page). G3 is mechanical
checks plus AI self-review, with human spot-checks of the diff.

Design documents are the source of truth. Code must follow them. If code and docs
conflict, one of them changes in the same task — report which and why.

**Where they live:** design docs in `draft/design/` (reading order in its `README.md`),
plans in `draft/plan/`. This file is the only place that states the location, so a
relocation is a one-line change — `AGENTS.md` and the coding rules refer to "the design
docs" by role, never by path.

`draft/*` is gitignored, so design docs and plans have no version history. Treat the
write-back in §6 as the durable record.

---

## 1. G1 — design gate

Enter here when the work changes an architectural decision, adds a subsystem, or
depends on an undecided question.

- **Check `draft/design/99-open-questions.md` at task start.** Items under 待决 are
  NOT decided. Do not rely on a cached list — it changes between sessions. If the
  task depends on one, stop and align with the user first. If 待决 is empty, all 已决
  entries are binding.
- Design changes are discussed with the user, then written into the owning
  `draft/design/*.md` section. A decision is not aligned until it is in a 已决 table
  with its landing point.
- Do not implement an undecided item as if decided, and do not write speculative code
  for one (§8).

---

## 2. G2 — plan gate

Every work order gets a plan file at `draft/plan/<id>-<slug>.md` before code. `<id>`
is the milestone segment it belongs to (`m7-c`, `m8-a`) or `fix-<topic>` for
correctness work outside a milestone.

**Scope rule.** A trivial, single-concern change with an obvious landing point needs
no plan file — do it and report. A plan is required when the change introduces a
subsystem, spans multiple crates, changes public APIs, alters persistence or
protocols, changes ownership boundaries, needs coordinated rollout, or decomposes
into more than two commits.

**Do not write code until the user approves the plan.** A hard stop — no "just the
struct definitions" first, no proceeding on a stated assumption. Never hide an
unresolved requirement behind a working assumption. Ask as concrete questions, say
why each answer changes the implementation, and recommend one.

Keep it to one page of substance. Enough that a wrong assumption is visible in 20
lines instead of 500. Show the end-to-end flow before implementation steps. Keep it
proportional — this prevents rework, it is not speculative design.

Write the file **and** show the plan in the reply: the file lets a later session
resume without reinventing it, the reply is what the user reviews. If the file
already exists, read and update it rather than starting over, and record decisions
made in conversation so the next session inherits them.

### Plan template

```markdown
# <id> <标题>

**目标**：<一句话>
**不做**：<边界>
**设计依据**：draft/design/0X-xxx.md §Y（已决：99 §Z）

## 现状与问题
<当前行为、为什么不够，附 `文件:行号`>

## 端到端流程
<StateA> --<event>--> <StateB> --<event>--> <StateC>

## 影响面
| 维度 | 影响 |
| --- | --- |
| crate / 模块 | |
| 公开 API | |
| 持久化 / 协议 / 兼容 | |
| 并发 / 恢复 / 观测 | |

## 任务表
| # | 任务 | 依赖 | 落点 | 状态 |
| - | --- | --- | --- | --- |
| 1 | | — | | 待办 |

### T1 <任务名>
- **设计锚点**：draft/design/0X-xxx.md §Y
- **生产入口**：<crate>::<module>::<symbol>（真实流量经过的那个入口）
- **测试**：<测试名>，从生产入口进入
- **验收命令**：`cargo test -p <crate> <test_name>`
- **反证记录**：<把缺陷改回去后哪个测试变红；实测后填>

## 待确认
1. <问题>？ → 影响：<改什么>；建议：<推荐>
```

### Task row rules

- **One task = one commit = one concern.** Each task is independently reviewable and
  passes the §9 checks on its own. Never one giant diff.
- Every task carries a **生产入口**: the RPC handler, HTTP route, or role ticker that
  real traffic passes through. A task whose test only reaches an internal method is
  not covered — see rust-style.md §14.1. Checked by review today; `cargo xtask
  entrypoint` is planned (§9).
- Every task carries a **验收命令** that a reader can run, and a **反证记录** filled
  in after implementation.
- Dependencies are explicit. Every task leaves the tree shippable: no task requires a
  later one to compile, pass tests, or avoid breaking behavior.
- **Stop and ask** on ambiguous acceptance criteria, circular dependencies, a task
  that cannot be made shippable without a stub or flag, an unknown test strategy, or
  an unclear goal/non-goal boundary.

Status values: `待办` → `进行中` → `已落`（commit 已产生）→ `已回写`（§6 完成）.

---

## 3. Implementation loop

Per task row, in order:

1. **Read context.** The target module, its callers, its tests, the nearest `mod.rs`
   re-exports, related domain types, config constants, serialization. Search for an
   existing helper before writing one. Then write in the module's style, not yours.
2. **Implement the smallest complete change.** Tied to the requested behavior; no
   opportunistic refactor, dependency bump, broad formatting, or unrelated cleanup.
   Small still means complete: all callers, tests, serialization, and validation paths
   updated.
3. **Verify narrow to broad** (rust-style.md §16).
4. **Falsify the guard.** Re-introduce the defect, confirm the test goes red, restore.
   Record the result in 反证记录.
5. **Self-review** (§7), fix findings.
6. **Update the task row** status and 反证记录 in the plan file.
7. **Report falsifiably**: what changed, commands run and their results, compatibility
   impact, skipped checks, follow-ups.

### The alignment gate during implementation

**Stop and align** before writing code when any of these holds:

- a new module, subsystem, or crate;
- a new `trait`, generic parameter, or type-level abstraction;
- a change to a journal entry, snapshot, RPC payload, or any persisted layout;
- a new external dependency;
- a new cross-crate edge (and any edge that violates the `AGENTS.md` §4 layering);
- two or more designs are defensible and the request does not imply the choice;
- a behavior the request does not specify and the repo does not answer.

Otherwise **proceed**. Mention these in the report afterwards; do not ask permission
beforehand, because the user already asked for the work and a size question is pure
interruption:

- a function over ~60 lines, or more than ~5 files touched;
- a public API signature changed;
- a new lock (`Mutex`/`RwLock`) or interior mutability added — a plain `Arc` does not
  count, shared ownership is routine here;
- an existing test modified rather than added to.

If the implementation starts feeling complicated, that is a design problem surfacing
late. Stop and align, even mid-change.

---

## 4. G3 — delivery gate

`ep-review` reviews the working-tree diff or a commit range and produces a findings
table. **Present the table and wait for the user's decision before fixing anything.**
Findings are not applied silently.

| # | 位置 | 类别 | 结论 | 依据 | 建议 | 接受/拒绝 |
| - | --- | --- | --- | --- | --- | --- |

Review across correctness, safety, design, and performance on the critical paths
(object read/write, RPC, metadata operations, raft apply). Beyond generic review, walk
the defect classes this repository has actually shipped — each row below is a real
past bug:

| 类别 | 形态 | 检查方式 |
| --- | --- | --- |
| 生产路径未接线 | 机制完整、有单测、过 CI，但零生产调用方（`allows_bucket`、compaction/scrub/reclaim） | grep 调用方是否全在 `#[cfg(test)]` 下（`cargo xtask deadwire` 计划中，见 §9）|
| 绕过保护 | 生产走无保护变体，带保护的那个只有测试调（`tombstone_blob` vs `delete`） | 从生产入口反向追到底层，看走的是哪条 |
| 静默成功 | 接受请求、返回 200/Ok、实际没做（`range` 被忽略、元数据被丢弃、`.map(\|_\| ())` 吃掉有意义的 bool） | 每个入参在实现里都被读到了吗？每个返回值都被判断了吗？ |
| 白名单漏项 | allow-list 少一项就丢数据（GC keep-set 漏 `fs`/`upload`） | 能改成排除法吗？漏项应当默认安全而非默认失败 |
| 状态机死锁 | 某状态无任何转移能离开（Job `Created` 永不派发） | 每个状态的入边与出边都存在吗？ |
| 不可见错配 | 行为错了但读起来正常（反亲和堆叠、Range 返全量） | 错了会有人发现吗？没人发现的错误需要主动断言 |
| 正反馈故障 | 用消耗资源的方式响应资源耗尽（ENOSPC → RepairDisk） | 失败处置会加剧失败原因吗？ |
| 观测缺口 | 指标注册在没有端点的进程里 | 注册点与端点在同一个角色里吗？ |
| 术语泄漏 / 分层违规 | cubefs/MinIO 词汇进标识符；新增横向 crate 边 | 术语靠人读；分层用 `cargo xtask layers` |

Do not include commit-message, wording, or formatting nits — the hooks own those.

---

## 5. Commit convention

Commits are local; there is no issue or PR flow. `ep-commit` owns this step.

```
<type>(<scope>): <description>

<body: 为什么这么做；设计锚点；plan id>
```

- `type`: `feat` · `fix` · `docs` · `style` · `refactor` · `perf` · `test` · `chore`
- `scope`: crate short name — `pd` · `meta` · `store` · `gateway` · `ec` · `rpc` ·
  `client` · `node` · `proto` · `worker` · `rocks` · `telemetry` · `xtask`. Use the
  plan id when a task genuinely spans crates.
- Subject in English, imperative, no trailing period. Body states why, cites the
  design section and plan task (`plan: m7-c T2`).
- No tool watermarks, no co-author trailers unless the user asks.
- Stage only files belonging to this task. **Ask for confirmation before every commit,
  even if earlier conversations contained approvals.** `git push`, `git rebase`,
  `git reset` are refused by the git guard hook — the user runs those.
- A commit message must not claim validation that was not run.

---

## 6. Write-back (ep-doc-sync)

The durable record of what actually shipped. Run at the end of a work order, not per
commit.

1. **`draft/design/07-iteration-plan.md`** — append or update the milestone's
   **实际交付** block: what landed, what was cut, what deviated from the plan and why.
   A milestone claim that the code does not support is worse than no claim: M6 recorded
   hier LIST as delivered while `s3compat` had no `list` function at all.
2. **`draft/design/99-open-questions.md`** — add a 已决 table for decisions made during
   the work, each with its landing point (`file.rs`, design §). Remove resolved 待决
   items.
3. **The owning `draft/design/0X-*.md` section** — update any behavior description the
   change altered (rust-style.md §12), and add `INVARIANT(design 0X §Y)` comments at
   the enforcing code site for new invariants.
4. **Record the landing point.** Every 已决 row and 实际交付 entry names the file and
   module it landed in — that is the only traceability record now that code carries no
   header anchors (rust-style.md §11). Nothing checks this mechanically, so a 落点 naming
   a file that has since moved stays wrong until a reader notices.
5. **`ep-status` afterwards**: read 07, 99, `git log`, and the plan files, and report
   real progress against claimed progress. Discrepancies are findings, not noise.

---

## 7. Self-review checklist (run before every report)

Answer each; a "no" without justification means the change is not done.

- **Placement**: is every new item in the crate/module where a maintainer would look
  first? Does the dependency graph still match the `AGENTS.md` §4 layers?
- **Duplication**: did I search for an existing helper/type before writing a new one?
  (Name the search performed.)
- **Deletion**: does this change make any existing code dead? Dead code is removed in
  the same change.
- **Wiring**: is the new mechanism reachable from a production entry point, and does
  the test enter through that entry point?
- **Falsification**: did I confirm each new guard's test goes red when the defect is
  re-introduced?
- **Traceability**: is the design section cited beside the decision it explains, and is
  the landing point recorded on the doc side? Do new invariants
  have `INVARIANT` comments and tests?
- **Surface**: did the public API grow? Each new `pub` item needs a current caller.
- **Terminology**: no cubefs/MinIO vocabulary in identifiers, RPC, logs, metrics?
- **TODO discipline**: every `TODO` left behind reads `// TODO(scope): what + why
  deferred`; no `todo!()` in reachable production paths.
- **Scope**: is every hunk in the diff attributable to the stated task?

---

## 8. Session budget and stopping rule

When executing autonomously (long tasks, loops, multi-step plans):

- **One concern per change.** Land work as a sequence of small complete changes, each
  independently reviewable and passing the §9 checks.
- **Refactor debt is declared, not squeezed in.** If mid-task you discover the
  surrounding code needs restructuring, finish or checkpoint the current concern, then
  report the needed refactor as a separate proposed task. Exception: trivial
  renames/moves fully contained in files the task already touches.
- **Stop condition.** If after two attempts an approach still fights the existing
  structure (needs a layering violation, a wide `pub` opening, or copy-paste), stop and
  report the friction instead of forcing the third attempt. Structural friction is a
  design signal, not an implementation obstacle.
- **No speculative code.** Do not write code for undecided design points (§1) or
  "we'll need it later" functionality. Empty trait impls, placeholder modules, and
  unused config knobs count as speculative.

---

## 9. Mechanical gates

Checks live in `xtask` so they are testable and reusable; hooks only wire them up.

| Gate | When | Blocks on |
| --- | --- | --- |
| `rustfmt` single file | after every Write/Edit of a `.rs` file | — (formats in place) |
| `cargo xtask gate` | before `git commit` | layers · docrefs · vocab · headers · registry |
| `clippy -D warnings`, `test --lib --bins` | before `git commit`, staged crates only | any failure |
| git guard | on `git push` / `rebase` / `reset` | always denies |
| session brief | at session start | injects current 待决 + milestone |

`cargo xtask gate` is a fast local gate, not a CI substitute. Integration suites that
bind real addresses stay out of the commit gate; run them explicitly.

A gate is only trusted once it has been shown to fail: after adding one, break the
thing it guards and confirm it goes red.

**Built today**: `layers`, `headers`, `registry`, the four hooks, and the six `ep-*`
skills in §10. Each check was falsified on introduction.

**Deliberately not mechanical**: design-citation shape (rust-style §11) and terminology
(§12). Checks for both were written and then removed — policing comment content fired on
ordinary English, and a gate people learn to bypass is worse than a review rule they
read. Both remain in the review classes (§4) and the self-review checklist (§7).

**Planned, not built** — until they exist, the rules they would enforce are checked by
review only, and this list is the record of that gap:

| Planned | Would catch |
| --- | --- |
| `cargo xtask deadwire` | a pub item whose only callers are under `#[cfg(test)]` — the "implemented, tested, never wired" class |
| `cargo xtask entrypoint` | a task whose declared 生产入口 symbol is absent or unreferenced by tests |
| CI workflow | the whole gate on Linux; the iteration plan's M0 criterion ("break a rule, CI must go red") is not satisfied by a local hook |

---

## 10. Skill index

Each is registered in `AGENTS.md` §7; `cargo xtask registry` holds the directory name,
the frontmatter, and that registry to the same values, so a skill named here can actually
be invoked.

| Step | Skill |
| --- | --- |
| Plan a work order, produce the task table | `ep-plan` |
| Execute one task row | `ep-implement` |
| Review a diff or commit range | `ep-review` |
| Commit one concern | `ep-commit` |
| Write back to 07 / 99 / design sections | `ep-doc-sync` |
| Report real progress vs claimed progress | `ep-status` |
