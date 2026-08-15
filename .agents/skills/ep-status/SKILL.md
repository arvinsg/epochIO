---
name: ep-status
description: Report epochIO's real progress against what the docs claim, by reading the iteration plan, the open-questions record, the plan files, git log, and the code that is supposed to back each claim. Use when the user asks where the project stands, before starting a milestone, when a delivery claim needs verification, or when picking up work after a break.
---

# ep-status

Answers "where are we" with evidence rather than with the documents' own summary. The
question worth answering is not what the iteration plan says shipped — it is whether the
code agrees.

This exists because the plan once recorded a feature as delivered when the function did
not exist. A status report that trusts the docs reproduces that error instead of catching
it.

## When to Use

- The user asks where the project stands, or what is next
- A milestone segment is about to start
- A delivery claim needs checking before it is built on
- Work resumes after a break and the last session's state is unclear

## Step 1: Read the claims

| Source | What it claims |
| --- | --- |
| iteration plan | milestone deliverables, acceptance criteria, 实际交付 blocks |
| open-questions record | 待决 items (binding gate) and 已决 decisions with their 落点 |
| `draft/plan/*.md` | task rows and their status (`待办` / `进行中` / `已落` / `已回写`) |
| `git log --oneline` | what actually landed, and in what order |

## Step 2: Verify the claims against the code

For each recent 实际交付 claim and each 已决 落点, check the code:

| Check | How |
| --- | --- |
| The named file, module, or symbol exists | `rg -w '<symbol>'`, read the file |
| It is reachable from a production entry point | trace from the RPC handler, HTTP route, or role ticker |
| Its tests exist and name it | `rg '<test_name>'` |
| The acceptance criterion was actually exercised | find the test or bench that runs it, or report that none does |

A claim whose only support is a unit test, while the milestone's criterion names an
end-to-end scenario, is **partially delivered** — say so in those words.

Sample, do not audit everything: the most recent segment in full, plus any claim the user
is about to build on.

## Step 3: Report

```markdown
## 当前位置
<milestone / segment, and what the last commit landed>

## 声称 vs 实际
| 项 | 文档声称 | 代码事实 | 判定 |
| --- | --- | --- | --- |
| ... | 07 §M7 已交付 | `file.rs:88` 存在，生产入口未接 | 部分交付 |

## 开放问题
<待决 items verbatim, or "无未决项">

## 进行中
<plan rows at 进行中 or 待办, with their plan id>

## 下一步候选
<ordered, with what each unblocks>
```

Mark each verdict as 已交付 / 部分交付 / 未交付 / 文档滞后, and cite `file:line` for
every 代码事实 cell.

## Rules

- Every discrepancy is a finding, not noise. Report it even when it is the docs that are
  wrong rather than the code.
- Distinguish "not built" from "built but not wired". They need different work.
- Name what was not verified. A report that implies full coverage of a 4000-line design
  set is itself a false claim.
- Do not fix anything here. Discrepancies become `ep-plan` input or `ep-doc-sync` work.

## Do Not

- Do not summarize the iteration plan back to the user; they wrote it.
- Do not treat a 已决 table as evidence that code exists — it records an intent and a
  landing point, both of which can be stale.
- Do not report progress as a percentage. Name what works and what does not.

## Related

- Plan the next segment → `ep-plan`
- Correct a stale claim → `ep-doc-sync`
- Verify a specific diff → `ep-review`

## Checklist

- [ ] Iteration plan, open-questions record, plan files, and `git log` read
- [ ] Recent 实际交付 claims and 已决 落点 checked against the code
- [ ] Production reachability traced, not assumed, for each verified claim
- [ ] Verdicts marked 已交付 / 部分交付 / 未交付 / 文档滞后 with `file:line` evidence
- [ ] 待决 items reported verbatim
- [ ] In-flight plan rows listed with their plan id
- [ ] Unverified areas named
