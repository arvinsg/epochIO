---
name: ep-review
description: Review an epochIO working-tree diff or commit range against the defect classes this repository has actually shipped, then present an accept/reject findings table and wait for the user's decision before changing anything. Use when the user asks to review a change or diff, after finishing a task row, or before committing on a data, metadata, or raft path.
---

# ep-review

Reads a diff the way the four-way audit did: not looking for style, looking for the
shapes that pass a green test suite and still lose data. Every row of the defect table
below is a real past bug in this repository. Findings are presented for the user's
decision; this workflow does not change code.

Full loop: `.agents/rules/workflow.md` §4.

## When to Use

- The user asks to review a change, a diff, or a commit range
- A task row is finished and about to be committed
- The change touches object read/write, metadata, raft apply, placement, or deletion
- A mechanism was added and it is not obvious that production reaches it

## Step 1: Locate the diff

| Target | Command |
| --- | --- |
| Working tree (default) | `git diff` and `git status --short` |
| Staged only | `git diff --cached` |
| A commit range | `git diff <base>..<head>` |
| A single commit | `git show <sha>` |

State which target was reviewed. If the working tree mixes several concerns, say so —
that is itself a finding.

## Step 2: Read the changed files in full

Not the hunks. Read each changed file, plus the callers of anything whose signature or
behavior moved, plus the tests that claim to cover it. A diff cannot show that a guard is
never called.

## Step 3: Walk the defect classes

For each, answer the question in the right column. "No" is a finding.

| Class | Shape it takes | Ask |
| --- | --- | --- |
| Production path not wired | mechanism complete, unit-tested, green, zero production callers | are the callers all under `#[cfg(test)]`? |
| Guard bypassed | production calls the unprotected variant; the protected one is test-only | trace from the entry point down — which path does real traffic take? |
| Silent success | request accepted, `Ok`/200 returned, nothing done: an ignored parameter, a dropped `bool`, `.map(\|_\| ())` | is every input read? is every meaningful return value branched on? |
| Allow-list omission | a whitelist where a missing entry means data loss | can it be an exclusion list instead, so a new case is safe by default? |
| State-machine deadlock | a state no transition leaves | does every state have both an in-edge and an out-edge? |
| Invisible mismatch | wrong but reads fine: anti-affinity piling shards, a range request answered with the whole object | if this were wrong, would anyone notice? if not, assert it |
| Positive-feedback failure | responding to exhaustion by consuming more of it | does the failure handling worsen its own cause? |
| Observability gap | a metric registered in a process that exposes no endpoint | are the registration and the endpoint in the same role? |
| Vocabulary and layering | cubefs/MinIO terms in identifiers; a new sideways crate edge | read the identifiers; `cargo xtask layers` for the edge |

Then check the change's own claims: does each new guard have a test that enters at the
production entry point, and does the plan row's 反证记录 record a measured mutation? An
unfalsified guard is a finding.

## Step 4: Present the findings table

```markdown
| # | 位置 | 类别 | 结论 | 依据 | 建议 | 接受/拒绝 |
| - | --- | --- | --- | --- | --- | --- |
| 1 | `file.rs:120` | 生产路径未接线 | ... | `file.rs:120` 与 `handler.rs:88` | ... | |
```

One row per finding, most severe first, each with a `file:line` basis. State plainly when
nothing was found — an empty table is a result, not a failure.

Then **stop and wait**. Do not apply fixes, do not stage, do not commit.

## Step 5: Apply only what the user accepts

Fix accepted rows one concern at a time, re-verify per `.agents/rules/rust-style.md` §16,
and leave rejected rows alone. If a fix turns out larger than a row, propose it as a task
instead of growing this change.

## Rules

- Severity is about consequence, not size: data loss > space leak > permission bypass >
  wrong-but-visible > cleanup.
- Judge the critical paths harder — object read/write, RPC, metadata operations, raft
  apply. A likely regression there must produce a finding with a concrete suggestion.
- Cite `file:line` for every claim. A finding without a location cannot be checked.
- Report what was not reviewed (skipped files, generated code) rather than implying full
  coverage.

## Do Not

- Do not report commit-message, wording, or formatting nits — the hooks own those.
- Do not fix anything before the user decides.
- Do not restate the diff as a summary; the user can read the diff.
- Do not claim a path is covered without naming the test.

## Related

- The row being reviewed → `ep-implement`
- Commit after acceptance → `ep-commit`
- Verify claims against the docs → `ep-status`

## Checklist

- [ ] Review target stated
- [ ] Every changed file read in full, plus movers' callers and tests
- [ ] All nine defect classes walked, with an answer each
- [ ] New guards checked for production-path entry and a measured 反证记录
- [ ] Findings table presented, most severe first, each with `file:line`
- [ ] Waited for the user's accept/reject before changing anything
- [ ] Anything not reviewed named explicitly
