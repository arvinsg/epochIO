---
name: ep-implement
description: Execute one task row from an approved epochIO plan — read context, make the smallest complete change, verify narrow to broad, falsify the guard, self-review, and update the row. Use when implementing an approved task, resuming a plan part-way through, or when the user names a plan id or task number.
---

# ep-implement

Executes exactly one task row: one concern, one commit's worth of change, verified and
falsified. The dominant defect class in this repository is a mechanism that is
implemented, unit-tested, and green while the production path is not wired to it or
bypasses its guard, so this workflow ends by proving the test fails when the defect
returns.

Full loop: `.agents/rules/workflow.md` §3. Coding rules: `.agents/rules/rust-style.md`.

## When to Use

- A plan row is approved and ready to build
- The user names a plan id and task number (`m7-c T2`)
- Work on a plan resumes in a new session
- A trivial single-concern change with an obvious landing point (no plan file needed —
  the steps below still apply from Step 2)

## Step 1: Load the row

Read the plan file. Confirm the user approved the plan, the row's dependencies are `已落`,
and the row does not depend on a 待决 item. If any of those fails, **stop and say which**.

Restate the row's 生产入口, 测试, and 验收命令 before touching code. If the row lacks
them, it was not planned — go back to `ep-plan` rather than inventing them now.

## Step 2: Read context

The target module, its callers, its tests, the nearest `mod.rs` re-exports, related
domain types, config constants, serialization. Search for an existing helper, fixture, or
domain type before writing one, and name the search.

**Then write in the module's style, not yours.** Precedence when the module and the rules
disagree is scoped — see `AGENTS.md` §2. Surface the conflict; never pick a side
silently.

## Step 3: Implement the smallest complete change

Tied to the row's behavior. No opportunistic refactor, dependency bump, broad
formatting, or unrelated cleanup. Small still means complete: every caller, test,
serialization path, and validation path updated in the same change.

Mid-work, the alignment gate still applies. **Stop and align** on: a new module or crate,
a new trait or type-level abstraction, a change to a persisted layout, a new dependency,
a new cross-crate edge, two defensible designs, or unspecified behavior. Report
afterwards (do not ask first) on: a function over ~60 lines, more than ~5 files, a
changed public API signature, a new lock, an existing test modified rather than added to.

If the implementation starts feeling complicated, that is a design problem surfacing
late. Stop, even mid-change.

## Step 4: Verify narrow to broad

```bash
cargo fmt --all -- --check
cargo check -p <crate> --all-targets
cargo test -p <crate> <test_name>
cargo clippy -p <crate> --all-targets --no-deps -- -D warnings --allow clippy::uninlined-format-args
cargo xtask gate
```

Widen to the scope actually touched. If a command could not run: name it, say why,
describe the narrower validation that passed, and do not claim the change is verified.

## Step 5: Falsify the guard

For every guard, validation, or fix in this row:

1. Re-introduce the defect (invert the condition, restore the bypassed call, drop the
   parameter read).
2. Run the row's 验收命令. **The test must go red.** If it stays green, the test is not
   covering the behavior — fix the test, not the record.
3. Restore, re-run, confirm green.
4. Write the result into the row's 反证记录: which mutation, which test went red.

A test that reaches the safe internal method while production calls the unsafe one proves
nothing. Enter at the row's 生产入口.

## Step 6: Self-review

Run the checklist in `.agents/rules/workflow.md` §7 — placement, duplication, deletion,
wiring, falsification, traceability, surface, terminology, TODO discipline, scope. A "no"
without justification means the change is not done. Fix findings before reporting.

## Step 7: Update the row and report

Set the row's status (`已落` once the commit exists, otherwise `进行中`) and fill 反证记录.

Report falsifiably: what changed and why, every command run and its result, compatibility
impact, anything skipped, follow-ups, and any "report afterwards" item from Step 3. No
"should work", no "looks good", no coverage claim without a test name.

## Rules

- One concern per change. Never one giant diff across rows.
- Refactor debt is declared, not squeezed in. Finish or checkpoint the row, then propose
  the refactor as a separate task. Exception: trivial renames fully inside files the row
  already touches.
- **Stop after two attempts.** If an approach still fights the structure — needs a
  layering violation, a wide `pub` opening, or copy-paste — report the friction instead of
  forcing a third attempt.
- New behavior gets tests; a bug fix gets a regression test entering at the production
  path.
- `debug_assert!` is not a check on a path that returns data to a caller.

## Do Not

- Do not write code for an undecided design point, or for a "we'll need it later" case.
- Do not mark a row `已落` with failing tests, a partial implementation, or an unresolved
  error.
- Do not record a 反证记录 that was not measured.
- Do not run `git commit`, `push`, `rebase`, or `reset` here — commits go through
  `ep-commit`, and the git guard hook refuses the rest.

## Related

- The row came from → `ep-plan`
- Review the diff → `ep-review`
- Commit it → `ep-commit`
- Write it back → `ep-doc-sync`

## Checklist

- [ ] Row loaded; plan approved; dependencies `已落`; no 待决 dependency
- [ ] Context read; existing-helper search named
- [ ] Change is the smallest complete one; alignment gate respected
- [ ] Narrow-to-broad validation run, results recorded
- [ ] `cargo xtask gate` passes
- [ ] Guard falsified: mutation named, test confirmed red, restored green
- [ ] 反证记录 filled with the measured result
- [ ] Self-review checklist answered
- [ ] Row status updated; report states commands and results, and any crossed threshold
