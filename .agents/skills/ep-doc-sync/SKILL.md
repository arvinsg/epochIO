---
name: ep-doc-sync
description: Write a finished epochIO work order back into the design docs — the iteration plan's 实际交付 block, the open-questions 已决 tables, and the owning design sections. Use when a work order is complete, when decisions were made during implementation, or when the user asks to sync or write back the docs.
---

# ep-doc-sync

The design docs are the source of truth and they live in a gitignored directory, so this
write-back is the durable record of what shipped. It runs once per work order, not per
commit.

A claim the code does not support is worse than no claim: the iteration plan once recorded
hierarchical LIST as delivered while the S3 compatibility layer had no `list` function at
all, and only a four-way audit caught it.

Full loop: `.agents/rules/workflow.md` §6.

## When to Use

- A work order's rows are all `已落`
- Decisions were made mid-implementation that no 已决 table records
- The change altered behavior a design section describes
- The user asks to write back, sync docs, or close out a milestone segment

## Step 1: Collect what actually shipped

Read the plan file's task table and `git log` for the work order's commits. For each row:
what landed, what was cut, what deviated from the plan and why.

Deviations are the valuable part. A write-back that only restates the plan is a plan, not
a record.

## Step 2: Iteration plan — 实际交付

Append or update the milestone segment's **实际交付** block. Cover:

| Item | Content |
| --- | --- |
| Delivered | what exists now, by module |
| Cut | what was in the plan and is not in the code, and why |
| Deviated | where the implementation differs from the plan, and what forced it |
| Deferred | what moved to a later milestone, named |

State the acceptance evidence in falsifiable form: the command run and its result, or
that it was not run. Never write that something is delivered without having seen it work.

## Step 3: Open questions — 已决

Add a 已决 table for every decision made during the work, each row carrying:

- the question as it was actually faced,
- the conclusion,
- the **落点**: the file and module where it landed, plus the design section.

Then remove any 待决 item this work order settled. A 待决 item that is now decided but
still listed will block the next session at the G1 gate for no reason.

## Step 4: Owning design sections

Update every design section whose described behavior changed — same task, not later. Add
`INVARIANT(design 0X §Y): ...` comments at the enforcing code site for new invariants, each
with at least one test referencing it.

Do not add header anchors to files. Traceability runs doc → code through the 落点 column;
the code side carries short-form citations beside the decisions they explain
(`.agents/rules/rust-style.md` §11).

## Step 5: Verify

```bash
cargo xtask gate
```

Then open every 落点 written in Step 3 and confirm the file and module exist. No check
does this for you.

Then re-read what was written and check it against the code once more. This step is the
one place in the loop where a false claim becomes permanent.

## Rules

- Chinese, matching the design docs. epochIO terminology only.
- Cite the file and module for every 落点. "In the meta crate" is not a landing point.
- Record limitations that remain, not only what works. A known gap written down is a
  future session's starting point; an unwritten one is a rediscovery.
- Keep the vocabulary of the design docs' terminology section; no cubefs/MinIO terms.

## Do Not

- Do not mark a milestone delivered on the strength of unit tests alone when its
  acceptance criterion names an end-to-end scenario.
- Do not delete a 待决 item the work did not actually settle.
- Do not write a 实际交付 block that omits the cuts — that is how the plan and the code
  drift apart.
- Do not reformat or restructure unrelated design sections while here.

## Related

- The plan being closed out → `ep-plan`
- The commits being recorded → `ep-commit`
- Verify the record against the code → `ep-status`

## Checklist

- [ ] Plan table and `git log` read; delivered / cut / deviated / deferred separated
- [ ] 实际交付 block written with falsifiable acceptance evidence
- [ ] 已决 table added, every row carrying a file-and-module 落点
- [ ] Settled 待决 items removed
- [ ] Changed design sections updated in this task
- [ ] New invariants carry `INVARIANT` comments and a test
- [ ] `cargo xtask gate` passes; every 落点 opened and confirmed present
- [ ] Remaining limitations recorded
