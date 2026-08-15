---
name: ep-commit
description: Commit one concern from an epochIO work order using the project's message convention, after the repository gate passes and the user confirms. Use when the user asks to commit, or when a verified task row is ready to land.
---

# ep-commit

Lands one concern as one commit. There is no issue or PR flow here — the commit sequence
and the write-back into the design docs are the whole record, so the message has to carry
why the change exists.

Full loop: `.agents/rules/workflow.md` §5.

## When to Use

- The user asks to commit
- A task row is implemented, verified, and falsified, and the user wants it landed

## Step 1: Confirm the user asked

**Ask for confirmation before every commit, even if earlier conversations contained
approvals** (`AGENTS.md` §1). An approval for the previous commit is not an approval for
this one.

## Step 2: Confirm it is one concern

| Working tree | Action |
| --- | --- |
| One task row's files | proceed |
| Several rows mixed | commit the rows separately, oldest dependency first |
| Unrelated edits present | leave them unstaged and say which files were excluded |

## Step 3: Stage only this concern

```bash
git status --short
git diff
git add <explicit paths>
```

Explicit paths, never `git add -A` while unrelated edits exist. Confirm with
`git diff --cached --stat` that the staged set is exactly the concern.

## Step 4: Compose the message

```
<type>(<scope>): <description>

<why the change exists; the design section; the plan row>
```

| Field | Values |
| --- | --- |
| `type` | `feat` · `fix` · `docs` · `style` · `refactor` · `perf` · `test` · `chore` |
| `scope` | `pd` · `meta` · `store` · `gateway` · `ec` · `rpc` · `client` · `node` · `proto` · `worker` · `rocks` · `telemetry` · `xtask`; the plan id when a row genuinely spans crates |
| subject | English, imperative, no trailing period |
| body | why, the design section in short form (`02 §1.6`), and the plan row (`plan: m7-c T2`) |

The body states what a reader in six months needs: why this, why not the obvious
alternative, what compatibility or rollout it implies. It must not claim validation that
was not run.

## Step 5: Show it and wait

Print the full title and body. Wait for approval, then commit with a heredoc so backticks
and newlines survive:

```bash
git commit -F - <<'MSG'
fix(store): make delete the only tombstone entry point

...
MSG
```

The commit-gate hook runs `cargo fmt --check`, `cargo xtask gate`, and the staged crates'
`cargo test --lib --bins` and `cargo clippy -D warnings`. A denial is the gate working —
read the reason, fix the cause, do not work around it.

## Step 6: Update the row

Set the plan row's status to `已落`. When the work order is complete, hand off to
`ep-doc-sync`.

## Rules

- One concern per commit. A commit that needs "and" in its subject is two commits.
- The tree must be shippable at every commit: it compiles, tests pass, no row depends on a
  later one.
- No tool watermarks, no co-author trailers unless the user asks.
- English subject and body, matching `AGENTS.md` §1.

## Do Not

- Do not run `git push`, `git rebase`, `git reset`, or `git commit --amend` — those are
  the user's, and the git guard hook refuses them.
- Do not `git add -A` when unrelated edits are present.
- Do not commit to work around a failing gate; fix what it reports.
- Do not commit design docs or plans — they live in a gitignored directory, and their
  durable record is the write-back.

## Related

- The row being landed → `ep-implement`
- Review before landing → `ep-review`
- Write the work order back → `ep-doc-sync`

## Checklist

- [ ] User confirmed this commit
- [ ] Staged set is exactly one concern; excluded files named
- [ ] Message follows `<type>(<scope>): <description>` with a why-carrying body
- [ ] Design section and plan row cited
- [ ] Title and body shown and approved before committing
- [ ] Gate passed (or its denial fixed at the cause)
- [ ] Plan row set to `已落`
