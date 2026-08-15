# epochIO agent configuration

Cross-tool AI agent config hub. **`.agents/` is the single canonical source**; other
agent tools read it through symlinks.

## Layout

```text
.agents/
  README.md
  rules/
    rust-style.md      # detailed Rust coding rules (load on demand)
    workflow.md        # PDD loop: gates, plan template, commit convention, write-back
  hooks/               # shell wrappers; the logic lives in `cargo xtask`
  skills/              # ep-* project skills
```

Root `AGENTS.md` is the always-on overview and red lines, and holds the skill registry
between `<!-- SKILLS_TABLE_START -->` / `<!-- SKILLS_TABLE_END -->`. `CLAUDE.md` is a
symlink to it, so Claude Code loads it automatically; Codex and Cursor read `AGENTS.md`
natively. Edit `AGENTS.md`, never `CLAUDE.md`.

## Skill paths

| Tool | Project path | Invocation |
| --- | --- | --- |
| Claude Code | `.claude/skills/` → `.agents/skills/` | `/ep-plan` |
| Cursor | `.cursor/skills/` → `.agents/skills/` | auto-discover |
| Codex | `.agents/skills/` (native) | `/skills` |

Edit skills only under `.agents/skills/ep-*/` — never through a symlink.

```bash
ln -sfn ../.agents/skills .claude/skills
ln -sfn ../.agents/skills .cursor/skills
```

## Rules

Read order for a new work order: `AGENTS.md` (always in context) →
`rules/workflow.md` (the loop) → `rules/rust-style.md` (the details, as needed).

Design documents are the source of truth. `rules/workflow.md` §0 is the single place
that states where they and the plan files live, so relocating them is a one-line change;
everything else refers to them by role. They are gitignored, so the durable record is
the write-back described in `rules/workflow.md` §6.
