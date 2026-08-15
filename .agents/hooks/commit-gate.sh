#!/usr/bin/env bash
# PreToolUse on Bash — block a commit when the repository checks fail.
#
# Self-filters on the command string instead of a settings-level
# `Bash(git commit *)` matcher, because that matcher is a prefix match and would
# miss the common chained form: `git add -A && git commit -m ...`.
#
# Clippy and tests run only for crates with staged .rs changes, following the
# narrow-to-broad rule in .agents/rules/rust-style.md §16. A full-workspace clippy
# on every commit would cost minutes.
#
# Tests are scoped to --lib --bins on purpose: the suites under crates/*/tests/
# bind real addresses and start multi-process clusters, and rust-style §14.2
# forbids depending on fixed ports, so those belong in an explicit run.
#
# This is a fast local gate, NOT a CI substitute.
set -u

payload=$(cat)
cmd=$(printf '%s' "$payload" | jq -r '.tool_input.command // empty')

case "$cmd" in
*"git commit"*) ;;
*) exit 0 ;;
esac

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

deny() {
  jq -n --arg r "$1" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $r
    }
  }'
  exit 0
}

# Cargo's progress lines would otherwise lead the denial message and push the
# actual violations out of view.
strip_progress() {
  grep -vE '^[[:space:]]+(Finished|Running|Compiling|Checking|Blocking|Downloaded|Updating)' || true
}

out=$(cargo fmt --all -- --check 2>&1) ||
  deny "cargo fmt --check failed. Run: cargo fmt --all

$out"

# Capture the status before filtering: a pipeline would report grep's status, and
# a gate whose failure is judged by matching its output text would pass silently
# the day that text changes.
gate_out=$(cargo xtask gate 2>&1)
gate_status=$?
gate_out=$(printf '%s\n' "$gate_out" | strip_progress)
[ "$gate_status" -eq 0 ] ||
  deny "cargo xtask gate failed:

$(printf '%s\n' "$gate_out" | tail -30)"

# Staged .rs paths -> nearest Cargo.toml -> package name.
pkgs=$(git diff --cached --name-only --diff-filter=ACM -- '*.rs' | while read -r f; do
  d=$(dirname "$f")
  while [ "$d" != "." ] && [ "$d" != "/" ]; do
    if [ -f "$d/Cargo.toml" ]; then
      grep -m1 '^name *= *' "$d/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/'
      break
    fi
    d=$(dirname "$d")
  done
done | sort -u)

[ -z "$pkgs" ] && exit 0

for p in $pkgs; do
  out=$(cargo test -p "$p" --lib --bins 2>&1) ||
    deny "cargo test -p $p --lib --bins failed:

$(printf '%s' "$out" | tail -40)"

  out=$(cargo clippy -p "$p" --all-targets --no-deps -- \
    -D warnings --allow clippy::uninlined-format-args 2>&1) ||
    deny "cargo clippy -p $p failed:

$(printf '%s' "$out" | tail -40)"
done

exit 0
