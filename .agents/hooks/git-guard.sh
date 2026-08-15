#!/usr/bin/env bash
# PreToolUse on Bash — refuse the git mutations AGENTS.md §1 reserves for the user.
#
# Self-filters on the command string instead of a settings-level matcher, because
# that matcher is a prefix match and would miss the chained form
# (`git add -A && git push`).
#
# Two rules keep it from firing on prose. A first version matched `git push`
# anywhere in the command string and denied a `git commit` whose message quoted
# that command in a heredoc — the guard fired on text, not on an action.
#
#   1. Heredoc bodies are data. Everything from the first `<<` marker onward is
#      message text, not commands, so it is cut before matching.
#   2. A git invocation is matched at command position — start of a line, or after
#      a shell separator — so "do not git push" in an echo stays allowed.
#
# `git commit` is NOT denied here: the commit gate checks its quality, and whether
# the user asked for it is a judgement no hook can make.
set -u

payload=$(cat)
cmd=$(printf '%s' "$payload" | jq -r '.tool_input.command // empty')
invocation=${cmd%%<<*}

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

# `git <subcommand>` at command position: line start, or after ; & | ( or a newline.
invokes() {
  printf '%s' "$invocation" |
    grep -qE "(^|[;&|(])[[:space:]]*git[[:space:]]+$1"
}

# History rewrites and remote publishing.
invokes 'push' &&
  deny "git push is the user's call (AGENTS.md §1). Ask them to run it."
invokes 'rebase' &&
  deny "git rebase rewrites history — ask the user to run it (AGENTS.md §1)."
invokes 'reset' &&
  deny "git reset can discard committed work — ask the user (AGENTS.md §1)."
invokes 'commit[[:space:]]+--amend' &&
  deny "git commit --amend rewrites a commit — ask the user (AGENTS.md §1)."

# Commands that throw away uncommitted work. The design docs and plans live in a
# gitignored directory, so these can destroy work git never had a copy of.
invokes 'restore' &&
  deny "git restore discards uncommitted changes. Back up the file and edit it instead."
invokes 'checkout[[:space:]]+--[[:space:]]' &&
  deny "git checkout -- discards uncommitted changes. Back up the file and edit it instead."
invokes 'clean[[:space:]]+-[a-zA-Z]*f' &&
  deny "git clean -f deletes untracked files, including gitignored design docs and plans."

exit 0
