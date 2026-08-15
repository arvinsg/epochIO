#!/usr/bin/env bash
# SessionStart — inject the open-question verdict and the current milestone.
#
# AGENTS.md §1 requires checking the open-questions record at task start and
# forbids relying on a cached list, because it changes between sessions. A rule
# only a model can forget is not enforced; this hook is what makes it hold.
#
# The brief stays short on purpose: the 待决 section carries a long rolling
# changelog, and pasting it into every session would bury the one line that
# matters — whether anything is undecided right now.
set -u

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

DESIGN_DIR="draft/design"
OPEN_QUESTIONS="$DESIGN_DIR/99-open-questions.md"
ITERATION_PLAN="$DESIGN_DIR/07-iteration-plan.md"

brief=""
add() { brief="${brief}$1"$'\n'; }

if [ -f "$OPEN_QUESTIONS" ]; then
  # The 待决 block runs from the title to the first 已决 heading.
  pending=$(awk '/^## 已决/{exit} {print}' "$OPEN_QUESTIONS")
  version=$(printf '%s' "$pending" | grep -oE 'v[0-9]+\.[0-9]+' | head -1)
  if printf '%s' "$pending" | grep -q '当前无未决项'; then
    add "开放问题：无未决项（记录至 ${version:-未标注}）。已决表中的决定均为约束。"
  else
    add "开放问题：**有未决项** —— 动手前读 $OPEN_QUESTIONS 的待决段，勿按已决处理："
    add "$(printf '%s' "$pending" | tail -n +2 | head -c 600)"
  fi
else
  add "开放问题：$OPEN_QUESTIONS 不存在 —— 设计文档位置可能已变，见 .agents/rules/workflow.md §0。"
fi

if [ -f "$ITERATION_PLAN" ]; then
  latest=$(grep -n '^\*\*实际交付' "$ITERATION_PLAN" | tail -1 | cut -d: -f2- | cut -c1-120)
  [ -n "$latest" ] && add "最近交付：$latest"
fi

dirty=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
[ "${dirty:-0}" -gt 0 ] && add "工作树：$dirty 个文件未提交。"

jq -n --arg ctx "$brief" '{
  hookSpecificOutput: {
    hookEventName: "SessionStart",
    additionalContext: $ctx
  }
}'
