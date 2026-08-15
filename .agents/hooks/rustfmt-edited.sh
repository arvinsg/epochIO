#!/usr/bin/env bash
# PostToolUse on Write|Edit — format a single Rust file in place.
# Reads the hook payload on stdin; non-Rust paths exit silently.
set -u

file=$(jq -r '.tool_response.filePath // .tool_input.file_path // empty')

case "$file" in
*.rs) ;;
*) exit 0 ;;
esac

[ -f "$file" ] || exit 0

rustfmt --edition 2024 "$file" 2>/dev/null || true
