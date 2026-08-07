#!/usr/bin/env bash
# Add Apache 2.0 license headers to all Rust source files in the workspace.
#
# Usage:
#   ./scripts/license-headers.sh          # preview mode: show what would change
#   ./scripts/license-headers.sh --apply  # actually write the headers
set -euo pipefail

APPLY=false
if [[ "${1:-}" == "--apply" ]]; then
    APPLY=true
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

LICENSE_RS=$(cat <<'EOF'
// Copyright 2026 arvinsg
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

EOF
)

LICENSE_TOML=$(cat <<'EOF'
# Copyright 2026 arvinsg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

EOF
)

MARKER="Licensed under the Apache License, Version 2.0"

count=0
skipped=0

add_header() {
    local file="$1"
    local license="$2"

    # Skip if already has the license header
    if grep -qF "$MARKER" "$file"; then
        ((skipped++)) || true
        return
    fi

    if $APPLY; then
        local tmp
        tmp=$(mktemp)
        {
            printf '%s\n\n' "$license"
            cat "$file"
        } > "$tmp"
        mv "$tmp" "$file"
    fi
    ((count++)) || true
}

while IFS= read -r -d '' file; do
    add_header "$file" "$LICENSE_RS"
done < <(find "$ROOT_DIR"/crates "$ROOT_DIR"/xtask -type f -name '*.rs' -print0)

while IFS= read -r -d '' file; do
    add_header "$file" "$LICENSE_TOML"
done < <(find "$ROOT_DIR"/crates "$ROOT_DIR"/xtask -type f -name '*.toml' -print0)

# Also handle root-level TOML files (Cargo.toml, rust-toolchain.toml, clippy.toml, etc.)
while IFS= read -r -d '' file; do
    add_header "$file" "$LICENSE_TOML"
done < <(find "$ROOT_DIR" -maxdepth 1 -type f -name '*.toml' -print0)

if $APPLY; then
    echo "Done. Added license header to $count file(s). Skipped $skipped file(s) (already had header)."
else
    echo "Preview mode: would add license header to $count file(s)."
    echo "Skipped $skipped file(s) (already have header)."
    echo ""
    echo "Run with --apply to actually write headers:"
    echo "  ./scripts/license-headers.sh --apply"
fi