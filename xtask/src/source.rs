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

//! Source-tree traversal shared by the repository checks.

use std::path::{Path, PathBuf};

/// Directories holding first-party Rust sources. Everything else in the tree
/// (build output, vendored code) is out of scope for the checks.
const SOURCE_DIRS: &[&str] = &["crates", "xtask"];

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask manifest dir always has a parent (the workspace root)")
        .to_path_buf()
}

/// Every first-party `.rs` file, sorted so reports are stable across runs.
pub fn rust_files() -> Result<Vec<PathBuf>, String> {
    let root = workspace_root();
    let mut files = Vec::new();
    for dir in SOURCE_DIRS {
        collect(&root.join(dir), &mut files)?;
    }
    files.sort();
    Ok(files)
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read {}: {e}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            if entry.file_name() == "target" {
                continue;
            }
            collect(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// Workspace-relative form of `path`, for violation messages.
pub fn rel(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .unwrap_or(path)
        .display()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_files_covers_both_source_dirs() {
        let files = rust_files().expect("traversal should succeed");
        assert!(files.iter().any(|p| rel(p).starts_with("crates/")));
        assert!(files.iter().any(|p| rel(p) == "xtask/src/source.rs"));
        assert!(
            files
                .iter()
                .all(|p| p.extension().is_some_and(|e| e == "rs"))
        );
    }

    #[test]
    fn traversal_is_sorted() {
        let files = rust_files().expect("traversal should succeed");
        let mut sorted = files.clone();
        sorted.sort();
        assert_eq!(files, sorted, "reports depend on a stable order");
    }
}
