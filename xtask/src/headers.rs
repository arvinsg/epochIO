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

//! License-header verification.
//!
//! Every first-party Rust file opens with the Apache-2.0 header. `scripts/`
//! `license-headers.sh --apply` writes them; this check is what makes a missing
//! one fail instead of shipping.

use crate::report::Outcome;
use crate::source;

/// First line of the header. Matching one line is enough to catch the real
/// failure — a new file created without running the script.
const HEADER_FIRST_LINE: &str = "// Copyright 2026 arvinsg";

pub fn run() -> std::process::ExitCode {
    crate::report::report("headers", check())
}

pub fn check() -> Result<Outcome, String> {
    let files = source::rust_files()?;
    let mut violations = Vec::new();

    for file in &files {
        let content =
            std::fs::read_to_string(file).map_err(|e| format!("read {}: {e}", file.display()))?;
        if content.lines().next() != Some(HEADER_FIRST_LINE) {
            violations.push(format!(
                "{}: missing Apache-2.0 header — run scripts/license-headers.sh --apply",
                source::rel(file)
            ));
        }
    }

    Ok(Outcome::new(files.len(), "files", violations))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_source_file_carries_the_header() {
        let outcome = check().expect("check should run");
        assert!(
            outcome.is_clean(),
            "files missing the license header: {:?}",
            outcome.violations()
        );
    }

    #[test]
    fn header_is_matched_on_the_first_line_only() {
        let shifted = format!("\n{HEADER_FIRST_LINE}\n");
        assert_ne!(shifted.lines().next(), Some(HEADER_FIRST_LINE));
    }
}
