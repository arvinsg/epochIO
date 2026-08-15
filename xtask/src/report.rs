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

//! Uniform reporting for the repository checks, so every check prints and exits
//! the same way and `gate` can aggregate them.

use std::process::ExitCode;

/// What a check examined and what it found.
///
/// A tool-level failure (unreadable file, malformed manifest) is an `Err` from
/// the check instead: an empty `violations` list means the repository is clean,
/// never that the check could not run.
pub struct Outcome {
    checked: usize,
    /// Plural noun naming what `checked` counts — "files", "crates", "skills".
    unit: &'static str,
    violations: Vec<String>,
}

impl Outcome {
    pub fn new(checked: usize, unit: &'static str, violations: Vec<String>) -> Self {
        Self {
            checked,
            unit,
            violations,
        }
    }

    pub fn violations(&self) -> &[String] {
        &self.violations
    }

    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Prints `outcome` under the command's name and maps it to an exit code.
pub fn report(command: &str, outcome: Result<Outcome, String>) -> ExitCode {
    match outcome {
        Ok(o) if o.is_clean() => {
            println!("xtask {command}: OK ({} {} checked)", o.checked, o.unit);
            ExitCode::SUCCESS
        }
        Ok(o) => {
            eprintln!(
                "xtask {command}: {} violation(s) found:",
                o.violations.len()
            );
            for v in &o.violations {
                eprintln!("  - {v}");
            }
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("xtask {command}: error: {e}");
            ExitCode::FAILURE
        }
    }
}
