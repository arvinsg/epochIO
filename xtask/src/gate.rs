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

//! Every repository check in one command, for the commit hook and for CI.
//!
//! A fast local gate, not a substitute for the full test suite: the integration
//! suites bind real addresses and belong in an explicit run.
//!
//! Each check reports on its own so a failure names itself; the command fails if
//! any of them does. A check that could not run counts as a failure — a gate that
//! stays silent when it breaks is worse than no gate.

use std::process::ExitCode;

use crate::report::Outcome;
use crate::{headers, layers, registry};

type Check = (&'static str, fn() -> Result<Outcome, String>);

const CHECKS: &[Check] = &[
    ("layers", layers::check),
    ("headers", headers::check),
    ("registry", registry::check),
];

pub fn run() -> ExitCode {
    let mut failed = Vec::new();

    for (name, check) in CHECKS {
        match check() {
            Ok(outcome) if outcome.is_clean() => println!("xtask gate: {name} OK"),
            Ok(outcome) => {
                eprintln!(
                    "xtask gate: {name} — {} violation(s):",
                    outcome.violations().len()
                );
                for v in outcome.violations() {
                    eprintln!("  - {v}");
                }
                failed.push(*name);
            }
            Err(e) => {
                eprintln!("xtask gate: {name} — error: {e}");
                failed.push(*name);
            }
        }
    }

    if failed.is_empty() {
        println!("xtask gate: OK ({} checks)", CHECKS.len());
        ExitCode::SUCCESS
    } else {
        eprintln!("xtask gate: FAILED ({})", failed.join(", "));
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_check_is_registered_once() {
        let mut names: Vec<&str> = CHECKS.iter().map(|(n, _)| *n).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "a check is registered twice");
        assert_eq!(count, 3, "update this count when adding a check");
    }

    #[test]
    fn the_repository_passes_its_own_gate() {
        for (name, check) in CHECKS {
            let outcome = check().unwrap_or_else(|e| panic!("{name} failed to run: {e}"));
            assert!(outcome.is_clean(), "{name}: {:?}", outcome.violations());
        }
    }
}
