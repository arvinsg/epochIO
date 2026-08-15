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

//! cargo xtask — repository automation (invoked as `cargo xtask <command>`).

use std::process::ExitCode;

mod gate;
mod headers;
mod layers;
mod registry;
mod report;
mod source;

fn main() -> ExitCode {
    let command = std::env::args().nth(1);
    match command.as_deref() {
        Some("layers") => layers::run(),
        Some("headers") => headers::run(),
        Some("registry") => registry::run(),
        Some("gate") => gate::run(),
        Some(other) => {
            eprintln!("xtask: unknown command '{other}'");
            usage();
            ExitCode::FAILURE
        }
        None => {
            usage();
            ExitCode::FAILURE
        }
    }
}

fn usage() {
    eprintln!("usage: cargo xtask <command>");
    eprintln!("commands:");
    eprintln!("  gate       run every check below (commit gate)");
    eprintln!("  layers     verify crate dependency layering (06 §0)");
    eprintln!("  headers    verify the Apache-2.0 header on every source file");
    eprintln!("  registry   verify skill directories and the AGENTS.md registry agree");
}
