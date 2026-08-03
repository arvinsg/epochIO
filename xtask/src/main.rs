//! cargo xtask — repository automation (invoked as `cargo xtask <command>`).
//!
//! Design: docs/design/06-code-layout.md §10

use std::process::ExitCode;

mod layers;

fn main() -> ExitCode {
    let command = std::env::args().nth(1);
    match command.as_deref() {
        Some("layers") => layers::run(),
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
    eprintln!("  layers    verify crate dependency layering (docs/design/06 §0)");
}
