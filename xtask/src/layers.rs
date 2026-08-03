//! Dependency-layering verification.
//!
//! Enforces the L0–L4 layering defined in docs/design/06-code-layout.md §0
//! (mirrored in AGENTS.md §9.1): a workspace crate may depend only on crates in
//! a strictly lower layer (same-layer dependencies are forbidden), and
//! `epoch-ec` must stay pure computation (no tokio / I/O crates).

use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Authoritative crate→layer whitelist. Mirrors docs/design/06 §0; adding a new
/// crate to the workspace requires registering it here.
const LAYERS: &[(&str, u8)] = &[
    ("epoch-proto", 0),
    ("epoch-telemetry", 0),
    ("epoch-ec", 1),
    ("epoch-rpc", 1),
    ("epoch-rocks", 1),
    ("epoch-store", 2),
    ("epoch-client", 2),
    ("epoch-pd", 3),
    ("epoch-meta", 3),
    ("epoch-gateway", 3),
    ("epoch-worker", 3),
    ("epoch-node", 4),
];

/// Crates `epoch-ec` must never depend on: async runtimes and I/O libraries.
/// `epoch-ec` is pure computation; I/O is injected via traits (06 §0).
const EC_FORBIDDEN: &[&str] = &[
    "tokio",
    "tokio-util",
    "mio",
    "async-std",
    "smol",
    "rocksdb",
    "hyper",
    "reqwest",
    "tonic",
];

/// Runs the layer check and reports the outcome.
pub fn run() -> ExitCode {
    match check() {
        Ok(violations) if violations.is_empty() => {
            println!("xtask layers: OK ({} crates checked)", LAYERS.len());
            ExitCode::SUCCESS
        }
        Ok(violations) => {
            eprintln!("xtask layers: {} violation(s) found:", violations.len());
            for v in &violations {
                eprintln!("  - {v}");
            }
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("xtask layers: error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Scans every crate manifest and returns the list of layering violations.
///
/// `Err` is reserved for tool-level failures (e.g. unreadable manifests); an
/// empty `Ok` vector means the workspace is clean.
fn check() -> Result<Vec<String>, String> {
    let crates_dir = workspace_root().join("crates");

    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&crates_dir)
        .map_err(|e| format!("read {}: {e}", crates_dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.join("Cargo.toml").is_file())
        .collect();
    dirs.sort();

    let mut violations = Vec::new();
    for dir in dirs {
        let manifest_path = dir.join("Cargo.toml");
        let content = std::fs::read_to_string(&manifest_path)
            .map_err(|e| format!("read {}: {e}", manifest_path.display()))?;
        let manifest: toml::Value = toml::from_str(&content)
            .map_err(|e| format!("parse {}: {e}", manifest_path.display()))?;

        let name = manifest
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| format!("{}: missing package.name", manifest_path.display()))?;

        let Some(self_layer) = layer_of(name) else {
            violations.push(format!(
                "crate '{name}' is not registered in the layer table \
                 (update xtask/src/layers.rs and docs/design/06 §0)"
            ));
            continue;
        };

        for dep in dep_names(&manifest) {
            if let Some(dep_layer) = layer_of(&dep)
                && !dep_allowed(self_layer, dep_layer)
            {
                violations.push(format!(
                    "{name} (L{self_layer}) depends on {dep} (L{dep_layer}): \
                     only strictly-lower layers are allowed (same-layer deps forbidden)"
                ));
            }
            if ec_dep_forbidden(name, &dep) {
                violations.push(format!(
                    "epoch-ec must be pure computation but depends on '{dep}' \
                     (no tokio / I/O crates; 06 §0)"
                ));
            }
        }
    }

    Ok(violations)
}

/// A dependency is allowed only if it sits in a strictly lower layer.
fn dep_allowed(self_layer: u8, dep_layer: u8) -> bool {
    dep_layer < self_layer
}

/// `epoch-ec` (pure computation) must not depend on async runtimes or I/O crates.
fn ec_dep_forbidden(crate_name: &str, dep: &str) -> bool {
    crate_name == "epoch-ec" && EC_FORBIDDEN.contains(&dep)
}

fn layer_of(name: &str) -> Option<u8> {
    LAYERS.iter().find(|(n, _)| *n == name).map(|(_, l)| *l)
}

/// Collects dependency names from the normal, build and dev dependency tables.
///
/// Uses the table key as the crate name; this workspace does not use the
/// `package = "..."` rename form, so the key is authoritative.
fn dep_names(manifest: &toml::Value) -> Vec<String> {
    let mut names = Vec::new();
    for section in ["dependencies", "build-dependencies", "dev-dependencies"] {
        if let Some(table) = manifest.get(section).and_then(|v| v.as_table()) {
            names.extend(table.keys().cloned());
        }
    }
    names
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask manifest dir always has a parent (the workspace root)")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer_table_covers_every_crate() {
        assert_eq!(LAYERS.len(), 12);
        assert_eq!(layer_of("epoch-proto"), Some(0));
        assert_eq!(layer_of("epoch-node"), Some(4));
        assert_eq!(layer_of("not-a-crate"), None);
    }

    #[test]
    fn only_strictly_lower_layers_are_allowed() {
        assert!(dep_allowed(4, 0));
        assert!(dep_allowed(2, 1));
        assert!(
            !dep_allowed(3, 3),
            "L3 crates must not depend on each other"
        );
        assert!(!dep_allowed(1, 2), "upward dependencies are forbidden");
    }

    #[test]
    fn workspace_passes_its_own_layer_check() {
        assert!(
            check().expect("layer check should run").is_empty(),
            "the repository must satisfy its own layering rules"
        );
    }

    #[test]
    fn epoch_ec_forbids_io_crates() {
        assert!(ec_dep_forbidden("epoch-ec", "tokio"));
        assert!(ec_dep_forbidden("epoch-ec", "rocksdb"));
        // Pure-computation deps stay allowed.
        assert!(!ec_dep_forbidden("epoch-ec", "blake3"));
        // The rule targets epoch-ec only.
        assert!(!ec_dep_forbidden("epoch-store", "tokio"));
    }
}
