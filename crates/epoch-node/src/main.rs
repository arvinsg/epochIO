//! epochio: the single service binary — `epochio --role pd|meta|data`, plus
//! `epochio dev` to bring up a local cluster for development (M4).
//!
//! Design: docs/design/06-code-layout.md §12; docs/design/00-overview.md §3
//!
//! `epochio --role data --config <file> --node <id>` assembles the storage
//! engine and serves the data plane until Ctrl-C; `--role pd --node <id>` runs
//! one PD replica (raft peer + control plane). `epochio dev --config <file>`
//! spawns every `[[pdnode]]` and `[[node]]` in the config as child processes and
//! supervises them until Ctrl-C.

use std::process::ExitCode;

use epoch_node::{ClusterConfig, roles, shutdown};
use epoch_proto::NodeId;
use epoch_telemetry::logging::{self, LogFormat};

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(e) = logging::init(LogFormat::Pretty, "info") {
        eprintln!("failed to initialize logging: {e}");
        return ExitCode::FAILURE;
    }

    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "dev") {
        return match run_dev(&args).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(msg) => {
                eprintln!("{msg}");
                ExitCode::FAILURE
            }
        };
    }

    let Some(role) = parse_role(&args) else {
        eprintln!(
            "usage: epochio --role <pd|meta|data> [--config <file>] [--node <id>]  |  epochio dev --config <file>"
        );
        return ExitCode::FAILURE;
    };

    let result = match role.as_str() {
        "data" => run_data(&args).await,
        "pd" => run_pd(&args).await,
        "meta" => run_meta(&args).await,
        "gateway" => run_gateway(&args).await,
        other => Err(format!("role {other:?} is not implemented yet")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("{msg}");
            ExitCode::FAILURE
        }
    }
}

/// Loads the config and runs the data role for `--node <id>` until Ctrl-C.
async fn run_data(args: &[String]) -> Result<(), String> {
    let config = load_config(args)?;
    let node_id: u32 = flag(args, "--node")
        .ok_or("--node <id> is required for the data role")?
        .parse()
        .map_err(|_| "--node must be a u32".to_string())?;
    roles::data::run(&config, NodeId::new(node_id), shutdown::ctrl_c())
        .await
        .map_err(|e| format!("data role: {e}"))
}

/// Loads the config and runs one PD replica for `--node <id>` until Ctrl-C.
async fn run_pd(args: &[String]) -> Result<(), String> {
    let config = load_config(args)?;
    let node_id: u64 = flag(args, "--node")
        .ok_or("--node <id> is required for the pd role")?
        .parse()
        .map_err(|_| "--node must be a u64".to_string())?;
    roles::pd::run(&config, node_id, shutdown::ctrl_c())
        .await
        .map_err(|e| format!("pd role: {e}"))
}

/// Loads the config and runs one MetaNode for `--node <id>` until Ctrl-C.
async fn run_meta(args: &[String]) -> Result<(), String> {
    let config = load_config(args)?;
    let node_id: u32 = flag(args, "--node")
        .ok_or("--node <id> is required for the meta role")?
        .parse()
        .map_err(|_| "--node must be a u32".to_string())?;
    roles::meta::run(&config, NodeId::new(node_id), shutdown::ctrl_c())
        .await
        .map_err(|e| format!("meta role: {e}"))
}

/// Loads the config and runs one S3 gateway on `--s3-addr <host:port>` until
/// Ctrl-C (default `127.0.0.1:9000`).
async fn run_gateway(args: &[String]) -> Result<(), String> {
    let config = load_config(args)?;
    let s3_addr = flag(args, "--s3-addr")
        .unwrap_or("127.0.0.1:9000")
        .parse()
        .map_err(|_| "--s3-addr must be host:port".to_string())?;
    roles::gateway::run(&config, s3_addr, shutdown::ctrl_c())
        .await
        .map_err(|e| format!("gateway role: {e}"))
}

/// Spawns every PD replica and data node in the config as child processes
/// (`epochio --role ... --config <same file> --node <id>`), then waits for
/// Ctrl-C and stops the children. This is the development bring-up (M4).
async fn run_dev(args: &[String]) -> Result<(), String> {
    let config_path = flag(args, "--config")
        .ok_or("--config <file> is required for dev")?
        .to_string();
    let text = std::fs::read_to_string(&config_path).map_err(|e| format!("read config: {e}"))?;
    let config = ClusterConfig::parse(&text).map_err(|e| format!("config: {e}"))?;

    let exe = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
    let mut children = Vec::new();
    let mut spawn = |role: &str, id: u64| -> Result<(), String> {
        let child = std::process::Command::new(&exe)
            .args([
                "--role",
                role,
                "--config",
                &config_path,
                "--node",
                &id.to_string(),
            ])
            .spawn()
            .map_err(|e| format!("spawn {role} {id}: {e}"))?;
        children.push(child);
        Ok(())
    };
    for spec in &config.pdnodes {
        spawn("pd", spec.id)?;
    }
    for spec in &config.nodes {
        spawn("data", u64::from(spec.id))?;
        // Each node also hosts a MetaNode (M5): the meta role reuses the
        // `[[node]]` spec (its own listen addr is derived there).
        spawn("meta", u64::from(spec.id))?;
    }
    // One S3 gateway on the default S3 port (M6). It registers with PD and
    // discovers the cluster; a single gateway suffices for the dev MVP.
    {
        let child = std::process::Command::new(&exe)
            .args(["--role", "gateway", "--config", &config_path])
            .spawn()
            .map_err(|e| format!("spawn gateway: {e}"))?;
        children.push(child);
    }
    tracing::info!(children = children.len(), "dev cluster up; Ctrl-C to stop");

    shutdown::ctrl_c().await;
    tracing::info!("stopping dev cluster");
    for child in &mut children {
        let _ = child.kill();
        let _ = child.wait();
    }
    Ok(())
}

/// Reads and parses `--config <file>`.
fn load_config(args: &[String]) -> Result<ClusterConfig, String> {
    let path = flag(args, "--config").ok_or("--config <file> is required")?;
    let text = std::fs::read_to_string(path).map_err(|e| format!("read config: {e}"))?;
    ClusterConfig::parse(&text).map_err(|e| format!("config: {e}"))
}

/// Returns the value following `name` in `args`, if present.
fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next().map(String::as_str);
        }
    }
    None
}

/// Extracts and validates the `--role` argument, accepting only
/// `pd`/`meta`/`data`/`gateway`.
fn parse_role(args: &[String]) -> Option<String> {
    flag(args, "--role")
        .filter(|v| matches!(*v, "pd" | "meta" | "data" | "gateway"))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_valid_roles() {
        for role in ["pd", "meta", "data", "gateway"] {
            assert_eq!(
                parse_role(&args(&["epochio", "--role", role])).as_deref(),
                Some(role)
            );
        }
    }

    #[test]
    fn rejects_unknown_or_missing_role() {
        assert_eq!(parse_role(&args(&["epochio"])), None);
        assert_eq!(parse_role(&args(&["epochio", "--role"])), None);
        assert_eq!(parse_role(&args(&["epochio", "--role", "bogus"])), None);
    }

    #[test]
    fn flag_reads_the_following_value() {
        let a = args(&["epochio", "--config", "cfg.toml", "--node", "2"]);
        assert_eq!(flag(&a, "--config"), Some("cfg.toml"));
        assert_eq!(flag(&a, "--node"), Some("2"));
        assert_eq!(flag(&a, "--missing"), None);
    }
}
