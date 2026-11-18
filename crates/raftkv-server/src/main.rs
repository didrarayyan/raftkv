//! Entry point for the `raftkvd` Raft key-value daemon.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process;

use clap::Parser;
use raftkv_server::config::ServerConfig;
use raftkv_server::run_with_shutdown;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "raftkvd", about = "Replicated Raft key-value store daemon")]
struct Cli {
    /// Path to the node TOML configuration file.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

fn single_line(err: &dyn std::fmt::Display) -> String {
    err.to_string().replace(['\r', '\n'], " ")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut term_sig) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term_sig.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            eprintln!("invalid arguments: {}", single_line(&err));
            process::exit(2);
        }
    };

    let cfg = match ServerConfig::load(&cli.config) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("configuration error: {}", single_line(&err));
            process::exit(2);
        }
    };

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            tracing::error!(error = %err, "failed to initialize Tokio runtime");
            process::exit(1);
        }
    };

    if let Err(err) = runtime.block_on(run_with_shutdown(cfg, shutdown_signal())) {
        tracing::error!(error = %err, "raftkvd terminated with error");
        process::exit(1);
    }
}
