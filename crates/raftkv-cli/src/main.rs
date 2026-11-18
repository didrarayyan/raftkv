//! Command-line tool (`raftkvctl`) for interacting with a `raftkv` cluster.

#![forbid(unsafe_code)]

use std::io::Read;
use std::process;

use clap::{Args, Parser, Subcommand, ValueEnum};
use raftkv_cli::{
    Client, OutputFormat, compute_prefix_end, evaluate_health, format_cas, format_delete,
    format_get, format_health, format_put, format_range, format_status, parse_duration,
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum OutputArg {
    Table,
    Json,
}

impl From<OutputArg> for OutputFormat {
    fn from(arg: OutputArg) -> Self {
        match arg {
            OutputArg::Table => Self::Table,
            OutputArg::Json => Self::Json,
        }
    }
}

#[derive(Debug, Parser, PartialEq, Eq)]
#[command(
    name = "raftkvctl",
    about = "Client CLI for the raftkv distributed KV store"
)]
struct Cli {
    /// Comma-separated cluster `host:port` addresses (falls back to `RAFTKV_ENDPOINTS`).
    #[arg(long, global = true, value_name = "HOST:PORT,...")]
    endpoints: Option<String>,

    /// Output format.
    #[arg(long, global = true, value_enum, default_value_t = OutputArg::Table)]
    output: OutputArg,

    /// Overall client operation timeout (e.g. 500ms, 5s, 1m).
    #[arg(long, global = true, default_value = "5s", value_name = "DURATION")]
    timeout: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
enum Command {
    /// Write a key-value pair (pass '-' as value to read raw bytes from stdin).
    Put {
        key: String,
        #[arg(allow_hyphen_values = true)]
        value: String,
    },
    /// Fetch the value for a key.
    Get {
        key: String,
        /// Allow serving locally from a non-leader without quorum `ReadIndex`.
        #[arg(long)]
        stale: bool,
    },
    /// Delete a key.
    Del { key: String },
    /// Scan a half-open key range [start, end) or prefix.
    Range {
        /// Inclusive start key (conflicts with --prefix).
        #[arg(value_name = "START")]
        start: Option<String>,
        /// Exclusive end key.
        #[arg(value_name = "END")]
        end: Option<String>,
        /// Scan all keys beginning with PREFIX.
        #[arg(long, conflicts_with_all = ["start", "end"], value_name = "PREFIX")]
        prefix: Option<String>,
        /// Maximum number of pairs to return.
        #[arg(long, default_value_t = 1000)]
        limit: u32,
        /// Allow serving locally from a non-leader without quorum `ReadIndex`.
        #[arg(long)]
        stale: bool,
    },
    /// Atomically update a key when it matches --expected or is --absent.
    Cas {
        key: String,
        #[command(flatten)]
        condition: CasCondition,
        #[arg(value_name = "NEW_VALUE")]
        value: String,
    },
    /// Print Raft role, term, commit/applied indices, and peer progress for all endpoints.
    Status,
    /// Check cluster quorum reachability and single-leader agreement (exit 0 healthy, 1 unhealthy).
    Health,
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[group(required = true, multiple = false)]
struct CasCondition {
    /// Required current value for CAS to swap.
    #[arg(long, value_name = "VAL")]
    expected: Option<String>,
    /// Require key to currently be absent.
    #[arg(long)]
    absent: bool,
}

fn single_line(msg: &dyn std::fmt::Display) -> String {
    msg.to_string().replace(['\r', '\n'], " ")
}

fn resolve_endpoints(flag: Option<&str>) -> Result<Vec<String>, String> {
    let raw = match flag {
        Some(v) => v.to_owned(),
        None => std::env::var("RAFTKV_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:7001".to_owned()),
    };
    let eps: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if eps.is_empty() {
        return Err("endpoints list must not be empty".to_owned());
    }
    Ok(eps)
}

fn read_put_value(raw: String) -> Result<Vec<u8>, std::io::Error> {
    if raw == "-" {
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf)?;
        Ok(buf)
    } else {
        Ok(raw.into_bytes())
    }
}

async fn run_command(cli: Cli) -> Result<(String, i32), (String, i32)> {
    let timeout = parse_duration(&cli.timeout).map_err(|e| (e, 2))?;
    let endpoints = resolve_endpoints(cli.endpoints.as_deref()).map_err(|e| (e, 2))?;
    let mut client = Client::connect(endpoints.clone())
        .map_err(|e| (single_line(&e), 2))?
        .with_timeout(timeout);
    let out_fmt: OutputFormat = cli.output.into();

    match cli.command {
        Command::Put { key, value } => {
            let val_bytes = read_put_value(value).map_err(|e| (single_line(&e), 1))?;
            let res = client
                .put(key.into_bytes(), val_bytes)
                .await
                .map_err(|e| (single_line(&e), 1))?;
            Ok((format_put(&res, out_fmt), 0))
        }
        Command::Get { key, stale } => {
            let res = client
                .get(key.into_bytes(), stale)
                .await
                .map_err(|e| (single_line(&e), 1))?;
            Ok((format_get(&res, out_fmt), 0))
        }
        Command::Del { key } => {
            let res = client
                .delete(key.into_bytes())
                .await
                .map_err(|e| (single_line(&e), 1))?;
            Ok((format_delete(&res, out_fmt), 0))
        }
        Command::Range {
            start,
            end,
            prefix,
            limit,
            stale,
        } => {
            let (start_bytes, end_bytes) = if let Some(pfx) = prefix {
                let pfx_bytes = pfx.into_bytes();
                let pfx_end = compute_prefix_end(&pfx_bytes).unwrap_or_default();
                (pfx_bytes, pfx_end)
            } else {
                (
                    start.map_or_default(String::into_bytes),
                    end.map_or_default(String::into_bytes),
                )
            };
            let res = client
                .range(start_bytes, end_bytes, limit, stale)
                .await
                .map_err(|e| (single_line(&e), 1))?;
            Ok((format_range(&res, out_fmt), 0))
        }
        Command::Cas {
            key,
            condition,
            value,
        } => {
            let expected = if condition.absent {
                None
            } else {
                condition.expected.map(|s| s.into_bytes().into())
            };
            let res = client
                .cas(key.into_bytes(), expected, value.into_bytes())
                .await
                .map_err(|e| (single_line(&e), 1))?;
            Ok((format_cas(&res, out_fmt), 0))
        }
        Command::Status => {
            let reports = client.status_all().await;
            let exit_code = i32::from(reports.iter().all(|r| r.status.is_err()));
            Ok((format_status(&reports, out_fmt), exit_code))
        }
        Command::Health => {
            let reports = client.status_all().await;
            match evaluate_health(endpoints.len(), &reports) {
                Ok(summary) => Ok((format_health(&summary, out_fmt), 0)),
                Err(reason) => Err((reason, 1)),
            }
        }
    }
}

fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            let _ = err.print();
            process::exit(2);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to start runtime: {}", single_line(&err));
            process::exit(1);
        }
    };

    match rt.block_on(run_command(cli)) {
        Ok((output, code)) => {
            println!("{output}");
            if code != 0 {
                process::exit(code);
            }
        }
        Err((err, code)) => {
            eprintln!("{err}");
            process::exit(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clap_parses_every_subcommand_and_rejects_conflicting_flags() {
        let put = Cli::try_parse_from(["raftkvctl", "put", "foo", "-"]).expect("parse put stdin");
        assert_eq!(
            put.command,
            Command::Put {
                key: "foo".to_owned(),
                value: "-".to_owned(),
            }
        );

        let get = Cli::try_parse_from([
            "raftkvctl",
            "--endpoints",
            "127.0.0.1:7001,127.0.0.1:7002",
            "--output",
            "json",
            "--timeout",
            "2s",
            "get",
            "k1",
            "--stale",
        ])
        .expect("parse get");
        assert_eq!(get.output, OutputArg::Json);
        assert_eq!(get.timeout, "2s");
        assert_eq!(
            get.command,
            Command::Get {
                key: "k1".to_owned(),
                stale: true,
            }
        );

        let del = Cli::try_parse_from(["raftkvctl", "del", "k1"]).expect("parse del");
        assert_eq!(
            del.command,
            Command::Del {
                key: "k1".to_owned()
            }
        );

        let range = Cli::try_parse_from([
            "raftkvctl",
            "range",
            "--prefix",
            "user/",
            "--limit",
            "50",
            "--stale",
        ])
        .expect("parse range prefix");
        assert_eq!(
            range.command,
            Command::Range {
                start: None,
                end: None,
                prefix: Some("user/".to_owned()),
                limit: 50,
                stale: true,
            }
        );
        assert!(Cli::try_parse_from(["raftkvctl", "range", "a", "--prefix", "b"]).is_err());

        let cas_exp = Cli::try_parse_from(["raftkvctl", "cas", "k", "--expected", "old", "new"])
            .expect("cas");
        assert_eq!(
            cas_exp.command,
            Command::Cas {
                key: "k".to_owned(),
                condition: CasCondition {
                    expected: Some("old".to_owned()),
                    absent: false,
                },
                value: "new".to_owned(),
            }
        );

        let cas_abs =
            Cli::try_parse_from(["raftkvctl", "cas", "k", "--absent", "new"]).expect("cas absent");
        assert_eq!(
            cas_abs.command,
            Command::Cas {
                key: "k".to_owned(),
                condition: CasCondition {
                    expected: None,
                    absent: true,
                },
                value: "new".to_owned(),
            }
        );
        assert!(
            Cli::try_parse_from(["raftkvctl", "cas", "k", "--expected", "a", "--absent", "b"])
                .is_err()
        );
        assert!(Cli::try_parse_from(["raftkvctl", "cas", "k", "new"]).is_err());

        assert_eq!(
            Cli::try_parse_from(["raftkvctl", "status"])
                .expect("status")
                .command,
            Command::Status
        );
        assert_eq!(
            Cli::try_parse_from(["raftkvctl", "health"])
                .expect("health")
                .command,
            Command::Health
        );
    }
}
