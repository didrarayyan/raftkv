//! `raft-sim` parallel seed sweep CLI binary.

#![forbid(unsafe_code)]

use std::process::ExitCode;
use std::sync::mpsc;
use std::thread;

use clap::Parser;
use raft_sim::{Sim, SimConfig};

#[derive(Debug, Parser)]
#[command(
    name = "raft-sim",
    about = "Deterministic Raft cluster simulation and invariant sweep"
)]
struct Cli {
    /// Inclusive or half-open seed range in START..END format (e.g. 0..2000).
    #[arg(long, default_value = "0..2000")]
    seeds: String,
    /// Single seed override to reproduce one specific run.
    #[arg(long)]
    seed: Option<u64>,
    /// Cluster size (3 or 5 voting members).
    #[arg(long, default_value_t = 3)]
    nodes: usize,
    /// Virtual ticks per seed run.
    #[arg(long, default_value_t = 20_000)]
    ticks: u64,
}

fn parse_range(spec: &str) -> Result<(u64, u64), String> {
    if let Some((start_s, end_s)) = spec.split_once("..") {
        let start = start_s
            .parse::<u64>()
            .map_err(|_| format!("invalid start seed in '{spec}'"))?;
        let end = end_s
            .parse::<u64>()
            .map_err(|_| format!("invalid end seed in '{spec}'"))?;
        if end <= start {
            return Err(format!("empty seed range '{spec}'"));
        }
        Ok((start, end))
    } else {
        let single = spec
            .parse::<u64>()
            .map_err(|_| format!("invalid seed range '{spec}'"))?;
        Ok((single, single + 1))
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.nodes != 3 && cli.nodes != 5 {
        eprintln!("--nodes must be 3 or 5, got {}", cli.nodes);
        return ExitCode::from(2);
    }

    let (start, end) = if let Some(s) = cli.seed {
        (s, s + 1)
    } else {
        match parse_range(&cli.seeds) {
            Ok(r) => r,
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(2);
            }
        }
    };

    let seed_list: Vec<u64> = (start..end).collect();
    let workers = thread::available_parallelism()
        .map_or(4, std::num::NonZero::get)
        .min(seed_list.len().max(1));

    let (tx, rx) = mpsc::channel();
    let chunk_size = seed_list.len().div_ceil(workers);

    thread::scope(|scope| {
        for chunk in seed_list.chunks(chunk_size) {
            let tx = tx.clone();
            let nodes = cli.nodes;
            let ticks = cli.ticks;
            scope.spawn(move || {
                for &seed in chunk {
                    let mut cfg = SimConfig::standard(seed, nodes);
                    cfg.total_ticks = ticks;
                    let mut sim = Sim::new(cfg);
                    let res = sim.run();
                    let is_err = res.is_err();
                    let _ = tx.send(res);
                    if is_err {
                        break;
                    }
                }
            });
        }
        drop(tx);
    });

    let mut total_ops = 0usize;
    let mut passed = 0usize;
    for res in rx {
        match res {
            Ok(report) => {
                passed += 1;
                total_ops += report.completed_ops;
            }
            Err(violation) => {
                eprintln!(
                    "SIMULATION VIOLATION: seed={} nodes={} tick={} error={}",
                    violation.seed, violation.node_count, violation.tick, violation.message
                );
                eprintln!(
                    "Recent trace (last {} events):",
                    violation.recent_trace.len()
                );
                for line in &violation.recent_trace {
                    eprintln!("  {line}");
                }
                eprintln!(
                    "Reproduce: cargo run -p raft-sim --release -- --seed {} --nodes {}",
                    violation.seed, violation.node_count
                );
                return ExitCode::from(1);
            }
        }
    }

    println!(
        "PASS: {passed} seeds ({start}..{end}), nodes={}, ticks={}, total_ops={total_ops}",
        cli.nodes, cli.ticks
    );
    ExitCode::SUCCESS
}
