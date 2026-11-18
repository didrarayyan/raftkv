//! Deterministic simulation harness and correctness checkers for `raftkv`.
//!
//! Role: executes `raft-core`, `raftkv-kv`, and `raftkv-wal` (over `FaultFs`) in
//! a single-threaded virtual world with reordered/dropped/duplicated packets,
//! partitions, process crashes with torn writes, and a linearizability checker.
//!
//! Invariants:
//! - Execution is completely reproducible from a single `ChaCha8Rng` seed.
//! - Safety invariants I1 through I8 are verified after every simulation step.
//!
//! Deliberately omitted:
//! - Real wall-clock time, OS sockets, or async runtimes.

#![forbid(unsafe_code)]
