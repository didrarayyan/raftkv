//! Node server runtime and gRPC transport for `raftkv`.
//!
//! Role: wires `raft-core`, `raftkv-kv`, and `raftkv-wal` onto a tokio runtime
//! with a single-owner driver loop, a dedicated WAL OS thread, per-peer gRPC
//! senders, and client-facing `Kv` and `Admin` services.
//!
//! Invariants:
//! - Peer messages and client acknowledgements wait until the corresponding `Ready`
//!   has been synced by the WAL thread.
//! - Any WAL I/O error aborts the process immediately (fail-stop).
//!
//! Deliberately omitted:
//! - Raft consensus rules, KV state machine semantics, TLS, or authentication.

#![forbid(unsafe_code)]
