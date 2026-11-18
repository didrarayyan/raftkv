//! Deterministic replicated key-value state machine.
//!
//! Role: applies committed Raft payloads into an ordered key-value map (`BTreeMap`),
//! provides exactly-once write deduplication via bounded client sessions, and
//! encodes and restores point-in-time snapshots.
//!
//! Invariants:
//! - State transitions are completely deterministic and identical across replicas.
//! - Session eviction at capacity (4096) selects the lowest last-applied log index.
//! - Input size violations return typed errors rather than panicking.
//!
//! Deliberately omitted:
//! - Any disk I/O, networking, leases, TTLs, watches, or multi-key transactions.

#![forbid(unsafe_code)]

mod codec;
mod store;

pub use codec::{
    Command, CommandDecodeError, MAX_KEY_BYTES, MAX_SESSIONS, MAX_VALUE_BYTES, Op, OpResult,
};
pub use store::{Applied, ApplyError, KvStore, RestoreError};
