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
//!
//! # Examples
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use bytes::Bytes;
//! use raft_core::{Index, Payload};
//! use raftkv_kv::{Applied, Command, KvStore, Op, OpResult};
//!
//! let mut store = KvStore::new();
//! let cmd = Command {
//!     client_id: 42,
//!     request_id: 1,
//!     op: Op::Put {
//!         key: Bytes::from_static(b"region"),
//!         value: Bytes::from_static(b"us-east"),
//!     },
//! };
//! let payload = Payload::Command(cmd.encode());
//! assert_eq!(store.apply(Index(1), &payload)?, Applied::Done(OpResult::Put));
//! assert_eq!(store.apply(Index(2), &payload)?, Applied::Duplicate(OpResult::Put));
//!
//! let mut restored = KvStore::new();
//! restored.restore(store.applied_index(), &store.snapshot())?;
//! assert_eq!(restored.get(b"region").map(Bytes::as_ref), Some(b"us-east".as_slice()));
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod codec;
mod store;

pub use codec::{
    Command, CommandDecodeError, MAX_KEY_BYTES, MAX_SESSIONS, MAX_VALUE_BYTES, Op, OpResult,
};
pub use store::{Applied, ApplyError, KvStore, RestoreError};
