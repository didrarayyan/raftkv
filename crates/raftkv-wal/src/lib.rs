//! Durable write-ahead log, hard-state storage, and snapshot persistence.
//!
//! Role: persists Raft log entries, hard state, and snapshots with CRC32
//! checksums, segmented log rotation, atomic snapshot writes, and torn-tail
//! recovery through a minimal filesystem abstraction (`Fs`).
//!
//! Invariants:
//! - Entries are written before hard state in every persisted batch.
//! - Snapshots are synced and renamed atomically before obsolete segments are deleted.
//! - Any I/O error on write or sync permanently poisons the WAL (fail-stop).
//! - Recovery never silently truncates a non-final segment on corruption.
//!
//! Deliberately omitted:
//! - Async runtime integration (`tokio`), direct `std::fs` calls outside `StdFs`,
//!   or continuing after a failed fsync.
//!
//! # Examples
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use bytes::Bytes;
//! use raft_core::{Entry, HardState, Index, Payload, Term};
//! use raftkv_wal::{Batch, StdFs, Wal, WalOptions};
//!
//! let dir = std::env::temp_dir().join(format!("raftkv_wal_doc_{}", std::process::id()));
//! let _ = std::fs::remove_dir_all(&dir);
//! {
//!     let (mut wal, _) = Wal::open(StdFs, &dir, WalOptions::default())?;
//!     let entries = [Entry {
//!         term: Term(1),
//!         index: Index(1),
//!         payload: Payload::Command(Bytes::from_static(b"hello")),
//!     }];
//!     let hs = HardState { term: Term(1), voted_for: None, commit: Index(1) };
//!     wal.persist(Batch { snapshot: None, hard_state: Some(&hs), entries: &entries })?;
//! }
//! let (_wal, recovered) = Wal::open(StdFs, &dir, WalOptions::default())?;
//! assert_eq!(recovered.entries.len(), 1);
//! assert_eq!(recovered.hard_state.commit, Index(1));
//! let _ = std::fs::remove_dir_all(&dir);
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod format;
mod fs;
#[cfg(test)]
mod tests;
mod wal;

#[cfg(any(test, feature = "fault-injection"))]
pub use fs::{FaultFile, FaultFs};
pub use fs::{Fs, FsFile, StdFile, StdFs};
pub use wal::{Batch, Wal, WalError, WalOptions};
