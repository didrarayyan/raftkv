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
