//! Pure Raft consensus state machine.
//!
//! Role: implements leader election with `PreVote` and `CheckQuorum`, log replication
//! with fast backtracking, `ReadIndex` linearizable reads, and snapshot compaction
//! as a deterministic function of inputs (`tick`, `step`, `propose`, `read_index`).
//!
//! Invariants:
//! - Deterministic across runs given identical configuration, seed, and inputs.
//! - Uses ordered maps (`BTreeMap`) exclusively so iteration order never varies.
//! - Durability contract: callers must persist `Ready` snapshot, entries, and
//!   hard state to stable storage before sending `Ready.messages`, applying
//!   committed entries, or calling `advance()`.
//!
//! Deliberately omitted:
//! - Any I/O, networking, filesystem access, async runtime, or wall-clock reads.
//! - Dynamic cluster membership changes or multi-Raft sharding.

#![forbid(unsafe_code)]
