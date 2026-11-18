//! Raft protocol message definitions exchanged between peers via the driver.

use crate::types::{Entry, Index, NodeId, Term};

/// Envelope carrying a single Raft protocol RPC or response between two nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Sender node identifier.
    pub from: NodeId,
    /// Recipient node identifier.
    pub to: NodeId,
    /// Sender's current (or proposed, for `PreVote`) election term.
    pub term: Term,
    /// Typed protocol payload.
    pub body: MessageBody,
}

/// Outcome carried by an `AppendEntriesResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendResult {
    /// Follower accepted the append up through `match_index`.
    Accepted {
        /// Highest log index known to match the leader on the follower.
        match_index: Index,
    },
    /// Follower rejected the append due to a term or log mismatch at `prev_index`.
    Rejected {
        /// Suggested index where the leader should retry probing.
        conflict_index: Index,
        /// Term of the conflicting entry on the follower, or `Term(0)` if log is shorter.
        conflict_term: Term,
    },
}

/// Body variants of a [`Message`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageBody {
    /// Non-term-bumping pre-election check sent before becoming `Candidate`.
    PreVote {
        /// Sender's last log index.
        last_index: Index,
        /// Sender's last log term.
        last_term: Term,
    },
    /// Response to [`MessageBody::PreVote`].
    PreVoteResponse {
        /// Whether the voter would grant a real vote without a live leader check blocking it.
        granted: bool,
    },
    /// Leader election vote request.
    RequestVote {
        /// Candidate's last log index.
        last_index: Index,
        /// Candidate's last log term.
        last_term: Term,
    },
    /// Response to [`MessageBody::RequestVote`].
    RequestVoteResponse {
        /// Whether the vote was granted in `Message::term`.
        granted: bool,
    },
    /// Heartbeat or log replication RPC from a leader.
    AppendEntries {
        /// Log index immediately preceding `entries`.
        prev_index: Index,
        /// Term of the entry at `prev_index`.
        prev_term: Term,
        /// Entries to replicate (empty for heartbeats).
        entries: Vec<Entry>,
        /// Leader's current commit index.
        leader_commit: Index,
        /// Optional linearizable read correlation token (`0` if none).
        read_ctx: u64,
    },
    /// Response to [`MessageBody::AppendEntries`].
    AppendEntriesResponse {
        /// Acceptance or conflict hint.
        result: AppendResult,
        /// Echoed `read_ctx` from the triggering `AppendEntries`.
        read_ctx: u64,
    },
}
