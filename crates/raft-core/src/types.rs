//! Core domain newtypes and data structures for `raft-core`.

use std::fmt;

use bytes::Bytes;

/// Unique identifier for a node in the Raft cluster.
///
/// Zero is reserved as a sentinel for "none" on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Returns `true` if this identifier is valid (non-zero).
    pub const fn is_valid(self) -> bool {
        self.0 != 0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonically increasing Raft election term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

impl Term {
    /// Returns the immediately following term.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 1-based position of an entry in the Raft log (`0` means before the first entry).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Index(pub u64);

impl Index {
    /// Returns the immediately following log index.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the preceding log index, saturating at zero.
    #[must_use]
    pub const fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Payload carried by a replicated Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Leader-appended barrier entry used to commit prior-term logs on election.
    Noop,
    /// Opaque state-machine command bytes.
    Command(Bytes),
}

impl Payload {
    /// Approximate encoded payload size used for `max_append_bytes` batching.
    pub fn byte_size(&self) -> usize {
        match self {
            Self::Noop => 16,
            Self::Command(data) => 16 + data.len(),
        }
    }
}

/// A single entry in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Election term in which the entry was created.
    pub term: Term,
    /// 1-based log position of the entry.
    pub index: Index,
    /// Entry payload.
    pub payload: Payload,
}

/// Durable Raft state that must survive process restarts before messages leave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HardState {
    /// Latest term this node has seen.
    pub term: Term,
    /// Candidate that received this node's vote in `term`, if any.
    pub voted_for: Option<NodeId>,
    /// Highest log index known to be committed.
    pub commit: Index,
}

/// Metadata identifying a point-in-time state machine snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotMeta {
    /// Highest log index included in the snapshot.
    pub index: Index,
    /// Election term of the entry at `index`.
    pub term: Term,
}

/// Complete snapshot payload together with its log metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Log position covered by `data`.
    pub meta: SnapshotMeta,
    /// Opaque serialized state machine image.
    pub data: Bytes,
}

/// Role of a Raft node within its current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive replica accepting log entries from a leader.
    Follower,
    /// Node gathering pre-votes without incrementing its durable term.
    PreCandidate,
    /// Node gathering real votes after incrementing its durable term.
    Candidate,
    /// Elected leader driving replication and client proposals.
    Leader,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Follower => f.write_str("follower"),
            Self::PreCandidate => f.write_str("pre_candidate"),
            Self::Candidate => f.write_str("candidate"),
            Self::Leader => f.write_str("leader"),
        }
    }
}

/// Volatile leadership observation emitted when role or leader changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SoftState {
    /// Current leader if known to this node.
    pub leader_id: Option<NodeId>,
    /// Current node role.
    pub role: Role,
}

/// Confirmed linearizable read point emitted once a heartbeat quorum replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadState {
    /// Caller-supplied correlation identifier.
    pub ctx: u64,
    /// Minimum applied index required before serving the read.
    pub index: Index,
}

/// Location assigned to an accepted leader proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposal {
    /// Log index assigned to the proposal.
    pub index: Index,
    /// Leader term in which the proposal was appended.
    pub term: Term,
}

/// Per-peer replication tracking snapshot exposed in [`Status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerStatus {
    /// Identifier of the peer node.
    pub id: NodeId,
    /// Highest log index known to be replicated on this peer.
    pub match_index: Index,
    /// Next log index the leader will attempt to send to this peer.
    pub next_index: Index,
    /// Whether the peer responded since the previous `CheckQuorum` window.
    pub recently_active: bool,
}

/// Observable node status snapshot for diagnostics and admin RPCs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// Local node identifier.
    pub id: NodeId,
    /// Current node role.
    pub role: Role,
    /// Current election term.
    pub term: Term,
    /// Known cluster leader, if any.
    pub leader_id: Option<NodeId>,
    /// Highest committed log index.
    pub commit: Index,
    /// Highest index handed to the driver for state-machine application.
    pub applied: Index,
    /// Last index currently present in the in-memory log or snapshot.
    pub last_log_index: Index,
    /// Index of the latest compacted or installed snapshot.
    pub snapshot_index: Index,
    /// Replication status of remote peers (populated only on the leader).
    pub peers: Vec<PeerStatus>,
}

/// Initial state loaded from durable storage when constructing a node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Recovered {
    /// Recovered durable term, vote, and commit index.
    pub hard_state: HardState,
    /// Latest durable snapshot, if any.
    pub snapshot: Option<Snapshot>,
    /// Contiguous log entries starting immediately after `snapshot.meta.index` (or `1`).
    pub entries: Vec<Entry>,
}
