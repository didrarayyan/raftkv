//! Typed error definitions for `raft-core`.

use thiserror::Error;

use crate::types::{Index, NodeId};

/// Errors returned when validating node configuration or recovered storage state.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Node identifier was zero.
    #[error("node id must be non-zero")]
    InvalidNodeId,
    /// Cluster peer list was empty or exceeded the maximum size of 7.
    #[error("cluster must have between 1 and 7 peers, got {0}")]
    InvalidClusterSize(usize),
    /// A peer identifier in the configuration was zero.
    #[error("peer id must be non-zero")]
    ZeroPeerId,
    /// Duplicate peer identifier found in the peer list.
    #[error("duplicate peer id {0}")]
    DuplicatePeer(NodeId),
    /// Local node identifier was omitted from `peers`.
    #[error("local node id {0} is missing from peers")]
    SelfNotInPeers(NodeId),
    /// Election and heartbeat tick counts violate `election_ticks >= 3 * heartbeat_ticks`.
    #[error(
        "invalid ticks: heartbeat_ticks ({heartbeat_ticks}) must be > 0 and election_ticks ({election_ticks}) >= 3 * heartbeat_ticks"
    )]
    InvalidTimers {
        /// Configured election timeout baseline.
        election_ticks: u32,
        /// Configured leader heartbeat interval.
        heartbeat_ticks: u32,
    },
    /// Batch size limits must be strictly positive.
    #[error("max_append_entries and max_append_bytes must be non-zero")]
    ZeroAppendLimit,
    /// Recovered log entries were not contiguous after the snapshot index.
    #[error("recovered entry at index {actual} expected index {expected}")]
    NonContiguousLog {
        /// Expected next entry index.
        expected: Index,
        /// Actual entry index found in recovered entries.
        actual: Index,
    },
}

/// Error returned when proposing a command to a node that is not the leader.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    /// Node is currently not the cluster leader.
    #[error("node is not the leader")]
    NotLeader {
        /// Known current leader, if one has been observed in this term.
        leader: Option<NodeId>,
    },
}

/// Error returned when requesting a linearizable read index.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// Node is currently not the cluster leader.
    #[error("node is not the leader")]
    NotLeader {
        /// Known current leader, if one has been observed in this term.
        leader: Option<NodeId>,
    },
    /// A pending read with the same correlation context is already registered.
    #[error("duplicate read context")]
    DuplicateContext,
    /// Zero is reserved for messages without a read context.
    #[error("read context must be non-zero")]
    InvalidContext,
}
