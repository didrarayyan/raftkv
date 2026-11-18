//! Configuration parameters and validation for a Raft node.

use std::collections::BTreeSet;

use crate::error::ConfigError;
use crate::types::NodeId;

/// Static configuration for a Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Local node identifier (non-zero).
    pub id: NodeId,
    /// All voting members including `id` (1 to 7 entries).
    pub peers: Vec<NodeId>,
    /// Baseline election timeout in logical ticks; randomized in `[election_ticks, 2 * election_ticks)`.
    pub election_ticks: u32,
    /// Interval between leader heartbeats in logical ticks.
    pub heartbeat_ticks: u32,
    /// Maximum number of log entries per `AppendEntries` message.
    pub max_append_entries: usize,
    /// Soft byte limit per `AppendEntries` message; at least one entry is always sent when due.
    pub max_append_bytes: usize,
    /// Whether followers run a `PreVote` round before incrementing their term.
    pub pre_vote: bool,
    /// Whether leaders step down when fewer than a quorum of peers respond within `election_ticks`.
    pub check_quorum: bool,
    /// Seed for the deterministic `ChaCha8Rng` driving randomized election timeouts.
    pub seed: u64,
}

impl Config {
    /// Builds a configuration with standard defaults (`10` election ticks, `2` heartbeat ticks,
    /// `64` entries / `1 MiB` per append, `PreVote` and `CheckQuorum` enabled).
    pub fn with_defaults(id: NodeId, peers: Vec<NodeId>, seed: u64) -> Self {
        Self {
            id,
            peers,
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_append_entries: 64,
            max_append_bytes: 1024 * 1024,
            pre_vote: true,
            check_quorum: true,
            seed,
        }
    }

    /// Validates all configuration invariants.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.id.is_valid() {
            return Err(ConfigError::InvalidNodeId);
        }
        if self.peers.is_empty() || self.peers.len() > 7 {
            return Err(ConfigError::InvalidClusterSize(self.peers.len()));
        }
        let mut seen = BTreeSet::new();
        for &peer in &self.peers {
            if !peer.is_valid() {
                return Err(ConfigError::ZeroPeerId);
            }
            if !seen.insert(peer) {
                return Err(ConfigError::DuplicatePeer(peer));
            }
        }
        if !seen.contains(&self.id) {
            return Err(ConfigError::SelfNotInPeers(self.id));
        }
        if self.heartbeat_ticks == 0 || self.election_ticks < self.heartbeat_ticks.saturating_mul(3)
        {
            return Err(ConfigError::InvalidTimers {
                election_ticks: self.election_ticks,
                heartbeat_ticks: self.heartbeat_ticks,
            });
        }
        if self.max_append_entries == 0 || self.max_append_bytes == 0 {
            return Err(ConfigError::ZeroAppendLimit);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_single_and_three_node_configs_pass() {
        let single = Config::with_defaults(NodeId(1), vec![NodeId(1)], 7);
        assert!(single.validate().is_ok());

        let three = Config::with_defaults(NodeId(2), vec![NodeId(1), NodeId(2), NodeId(3)], 7);
        assert!(three.validate().is_ok());
    }

    #[test]
    fn invalid_configs_are_rejected_with_specific_errors() {
        let mut cfg = Config::with_defaults(NodeId(0), vec![NodeId(1)], 1);
        assert_eq!(cfg.validate(), Err(ConfigError::InvalidNodeId));

        cfg = Config::with_defaults(NodeId(1), vec![], 1);
        assert_eq!(cfg.validate(), Err(ConfigError::InvalidClusterSize(0)));

        let eight: Vec<NodeId> = (1..=8).map(NodeId).collect();
        cfg = Config::with_defaults(NodeId(1), eight, 1);
        assert_eq!(cfg.validate(), Err(ConfigError::InvalidClusterSize(8)));

        cfg = Config::with_defaults(NodeId(1), vec![NodeId(1), NodeId(0)], 1);
        assert_eq!(cfg.validate(), Err(ConfigError::ZeroPeerId));

        cfg = Config::with_defaults(NodeId(1), vec![NodeId(1), NodeId(2), NodeId(2)], 1);
        assert_eq!(cfg.validate(), Err(ConfigError::DuplicatePeer(NodeId(2))));

        cfg = Config::with_defaults(NodeId(4), vec![NodeId(1), NodeId(2), NodeId(3)], 1);
        assert_eq!(cfg.validate(), Err(ConfigError::SelfNotInPeers(NodeId(4))));

        cfg = Config::with_defaults(NodeId(1), vec![NodeId(1)], 1);
        cfg.election_ticks = 5;
        cfg.heartbeat_ticks = 2;
        assert_eq!(
            cfg.validate(),
            Err(ConfigError::InvalidTimers {
                election_ticks: 5,
                heartbeat_ticks: 2,
            })
        );

        cfg = Config::with_defaults(NodeId(1), vec![NodeId(1)], 1);
        cfg.max_append_entries = 0;
        assert_eq!(cfg.validate(), Err(ConfigError::ZeroAppendLimit));
    }
}
