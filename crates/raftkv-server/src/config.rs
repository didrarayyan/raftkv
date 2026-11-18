//! TOML configuration loading and validation for `raftkvd`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use raft_core::{Config as RaftConfig, ConfigError, NodeId};
use serde::Deserialize;
use thiserror::Error;

/// Default Raft tick interval in milliseconds.
pub const DEFAULT_TICK_MS: u64 = 50;
/// Default baseline election timeout in logical ticks.
pub const DEFAULT_ELECTION_TICKS: u32 = 10;
/// Default leader heartbeat interval in logical ticks.
pub const DEFAULT_HEARTBEAT_TICKS: u32 = 2;
/// Default maximum log entries per `AppendEntries` RPC.
pub const DEFAULT_MAX_APPEND_ENTRIES: usize = 64;
/// Default soft byte cap per `AppendEntries` RPC (1 MiB).
pub const DEFAULT_MAX_APPEND_BYTES: usize = 1024 * 1024;
/// Default applied-entry interval between local state-machine snapshots.
pub const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 10_000;
/// Default minimum log entries retained below a snapshot in the WAL.
pub const DEFAULT_RETAIN_ENTRIES: u64 = 1024;

/// Default maximum client key size in bytes.
pub const DEFAULT_MAX_KEY_BYTES: usize = raftkv_kv::MAX_KEY_BYTES;
/// Default maximum client value size in bytes.
pub const DEFAULT_MAX_VALUE_BYTES: usize = raftkv_kv::MAX_VALUE_BYTES;
/// Default maximum streamed snapshot size in bytes (1 GiB).
pub const DEFAULT_MAX_SNAPSHOT_BYTES: usize = 1024 * 1024 * 1024;
/// Default server-side deadline for client KV and Admin RPCs.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5000;

/// Errors returned while loading or validating [`ServerConfig`].
#[derive(Debug, Error)]
pub enum ServerConfigError {
    /// Failed to read the configuration file from disk.
    #[error("failed to read config file {path}: {source}")]
    Io {
        /// Path that failed to open or read.
        path: PathBuf,
        /// Underlying OS I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Failed to parse TOML syntax or types.
    #[error("invalid TOML config: {0}")]
    Toml(#[from] toml::de::Error),
    /// Server configuration failed semantic validation.
    #[error("invalid config: {0}")]
    Validation(String),
    /// Core Raft configuration validation failed.
    #[error("invalid raft config: {0}")]
    Raft(#[from] ConfigError),
}

/// Top-level daemon configuration deserialized from `raftkvd`'s TOML file.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Local Raft node identifier (must be non-zero).
    pub node_id: u64,
    /// Socket address (`ip:port`) the gRPC server binds to.
    pub listen_addr: String,
    /// Directory storing the node lock file, WAL segments, and snapshots.
    pub data_dir: PathBuf,
    /// Static cluster membership definition.
    pub cluster: ClusterConfig,
    /// Optional Raft protocol timing and batching overrides.
    #[serde(default)]
    pub raft: RaftSettings,
    /// Optional size and timeout limits.
    #[serde(default)]
    pub limits: LimitSettings,
}

/// Static peer list under `[cluster]`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// Voting members of the cluster (1 to 7 entries).
    pub peers: Vec<PeerEndpoint>,
}

/// Mapping from a cluster member ID to its advertised gRPC endpoint (`host:port`).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PeerEndpoint {
    /// Non-zero Raft node identifier.
    pub id: u64,
    /// Reachable `host:port` gRPC address for this node.
    pub addr: String,
}

/// Optional Raft parameters under `[raft]`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct RaftSettings {
    /// Logical clock tick interval in milliseconds.
    pub tick_ms: u64,
    /// Baseline election timeout in logical ticks.
    pub election_ticks: u32,
    /// Heartbeat period in logical ticks.
    pub heartbeat_ticks: u32,
    /// Maximum entries included in a single `AppendEntries`.
    pub max_append_entries: usize,
    /// Soft byte cap for a single `AppendEntries`.
    pub max_append_bytes: usize,
    /// Applied entry threshold after which the driver writes a snapshot.
    pub snapshot_threshold: u64,
    /// Trailing entries retained below a snapshot before pruning old WAL segments.
    pub retain_entries: u64,
}

impl Default for RaftSettings {
    fn default() -> Self {
        Self {
            tick_ms: DEFAULT_TICK_MS,
            election_ticks: DEFAULT_ELECTION_TICKS,
            heartbeat_ticks: DEFAULT_HEARTBEAT_TICKS,
            max_append_entries: DEFAULT_MAX_APPEND_ENTRIES,
            max_append_bytes: DEFAULT_MAX_APPEND_BYTES,
            snapshot_threshold: DEFAULT_SNAPSHOT_THRESHOLD,
            retain_entries: DEFAULT_RETAIN_ENTRIES,
        }
    }
}

/// Optional server request and payload bounds under `[limits]`.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct LimitSettings {
    /// Maximum allowed key byte length (`1..=1024`).
    pub max_key_bytes: usize,
    /// Maximum allowed value byte length (`0..=1 MiB`).
    pub max_value_bytes: usize,
    /// Maximum allowed incoming `InstallSnapshot` stream payload in bytes.
    pub max_snapshot_bytes: usize,
    /// Server-side deadline in milliseconds for client requests.
    pub request_timeout_ms: u64,
}

impl Default for LimitSettings {
    fn default() -> Self {
        Self {
            max_key_bytes: DEFAULT_MAX_KEY_BYTES,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT_BYTES,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
        }
    }
}

impl ServerConfig {
    /// Reads and validates a configuration file at `path`.
    pub fn load(path: &Path) -> Result<Self, ServerConfigError> {
        let raw = fs::read_to_string(path).map_err(|source| ServerConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_str(&raw)
    }

    /// Parses and validates a TOML configuration string.
    pub fn from_toml_str(raw: &str) -> Result<Self, ServerConfigError> {
        let cfg: Self = toml::from_str(raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validates all address, membership, Raft, and resource limit invariants.
    pub fn validate(&self) -> Result<(), ServerConfigError> {
        self.listen_socket_addr()?;
        if self.data_dir.as_os_str().is_empty() {
            return Err(ServerConfigError::Validation(
                "data_dir must not be empty".to_owned(),
            ));
        }
        if self.raft.tick_ms == 0 {
            return Err(ServerConfigError::Validation(
                "raft.tick_ms must be > 0".to_owned(),
            ));
        }
        if self.raft.snapshot_threshold == 0 {
            return Err(ServerConfigError::Validation(
                "raft.snapshot_threshold must be > 0".to_owned(),
            ));
        }
        if self.limits.max_key_bytes == 0 || self.limits.max_key_bytes > raftkv_kv::MAX_KEY_BYTES {
            return Err(ServerConfigError::Validation(format!(
                "limits.max_key_bytes must be in 1..={}",
                raftkv_kv::MAX_KEY_BYTES
            )));
        }
        if self.limits.max_value_bytes == 0
            || self.limits.max_value_bytes > raftkv_kv::MAX_VALUE_BYTES
        {
            return Err(ServerConfigError::Validation(format!(
                "limits.max_value_bytes must be in 1..={}",
                raftkv_kv::MAX_VALUE_BYTES
            )));
        }
        if self.limits.max_snapshot_bytes == 0 {
            return Err(ServerConfigError::Validation(
                "limits.max_snapshot_bytes must be > 0".to_owned(),
            ));
        }
        if self.limits.request_timeout_ms == 0 {
            return Err(ServerConfigError::Validation(
                "limits.request_timeout_ms must be > 0".to_owned(),
            ));
        }

        let mut seen_ids = BTreeSet::new();
        for peer in &self.cluster.peers {
            if peer.id == 0 {
                return Err(ServerConfigError::Validation(
                    "peer id must be non-zero".to_owned(),
                ));
            }
            if !seen_ids.insert(peer.id) {
                return Err(ServerConfigError::Validation(format!(
                    "duplicate peer id {}",
                    peer.id
                )));
            }
            if peer.addr.trim().is_empty()
                || peer.addr.contains("://")
                || peer.addr.contains('/')
                || format!("http://{}", peer.addr)
                    .parse::<tonic::transport::Uri>()
                    .is_err()
            {
                return Err(ServerConfigError::Validation(format!(
                    "invalid peer address '{}' for node {}",
                    peer.addr, peer.id
                )));
            }
        }

        self.to_raft_config(0).validate()?;
        Ok(())
    }

    /// Parses [`Self::listen_addr`] into a concrete socket address.
    pub fn listen_socket_addr(&self) -> Result<SocketAddr, ServerConfigError> {
        self.listen_addr.parse::<SocketAddr>().map_err(|err| {
            ServerConfigError::Validation(format!(
                "invalid listen_addr '{}': {err}",
                self.listen_addr
            ))
        })
    }

    /// Returns the local node ID as [`raft_core::NodeId`].
    pub const fn local_node_id(&self) -> NodeId {
        NodeId(self.node_id)
    }

    /// Returns a mapping from every cluster member ID to its advertised `host:port` address.
    pub fn peer_map(&self) -> BTreeMap<NodeId, String> {
        self.cluster
            .peers
            .iter()
            .map(|p| (NodeId(p.id), p.addr.clone()))
            .collect()
    }

    /// Builds a [`raft_core::Config`] seeded with `seed`.
    pub fn to_raft_config(&self, seed: u64) -> RaftConfig {
        RaftConfig {
            id: NodeId(self.node_id),
            peers: self.cluster.peers.iter().map(|p| NodeId(p.id)).collect(),
            election_ticks: self.raft.election_ticks,
            heartbeat_ticks: self.raft.heartbeat_ticks,
            max_append_entries: self.raft.max_append_entries,
            max_append_bytes: self.raft.max_append_bytes,
            pre_vote: true,
            check_quorum: true,
            seed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config_with_defaults() {
        let toml = r#"
            node_id = 1
            listen_addr = "127.0.0.1:7001"
            data_dir = "./data-1"

            [cluster]
            peers = [
              { id = 1, addr = "127.0.0.1:7001" },
              { id = 2, addr = "127.0.0.1:7002" },
              { id = 3, addr = "127.0.0.1:7003" },
            ]
        "#;
        let cfg = ServerConfig::from_toml_str(toml).expect("valid config");
        assert_eq!(cfg.raft.tick_ms, 50);
        assert_eq!(cfg.raft.election_ticks, 10);
        assert_eq!(cfg.raft.heartbeat_ticks, 2);
        assert_eq!(cfg.raft.snapshot_threshold, 10_000);
        assert_eq!(cfg.limits.request_timeout_ms, 5000);
    }

    #[test]
    fn rejects_unknown_keys_and_invalid_values() {
        let unknown = r#"
            node_id = 1
            listen_addr = "127.0.0.1:7001"
            data_dir = "./d"
            bogus = true
            [cluster]
            peers = [{ id = 1, addr = "127.0.0.1:7001" }]
        "#;
        assert!(ServerConfig::from_toml_str(unknown).is_err());

        let zero_tick = r#"
            node_id = 1
            listen_addr = "127.0.0.1:7001"
            data_dir = "./d"
            [cluster]
            peers = [{ id = 1, addr = "127.0.0.1:7001" }]
            [raft]
            tick_ms = 0
        "#;
        assert!(ServerConfig::from_toml_str(zero_tick).is_err());
    }
}
