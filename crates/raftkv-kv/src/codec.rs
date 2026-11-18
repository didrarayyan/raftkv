//! Prost-encoded command and snapshot codecs for `raftkv-kv`.

use bytes::{Bytes, BytesMut};
use prost::Message as _;

/// Maximum allowed key length in bytes (`1 KiB`).
pub const MAX_KEY_BYTES: usize = 1024;
/// Maximum allowed value length in bytes (`1 MiB`).
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// Maximum number of client sessions retained for exactly-once deduplication.
pub const MAX_SESSIONS: usize = 4096;

/// Client write command committed through the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// Unique client identifier for session deduplication.
    pub client_id: u64,
    /// Monotonically increasing per-client request sequence number (`1, 2, ...`).
    pub request_id: u64,
    /// State-machine mutation operation.
    pub op: Op,
}

/// Supported single-key mutation operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Insert or overwrite `key` with `value`.
    Put {
        /// Target key bytes (`1..=1024` bytes).
        key: Bytes,
        /// Value bytes (`0..=1 MiB`).
        value: Bytes,
    },
    /// Remove `key` if present.
    Delete {
        /// Target key bytes (`1..=1024` bytes).
        key: Bytes,
    },
    /// Atomically replace `key` with `value` when current value matches `expected`.
    Cas {
        /// Target key bytes (`1..=1024` bytes).
        key: Bytes,
        /// Expected current value, or `None` to require the key to be absent.
        expected: Option<Bytes>,
        /// New value written if the comparison succeeds.
        value: Bytes,
    },
}

/// Result produced by applying an [`Op`] to the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    /// Result of a [`Op::Put`].
    Put,
    /// Result of a [`Op::Delete`].
    Delete {
        /// Whether `key` existed prior to deletion.
        existed: bool,
    },
    /// Result of a [`Op::Cas`].
    Cas {
        /// Whether the swap succeeded.
        swapped: bool,
        /// Observed value after the CAS operation (`None` if absent).
        current: Option<Bytes>,
    },
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireCommand {
    #[prost(uint64, tag = "1")]
    pub client_id: u64,
    #[prost(uint64, tag = "2")]
    pub request_id: u64,
    #[prost(oneof = "WireOp", tags = "3, 4, 5")]
    pub op: Option<WireOp>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum WireOp {
    #[prost(message, tag = "3")]
    Put(WirePut),
    #[prost(message, tag = "4")]
    Delete(WireDelete),
    #[prost(message, tag = "5")]
    Cas(WireCas),
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WirePut {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub value: Bytes,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireDelete {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireCas {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    #[prost(bytes = "bytes", optional, tag = "2")]
    pub expected: Option<Bytes>,
    #[prost(bytes = "bytes", tag = "3")]
    pub value: Bytes,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireSnapshot {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<WireKvEntry>,
    #[prost(message, repeated, tag = "2")]
    pub sessions: Vec<WireSession>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireKvEntry {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub value: Bytes,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireSession {
    #[prost(uint64, tag = "1")]
    pub client_id: u64,
    #[prost(uint64, tag = "2")]
    pub last_request_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_applied_index: u64,
    #[prost(oneof = "WireOpResult", tags = "4, 5, 6")]
    pub last_result: Option<WireOpResult>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum WireOpResult {
    #[prost(bool, tag = "4")]
    Put(bool),
    #[prost(bool, tag = "5")]
    DeleteExisted(bool),
    #[prost(message, tag = "6")]
    Cas(WireCasResult),
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WireCasResult {
    #[prost(bool, tag = "1")]
    pub swapped: bool,
    #[prost(bytes = "bytes", optional, tag = "2")]
    pub current: Option<Bytes>,
}

impl Command {
    /// Serializes the command into protobuf bytes for inclusion in [`raft_core::Payload::Command`].
    pub fn encode(&self) -> Bytes {
        let wire_op = match &self.op {
            Op::Put { key, value } => WireOp::Put(WirePut {
                key: key.clone(),
                value: value.clone(),
            }),
            Op::Delete { key } => WireOp::Delete(WireDelete { key: key.clone() }),
            Op::Cas {
                key,
                expected,
                value,
            } => WireOp::Cas(WireCas {
                key: key.clone(),
                expected: expected.clone(),
                value: value.clone(),
            }),
        };
        let wire = WireCommand {
            client_id: self.client_id,
            request_id: self.request_id,
            op: Some(wire_op),
        };
        let mut buf = BytesMut::with_capacity(wire.encoded_len());
        wire.encode_raw(&mut buf);
        buf.freeze()
    }

    /// Decodes a command from protobuf bytes.
    pub fn decode(data: &[u8]) -> Result<Self, CommandDecodeError> {
        let wire = WireCommand::decode(data)?;
        let op = match wire.op {
            Some(WireOp::Put(p)) => Op::Put {
                key: p.key,
                value: p.value,
            },
            Some(WireOp::Delete(d)) => Op::Delete { key: d.key },
            Some(WireOp::Cas(c)) => Op::Cas {
                key: c.key,
                expected: c.expected,
                value: c.value,
            },
            None => return Err(CommandDecodeError::MissingOp),
        };
        Ok(Self {
            client_id: wire.client_id,
            request_id: wire.request_id,
            op,
        })
    }
}

/// Errors encountered while decoding a raw command payload.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CommandDecodeError {
    /// Protobuf wire decoding failed.
    #[error("failed to decode command protobuf: {0}")]
    Prost(#[from] prost::DecodeError),
    /// Command envelope did not contain an operation variant.
    #[error("command message missing operation")]
    MissingOp,
}

impl From<&OpResult> for WireOpResult {
    fn from(res: &OpResult) -> Self {
        match res {
            OpResult::Put => Self::Put(true),
            OpResult::Delete { existed } => Self::DeleteExisted(*existed),
            OpResult::Cas { swapped, current } => Self::Cas(WireCasResult {
                swapped: *swapped,
                current: current.clone(),
            }),
        }
    }
}

impl From<WireOpResult> for OpResult {
    fn from(wire: WireOpResult) -> Self {
        match wire {
            WireOpResult::Put(_) => Self::Put,
            WireOpResult::DeleteExisted(existed) => Self::Delete { existed },
            WireOpResult::Cas(c) => Self::Cas {
                swapped: c.swapped,
                current: c.current,
            },
        }
    }
}
