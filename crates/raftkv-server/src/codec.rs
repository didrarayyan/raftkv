//! Bidirectional conversions between `raft_core` messages and `raftkv_proto` wire types.

use bytes::{Bytes, BytesMut};
use raft_core::{
    AppendResult, Entry, Index, Message, MessageBody, NodeId, Payload, Role, Snapshot,
    SnapshotMeta, Status, Term,
};
use raftkv_proto::{
    AppendRequest, AppendResponse, Entry as ProtoEntry, PeerProgress, RaftMessage,
    Role as ProtoRole, SnapshotChunk, SnapshotResponse, StatusResponse, VoteRequest, VoteResponse,
    raft_message::Body as ProtoBody,
};
use thiserror::Error;

/// Maximum snapshot chunk size emitted on an `InstallSnapshot` gRPC stream (1 MiB).
pub const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

/// Errors returned when encoding or decoding Raft wire messages.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    /// Wire message lacked a valid sender or recipient identifier.
    #[error("message must have non-zero from ({from}) and to ({to}) node IDs")]
    InvalidNodeIds {
        /// Raw `from` field from wire message.
        from: u64,
        /// Raw `to` field from wire message.
        to: u64,
    },
    /// Wire message omitted the `oneof body` field.
    #[error("wire message body is missing")]
    MissingBody,
    /// `InstallSnapshot` must be sent via the streaming RPC rather than `Step`.
    #[error("InstallSnapshot cannot be encoded as a unary RaftMessage")]
    SnapshotMustStream,
}

/// Errors returned when reassembling streamed `SnapshotChunk` frames.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SnapshotAssembleError {
    /// Stream ended without sending any chunk.
    #[error("snapshot stream was empty")]
    EmptyStream,
    /// Chunks within a single stream disagreed on sender, term, index, or snapshot term.
    #[error("inconsistent metadata across snapshot stream chunks")]
    InconsistentHeader,
    /// Accumulated payload bytes exceeded the configured limit.
    #[error("snapshot payload exceeds maximum allowed {max_bytes} bytes")]
    TooLarge {
        /// Configured `limits.max_snapshot_bytes` bound.
        max_bytes: usize,
    },
}

/// Converts a single [`Entry`] into its protobuf representation.
pub fn entry_to_proto(entry: &Entry) -> ProtoEntry {
    match &entry.payload {
        Payload::Noop => ProtoEntry {
            term: entry.term.0,
            index: entry.index.0,
            noop: true,
            data: Bytes::new(),
        },
        Payload::Command(data) => ProtoEntry {
            term: entry.term.0,
            index: entry.index.0,
            noop: false,
            data: data.clone(),
        },
    }
}

/// Converts a protobuf [`ProtoEntry`] into a domain [`Entry`].
pub fn proto_to_entry(entry: ProtoEntry) -> Entry {
    let payload = if entry.noop {
        Payload::Noop
    } else {
        Payload::Command(entry.data)
    };
    Entry {
        term: Term(entry.term),
        index: Index(entry.index),
        payload,
    }
}

/// Encodes a unary Raft [`Message`] into a [`RaftMessage`].
pub fn encode_step_message(msg: &Message) -> Result<RaftMessage, CodecError> {
    if !msg.from.is_valid() || !msg.to.is_valid() {
        return Err(CodecError::InvalidNodeIds {
            from: msg.from.0,
            to: msg.to.0,
        });
    }

    let body = match &msg.body {
        MessageBody::PreVote {
            last_index,
            last_term,
        } => ProtoBody::PreVote(VoteRequest {
            last_index: last_index.0,
            last_term: last_term.0,
        }),
        MessageBody::PreVoteResponse { granted } => {
            ProtoBody::PreVoteResponse(VoteResponse { granted: *granted })
        }
        MessageBody::RequestVote {
            last_index,
            last_term,
        } => ProtoBody::RequestVote(VoteRequest {
            last_index: last_index.0,
            last_term: last_term.0,
        }),
        MessageBody::RequestVoteResponse { granted } => {
            ProtoBody::RequestVoteResponse(VoteResponse { granted: *granted })
        }
        MessageBody::AppendEntries {
            prev_index,
            prev_term,
            entries,
            leader_commit,
            read_ctx,
        } => ProtoBody::AppendEntries(AppendRequest {
            prev_index: prev_index.0,
            prev_term: prev_term.0,
            entries: entries.iter().map(entry_to_proto).collect(),
            leader_commit: leader_commit.0,
            read_ctx: *read_ctx,
        }),
        MessageBody::AppendEntriesResponse { result, read_ctx } => {
            let wire = match *result {
                AppendResult::Accepted { match_index } => AppendResponse {
                    accepted: true,
                    match_index: match_index.0,
                    conflict_index: 0,
                    conflict_term: 0,
                    read_ctx: *read_ctx,
                },
                AppendResult::Rejected {
                    conflict_index,
                    conflict_term,
                } => AppendResponse {
                    accepted: false,
                    match_index: 0,
                    conflict_index: conflict_index.0,
                    conflict_term: conflict_term.0,
                    read_ctx: *read_ctx,
                },
            };
            ProtoBody::AppendEntriesResponse(wire)
        }
        MessageBody::InstallSnapshot { .. } => return Err(CodecError::SnapshotMustStream),
        MessageBody::InstallSnapshotResponse { match_index } => {
            ProtoBody::InstallSnapshotResponse(SnapshotResponse {
                match_index: match_index.0,
            })
        }
    };

    Ok(RaftMessage {
        from: msg.from.0,
        to: msg.to.0,
        term: msg.term.0,
        body: Some(body),
    })
}

/// Decodes a unary protobuf [`RaftMessage`] into a domain [`Message`].
pub fn decode_step_message(wire: RaftMessage) -> Result<Message, CodecError> {
    if wire.from == 0 || wire.to == 0 {
        return Err(CodecError::InvalidNodeIds {
            from: wire.from,
            to: wire.to,
        });
    }
    let body = match wire.body.ok_or(CodecError::MissingBody)? {
        ProtoBody::PreVote(req) => MessageBody::PreVote {
            last_index: Index(req.last_index),
            last_term: Term(req.last_term),
        },
        ProtoBody::PreVoteResponse(resp) => MessageBody::PreVoteResponse {
            granted: resp.granted,
        },
        ProtoBody::RequestVote(req) => MessageBody::RequestVote {
            last_index: Index(req.last_index),
            last_term: Term(req.last_term),
        },
        ProtoBody::RequestVoteResponse(resp) => MessageBody::RequestVoteResponse {
            granted: resp.granted,
        },
        ProtoBody::AppendEntries(req) => MessageBody::AppendEntries {
            prev_index: Index(req.prev_index),
            prev_term: Term(req.prev_term),
            entries: req.entries.into_iter().map(proto_to_entry).collect(),
            leader_commit: Index(req.leader_commit),
            read_ctx: req.read_ctx,
        },
        ProtoBody::AppendEntriesResponse(resp) => {
            let result = if resp.accepted {
                AppendResult::Accepted {
                    match_index: Index(resp.match_index),
                }
            } else {
                AppendResult::Rejected {
                    conflict_index: Index(resp.conflict_index),
                    conflict_term: Term(resp.conflict_term),
                }
            };
            MessageBody::AppendEntriesResponse {
                result,
                read_ctx: resp.read_ctx,
            }
        }
        ProtoBody::InstallSnapshotResponse(resp) => MessageBody::InstallSnapshotResponse {
            match_index: Index(resp.match_index),
        },
    };

    Ok(Message {
        from: NodeId(wire.from),
        to: NodeId(wire.to),
        term: Term(wire.term),
        body,
    })
}

/// Splits a [`Snapshot`] into `<= 1 MiB` [`SnapshotChunk`] frames for streaming RPC delivery.
pub fn encode_snapshot_chunks(
    from: NodeId,
    to: NodeId,
    term: Term,
    snapshot: &Snapshot,
) -> Vec<SnapshotChunk> {
    if snapshot.data.is_empty() {
        return vec![SnapshotChunk {
            from: from.0,
            to: to.0,
            term: term.0,
            index: snapshot.meta.index.0,
            snapshot_term: snapshot.meta.term.0,
            data: Bytes::new(),
        }];
    }

    let total = snapshot.data.len();
    let num_chunks = total.div_ceil(SNAPSHOT_CHUNK_BYTES);
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut offset = 0usize;
    while offset < total {
        let end = (offset + SNAPSHOT_CHUNK_BYTES).min(total);
        chunks.push(SnapshotChunk {
            from: from.0,
            to: to.0,
            term: term.0,
            index: snapshot.meta.index.0,
            snapshot_term: snapshot.meta.term.0,
            data: snapshot.data.slice(offset..end),
        });
        offset = end;
    }
    chunks
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamHeader {
    from: u64,
    to: u64,
    term: u64,
    index: u64,
    snapshot_term: u64,
}

/// Incremental validator and buffer for incoming [`SnapshotChunk`] streams.
#[derive(Debug)]
pub struct SnapshotAssembler {
    max_bytes: usize,
    header: Option<StreamHeader>,
    buf: BytesMut,
}

impl SnapshotAssembler {
    /// Creates a new assembler bounded by `max_bytes`.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            header: None,
            buf: BytesMut::new(),
        }
    }

    /// Appends one chunk, failing fast on header mismatches or `max_bytes` overflow.
    pub fn push(&mut self, chunk: &SnapshotChunk) -> Result<(), SnapshotAssembleError> {
        let hdr = StreamHeader {
            from: chunk.from,
            to: chunk.to,
            term: chunk.term,
            index: chunk.index,
            snapshot_term: chunk.snapshot_term,
        };
        match self.header {
            None => self.header = Some(hdr),
            Some(expected) if expected != hdr => {
                return Err(SnapshotAssembleError::InconsistentHeader);
            }
            Some(_) => {}
        }

        let new_len = self.buf.len().checked_add(chunk.data.len()).ok_or(
            SnapshotAssembleError::TooLarge {
                max_bytes: self.max_bytes,
            },
        )?;
        if new_len > self.max_bytes {
            return Err(SnapshotAssembleError::TooLarge {
                max_bytes: self.max_bytes,
            });
        }
        self.buf.extend_from_slice(&chunk.data);
        Ok(())
    }

    /// Finalizes the stream into a [`Message`] carrying [`MessageBody::InstallSnapshot`].
    pub fn finish(self) -> Result<Message, SnapshotAssembleError> {
        let hdr = self.header.ok_or(SnapshotAssembleError::EmptyStream)?;
        Ok(Message {
            from: NodeId(hdr.from),
            to: NodeId(hdr.to),
            term: Term(hdr.term),
            body: MessageBody::InstallSnapshot {
                snapshot: Snapshot {
                    meta: SnapshotMeta {
                        index: Index(hdr.index),
                        term: Term(hdr.snapshot_term),
                    },
                    data: self.buf.freeze(),
                },
            },
        })
    }
}

/// Converts a domain [`Status`] into a gRPC [`StatusResponse`].
pub fn status_to_proto(status: Status) -> StatusResponse {
    let role = match status.role {
        Role::Follower => ProtoRole::Follower,
        Role::PreCandidate => ProtoRole::PreCandidate,
        Role::Candidate => ProtoRole::Candidate,
        Role::Leader => ProtoRole::Leader,
    };

    StatusResponse {
        node_id: status.id.0,
        role: role as i32,
        term: status.term.0,
        leader_id: status.leader_id.map_or(0, |id| id.0),
        commit_index: status.commit.0,
        applied_index: status.applied.0,
        last_log_index: status.last_log_index.0,
        snapshot_index: status.snapshot_index.0,
        peers: status
            .peers
            .into_iter()
            .map(|p| PeerProgress {
                node_id: p.id.0,
                match_index: p.match_index.0,
                next_index: p.next_index.0,
                recently_active: p.recently_active,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_payload() -> impl Strategy<Value = Payload> {
        prop_oneof![
            Just(Payload::Noop),
            prop::collection::vec(any::<u8>(), 0..256).prop_map(|v| Payload::Command(v.into())),
        ]
    }

    fn arb_entry() -> impl Strategy<Value = Entry> {
        (1..=100u64, 1..=10_000u64, arb_payload()).prop_map(|(term, index, payload)| Entry {
            term: Term(term),
            index: Index(index),
            payload,
        })
    }

    fn arb_step_body() -> impl Strategy<Value = MessageBody> {
        prop_oneof![
            (any::<u64>(), any::<u64>()).prop_map(|(i, t)| MessageBody::PreVote {
                last_index: Index(i),
                last_term: Term(t),
            }),
            any::<bool>().prop_map(|granted| MessageBody::PreVoteResponse { granted }),
            (any::<u64>(), any::<u64>()).prop_map(|(i, t)| MessageBody::RequestVote {
                last_index: Index(i),
                last_term: Term(t),
            }),
            any::<bool>().prop_map(|granted| MessageBody::RequestVoteResponse { granted }),
            (
                any::<u64>(),
                any::<u64>(),
                prop::collection::vec(arb_entry(), 0..8),
                any::<u64>(),
                any::<u64>(),
            )
                .prop_map(|(pi, pt, entries, lc, read_ctx)| {
                    MessageBody::AppendEntries {
                        prev_index: Index(pi),
                        prev_term: Term(pt),
                        entries,
                        leader_commit: Index(lc),
                        read_ctx,
                    }
                }),
            (any::<bool>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(accepted, a, b, read_ctx)| MessageBody::AppendEntriesResponse {
                    result: if accepted {
                        AppendResult::Accepted {
                            match_index: Index(a),
                        }
                    } else {
                        AppendResult::Rejected {
                            conflict_index: Index(a),
                            conflict_term: Term(b),
                        }
                    },
                    read_ctx,
                },
            ),
            any::<u64>().prop_map(|m| MessageBody::InstallSnapshotResponse {
                match_index: Index(m),
            }),
        ]
    }

    proptest! {
        #[test]
        fn step_message_round_trips_every_variant(
            from in 1..=100u64,
            to in 1..=100u64,
            term in 0..=10_000u64,
            body in arb_step_body(),
        ) {
            let msg = Message {
                from: NodeId(from),
                to: NodeId(to),
                term: Term(term),
                body,
            };
            let wire = encode_step_message(&msg).expect("encode step message");
            let decoded = decode_step_message(wire).expect("decode step message");
            prop_assert_eq!(decoded, msg);
        }
    }

    #[test]
    fn snapshot_chunk_round_trip_handles_empty_and_multi_chunk() {
        for size in [
            0usize,
            17,
            SNAPSHOT_CHUNK_BYTES,
            SNAPSHOT_CHUNK_BYTES + 123,
            2 * SNAPSHOT_CHUNK_BYTES + 1,
        ] {
            let data: Bytes = vec![0xABu8; size].into();
            let snap = Snapshot {
                meta: SnapshotMeta {
                    index: Index(42),
                    term: Term(7),
                },
                data: data.clone(),
            };
            let chunks = encode_snapshot_chunks(NodeId(1), NodeId(2), Term(7), &snap);
            for chunk in &chunks {
                assert!(chunk.data.len() <= SNAPSHOT_CHUNK_BYTES);
            }
            let mut assembler = SnapshotAssembler::new(size.max(1));
            for chunk in chunks {
                assembler.push(&chunk).expect("push chunk");
            }
            let msg = assembler.finish().expect("finish snapshot");
            assert_eq!(
                msg,
                Message {
                    from: NodeId(1),
                    to: NodeId(2),
                    term: Term(7),
                    body: MessageBody::InstallSnapshot { snapshot: snap },
                }
            );
        }
    }

    #[test]
    fn snapshot_assembler_rejects_oversized_payload() {
        let mut assembler = SnapshotAssembler::new(10);
        let chunk = SnapshotChunk {
            from: 1,
            to: 2,
            term: 1,
            index: 1,
            snapshot_term: 1,
            data: Bytes::from_static(&[1u8; 11]),
        };
        assert_eq!(
            assembler.push(&chunk),
            Err(SnapshotAssembleError::TooLarge { max_bytes: 10 })
        );
    }
}
