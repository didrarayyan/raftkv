//! Deterministic replicated key-value state machine and client session table.

use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::{Bytes, BytesMut};
use prost::Message as _;
use raft_core::{Index, Payload};
use thiserror::Error;

use crate::codec::{
    Command, CommandDecodeError, MAX_KEY_BYTES, MAX_SESSIONS, MAX_VALUE_BYTES, Op, OpResult,
    WireKvEntry, WireOpResult, WireSession, WireSnapshot,
};

/// Outcome of applying a committed Raft log entry to [`KvStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// Entry was a Raft barrier no-op.
    Noop,
    /// Command executed cleanly for the first time.
    Done(OpResult),
    /// Command matched the client's last applied `request_id`; cached result returned without re-applying.
    Duplicate(OpResult),
    /// Command carried a `request_id` strictly older than the client's last applied request.
    Outdated,
}

/// Errors returned when [`KvStore::apply`] rejects malformed or oversized input.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ApplyError {
    /// Key was empty (minimum 1 byte required).
    #[error("key must not be empty")]
    EmptyKey,
    /// Key exceeded [`MAX_KEY_BYTES`].
    #[error("key length {0} exceeds maximum of {MAX_KEY_BYTES} bytes")]
    KeyTooLarge(usize),
    /// Value exceeded [`MAX_VALUE_BYTES`].
    #[error("value length {0} exceeds maximum of {MAX_VALUE_BYTES} bytes")]
    ValueTooLarge(usize),
    /// Command bytes could not be decoded.
    #[error("invalid command payload: {0}")]
    InvalidCommand(#[from] CommandDecodeError),
}

/// Errors returned when restoring a snapshot via [`KvStore::restore`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RestoreError {
    /// Protobuf snapshot decoding failed.
    #[error("failed to decode snapshot: {0}")]
    Decode(#[from] prost::DecodeError),
    /// Snapshot contained a malformed session or oversized entry.
    #[error("corrupt snapshot data: {0}")]
    InvalidSnapshot(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionRecord {
    request_id: u64,
    result: OpResult,
    applied_index: u64,
}

/// In-memory ordered key-value store with deterministic client deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KvStore {
    data: BTreeMap<Bytes, Bytes>,
    sessions: BTreeMap<u64, SessionRecord>,
    applied_index: Index,
}

impl KvStore {
    /// Creates an empty key-value store at log index `0`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one committed Raft log entry at `index` deterministically.
    pub fn apply(&mut self, index: Index, payload: &Payload) -> Result<Applied, ApplyError> {
        let bytes = match payload {
            Payload::Noop => {
                self.applied_index = self.applied_index.max(index);
                return Ok(Applied::Noop);
            }
            Payload::Command(bytes) => bytes,
        };

        let cmd = Command::decode(bytes)?;
        validate_op(&cmd.op)?;

        if cmd.client_id != 0
            && let Some(rec) = self.sessions.get(&cmd.client_id)
        {
            if cmd.request_id == rec.request_id {
                self.applied_index = self.applied_index.max(index);
                return Ok(Applied::Duplicate(rec.result.clone()));
            }
            if cmd.request_id < rec.request_id {
                self.applied_index = self.applied_index.max(index);
                return Ok(Applied::Outdated);
            }
        }

        let result = self.execute_op(cmd.op);

        if cmd.client_id != 0 {
            if !self.sessions.contains_key(&cmd.client_id)
                && self.sessions.len() >= MAX_SESSIONS
                && let Some(&evict_id) = self
                    .sessions
                    .iter()
                    .min_by_key(|(cid, r)| (r.applied_index, **cid))
                    .map(|(cid, _)| cid)
            {
                self.sessions.remove(&evict_id);
            }
            self.sessions.insert(
                cmd.client_id,
                SessionRecord {
                    request_id: cmd.request_id,
                    result: result.clone(),
                    applied_index: index.0,
                },
            );
        }

        self.applied_index = self.applied_index.max(index);
        Ok(Applied::Done(result))
    }

    /// Reads the value associated with `key`, if present.
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        self.data.get(key)
    }

    /// Returns up to `limit` ordered `(key, value)` pairs in `[start, end)`.
    ///
    /// An empty `end` slice denotes an unbounded upper range. The returned boolean is `true`
    /// when additional matching keys exist beyond `limit`.
    pub fn range(&self, start: &[u8], end: &[u8], limit: usize) -> (Vec<(Bytes, Bytes)>, bool) {
        if !end.is_empty() && start >= end {
            return (Vec::new(), false);
        }
        let end_bound = if end.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end)
        };

        let mut iter = self
            .data
            .range::<[u8], _>((Bound::Included(start), end_bound));
        let mut items = Vec::with_capacity(limit.min(256));
        for _ in 0..limit {
            match iter.next() {
                Some((k, v)) => items.push((k.clone(), v.clone())),
                None => return (items, false),
            }
        }
        let more = iter.next().is_some();
        (items, more)
    }

    /// Highest Raft log index applied to this state machine.
    pub fn applied_index(&self) -> Index {
        self.applied_index
    }

    /// Serializes the full KV map and client session table into a snapshot image.
    pub fn snapshot(&self) -> Bytes {
        let entries = self
            .data
            .iter()
            .map(|(k, v)| WireKvEntry {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        let sessions = self
            .sessions
            .iter()
            .map(|(&client_id, rec)| WireSession {
                client_id,
                last_request_id: rec.request_id,
                last_applied_index: rec.applied_index,
                last_result: Some(WireOpResult::from(&rec.result)),
            })
            .collect();
        let wire = WireSnapshot { entries, sessions };
        let mut buf = BytesMut::with_capacity(wire.encoded_len());
        wire.encode_raw(&mut buf);
        buf.freeze()
    }

    /// Replaces the entire store state from a serialized snapshot image at `index`.
    pub fn restore(&mut self, index: Index, data: &[u8]) -> Result<(), RestoreError> {
        let wire = WireSnapshot::decode(data)?;
        let mut next_data = BTreeMap::new();
        for entry in wire.entries {
            if entry.key.is_empty() || entry.key.len() > MAX_KEY_BYTES {
                return Err(RestoreError::InvalidSnapshot("invalid key length"));
            }
            if entry.value.len() > MAX_VALUE_BYTES {
                return Err(RestoreError::InvalidSnapshot("value exceeds limit"));
            }
            next_data.insert(entry.key, entry.value);
        }

        let mut next_sessions = BTreeMap::new();
        for sess in wire.sessions {
            let Some(res) = sess.last_result else {
                return Err(RestoreError::InvalidSnapshot("session missing result"));
            };
            next_sessions.insert(
                sess.client_id,
                SessionRecord {
                    request_id: sess.last_request_id,
                    result: OpResult::from(res),
                    applied_index: sess.last_applied_index,
                },
            );
        }

        self.data = next_data;
        self.sessions = next_sessions;
        self.applied_index = index;
        Ok(())
    }

    fn execute_op(&mut self, op: Op) -> OpResult {
        match op {
            Op::Put { key, value } => {
                self.data.insert(key, value);
                OpResult::Put
            }
            Op::Delete { key } => {
                let existed = self.data.remove(&key).is_some();
                OpResult::Delete { existed }
            }
            Op::Cas {
                key,
                expected,
                value,
            } => {
                let matches = self.data.get(&key) == expected.as_ref();
                if matches {
                    self.data.insert(key, value.clone());
                    OpResult::Cas {
                        swapped: true,
                        current: Some(value),
                    }
                } else {
                    OpResult::Cas {
                        swapped: false,
                        current: self.data.get(&key).cloned(),
                    }
                }
            }
        }
    }
}

fn validate_op(op: &Op) -> Result<(), ApplyError> {
    let (key, value, expected) = match op {
        Op::Put { key, value } => (key, Some(value), None),
        Op::Delete { key } => (key, None, None),
        Op::Cas {
            key,
            expected,
            value,
        } => (key, Some(value), expected.as_ref()),
    };

    if key.is_empty() {
        return Err(ApplyError::EmptyKey);
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(ApplyError::KeyTooLarge(key.len()));
    }
    if let Some(val) = value
        && val.len() > MAX_VALUE_BYTES
    {
        return Err(ApplyError::ValueTooLarge(val.len()));
    }
    if let Some(exp) = expected
        && exp.len() > MAX_VALUE_BYTES
    {
        return Err(ApplyError::ValueTooLarge(exp.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn apply_cmd(
        store: &mut KvStore,
        idx: u64,
        client_id: u64,
        request_id: u64,
        op: Op,
    ) -> Result<Applied, ApplyError> {
        let cmd = Command {
            client_id,
            request_id,
            op,
        };
        store.apply(Index(idx), &Payload::Command(cmd.encode()))
    }

    #[test]
    fn put_delete_and_cas_including_absent_key_work() {
        let mut store = KvStore::new();

        // CAS requiring absent key succeeds initially, then fails on second attempt.
        let r1 = apply_cmd(
            &mut store,
            1,
            1,
            1,
            Op::Cas {
                key: Bytes::from_static(b"k"),
                expected: None,
                value: Bytes::from_static(b"v1"),
            },
        )
        .unwrap();
        assert_eq!(
            r1,
            Applied::Done(OpResult::Cas {
                swapped: true,
                current: Some(Bytes::from_static(b"v1")),
            })
        );

        let r2 = apply_cmd(
            &mut store,
            2,
            1,
            2,
            Op::Cas {
                key: Bytes::from_static(b"k"),
                expected: None,
                value: Bytes::from_static(b"v2"),
            },
        )
        .unwrap();
        assert_eq!(
            r2,
            Applied::Done(OpResult::Cas {
                swapped: false,
                current: Some(Bytes::from_static(b"v1")),
            })
        );

        let r3 = apply_cmd(
            &mut store,
            3,
            1,
            3,
            Op::Cas {
                key: Bytes::from_static(b"k"),
                expected: Some(Bytes::from_static(b"v1")),
                value: Bytes::from_static(b"v2"),
            },
        )
        .unwrap();
        assert_eq!(
            r3,
            Applied::Done(OpResult::Cas {
                swapped: true,
                current: Some(Bytes::from_static(b"v2")),
            })
        );

        assert_eq!(store.get(b"k"), Some(&Bytes::from_static(b"v2")));

        let d1 = apply_cmd(
            &mut store,
            4,
            1,
            4,
            Op::Delete {
                key: Bytes::from_static(b"k"),
            },
        )
        .unwrap();
        assert_eq!(d1, Applied::Done(OpResult::Delete { existed: true }));

        let d2 = apply_cmd(
            &mut store,
            5,
            1,
            5,
            Op::Delete {
                key: Bytes::from_static(b"k"),
            },
        )
        .unwrap();
        assert_eq!(d2, Applied::Done(OpResult::Delete { existed: false }));
    }

    #[test]
    fn duplicate_request_returns_cached_result_without_reapplying() {
        let mut store = KvStore::new();
        let first = apply_cmd(
            &mut store,
            1,
            10,
            1,
            Op::Delete {
                key: Bytes::from_static(b"missing"),
            },
        )
        .unwrap();
        assert_eq!(first, Applied::Done(OpResult::Delete { existed: false }));

        // Insert key via another client, then replay client 10's request_id=1 delete.
        apply_cmd(
            &mut store,
            2,
            20,
            1,
            Op::Put {
                key: Bytes::from_static(b"missing"),
                value: Bytes::from_static(b"present"),
            },
        )
        .unwrap();

        let dup = apply_cmd(
            &mut store,
            3,
            10,
            1,
            Op::Delete {
                key: Bytes::from_static(b"missing"),
            },
        )
        .unwrap();
        assert_eq!(dup, Applied::Duplicate(OpResult::Delete { existed: false }));
        // Key must remain present because the duplicate was not re-executed.
        assert_eq!(store.get(b"missing"), Some(&Bytes::from_static(b"present")));
    }

    #[test]
    fn outdated_request_id_is_rejected_without_mutation() {
        let mut store = KvStore::new();
        apply_cmd(
            &mut store,
            1,
            7,
            1,
            Op::Put {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"1"),
            },
        )
        .unwrap();
        apply_cmd(
            &mut store,
            2,
            7,
            2,
            Op::Put {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"2"),
            },
        )
        .unwrap();

        let out = apply_cmd(
            &mut store,
            3,
            7,
            1,
            Op::Put {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"stale"),
            },
        )
        .unwrap();
        assert_eq!(out, Applied::Outdated);
        assert_eq!(store.get(b"a"), Some(&Bytes::from_static(b"2")));
    }

    #[test]
    fn session_eviction_is_deterministic_across_replicas() {
        let mut a = KvStore::new();
        let mut b = KvStore::new();

        for i in 1..=(MAX_SESSIONS as u64 + 5) {
            let op = Op::Put {
                key: Bytes::from(format!("k{i}")),
                value: Bytes::from(format!("v{i}")),
            };
            apply_cmd(&mut a, i, i, 1, op.clone()).unwrap();
            apply_cmd(&mut b, i, i, 1, op).unwrap();
        }

        assert_eq!(a, b);
        assert_eq!(a.sessions.len(), MAX_SESSIONS);
        // Clients 1..=5 had the lowest last_applied_index and were evicted; client 6 stays.
        assert!(!a.sessions.contains_key(&1));
        assert!(!a.sessions.contains_key(&5));
        assert!(a.sessions.contains_key(&6));
    }

    #[test]
    fn apply_rejects_oversized_and_empty_inputs() {
        let mut store = KvStore::new();

        assert_eq!(
            apply_cmd(
                &mut store,
                1,
                1,
                1,
                Op::Put {
                    key: Bytes::new(),
                    value: Bytes::from_static(b"x"),
                },
            ),
            Err(ApplyError::EmptyKey)
        );
        assert_eq!(
            apply_cmd(
                &mut store,
                2,
                1,
                1,
                Op::Put {
                    key: Bytes::from(vec![b'a'; MAX_KEY_BYTES + 1]),
                    value: Bytes::from_static(b"x"),
                },
            ),
            Err(ApplyError::KeyTooLarge(MAX_KEY_BYTES + 1))
        );
        assert_eq!(
            apply_cmd(
                &mut store,
                3,
                1,
                1,
                Op::Put {
                    key: Bytes::from_static(b"k"),
                    value: Bytes::from(vec![0u8; MAX_VALUE_BYTES + 1]),
                },
            ),
            Err(ApplyError::ValueTooLarge(MAX_VALUE_BYTES + 1))
        );
    }

    #[test]
    fn range_ordering_limit_more_and_unbounded_end() {
        let mut store = KvStore::new();
        for (idx, k) in ["b", "a", "d", "c", "e"].into_iter().enumerate() {
            apply_cmd(
                &mut store,
                idx as u64 + 1,
                1,
                idx as u64 + 1,
                Op::Put {
                    key: Bytes::from(k),
                    value: Bytes::from(format!("val-{k}")),
                },
            )
            .unwrap();
        }

        let (items, more) = store.range(b"b", b"e", 2);
        assert!(more);
        assert_eq!(
            items,
            vec![
                (Bytes::from_static(b"b"), Bytes::from_static(b"val-b")),
                (Bytes::from_static(b"c"), Bytes::from_static(b"val-c")),
            ]
        );

        let (items2, more2) = store.range(b"d", b"", 5);
        assert!(!more2);
        assert_eq!(
            items2,
            vec![
                (Bytes::from_static(b"d"), Bytes::from_static(b"val-d")),
                (Bytes::from_static(b"e"), Bytes::from_static(b"val-e")),
            ]
        );

        let (empty, more3) = store.range(b"z", b"a", 10);
        assert!(!more3);
        assert_eq!(empty, []);
    }

    fn op_strategy() -> impl Strategy<Value = (u64, u64, Op)> {
        let key = prop::sample::select(vec![
            Bytes::from_static(b"k0"),
            Bytes::from_static(b"k1"),
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"k3"),
        ]);
        let val = prop::sample::select(vec![
            Bytes::from_static(b"v0"),
            Bytes::from_static(b"v1"),
            Bytes::from_static(b"v2"),
        ]);
        let cid = 1u64..=4;
        let rid = 1u64..=8;
        let op = prop_oneof![
            (key.clone(), val.clone()).prop_map(|(k, v)| Op::Put { key: k, value: v }),
            key.clone().prop_map(|k| Op::Delete { key: k }),
            (key, prop::option::of(val.clone()), val).prop_map(|(k, expected, value)| Op::Cas {
                key: k,
                expected,
                value,
            }),
        ];
        (cid, rid, op)
    }

    proptest! {
        #[test]
        fn snapshot_restore_produces_identical_observable_state(
            cmds in proptest::collection::vec(op_strategy(), 1..60),
            split_ratio in 0usize..=100,
        ) {
            let split = (cmds.len() * split_ratio) / 100;
            let mut full = KvStore::new();
            let mut checkpoint = KvStore::new();

            for (i, (cid, rid, op)) in cmds.iter().enumerate() {
                let idx = Index(i as u64 + 1);
                let payload = Payload::Command(Command {
                    client_id: *cid,
                    request_id: *rid,
                    op: op.clone(),
                }.encode());
                let r_full = full.apply(idx, &payload).unwrap();
                if i < split {
                    let r_chk = checkpoint.apply(idx, &payload).unwrap();
                    prop_assert_eq!(r_full, r_chk);
                }
            }

            if split > 0 {
                let snap = checkpoint.snapshot();
                let mut restored = KvStore::new();
                restored.restore(Index(split as u64), &snap).unwrap();
                for (i, (cid, rid, op)) in cmds.iter().enumerate().skip(split) {
                    let idx = Index(i as u64 + 1);
                    let payload = Payload::Command(Command {
                        client_id: *cid,
                        request_id: *rid,
                        op: op.clone(),
                    }.encode());
                    restored.apply(idx, &payload).unwrap();
                }
                prop_assert_eq!(restored, full);
            }
        }
    }
}
