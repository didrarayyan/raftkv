//! In-memory Raft log backed by a `VecDeque` anchored at the latest snapshot.

use std::collections::VecDeque;

use crate::error::ConfigError;
use crate::types::{Entry, Index, Snapshot, Term};

/// Contiguous Raft log entries above an optional compacted snapshot anchor.
#[derive(Debug, Clone)]
pub struct RaftLog {
    snapshot: Option<Snapshot>,
    snapshot_index: Index,
    snapshot_term: Term,
    entries: VecDeque<Entry>,
}

impl RaftLog {
    /// Builds a log from recovered storage state, verifying entry contiguity.
    pub fn from_recovered(
        snapshot: Option<Snapshot>,
        entries: Vec<Entry>,
    ) -> Result<Self, ConfigError> {
        let (snapshot_index, snapshot_term) = snapshot
            .as_ref()
            .map_or((Index(0), Term(0)), |s| (s.meta.index, s.meta.term));

        let mut expected = snapshot_index.next();
        for entry in &entries {
            if entry.index != expected {
                return Err(ConfigError::NonContiguousLog {
                    expected,
                    actual: entry.index,
                });
            }
            expected = expected.next();
        }

        Ok(Self {
            snapshot,
            snapshot_index,
            snapshot_term,
            entries: VecDeque::from(entries),
        })
    }

    /// Index of the latest compacted snapshot (`0` if none).
    pub fn snapshot_index(&self) -> Index {
        self.snapshot_index
    }

    /// Term of the entry covered by the latest snapshot (`0` if none).
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// Returns the stored snapshot, if one exists.
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// Index of the last entry present in memory or covered by the snapshot.
    pub fn last_index(&self) -> Index {
        self.entries.back().map_or(self.snapshot_index, |e| e.index)
    }

    /// Term of the last entry in memory or covered by the snapshot.
    pub fn last_term(&self) -> Term {
        self.entries.back().map_or(self.snapshot_term, |e| e.term)
    }

    /// Looks up the term at `index`, returning `None` if compacted or beyond the tail.
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index {
            return None;
        }
        let offset = (index.0 - self.snapshot_index.0 - 1) as usize;
        self.entries.get(offset).map(|e| e.term)
    }

    /// Returns `true` if the log holds an entry (or snapshot anchor) with `(index, term)`.
    pub fn matches(&self, index: Index, term: Term) -> bool {
        self.term_at(index) == Some(term)
    }

    /// Returns a reference to the in-memory entry at `index`, if present and uncompacted.
    pub fn entry_at(&self, index: Index) -> Option<&Entry> {
        if index <= self.snapshot_index {
            return None;
        }
        let offset = (index.0 - self.snapshot_index.0 - 1) as usize;
        self.entries.get(offset)
    }

    /// Returns a contiguous slice starting at `from`, bounded by `max_entries` and `max_bytes`.
    ///
    /// Always includes at least one entry whenever `from` is within the in-memory log.
    pub fn slice(&self, from: Index, max_entries: usize, max_bytes: usize) -> Vec<Entry> {
        if max_entries == 0 || from <= self.snapshot_index || from > self.last_index() {
            return Vec::new();
        }
        let start = (from.0 - self.snapshot_index.0 - 1) as usize;
        let mut out = Vec::new();
        let mut used_bytes = 0usize;

        for entry in self.entries.iter().skip(start) {
            let size = entry.payload.byte_size();
            if !out.is_empty()
                && (out.len() >= max_entries || used_bytes.saturating_add(size) > max_bytes)
            {
                break;
            }
            used_bytes = used_bytes.saturating_add(size);
            out.push(entry.clone());
        }
        out
    }

    /// Clones entries starting at `from` through the end of the log.
    pub fn entries_from(&self, from: Index) -> Vec<Entry> {
        if from > self.last_index() {
            return Vec::new();
        }
        let start_idx = from.max(self.snapshot_index.next());
        let start = (start_idx.0 - self.snapshot_index.0 - 1) as usize;
        self.entries.iter().skip(start).cloned().collect()
    }

    /// Removes all entries with index `>= index`.
    pub fn truncate_from(&mut self, index: Index) {
        if index <= self.snapshot_index {
            self.entries.clear();
            return;
        }
        let keep = (index.0 - self.snapshot_index.0 - 1) as usize;
        if keep < self.entries.len() {
            self.entries.truncate(keep);
        }
    }

    /// Appends contiguous entries whose indices follow `last_index()`.
    pub fn append(&mut self, entries: &[Entry]) {
        for entry in entries {
            debug_assert_eq!(entry.index, self.last_index().next());
            self.entries.push_back(entry.clone());
        }
    }

    /// Drops entries up to `snapshot.meta.index` while retaining any trailing entries.
    pub fn compact_to(&mut self, snapshot: Snapshot) {
        if snapshot.meta.index < self.snapshot_index {
            return;
        }
        if snapshot.meta.index == self.snapshot_index {
            self.snapshot = Some(snapshot);
            return;
        }
        let drop_count = (snapshot.meta.index.0 - self.snapshot_index.0) as usize;
        if drop_count >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(..drop_count);
        }
        self.snapshot_index = snapshot.meta.index;
        self.snapshot_term = snapshot.meta.term;
        self.snapshot = Some(snapshot);
    }

    /// Replaces the entire log state when installing a leader snapshot that diverges from local entries.
    pub fn install_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot_index = snapshot.meta.index;
        self.snapshot_term = snapshot.meta.term;
        self.entries.clear();
        self.snapshot = Some(snapshot);
    }

    /// Returns `true` if a candidate's `(last_term, last_index)` is at least as up to date as ours.
    pub fn is_up_to_date(&self, last_index: Index, last_term: Term) -> bool {
        (last_term, last_index) >= (self.last_term(), self.last_index())
    }

    /// Computes `(conflict_index, conflict_term)` for fast log backtracking when `prev_index` misses.
    pub fn conflict_info(&self, prev_index: Index) -> (Index, Term) {
        if prev_index > self.last_index() {
            return (self.last_index().next(), Term(0));
        }
        if prev_index <= self.snapshot_index {
            return (self.snapshot_index.next(), Term(0));
        }
        let offset = (prev_index.0 - self.snapshot_index.0 - 1) as usize;
        let conflict_term = self.entries[offset].term;
        let mut first_offset = offset;
        while first_offset > 0 && self.entries[first_offset - 1].term == conflict_term {
            first_offset -= 1;
        }
        (self.entries[first_offset].index, conflict_term)
    }

    /// Finds the last index in the log whose term matches `target_term`.
    pub fn last_index_with_term(&self, target_term: Term) -> Option<Index> {
        if target_term == Term(0) {
            return None;
        }
        for entry in self.entries.iter().rev() {
            if entry.term == target_term {
                return Some(entry.index);
            }
            if entry.term < target_term {
                return None;
            }
        }
        if self.snapshot_term == target_term {
            Some(self.snapshot_index)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use proptest::prelude::*;

    use super::*;
    use crate::types::{Payload, SnapshotMeta};

    /// Reference implementation backed by a flat `Vec<Entry>` plus a snapshot anchor.
    struct ModelLog {
        snapshot_index: Index,
        snapshot_term: Term,
        entries: Vec<Entry>,
    }

    impl ModelLog {
        fn new() -> Self {
            Self {
                snapshot_index: Index(0),
                snapshot_term: Term(0),
                entries: Vec::new(),
            }
        }

        fn last_index(&self) -> Index {
            self.entries.last().map_or(self.snapshot_index, |e| e.index)
        }

        fn last_term(&self) -> Term {
            self.entries.last().map_or(self.snapshot_term, |e| e.term)
        }

        fn term_at(&self, index: Index) -> Option<Term> {
            if index == self.snapshot_index {
                return Some(self.snapshot_term);
            }
            self.entries
                .iter()
                .find(|e| e.index == index)
                .map(|e| e.term)
        }

        fn append(&mut self, items: &[Entry]) {
            self.entries.extend_from_slice(items);
        }

        fn truncate_from(&mut self, index: Index) {
            self.entries.retain(|e| e.index < index);
        }

        fn compact_to(&mut self, index: Index, term: Term) {
            if index <= self.snapshot_index {
                return;
            }
            self.snapshot_index = index;
            self.snapshot_term = term;
            self.entries.retain(|e| e.index > index);
        }

        fn slice(&self, from: Index, max_entries: usize, max_bytes: usize) -> Vec<Entry> {
            if max_entries == 0 || from <= self.snapshot_index || from > self.last_index() {
                return Vec::new();
            }
            let mut out = Vec::new();
            let mut used = 0usize;
            for entry in self.entries.iter().filter(|e| e.index >= from) {
                let sz = entry.payload.byte_size();
                if !out.is_empty() && (out.len() >= max_entries || used + sz > max_bytes) {
                    break;
                }
                used += sz;
                out.push(entry.clone());
            }
            out
        }
    }

    #[derive(Debug, Clone)]
    enum LogOp {
        Append {
            count: usize,
            term_delta: u64,
            size: usize,
        },
        TruncateOffset(usize),
        CompactOffset(usize),
        Slice {
            offset: usize,
            max_entries: usize,
            max_bytes: usize,
        },
        Lookup(usize),
    }

    fn op_strategy() -> impl Strategy<Value = LogOp> {
        prop_oneof![
            (1usize..=6, 0u64..=2, 0usize..=128).prop_map(|(count, term_delta, size)| {
                LogOp::Append {
                    count,
                    term_delta,
                    size,
                }
            }),
            (0usize..=20).prop_map(LogOp::TruncateOffset),
            (0usize..=20).prop_map(LogOp::CompactOffset),
            (0usize..=20, 0usize..=8, 0usize..=256).prop_map(|(offset, max_entries, max_bytes)| {
                LogOp::Slice {
                    offset,
                    max_entries,
                    max_bytes,
                }
            }),
            (0usize..=25).prop_map(LogOp::Lookup),
        ]
    }

    proptest! {
        #[test]
        fn raft_log_matches_reference_vec_model(ops in proptest::collection::vec(op_strategy(), 1..120)) {
            let mut sut = RaftLog::from_recovered(None, Vec::new()).unwrap();
            let mut model = ModelLog::new();
            let mut current_term = Term(1);

            for op in ops {
                match op {
                    LogOp::Append { count, term_delta, size } => {
                        current_term = Term(current_term.0 + term_delta);
                        let base = model.last_index().0;
                        let batch: Vec<Entry> = (1..=count)
                            .map(|i| Entry {
                                term: current_term,
                                index: Index(base + i as u64),
                                payload: Payload::Command(Bytes::from(vec![0xAB; size])),
                            })
                            .collect();
                        sut.append(&batch);
                        model.append(&batch);
                    }
                    LogOp::TruncateOffset(off) => {
                        let span = model.last_index().0 - model.snapshot_index.0;
                        let idx = Index(model.snapshot_index.0 + 1 + (off as u64 % (span + 2)));
                        sut.truncate_from(idx);
                        model.truncate_from(idx);
                        current_term = model.last_term().max(Term(1));
                    }
                    LogOp::CompactOffset(off) => {
                        let span = model.last_index().0 - model.snapshot_index.0;
                        if span > 0 {
                            let idx = Index(model.snapshot_index.0 + 1 + (off as u64 % span));
                            if let Some(term) = model.term_at(idx) {
                                let snap = Snapshot {
                                    meta: SnapshotMeta { index: idx, term },
                                    data: Bytes::from_static(b"snap"),
                                };
                                sut.compact_to(snap);
                                model.compact_to(idx, term);
                            }
                        }
                    }
                    LogOp::Slice { offset, max_entries, max_bytes } => {
                        let from = Index(offset as u64);
                        prop_assert_eq!(
                            sut.slice(from, max_entries, max_bytes),
                            model.slice(from, max_entries, max_bytes)
                        );
                    }
                    LogOp::Lookup(idx) => {
                        let q = Index(idx as u64);
                        prop_assert_eq!(sut.term_at(q), model.term_at(q));
                        if let Some(t) = model.term_at(q) {
                            prop_assert!(sut.matches(q, t));
                        }
                    }
                }
                prop_assert_eq!(sut.last_index(), model.last_index());
                prop_assert_eq!(sut.last_term(), model.last_term());
                prop_assert_eq!(sut.snapshot_index(), model.snapshot_index);
                prop_assert_eq!(sut.snapshot_term(), model.snapshot_term);
            }
        }
    }

    #[test]
    fn from_recovered_rejects_gaps() {
        let err = RaftLog::from_recovered(
            None,
            vec![Entry {
                term: Term(1),
                index: Index(2),
                payload: Payload::Noop,
            }],
        )
        .unwrap_err();
        assert_eq!(
            err,
            ConfigError::NonContiguousLog {
                expected: Index(1),
                actual: Index(2),
            }
        );
    }

    #[test]
    fn conflict_info_reports_first_index_of_conflicting_term() {
        let mut log = RaftLog::from_recovered(None, Vec::new()).unwrap();
        let entries = vec![
            Entry {
                term: Term(1),
                index: Index(1),
                payload: Payload::Noop,
            },
            Entry {
                term: Term(2),
                index: Index(2),
                payload: Payload::Noop,
            },
            Entry {
                term: Term(2),
                index: Index(3),
                payload: Payload::Noop,
            },
            Entry {
                term: Term(2),
                index: Index(4),
                payload: Payload::Noop,
            },
            Entry {
                term: Term(4),
                index: Index(5),
                payload: Payload::Noop,
            },
        ];
        log.append(&entries);

        assert_eq!(log.conflict_info(Index(4)), (Index(2), Term(2)));
        assert_eq!(log.conflict_info(Index(7)), (Index(6), Term(0)));
        assert_eq!(log.last_index_with_term(Term(2)), Some(Index(4)));
        assert_eq!(log.last_index_with_term(Term(3)), None);
        assert_eq!(log.entry_at(Index(3)).map(|e| e.term), Some(Term(2)));
        assert_eq!(log.entries_from(Index(4)).len(), 2);
        assert!(log.is_up_to_date(Index(5), Term(4)));
        assert!(!log.is_up_to_date(Index(4), Term(4)));

        let snap = Snapshot {
            meta: SnapshotMeta {
                index: Index(10),
                term: Term(5),
            },
            data: Bytes::from_static(b"image"),
        };
        log.install_snapshot(snap);
        assert_eq!(log.snapshot().map(|s| s.meta.index), Some(Index(10)));
        assert_eq!(log.last_index(), Index(10));
        assert_eq!(log.entry_at(Index(10)), None);
    }
}
