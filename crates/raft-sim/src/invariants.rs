//! Safety invariants I1 through I8 verified on every simulation tick.

use std::collections::BTreeMap;

use raft_core::{Entry, Index, NodeId, Payload, Role, Snapshot, Term};

/// Immutable view of one simulated node's state used for invariant checking.
#[derive(Debug, Clone, Copy)]
pub struct NodeSnapshotView<'a> {
    /// Node identifier.
    pub id: NodeId,
    /// Whether the node is currently running.
    pub is_up: bool,
    /// Current volatile Raft role if running.
    pub role: Option<Role>,
    /// Current volatile Raft term if running.
    pub term: Option<Term>,
    /// Current commit index if running.
    pub commit: Option<Index>,
    /// Current applied index if running.
    pub applied: Option<Index>,
    /// Last durable snapshot stored in `FaultFs`.
    pub durable_snapshot: Option<&'a Snapshot>,
    /// Durable log entries recovered from `FaultFs`.
    pub durable_entries: &'a [Entry],
    /// Durable hard-state term recovered from `FaultFs`.
    pub durable_term: Term,
    /// Durable hard-state vote recovered from `FaultFs`.
    pub durable_voted_for: Option<NodeId>,
    /// Durable hard-state commit index recovered from `FaultFs`.
    pub durable_commit: Index,
}

/// Persistent cross-tick oracle verifying invariants I1 through I8.
#[derive(Debug, Default)]
pub struct InvariantChecker {
    quorum: usize,
    /// I1: Term -> leader id observed in that term.
    leaders_by_term: BTreeMap<Term, NodeId>,
    /// I2: Last known durable leader log entries per `(NodeId, Term)`.
    leader_logs: BTreeMap<(NodeId, Term), Vec<Entry>>,
    /// I4 & I5 & I8: Committed entries ever observed anywhere: `Index -> (Entry, commit_term)`.
    committed_entries: BTreeMap<Index, (Entry, Term)>,
    /// I5: Applied commands ever observed anywhere: `Index -> Payload`.
    applied_payloads: BTreeMap<Index, Payload>,
    /// I6: Monotonic durable term per node across restarts.
    max_durable_term: BTreeMap<NodeId, Term>,
    /// I6: Monotonic durable commit per node across restarts.
    max_durable_commit: BTreeMap<NodeId, Index>,
    /// I6: Monotonic applied index per node within an uptime epoch.
    last_up_applied: BTreeMap<NodeId, Index>,
    /// I7: Durable votes cast per (`NodeId`, `Term`).
    durable_votes: BTreeMap<(NodeId, Term), NodeId>,
}

impl InvariantChecker {
    /// Creates an invariant checker for a cluster of `node_count` voting members.
    pub fn new(node_count: usize) -> Self {
        Self {
            quorum: node_count / 2 + 1,
            ..Self::default()
        }
    }

    /// Checks all invariants I1 through I8 against `views` at `tick`.
    pub fn check(
        &mut self,
        tick: u64,
        views: &[NodeSnapshotView<'_>],
        newly_committed: &[(NodeId, Vec<Entry>)],
        state_changed: bool,
    ) -> Result<(), String> {
        let node_terms: BTreeMap<NodeId, Term> = views
            .iter()
            .filter_map(|v| v.term.map(|t| (v.id, t)))
            .collect();

        // I5 & record newly committed entries.
        for (node_id, entries) in newly_committed {
            let observer_term = node_terms.get(node_id).copied().unwrap_or(Term(0));
            for e in entries {
                if let Some((prev, prev_commit_term)) = self.committed_entries.get_mut(&e.index) {
                    if prev.term != e.term || prev.payload != e.payload {
                        return Err(format!(
                            "I5 StateMachineSafety violated at tick {tick} index {}: node {node_id} saw term {} vs {}",
                            e.index, e.term, prev.term
                        ));
                    }
                    if observer_term < *prev_commit_term {
                        *prev_commit_term = observer_term;
                    }
                } else {
                    self.committed_entries
                        .insert(e.index, (e.clone(), observer_term.max(e.term)));
                }
                if let Some(prev_p) = self.applied_payloads.get(&e.index) {
                    if prev_p != &e.payload {
                        return Err(format!(
                            "I5 StateMachineSafety violated at tick {tick} index {}: differing payload",
                            e.index
                        ));
                    }
                } else {
                    self.applied_payloads.insert(e.index, e.payload.clone());
                }
            }
        }

        for v in views {
            // I6: Monotonicity of durable term & commit across restarts.
            let prev_dt = self.max_durable_term.get(&v.id).copied().unwrap_or(Term(0));
            if v.durable_term < prev_dt {
                return Err(format!(
                    "I6 Monotonicity violated at tick {tick} on node {}: durable term {} < previous {}",
                    v.id, v.durable_term, prev_dt
                ));
            }
            self.max_durable_term.insert(v.id, v.durable_term);

            let prev_dc = self
                .max_durable_commit
                .get(&v.id)
                .copied()
                .unwrap_or(Index(0));
            if v.durable_commit < prev_dc {
                return Err(format!(
                    "I6 Monotonicity violated at tick {tick} on node {}: durable commit {} < previous {}",
                    v.id, v.durable_commit, prev_dc
                ));
            }
            self.max_durable_commit.insert(v.id, v.durable_commit);

            // I7: Vote stability across restarts.
            if let Some(voted) = v.durable_voted_for {
                if let Some(&prev_vote) = self.durable_votes.get(&(v.id, v.durable_term)) {
                    if prev_vote != voted {
                        return Err(format!(
                            "I7 VoteStability violated at tick {tick} on node {} term {}: voted {} after {}",
                            v.id, v.durable_term, voted, prev_vote
                        ));
                    }
                } else {
                    self.durable_votes.insert((v.id, v.durable_term), voted);
                }
            }

            if !v.is_up {
                self.last_up_applied.remove(&v.id);
                continue;
            }

            if let Some(applied) = v.applied {
                let prev_app = self.last_up_applied.get(&v.id).copied().unwrap_or(Index(0));
                if applied < prev_app {
                    return Err(format!(
                        "I6 Monotonicity violated at tick {tick} on node {}: applied {} < previous {}",
                        v.id, applied, prev_app
                    ));
                }
                self.last_up_applied.insert(v.id, applied);
            }

            // I1: Election safety & I2: Leader append-only.
            if v.role == Some(Role::Leader)
                && let Some(term) = v.term
            {
                if let Some(&prev_leader) = self.leaders_by_term.get(&term) {
                    if prev_leader != v.id {
                        return Err(format!(
                            "I1 ElectionSafety violated at tick {tick} term {term}: nodes {prev_leader} and {} both leaders",
                            v.id
                        ));
                    }
                } else {
                    self.leaders_by_term.insert(term, v.id);
                }

                if state_changed {
                    let prev_entries = self
                        .leader_logs
                        .entry((v.id, term))
                        .or_insert_with(|| v.durable_entries.to_vec());
                    for prev in prev_entries.iter() {
                        if let Some(curr) = durable_entry_at(v.durable_entries, prev.index)
                            && curr != prev
                        {
                            return Err(format!(
                                "I2 LeaderAppendOnly violated at tick {tick} on leader {} term {} index {}",
                                v.id, term, prev.index
                            ));
                        }
                    }
                    if v.durable_entries.len() > prev_entries.len() {
                        *prev_entries = v.durable_entries.to_vec();
                    }

                    // I4: Leader completeness for committed entries from <= this leader's term.
                    let snap_idx = v.durable_snapshot.map_or(Index(0), |s| s.meta.index);
                    for (&c_idx, (c_entry, commit_term)) in &self.committed_entries {
                        if c_idx <= snap_idx || *commit_term > term {
                            continue;
                        }
                        let present = durable_entry_at(v.durable_entries, c_idx).is_some_and(|e| {
                            e.term == c_entry.term && e.payload == c_entry.payload
                        });
                        if !present {
                            return Err(format!(
                                "I4 LeaderCompleteness violated at tick {tick}: leader {} term {term} missing committed index {} (entry_term={}, commit_term={})",
                                v.id, c_idx, c_entry.term, commit_term
                            ));
                        }
                    }
                }
            }
        }

        if state_changed {
            // I3: Log matching across durable logs of all nodes.
            for i in 0..views.len() {
                for j in (i + 1)..views.len() {
                    check_log_matching(tick, &views[i], &views[j])?;
                }
            }

            // I8: Every ever-committed entry is present on a quorum of durable node logs/snapshots.
            for (&c_idx, (c_entry, _)) in &self.committed_entries {
                let durable_holders = views
                    .iter()
                    .filter(|v| node_durably_holds(v, c_idx, c_entry))
                    .count();
                if durable_holders < self.quorum {
                    return Err(format!(
                        "I8 Durability violated at tick {tick}: committed index {c_idx} held durably by {durable_holders} < quorum {}",
                        self.quorum
                    ));
                }
            }
        }

        Ok(())
    }
}

fn durable_entry_at(entries: &[Entry], idx: Index) -> Option<&Entry> {
    let first = entries.first()?.index;
    if idx < first {
        return None;
    }
    let off = (idx.0 - first.0) as usize;
    entries.get(off).filter(|e| e.index == idx)
}

fn check_log_matching(
    tick: u64,
    a: &NodeSnapshotView<'_>,
    b: &NodeSnapshotView<'_>,
) -> Result<(), String> {
    let min_snap = a
        .durable_snapshot
        .map_or(Index(0), |s| s.meta.index)
        .max(b.durable_snapshot.map_or(Index(0), |s| s.meta.index));

    let mut matched_at_or_below = None;
    for ea in a.durable_entries {
        if ea.index <= min_snap {
            continue;
        }
        if let Some(eb) = durable_entry_at(b.durable_entries, ea.index)
            && ea.term == eb.term
        {
            if ea.payload != eb.payload {
                return Err(format!(
                    "I3 LogMatching violated at tick {tick} index {}: differing payload in same term {}",
                    ea.index, ea.term
                ));
            }
            matched_at_or_below = Some(ea.index);
        }
    }

    if let Some(max_match) = matched_at_or_below {
        for ea in a.durable_entries {
            if ea.index > min_snap
                && ea.index <= max_match
                && let Some(eb) = durable_entry_at(b.durable_entries, ea.index)
                && ea != eb
            {
                return Err(format!(
                    "I3 LogMatching violated at tick {tick}: entries diverge at {} <= matched {}",
                    ea.index, max_match
                ));
            }
        }
    }
    Ok(())
}

fn node_durably_holds(v: &NodeSnapshotView<'_>, idx: Index, expected: &Entry) -> bool {
    if let Some(snap) = v.durable_snapshot
        && snap.meta.index >= idx
    {
        return true;
    }
    durable_entry_at(v.durable_entries, idx)
        .is_some_and(|e| e.term == expected.term && e.payload == expected.payload)
}
