//! Pure Raft consensus state machine driver interface (`RaftNode` and `Ready`).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;

use crate::config::Config;
use crate::error::{ConfigError, ProposeError, ReadError};
use crate::log::RaftLog;
use crate::message::{AppendResult, Message, MessageBody};
use crate::progress::{Progress, ProgressState};
use crate::types::{
    Entry, HardState, Index, NodeId, Payload, PeerStatus, Proposal, ReadState, Recovered, Role,
    Snapshot, SoftState, Status, Term,
};

/// Work items produced by [`RaftNode::ready`] that the driver must process in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ready {
    /// Emitted when role or known leader changed since the previous `Ready`.
    pub soft_state: Option<SoftState>,
    /// Emitted when durable term, vote, or commit index changed since the last `Ready`.
    pub hard_state: Option<HardState>,
    /// Snapshot received from a leader that must be persisted and restored before continuing.
    pub snapshot: Option<Snapshot>,
    /// Log entries to persist; if `entries[0].index <= last persisted`, discard the old suffix first.
    pub entries: Vec<Entry>,
    /// Outbound protocol messages to deliver only after snapshot, entries, and hard state are synced.
    pub messages: Vec<Message>,
    /// Newly committed entries to apply to the state machine in order, exactly once.
    pub committed: Vec<Entry>,
    /// Confirmed linearizable reads ready to serve once `applied >= read_state.index`.
    pub read_states: Vec<ReadState>,
}

#[derive(Debug, Clone)]
struct PendingReady {
    last_entry_index: Option<Index>,
    snapshot_index: Option<Index>,
    hard_state: Option<HardState>,
    soft_state: Option<SoftState>,
    applied_to: Option<Index>,
}

#[derive(Debug, Clone)]
struct PendingReadRound {
    round_ctx: u64,
    acks: BTreeSet<NodeId>,
    reads: Vec<ReadState>,
}

/// Deterministic Raft state machine for a single cluster member.
#[derive(Debug)]
pub struct RaftNode {
    cfg: Config,
    log: RaftLog,
    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    commit: Index,
    applied: Index,
    persisted_index: Index,
    persisted_hard_state: HardState,
    last_soft_state: SoftState,
    pending_snapshot: Option<Snapshot>,
    pending_compact: Option<Snapshot>,
    rng: ChaCha8Rng,
    election_timeout: u32,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    check_quorum_elapsed: u32,
    ticks_since_leader_contact: u32,
    votes: BTreeMap<NodeId, bool>,
    progress: BTreeMap<NodeId, Progress>,
    queued_reads: Vec<u64>,
    pending_read_rounds: VecDeque<PendingReadRound>,
    ready_read_states: Vec<ReadState>,
    outbox: Vec<Message>,
    in_flight_ready: Option<PendingReady>,
}

impl RaftNode {
    /// Creates a new Raft node from validated configuration and recovered durable state.
    pub fn new(cfg: Config, recovered: Recovered) -> Result<Self, ConfigError> {
        cfg.validate()?;
        let log = RaftLog::from_recovered(recovered.snapshot, recovered.entries)?;
        let persisted_index = log.last_index();
        let commit = recovered
            .hard_state
            .commit
            .max(log.snapshot_index())
            .min(log.last_index());
        let applied = log.snapshot_index();
        let persisted_hard_state = recovered.hard_state;

        let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        let election_timeout = sample_election_timeout(&mut rng, cfg.election_ticks);
        let soft_state = SoftState {
            leader_id: None,
            role: Role::Follower,
        };

        Ok(Self {
            ticks_since_leader_contact: cfg.election_ticks,
            cfg,
            log,
            role: Role::Follower,
            term: persisted_hard_state.term,
            voted_for: persisted_hard_state.voted_for,
            leader_id: None,
            commit,
            applied,
            persisted_index,
            persisted_hard_state,
            last_soft_state: soft_state,
            pending_snapshot: None,
            pending_compact: None,
            rng,
            election_timeout,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            check_quorum_elapsed: 0,
            votes: BTreeMap::new(),
            progress: BTreeMap::new(),
            queued_reads: Vec::new(),
            pending_read_rounds: VecDeque::new(),
            ready_read_states: Vec::new(),
            outbox: Vec::new(),
            in_flight_ready: None,
        })
    }

    /// Advances logical time by one tick, driving election timers, heartbeats, and `CheckQuorum`.
    pub fn tick(&mut self) {
        match self.role {
            Role::Leader => self.tick_leader(),
            Role::Follower | Role::PreCandidate | Role::Candidate => self.tick_election(),
        }
    }

    /// Processes a single incoming peer message.
    pub fn step(&mut self, msg: Message) {
        if msg.to != self.cfg.id {
            return;
        }
        // E3 & E7: PreVote and PreVoteResponse never mutate receiver term or vote.
        match msg.body {
            MessageBody::PreVote {
                last_index,
                last_term,
            } => {
                self.handle_pre_vote(msg.from, msg.term, last_index, last_term);
                return;
            }
            MessageBody::PreVoteResponse { granted } => {
                self.handle_pre_vote_response(msg.from, msg.term, granted);
                return;
            }
            _ => {}
        }

        // E4: Ignore higher-term RequestVote if a leader has been heard within election_ticks.
        if let MessageBody::RequestVote { .. } = msg.body {
            let leader_alive = self.role == Role::Leader
                || (self.leader_id.is_some()
                    && self.ticks_since_leader_contact < self.cfg.election_ticks);
            if msg.term > self.term && leader_alive && (self.cfg.check_quorum || self.cfg.pre_vote)
            {
                return;
            }
        }

        // E7: Adopt any higher term from non-PreVote messages, clearing voted_for.
        if msg.term > self.term {
            self.become_follower(msg.term, None);
        }

        if msg.term < self.term {
            match msg.body {
                MessageBody::AppendEntries { read_ctx, .. } => {
                    self.send(
                        msg.from,
                        MessageBody::AppendEntriesResponse {
                            result: AppendResult::Rejected {
                                conflict_index: Index(1),
                                conflict_term: Term(0),
                            },
                            read_ctx,
                        },
                    );
                }
                MessageBody::RequestVote { .. } => {
                    self.send(
                        msg.from,
                        MessageBody::RequestVoteResponse { granted: false },
                    );
                }
                _ => {}
            }
            return;
        }

        match msg.body {
            MessageBody::RequestVote {
                last_index,
                last_term,
            } => self.handle_request_vote(msg.from, last_index, last_term),
            MessageBody::RequestVoteResponse { granted } => {
                self.handle_request_vote_response(msg.from, granted);
            }
            MessageBody::AppendEntries {
                prev_index,
                prev_term,
                entries,
                leader_commit,
                read_ctx,
            } => self.handle_append_entries(
                msg.from,
                prev_index,
                prev_term,
                &entries,
                leader_commit,
                read_ctx,
            ),
            MessageBody::AppendEntriesResponse { result, read_ctx } => {
                self.handle_append_entries_response(msg.from, result, read_ctx);
            }
            MessageBody::InstallSnapshot { snapshot } => {
                self.handle_install_snapshot(msg.from, snapshot);
            }
            MessageBody::InstallSnapshotResponse { match_index } => {
                self.handle_install_snapshot_response(msg.from, match_index);
            }
            MessageBody::PreVote { .. } | MessageBody::PreVoteResponse { .. } => {}
        }
    }

    /// Appends a command proposal on the leader and initiates replication immediately.
    pub fn propose(&mut self, data: Bytes) -> Result<Proposal, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let index = self.log.last_index().next();
        let term = self.term;
        self.log.append(&[Entry {
            term,
            index,
            payload: Payload::Command(data),
        }]);
        self.bcast_append();
        Ok(Proposal { index, term })
    }

    /// Registers a linearizable read request correlated by non-zero `ctx`.
    pub fn read_index(&mut self, ctx: u64) -> Result<(), ReadError> {
        if self.role != Role::Leader {
            return Err(ReadError::NotLeader {
                leader: self.leader_id,
            });
        }
        if ctx == 0 {
            return Err(ReadError::InvalidContext);
        }
        if self.has_read_ctx(ctx) {
            return Err(ReadError::DuplicateContext);
        }

        if self.has_committed_in_current_term() {
            self.enqueue_confirmed_term_read(ctx);
        } else {
            self.queued_reads.push(ctx);
        }
        Ok(())
    }

    /// Discards log entries up to `snapshot.meta.index` after the driver persists the snapshot.
    pub fn compact(&mut self, snapshot: Snapshot) {
        self.log.compact_to(snapshot);
        self.persisted_index = self.persisted_index.max(self.log.snapshot_index());
        self.commit = self.commit.max(self.log.snapshot_index());
        self.applied = self.applied.max(self.log.snapshot_index());
    }

    /// Returns pending work for the driver, or `None` if idle or awaiting `advance()`.
    pub fn ready(&mut self) -> Option<Ready> {
        if self.in_flight_ready.is_some() {
            return None;
        }

        let current_soft = SoftState {
            leader_id: self.leader_id,
            role: self.role,
        };
        let soft_state = (current_soft != self.last_soft_state).then_some(current_soft);

        let current_hard = HardState {
            term: self.term,
            voted_for: self.voted_for,
            commit: self.commit,
        };
        let hard_state = (current_hard != self.persisted_hard_state).then_some(current_hard);

        let snapshot = self.pending_snapshot.take();
        let entries = self.log.entries_from(self.persisted_index.next());
        let messages = std::mem::take(&mut self.outbox);

        let committed_end = self.commit.min(self.log.last_index());
        let committed = if committed_end > self.applied {
            let mut slice = self.log.entries_from(self.applied.next());
            slice.retain(|e| e.index <= committed_end);
            slice
        } else {
            Vec::new()
        };
        let read_states = std::mem::take(&mut self.ready_read_states);

        if soft_state.is_none()
            && hard_state.is_none()
            && snapshot.is_none()
            && entries.is_empty()
            && messages.is_empty()
            && committed.is_empty()
            && read_states.is_empty()
        {
            return None;
        }

        self.in_flight_ready = Some(PendingReady {
            last_entry_index: entries.last().map(|e| e.index),
            snapshot_index: snapshot.as_ref().map(|s| s.meta.index),
            hard_state,
            soft_state,
            applied_to: committed.last().map(|e| e.index),
        });

        Some(Ready {
            soft_state,
            hard_state,
            snapshot,
            entries,
            messages,
            committed,
            read_states,
        })
    }

    /// Marks the most recent [`Ready`] as persisted and applied by the driver.
    pub fn advance(&mut self) {
        let Some(pending) = self.in_flight_ready.take() else {
            return;
        };
        if let Some(soft) = pending.soft_state {
            self.last_soft_state = soft;
        }
        if let Some(hard) = pending.hard_state {
            self.persisted_hard_state = hard;
        }
        if let Some(snap_idx) = pending.snapshot_index {
            self.persisted_index = self.persisted_index.max(snap_idx);
            self.applied = self.applied.max(snap_idx);
        }
        if let Some(idx) = pending.last_entry_index {
            self.persisted_index = self.persisted_index.max(idx).min(self.log.last_index());
        }
        if let Some(applied) = pending.applied_to {
            self.applied = self.applied.max(applied);
        }
        if let Some(snap) = self.pending_compact.as_ref()
            && self.applied >= snap.meta.index
            && let Some(snap) = self.pending_compact.take()
        {
            self.log.compact_to(snap);
            self.persisted_index = self.persisted_index.max(self.log.snapshot_index());
        }
        if self.role == Role::Leader {
            self.maybe_commit();
        }
    }

    /// Returns a diagnostic snapshot of the node's current state.
    pub fn status(&self) -> Status {
        let peers = if self.role == Role::Leader {
            self.progress
                .iter()
                .map(|(&id, p)| PeerStatus {
                    id,
                    match_index: p.match_index,
                    next_index: p.next_index,
                    recently_active: p.recently_active,
                })
                .collect()
        } else {
            Vec::new()
        };

        Status {
            id: self.cfg.id,
            role: self.role,
            term: self.term,
            leader_id: self.leader_id,
            commit: self.commit,
            applied: self.applied,
            last_log_index: self.log.last_index(),
            snapshot_index: self.log.snapshot_index(),
            peers,
        }
    }

    fn quorum(&self) -> usize {
        self.cfg.peers.len() / 2 + 1
    }

    fn remote_peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        let self_id = self.cfg.id;
        self.cfg
            .peers
            .iter()
            .copied()
            .filter(move |&id| id != self_id)
    }

    fn has_committed_in_current_term(&self) -> bool {
        self.log.term_at(self.commit) == Some(self.term)
    }

    fn has_read_ctx(&self, ctx: u64) -> bool {
        self.queued_reads.contains(&ctx)
            || self
                .pending_read_rounds
                .iter()
                .any(|r| r.reads.iter().any(|s| s.ctx == ctx))
            || self.ready_read_states.iter().any(|s| s.ctx == ctx)
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = sample_election_timeout(&mut self.rng, self.cfg.election_ticks);
    }

    fn send(&mut self, to: NodeId, body: MessageBody) {
        self.outbox.push(Message {
            from: self.cfg.id,
            to,
            term: self.term,
            body,
        });
    }

    fn tick_election(&mut self) {
        self.election_elapsed = self.election_elapsed.saturating_add(1);
        self.ticks_since_leader_contact = self.ticks_since_leader_contact.saturating_add(1);

        if self.election_elapsed >= self.election_timeout {
            if self.cfg.pre_vote {
                self.become_pre_candidate();
            } else {
                self.become_candidate();
            }
        }
    }

    fn tick_leader(&mut self) {
        self.heartbeat_elapsed = self.heartbeat_elapsed.saturating_add(1);

        let mut timed_out_snapshots = Vec::new();
        for (&peer, p) in &mut self.progress {
            if let ProgressState::Snapshot { ticks, .. } = &mut p.state {
                *ticks = ticks.saturating_add(1);
                if *ticks >= self.cfg.election_ticks {
                    timed_out_snapshots.push(peer);
                }
            }
        }
        for peer in timed_out_snapshots {
            if let Some(p) = self.progress.get_mut(&peer) {
                p.become_probe();
            }
            self.send_append(peer);
        }

        if self.cfg.check_quorum {
            self.check_quorum_elapsed = self.check_quorum_elapsed.saturating_add(1);
            if self.check_quorum_elapsed >= self.cfg.election_ticks {
                self.check_quorum_elapsed = 0;
                let active_remotes = self.progress.values().filter(|p| p.recently_active).count();
                for p in self.progress.values_mut() {
                    p.recently_active = false;
                }
                if 1 + active_remotes < self.quorum() {
                    self.become_follower(self.term, None);
                    return;
                }
            }
        }

        if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
            self.heartbeat_elapsed = 0;
            let read_ctx = self.pending_read_rounds.back().map_or(0, |r| r.round_ctx);
            self.bcast_heartbeat(read_ctx);
        }
    }

    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader_id = leader_id;
        self.votes.clear();
        self.progress.clear();
        self.queued_reads.clear();
        self.pending_read_rounds.clear();
        self.ready_read_states.clear();
        self.reset_election_timer();
    }

    fn become_pre_candidate(&mut self) {
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        self.reset_election_timer();

        if 1 >= self.quorum() {
            self.become_candidate();
            return;
        }

        let proposed_term = self.term.next();
        let last_index = self.log.last_index();
        let last_term = self.log.last_term();
        let peers: Vec<NodeId> = self.remote_peers().collect();
        for peer in peers {
            self.outbox.push(Message {
                from: self.cfg.id,
                to: peer,
                term: proposed_term,
                body: MessageBody::PreVote {
                    last_index,
                    last_term,
                },
            });
        }
    }

    fn become_candidate(&mut self) {
        self.role = Role::Candidate;
        self.term = self.term.next();
        self.voted_for = Some(self.cfg.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        self.reset_election_timer();

        if 1 >= self.quorum() {
            self.become_leader();
            return;
        }

        let last_index = self.log.last_index();
        let last_term = self.log.last_term();
        let peers: Vec<NodeId> = self.remote_peers().collect();
        for peer in peers {
            self.send(
                peer,
                MessageBody::RequestVote {
                    last_index,
                    last_term,
                },
            );
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.cfg.id);
        self.votes.clear();
        self.heartbeat_elapsed = 0;
        self.check_quorum_elapsed = 0;
        self.ticks_since_leader_contact = 0;
        self.queued_reads.clear();
        self.pending_read_rounds.clear();
        self.ready_read_states.clear();

        let noop_index = self.log.last_index().next();
        self.log.append(&[Entry {
            term: self.term,
            index: noop_index,
            payload: Payload::Noop,
        }]);

        self.progress.clear();
        let peers: Vec<NodeId> = self.remote_peers().collect();
        for peer in peers {
            self.progress.insert(peer, Progress::new(noop_index));
        }

        self.bcast_append();
    }

    fn handle_pre_vote(
        &mut self,
        from: NodeId,
        proposed_term: Term,
        last_index: Index,
        last_term: Term,
    ) {
        let leader_alive = self.role == Role::Leader
            || (self.leader_id.is_some()
                && self.ticks_since_leader_contact < self.cfg.election_ticks);
        let granted = !leader_alive && self.log.is_up_to_date(last_index, last_term);
        let reply_term = if granted { proposed_term } else { self.term };
        self.outbox.push(Message {
            from: self.cfg.id,
            to: from,
            term: reply_term,
            body: MessageBody::PreVoteResponse { granted },
        });
    }

    fn handle_pre_vote_response(&mut self, from: NodeId, msg_term: Term, granted: bool) {
        if self.role != Role::PreCandidate {
            return;
        }
        if granted && msg_term != self.term.next() {
            return;
        }
        self.votes.insert(from, granted);
        let yes = self.votes.values().filter(|&&v| v).count();
        if yes >= self.quorum() {
            self.become_candidate();
            return;
        }
        let no = self.votes.values().filter(|&&v| !v).count();
        if no > self.cfg.peers.len() - self.quorum() {
            self.become_follower(self.term, None);
        }
    }

    fn handle_request_vote(&mut self, from: NodeId, last_index: Index, last_term: Term) {
        let can_vote = (self.voted_for.is_none() || self.voted_for == Some(from))
            && self.log.is_up_to_date(last_index, last_term);
        if can_vote {
            self.voted_for = Some(from);
            self.reset_election_timer();
        }
        self.send(from, MessageBody::RequestVoteResponse { granted: can_vote });
    }

    fn handle_request_vote_response(&mut self, from: NodeId, granted: bool) {
        if self.role != Role::Candidate {
            return;
        }
        self.votes.insert(from, granted);
        let yes = self.votes.values().filter(|&&v| v).count();
        if yes >= self.quorum() {
            self.become_leader();
            return;
        }
        let no = self.votes.values().filter(|&&v| !v).count();
        if no > self.cfg.peers.len() - self.quorum() {
            self.become_follower(self.term, None);
        }
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        prev_index: Index,
        prev_term: Term,
        entries: &[Entry],
        leader_commit: Index,
        read_ctx: u64,
    ) {
        if self.role != Role::Follower || self.leader_id != Some(from) {
            self.become_follower(self.term, Some(from));
        } else {
            self.reset_election_timer();
        }
        self.ticks_since_leader_contact = 0;

        // R7: Skip any prefix already covered by the local snapshot.
        let snap_idx = self.log.snapshot_index();
        let (eff_prev_index, eff_prev_term, eff_entries) = if prev_index < snap_idx {
            let skip = (snap_idx.0 - prev_index.0) as usize;
            if skip > entries.len() {
                let next_commit = leader_commit.min(snap_idx);
                if next_commit > self.commit {
                    self.commit = next_commit;
                }
                self.send(
                    from,
                    MessageBody::AppendEntriesResponse {
                        result: AppendResult::Accepted {
                            match_index: snap_idx,
                        },
                        read_ctx,
                    },
                );
                return;
            }
            if entries[skip - 1].term != self.log.snapshot_term() {
                self.send(
                    from,
                    MessageBody::AppendEntriesResponse {
                        result: AppendResult::Rejected {
                            conflict_index: snap_idx.next(),
                            conflict_term: Term(0),
                        },
                        read_ctx,
                    },
                );
                return;
            }
            (snap_idx, self.log.snapshot_term(), &entries[skip..])
        } else {
            (prev_index, prev_term, entries)
        };

        if !self.log.matches(eff_prev_index, eff_prev_term) {
            let (conflict_index, conflict_term) = self.log.conflict_info(eff_prev_index);
            self.send(
                from,
                MessageBody::AppendEntriesResponse {
                    result: AppendResult::Rejected {
                        conflict_index,
                        conflict_term,
                    },
                    read_ctx,
                },
            );
            return;
        }

        let mut truncate_at = None;
        let mut append_from = eff_entries.len();
        for (idx, entry) in eff_entries.iter().enumerate() {
            match self.log.term_at(entry.index) {
                Some(existing) if existing == entry.term => {}
                Some(_) => {
                    truncate_at = Some(entry.index);
                    append_from = idx;
                    break;
                }
                None => {
                    append_from = idx;
                    break;
                }
            }
        }

        if let Some(idx) = truncate_at {
            self.log.truncate_from(idx);
            self.persisted_index = self.persisted_index.min(idx.prev());
            if let Some(pending) = &mut self.in_flight_ready {
                pending.last_entry_index = pending
                    .last_entry_index
                    .map(|last| last.min(idx.prev()))
                    .filter(|last| *last > self.log.snapshot_index());
            }
        }
        if append_from < eff_entries.len() {
            self.log.append(&eff_entries[append_from..]);
        }

        let last_new_index = Index(eff_prev_index.0 + eff_entries.len() as u64);
        let next_commit = leader_commit.min(last_new_index);
        if next_commit > self.commit {
            self.commit = next_commit;
        }

        self.send(
            from,
            MessageBody::AppendEntriesResponse {
                result: AppendResult::Accepted {
                    match_index: last_new_index,
                },
                read_ctx,
            },
        );
    }

    fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        result: AppendResult,
        read_ctx: u64,
    ) {
        if self.role != Role::Leader {
            return;
        }
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        progress.recently_active = true;

        match result {
            AppendResult::Accepted { match_index } => {
                if match_index > progress.match_index {
                    progress.match_index = match_index;
                }
                if matches!(progress.state, ProgressState::Probe { .. }) {
                    progress.become_replicate();
                }
                progress.next_index = progress.next_index.max(progress.match_index.next());
                let should_send_more = matches!(progress.state, ProgressState::Replicate)
                    && progress.next_index <= self.log.last_index();

                self.maybe_commit();
                if should_send_more {
                    self.send_append(from);
                }
            }
            AppendResult::Rejected {
                conflict_index,
                conflict_term,
            } => {
                if let ProgressState::Probe {
                    last_probe_prev_index: Some(prev),
                    ..
                } = progress.state
                    && prev <= progress.match_index
                {
                    return;
                }
                if matches!(progress.state, ProgressState::Snapshot { .. }) {
                    return;
                }
                if conflict_term == Term(0) && conflict_index <= progress.match_index {
                    return;
                }

                let next = if conflict_term != Term(0)
                    && let Some(last_with_term) = self.log.last_index_with_term(conflict_term)
                {
                    last_with_term.next()
                } else {
                    conflict_index
                };
                progress.next_index = next.max(progress.match_index.next());
                progress.become_probe();
                self.send_append(from);
            }
        }

        if read_ctx != 0 {
            self.record_read_ack(from, read_ctx);
        }
    }

    fn handle_install_snapshot(&mut self, from: NodeId, snapshot: Snapshot) {
        if self.role != Role::Follower || self.leader_id != Some(from) {
            self.become_follower(self.term, Some(from));
        } else {
            self.reset_election_timer();
        }
        self.ticks_since_leader_contact = 0;

        let snap_idx = snapshot.meta.index;
        let snap_term = snapshot.meta.term;

        if snap_idx <= self.log.snapshot_index() {
            self.send(
                from,
                MessageBody::InstallSnapshotResponse {
                    match_index: self.log.snapshot_index(),
                },
            );
            return;
        }

        if self.log.matches(snap_idx, snap_term) {
            self.commit = self.commit.max(snap_idx);
            if self.applied >= snap_idx {
                self.log.compact_to(snapshot);
                self.persisted_index = self.persisted_index.max(snap_idx);
            } else {
                self.pending_compact = Some(snapshot);
            }
            self.send(
                from,
                MessageBody::InstallSnapshotResponse {
                    match_index: snap_idx,
                },
            );
            return;
        }

        self.log.install_snapshot(snapshot.clone());
        self.commit = self.commit.max(snap_idx);
        self.applied = self.applied.max(snap_idx);
        self.persisted_index = snap_idx;
        self.pending_compact = None;
        if let Some(pending) = &mut self.in_flight_ready {
            pending.last_entry_index = None;
            pending.applied_to = None;
        }
        self.pending_snapshot = Some(snapshot);

        self.send(
            from,
            MessageBody::InstallSnapshotResponse {
                match_index: snap_idx,
            },
        );
    }

    fn handle_install_snapshot_response(&mut self, from: NodeId, match_index: Index) {
        if self.role != Role::Leader {
            return;
        }
        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };
        progress.recently_active = true;
        if match_index > progress.match_index {
            progress.match_index = match_index;
        }
        progress.next_index = progress.match_index.next();
        progress.become_probe();
        let next_index = progress.next_index;
        self.maybe_commit();
        if next_index <= self.log.last_index() {
            self.send_append(from);
        }
    }

    fn bcast_append(&mut self) {
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let Some(&progress) = self.progress.get(&peer) else {
            return;
        };
        match progress.state {
            ProgressState::Snapshot { .. }
            | ProgressState::Probe {
                in_flight: true, ..
            } => return,
            ProgressState::Probe {
                in_flight: false, ..
            }
            | ProgressState::Replicate => {}
        }

        if progress.next_index <= self.log.snapshot_index() {
            if let Some(snap) = self.log.snapshot().cloned() {
                if let Some(p) = self.progress.get_mut(&peer) {
                    p.become_snapshot(snap.meta.index);
                }
                self.send(peer, MessageBody::InstallSnapshot { snapshot: snap });
            }
            return;
        }

        let prev_index = progress.next_index.prev();
        let Some(prev_term) = self.log.term_at(prev_index) else {
            return;
        };
        let entries = self.log.slice(
            progress.next_index,
            self.cfg.max_append_entries,
            self.cfg.max_append_bytes,
        );

        if let Some(p) = self.progress.get_mut(&peer) {
            match &mut p.state {
                ProgressState::Probe {
                    in_flight,
                    last_probe_prev_index,
                } => {
                    *in_flight = true;
                    *last_probe_prev_index = Some(prev_index);
                }
                ProgressState::Replicate => {
                    if let Some(last) = entries.last() {
                        p.next_index = last.index.next();
                    }
                }
                ProgressState::Snapshot { .. } => {}
            }
        }

        self.send(
            peer,
            MessageBody::AppendEntries {
                prev_index,
                prev_term,
                entries,
                leader_commit: self.commit,
                read_ctx: 0,
            },
        );
    }

    fn bcast_heartbeat(&mut self, read_ctx: u64) {
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            let Some(progress) = self.progress.get_mut(&peer) else {
                continue;
            };
            let state = progress.state;
            let next_index = progress.next_index;
            let match_index = progress.match_index;

            if matches!(state, ProgressState::Snapshot { .. }) && read_ctx == 0 {
                continue;
            }
            if matches!(state, ProgressState::Probe { .. }) {
                progress.become_probe();
                if next_index <= self.log.last_index() && read_ctx == 0 {
                    self.send_append(peer);
                    continue;
                }
            }

            let prev_index = if match_index >= self.log.snapshot_index() {
                match_index
            } else {
                self.log.snapshot_index()
            };
            let prev_term = self.log.term_at(prev_index).unwrap_or(Term(0));
            self.send(
                peer,
                MessageBody::AppendEntries {
                    prev_index,
                    prev_term,
                    entries: Vec::new(),
                    leader_commit: self.commit,
                    read_ctx,
                },
            );
        }
    }

    fn maybe_commit(&mut self) {
        let mut indices: Vec<Index> = Vec::with_capacity(self.cfg.peers.len());
        indices.push(self.persisted_index);
        for p in self.progress.values() {
            indices.push(p.match_index);
        }
        indices.sort_unstable();
        let quorum_pos = indices.len() - self.quorum();
        let candidate = indices[quorum_pos];
        if candidate > self.commit && self.log.term_at(candidate) == Some(self.term) {
            self.commit = candidate;
            self.flush_queued_reads();
        }
    }

    fn flush_queued_reads(&mut self) {
        if self.queued_reads.is_empty() || !self.has_committed_in_current_term() {
            return;
        }
        let queued = std::mem::take(&mut self.queued_reads);
        for ctx in queued {
            self.enqueue_confirmed_term_read(ctx);
        }
    }

    fn enqueue_confirmed_term_read(&mut self, ctx: u64) {
        let read_state = ReadState {
            ctx,
            index: self.commit,
        };
        if self.cfg.peers.len() == 1 {
            self.ready_read_states.push(read_state);
            return;
        }

        if let Some(last) = self.pending_read_rounds.back_mut()
            && self
                .outbox
                .iter()
                .any(|m| matches!(m.body, MessageBody::AppendEntries { read_ctx, .. } if read_ctx == last.round_ctx))
        {
            last.reads.push(read_state);
            return;
        }

        let mut acks = BTreeSet::new();
        acks.insert(self.cfg.id);
        self.pending_read_rounds.push_back(PendingReadRound {
            round_ctx: ctx,
            acks,
            reads: vec![read_state],
        });
        self.bcast_heartbeat(ctx);
    }

    fn record_read_ack(&mut self, from: NodeId, read_ctx: u64) {
        let Some(pos) = self
            .pending_read_rounds
            .iter()
            .position(|r| r.round_ctx == read_ctx)
        else {
            return;
        };

        for round in self.pending_read_rounds.iter_mut().take(pos + 1) {
            round.acks.insert(from);
        }

        let quorum = self.quorum();
        while let Some(front) = self.pending_read_rounds.front() {
            if front.acks.len() >= quorum {
                if let Some(done) = self.pending_read_rounds.pop_front() {
                    self.ready_read_states.extend(done.reads);
                }
            } else {
                break;
            }
        }
    }
}

fn sample_election_timeout(rng: &mut ChaCha8Rng, election_ticks: u32) -> u32 {
    rng.random_range(election_ticks..election_ticks.saturating_mul(2))
}
