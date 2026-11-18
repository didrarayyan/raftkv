//! Single-threaded deterministic cluster simulator driving real `raft-core`, `raftkv-kv`, and `raftkv-wal`.

use std::collections::{BTreeMap, VecDeque};
use std::path::Path;

use bytes::Bytes;
use raft_core::{
    Config, Entry, HardState, Index, NodeId, ProposeError, RaftNode, ReadError, Role, Snapshot,
    SnapshotMeta, Term,
};
use raftkv_kv::{Applied, KvStore};
use raftkv_wal::{Batch, FaultFs, Wal, WalError, WalOptions};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;

use crate::client::{
    ClientDriver, InFlightRequest, SimOpKind, SimOpOutcome, outcome_from_op_result,
};
use crate::invariants::{InvariantChecker, NodeSnapshotView};
use crate::linearizability::verify_linearizability;
use crate::nemesis::{FaultAction, Nemesis};
use crate::network::SimNetwork;

/// Configuration for one deterministic cluster simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Master PRNG seed controlling all randomness in the run.
    pub seed: u64,
    /// Number of voting Raft nodes (`3` or `5`).
    pub node_count: usize,
    /// Total virtual ticks to execute (including the heal window).
    pub total_ticks: u64,
    /// Number of clean-network ticks at the end of the run to verify liveness convergence.
    pub heal_ticks: u64,
    /// Minimum virtual message delay in ticks.
    pub min_delay: u64,
    /// Maximum virtual message delay in ticks.
    pub max_delay: u64,
    /// Baseline probability of dropping an in-flight network message.
    pub drop_prob: f64,
    /// Baseline probability of duplicating an in-flight network message.
    pub dup_prob: f64,
    /// Number of applied entries between automatic snapshots per node.
    pub snapshot_threshold: u64,
    /// Target WAL segment rotation size in bytes.
    pub wal_segment_bytes: u64,
    /// Entries retained below snapshots before pruning old WAL segments.
    pub retain_entries: u64,
    /// Enables automated nemesis faults during the run.
    pub nemesis_enabled: bool,
    /// Enables simulated KV clients issuing concurrent requests.
    pub clients_enabled: bool,
    /// Enables occasional injected disk `EIO` faults in the nemesis.
    pub allow_eio: bool,
}

impl SimConfig {
    /// Standard fault-injection simulation configuration for a given `seed` and cluster size.
    pub fn standard(seed: u64, node_count: usize) -> Self {
        Self {
            seed,
            node_count,
            total_ticks: 20_000,
            heal_ticks: 500,
            min_delay: 1,
            max_delay: 5,
            drop_prob: 0.05,
            dup_prob: 0.02,
            snapshot_threshold: 30,
            wal_segment_bytes: 2048,
            retain_entries: 10,
            nemesis_enabled: true,
            clients_enabled: true,
            allow_eio: true,
        }
    }
}

/// Summary metrics from a passing simulation run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    /// Seed executed.
    pub seed: u64,
    /// Cluster size.
    pub node_count: usize,
    /// Total ticks simulated.
    pub ticks_executed: u64,
    /// Completed client operations checked for linearizability.
    pub completed_ops: usize,
    /// Highest committed log index observed.
    pub max_commit: Index,
    /// Deterministic FNV-1a trace hash across every simulation step.
    pub trace_hash: u64,
}

/// Invariant or liveness failure detected during simulation.
#[derive(Debug, Clone)]
pub struct Violation {
    /// Seed that triggered the failure.
    pub seed: u64,
    /// Cluster size.
    pub node_count: usize,
    /// Simulation tick when the failure occurred.
    pub tick: u64,
    /// Human-readable violation summary.
    pub message: String,
    /// Recent trace events leading up to the failure (capped at 200).
    pub recent_trace: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeLifecycle {
    Up,
    Crashed { restart_tick: u64 },
    Halted,
}

#[derive(Debug, Clone)]
struct PendingSimWrite {
    term: Term,
    attempt_token: u64,
}

#[derive(Debug, Clone)]
struct PendingSimRead {
    key: Bytes,
    attempt_token: u64,
}

struct SimNode {
    id: NodeId,
    peers: Vec<NodeId>,
    base_seed: u64,
    fs: FaultFs,
    wal: Option<Wal<FaultFs>>,
    raft: Option<RaftNode>,
    kv: KvStore,
    lifecycle: NodeLifecycle,
    last_snapshot_index: Index,
    last_applied_term: Term,
    next_read_ctx: u64,
    pending_writes: BTreeMap<Index, PendingSimWrite>,
    pending_reads_by_ctx: BTreeMap<u64, PendingSimRead>,
    waiting_reads_by_index: BTreeMap<Index, Vec<PendingSimRead>>,
    durable_snapshot: Option<Snapshot>,
    durable_entries: Vec<Entry>,
    durable_hard_state: HardState,
}

impl SimNode {
    fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        seed: u64,
        wal_opts: WalOptions,
    ) -> Result<Self, String> {
        let fs = FaultFs::new(seed ^ id.0);
        let (wal, recovered) =
            Wal::open(fs.clone(), Path::new("/data"), wal_opts).map_err(|e| e.to_string())?;
        let mut kv = KvStore::new();
        if let Some(snap) = &recovered.snapshot {
            kv.restore(snap.meta.index, &snap.data)
                .map_err(|e| e.to_string())?;
        }
        let last_snapshot_index = recovered
            .snapshot
            .as_ref()
            .map_or(Index(0), |s| s.meta.index);
        let last_applied_term = recovered.snapshot.as_ref().map_or(Term(0), |s| s.meta.term);
        let durable_snapshot = recovered.snapshot.clone();
        let durable_entries = recovered.entries.clone();
        let durable_hard_state = recovered.hard_state;

        let cfg = Config::with_defaults(id, peers.clone(), seed ^ id.0.wrapping_mul(0x9E37_79B9));
        let raft = RaftNode::new(cfg, recovered).map_err(|e| e.to_string())?;

        Ok(Self {
            id,
            peers,
            base_seed: seed,
            fs,
            wal: Some(wal),
            raft: Some(raft),
            kv,
            lifecycle: NodeLifecycle::Up,
            last_snapshot_index,
            last_applied_term,
            next_read_ctx: 1,
            pending_writes: BTreeMap::new(),
            pending_reads_by_ctx: BTreeMap::new(),
            waiting_reads_by_index: BTreeMap::new(),
            durable_snapshot,
            durable_entries,
            durable_hard_state,
        })
    }

    fn is_up(&self) -> bool {
        self.lifecycle == NodeLifecycle::Up
    }

    fn view(&self) -> NodeSnapshotView<'_> {
        let status = self.raft.as_ref().map(RaftNode::status);
        NodeSnapshotView {
            id: self.id,
            is_up: self.is_up(),
            role: status.as_ref().map(|s| s.role),
            term: status.as_ref().map(|s| s.term),
            commit: status.as_ref().map(|s| s.commit),
            applied: Some(self.kv.applied_index()),
            durable_snapshot: self.durable_snapshot.as_ref(),
            durable_entries: &self.durable_entries,
            durable_term: self.durable_hard_state.term,
            durable_voted_for: self.durable_hard_state.voted_for,
            durable_commit: self.durable_hard_state.commit,
        }
    }

    fn crash(&mut self, restart_tick: u64, rng: &mut impl rand::Rng, torn_writes: bool) {
        self.wal = None;
        self.raft = None;
        self.kv = KvStore::new();
        self.pending_writes.clear();
        self.pending_reads_by_ctx.clear();
        self.waiting_reads_by_index.clear();
        self.fs.crash(rng, torn_writes);
        self.lifecycle = NodeLifecycle::Crashed { restart_tick };
    }

    fn halt_eio(&mut self) {
        self.wal = None;
        self.raft = None;
        self.pending_writes.clear();
        self.pending_reads_by_ctx.clear();
        self.waiting_reads_by_index.clear();
        self.lifecycle = NodeLifecycle::Halted;
    }

    fn restart(&mut self, wal_opts: WalOptions, tick: u64) -> Result<(), String> {
        let (wal, recovered) =
            Wal::open(self.fs.clone(), Path::new("/data"), wal_opts).map_err(|e| e.to_string())?;
        let mut kv = KvStore::new();
        if let Some(snap) = &recovered.snapshot {
            kv.restore(snap.meta.index, &snap.data)
                .map_err(|e| e.to_string())?;
        }
        self.last_snapshot_index = recovered
            .snapshot
            .as_ref()
            .map_or(Index(0), |s| s.meta.index);
        self.last_applied_term = recovered.snapshot.as_ref().map_or(Term(0), |s| s.meta.term);
        self.durable_snapshot.clone_from(&recovered.snapshot);
        self.durable_entries.clone_from(&recovered.entries);
        self.durable_hard_state = recovered.hard_state;

        let cfg = Config::with_defaults(
            self.id,
            self.peers.clone(),
            self.base_seed ^ self.id.0.wrapping_mul(0x9E37_79B9) ^ tick,
        );
        let raft = RaftNode::new(cfg, recovered).map_err(|e| e.to_string())?;
        self.wal = Some(wal);
        self.raft = Some(raft);
        self.kv = kv;
        self.lifecycle = NodeLifecycle::Up;
        Ok(())
    }
}

/// Single-threaded deterministic Raft cluster simulator.
pub struct Sim {
    cfg: SimConfig,
    rng: ChaCha8Rng,
    tick: u64,
    nodes: BTreeMap<NodeId, SimNode>,
    network: SimNetwork,
    nemesis: Nemesis,
    clients: ClientDriver,
    invariants: InvariantChecker,
    trace: VecDeque<String>,
    trace_hash: u64,
    wal_opts: WalOptions,
}

impl Sim {
    /// Initializes a deterministic simulation world from `cfg`.
    ///
    /// # Panics
    ///
    /// Panics if `cfg.node_count` produces an invalid `raft-core` configuration (for example `0` or `> 7`).
    pub fn new(cfg: SimConfig) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        let peers: Vec<NodeId> = (1..=cfg.node_count as u64).map(NodeId).collect();
        let wal_opts = WalOptions {
            segment_bytes: cfg.wal_segment_bytes,
            retain_entries: cfg.retain_entries,
        };
        let mut nodes = BTreeMap::new();
        for &id in &peers {
            let node = SimNode::new(id, peers.clone(), cfg.seed, wal_opts)
                .expect("INVARIANT: fresh FaultFs WAL open cannot fail");
            nodes.insert(id, node);
        }
        let network = SimNetwork::new(
            &peers,
            cfg.min_delay,
            cfg.max_delay,
            cfg.drop_prob,
            cfg.dup_prob,
        );
        let heal_start = cfg.total_ticks.saturating_sub(cfg.heal_ticks);
        let nemesis = Nemesis::new(heal_start, cfg.node_count, cfg.allow_eio);
        let clients = ClientDriver::new(3, 50);
        let invariants = InvariantChecker::new(cfg.node_count);

        Self {
            cfg,
            rng,
            tick: 0,
            nodes,
            network,
            nemesis,
            clients,
            invariants,
            trace: VecDeque::with_capacity(200),
            trace_hash: 0xcbf2_9ce4_8422_2325,
            wal_opts,
        }
    }

    /// Runs the configured simulation to completion, verifying I1-I8, linearizability, and liveness.
    pub fn run(&mut self) -> Result<RunReport, Violation> {
        let heal_start = self.cfg.total_ticks.saturating_sub(self.cfg.heal_ticks);
        let settle_only_start = self.cfg.total_ticks.saturating_sub(120);
        let mut entered_heal = false;

        for t in 0..self.cfg.total_ticks {
            self.tick = t;
            if t >= heal_start && !entered_heal {
                entered_heal = true;
                self.enter_heal_phase()?;
            }
            self.step_one_tick(t < settle_only_start)?;
        }

        if let Err(msg) = verify_linearizability(self.clients.history()) {
            return Err(self.violation(msg));
        }
        self.verify_liveness()?;

        let max_commit = self
            .nodes
            .values()
            .filter_map(|n| n.raft.as_ref().map(|r| r.status().commit))
            .max()
            .unwrap_or(Index(0));
        let completed_ops = self
            .clients
            .history()
            .iter()
            .filter(|o| o.complete_tick.is_some())
            .count();

        Ok(RunReport {
            seed: self.cfg.seed,
            node_count: self.cfg.node_count,
            ticks_executed: self.cfg.total_ticks,
            completed_ops,
            max_commit,
            trace_hash: self.trace_hash,
        })
    }

    /// Executes exactly `ticks` simulation steps without forcing a heal/liveness check (for scripted tests).
    pub fn step_ticks(&mut self, ticks: u64) -> Result<(), Violation> {
        for _ in 0..ticks {
            let t = self.tick;
            self.step_one_tick(true)?;
            self.tick = t + 1;
        }
        Ok(())
    }

    /// Isolates `node` in the simulated network (used by scripted scenarios).
    pub fn isolate_node(&mut self, node: NodeId) {
        self.network.isolate_node(node);
        self.record_event(format!("t={} isolate({})", self.tick, node));
    }

    /// Places `nodes` into partition group `group` (used by scripted scenarios).
    pub fn set_partition_group(&mut self, node: NodeId, group: usize) {
        self.network.set_partition_group(node, group);
    }

    /// Blocks one-way network traffic from `from` to `to`.
    pub fn block_link(&mut self, from: NodeId, to: NodeId) {
        self.network.block_link(from, to);
    }

    /// Heals all network partitions and restores clean delivery (used by scripted scenarios).
    pub fn heal_network(&mut self) {
        self.network.heal();
        self.record_event(format!("t={} heal_network", self.tick));
    }

    /// Crashes `node` immediately with optional torn-tail bytes (used by scripted scenarios).
    pub fn crash_node(&mut self, node: NodeId, restart_tick: u64, torn_writes: bool) {
        if let Some(n) = self.nodes.get_mut(&node)
            && n.is_up()
        {
            n.crash(restart_tick, &mut self.rng, torn_writes);
            self.record_event(format!("t={} crash({})", self.tick, node));
        }
    }

    /// Restarts `node` from its `FaultFs` WAL immediately (used by scripted scenarios).
    pub fn restart_node(&mut self, node: NodeId) -> Result<(), Violation> {
        if let Some(n) = self.nodes.get_mut(&node)
            && matches!(n.lifecycle, NodeLifecycle::Crashed { .. })
        {
            if let Err(err) = n.restart(self.wal_opts, self.tick) {
                return Err(self.violation(format!("restart failed on {node}: {err}")));
            }
            self.record_event(format!("t={} restart({})", self.tick, node));
        }
        Ok(())
    }

    /// Submits a proposal directly to `node` (used by scripted scenarios).
    pub fn propose_on(&mut self, node: NodeId, data: Bytes) -> Result<Index, ProposeError> {
        let Some(n) = self.nodes.get_mut(&node) else {
            return Err(ProposeError::NotLeader { leader: None });
        };
        let Some(raft) = n.raft.as_mut() else {
            return Err(ProposeError::NotLeader { leader: None });
        };
        raft.propose(data).map(|p| p.index)
    }

    /// Issues a `ReadIndex` directly on `node` (used by scripted scenarios).
    pub fn read_index_on(&mut self, node: NodeId, ctx: u64) -> Result<(), ReadError> {
        let Some(n) = self.nodes.get_mut(&node) else {
            return Err(ReadError::NotLeader { leader: None });
        };
        let Some(raft) = n.raft.as_mut() else {
            return Err(ReadError::NotLeader { leader: None });
        };
        raft.read_index(ctx)
    }

    /// Forces a snapshot & compaction on `node` at its current applied index.
    pub fn force_compact_on(&mut self, node: NodeId) -> Result<(), Violation> {
        let Some(n) = self.nodes.get_mut(&node) else {
            return Err(self.violation(format!("unknown node {node}")));
        };
        let applied = n.kv.applied_index();
        if applied > n.last_snapshot_index {
            let snap = Snapshot {
                meta: SnapshotMeta {
                    index: applied,
                    term: n.last_applied_term,
                },
                data: n.kv.snapshot(),
            };
            if let Some(wal) = n.wal.as_mut() {
                if let Err(err) = wal.save_snapshot(&snap) {
                    return Err(self.violation(format!("force snapshot failed on {node}: {err}")));
                }
                n.durable_snapshot = Some(snap.clone());
                n.durable_entries.retain(|e| e.index > snap.meta.index);
                n.last_snapshot_index = snap.meta.index;
            }
            if let Some(raft) = n.raft.as_mut() {
                raft.compact(snap);
            }
        }
        Ok(())
    }

    /// Reads the `KvStore` directly on `node` (used by scripted scenarios).
    pub fn kv_get(&self, node: NodeId, key: &[u8]) -> Option<Bytes> {
        self.nodes.get(&node).and_then(|n| n.kv.get(key).cloned())
    }

    /// Returns the `FaultFs` handle of `node` (used by scripted scenarios).
    pub fn node_fs(&self, node: NodeId) -> FaultFs {
        self.nodes[&node].fs.clone()
    }

    /// Returns the status of all running nodes.
    pub fn statuses(&self) -> BTreeMap<NodeId, raft_core::Status> {
        self.nodes
            .iter()
            .filter_map(|(&id, n)| n.raft.as_ref().map(|r| (id, r.status())))
            .collect()
    }

    /// Finds the current leader id if a unique live leader exists.
    pub fn leader(&self) -> Option<NodeId> {
        let mut leaders: Vec<NodeId> = self
            .nodes
            .iter()
            .filter_map(|(&id, n)| {
                (n.is_up()
                    && n.raft
                        .as_ref()
                        .is_some_and(|r| r.status().role == Role::Leader))
                .then_some(id)
            })
            .collect();
        if leaders.len() == 1 {
            leaders.pop()
        } else {
            None
        }
    }

    /// Returns the deterministic trace hash computed across all simulated steps.
    pub fn trace_hash(&self) -> u64 {
        self.trace_hash
    }

    /// Returns recorded client history.
    pub fn client_history(&self) -> &[crate::client::HistoryOp] {
        self.clients.history()
    }

    fn enter_heal_phase(&mut self) -> Result<(), Violation> {
        self.network.heal();
        let crashed_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| matches!(n.lifecycle, NodeLifecycle::Crashed { .. }))
            .map(|(&id, _)| id)
            .collect();
        for id in crashed_ids {
            self.restart_node(id)?;
        }
        self.record_event(format!("t={} heal_phase_start", self.tick));
        Ok(())
    }

    fn step_one_tick(&mut self, allow_new_client_ops: bool) -> Result<(), Violation> {
        let mut state_changed = false;

        // 1. Restart any crashed nodes whose restart tick has arrived.
        let due_restarts: Vec<NodeId> = self
            .nodes
            .iter()
            .filter_map(|(&id, n)| match n.lifecycle {
                NodeLifecycle::Crashed { restart_tick } if restart_tick <= self.tick => Some(id),
                _ => None,
            })
            .collect();
        for id in due_restarts {
            self.restart_node(id)?;
            state_changed = true;
        }

        // 2. Nemesis fault injection.
        if self.cfg.nemesis_enabled {
            let all_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            let up_ids: Vec<NodeId> = self
                .nodes
                .iter()
                .filter(|(_, n)| n.is_up())
                .map(|(&id, _)| id)
                .collect();
            let non_halted = self
                .nodes
                .values()
                .filter(|n| n.lifecycle != NodeLifecycle::Halted)
                .count();
            let curr_leader = self.leader();

            if let Some(action) = self.nemesis.maybe_schedule(
                self.tick,
                &all_ids,
                &up_ids,
                curr_leader,
                non_halted,
                &mut self.rng,
            ) {
                self.apply_fault(action);
                state_changed = true;
            }
        }

        // 3. Deliver due network messages.
        let due_msgs = self.network.drain_due(self.tick);
        for msg in due_msgs {
            self.mix_hash(&[1, msg.from.0, msg.to.0, msg.term.0]);
            if let Some(n) = self.nodes.get_mut(&msg.to)
                && n.is_up()
                && let Some(raft) = n.raft.as_mut()
            {
                raft.step(msg);
            }
        }

        // 4. Advance logical clock on every up node.
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for &id in &node_ids {
            if let Some(n) = self.nodes.get_mut(&id)
                && n.is_up()
                && let Some(raft) = n.raft.as_mut()
            {
                raft.tick();
            }
        }

        // 5. Simulated client requests.
        if self.cfg.clients_enabled {
            let live_ids: Vec<NodeId> = self
                .nodes
                .iter()
                .filter(|(_, n)| n.is_up())
                .map(|(&id, _)| id)
                .collect();
            let reqs = self
                .clients
                .step(self.tick, &live_ids, allow_new_client_ops, &mut self.rng);
            for req in reqs {
                self.dispatch_client_request(&req, &live_ids);
            }
        }

        // 6. Drive Ready -> WAL persist -> restore -> messages -> apply -> read_states -> compact -> advance.
        let mut newly_committed_all: Vec<(NodeId, Vec<Entry>)> = Vec::new();
        for &id in &node_ids {
            let (committed, had_ready) = self.drive_node_ready(id)?;
            if had_ready {
                state_changed = true;
            }
            if !committed.is_empty() {
                newly_committed_all.push((id, committed));
            }
        }

        // 7. Verify invariants I1-I8 across all nodes.
        let views: Vec<NodeSnapshotView<'_>> = self.nodes.values().map(SimNode::view).collect();
        for v in &views {
            mix_hash_into(
                &mut self.trace_hash,
                &[
                    2,
                    v.id.0,
                    u64::from(v.is_up),
                    v.durable_term.0,
                    v.durable_commit.0,
                    v.applied.map_or(0, |a| a.0),
                ],
            );
        }
        if let Err(err) =
            self.invariants
                .check(self.tick, &views, &newly_committed_all, state_changed)
        {
            return Err(self.violation(err));
        }

        Ok(())
    }

    fn apply_fault(&mut self, action: FaultAction) {
        match action {
            FaultAction::PartitionMinority(minority) => {
                for &id in self.nodes.keys() {
                    let group = usize::from(minority.contains(&id));
                    self.network.set_partition_group(id, group);
                }
                self.record_event(format!("t={} partition({:?})", self.tick, minority));
            }
            FaultAction::IsolateNode(id) => {
                self.network.isolate_node(id);
                self.record_event(format!("t={} isolate({})", self.tick, id));
            }
            FaultAction::HealNetwork => {
                self.network.heal();
                self.record_event(format!("t={} heal", self.tick));
            }
            FaultAction::CrashNode { node, restart_tick } => {
                if let Some(n) = self.nodes.get_mut(&node)
                    && n.is_up()
                {
                    n.crash(restart_tick, &mut self.rng, true);
                    self.record_event(format!(
                        "t={} crash({} -> restart@{restart_tick})",
                        self.tick, node
                    ));
                }
            }
            FaultAction::ShiftNetwork {
                min_delay,
                max_delay,
                drop_prob,
            } => {
                self.network.set_conditions(min_delay, max_delay, drop_prob);
                self.record_event(format!(
                    "t={} net(delay={min_delay}..={max_delay}, drop={drop_prob:.2})",
                    self.tick
                ));
            }
            FaultAction::InjectEio(node) => {
                if let Some(n) = self.nodes.get_mut(&node)
                    && n.is_up()
                {
                    n.fs.fail_next_io();
                    self.record_event(format!("t={} inject_eio({})", self.tick, node));
                }
            }
        }
    }

    fn dispatch_client_request(&mut self, req: &InFlightRequest, live_ids: &[NodeId]) {
        let token = req.attempt_token;
        let target = req.target;
        let Some(node) = self.nodes.get_mut(&target) else {
            return;
        };
        let Some(raft) = node.raft.as_mut() else {
            if let Some(retry) =
                self.clients
                    .on_retryable_error(token, None, self.tick, live_ids, &mut self.rng)
            {
                let _ = retry;
            }
            return;
        };

        match &req.kind {
            SimOpKind::Get => {
                let ctx = node.next_read_ctx;
                node.next_read_ctx += 1;
                match raft.read_index(ctx) {
                    Ok(()) => {
                        node.pending_reads_by_ctx.insert(
                            ctx,
                            PendingSimRead {
                                key: req.key.clone(),
                                attempt_token: token,
                            },
                        );
                    }
                    Err(ReadError::NotLeader { leader }) => {
                        if let Some(retry) = self.clients.on_retryable_error(
                            token,
                            leader,
                            self.tick,
                            live_ids,
                            &mut self.rng,
                        ) {
                            let _ = retry;
                        }
                    }
                    Err(ReadError::DuplicateContext | ReadError::InvalidContext) => {}
                }
            }
            SimOpKind::Put(_) | SimOpKind::Delete | SimOpKind::Cas { .. } => {
                let payload = req
                    .encode_write()
                    .expect("INVARIANT: write variant encodes");
                match raft.propose(payload) {
                    Ok(prop) => {
                        node.pending_writes.insert(
                            prop.index,
                            PendingSimWrite {
                                term: prop.term,
                                attempt_token: token,
                            },
                        );
                    }
                    Err(ProposeError::NotLeader { leader }) => {
                        if let Some(retry) = self.clients.on_retryable_error(
                            token,
                            leader,
                            self.tick,
                            live_ids,
                            &mut self.rng,
                        ) {
                            let _ = retry;
                        }
                    }
                }
            }
        }
    }

    fn drive_node_ready(&mut self, id: NodeId) -> Result<(Vec<Entry>, bool), Violation> {
        let mut all_committed = Vec::new();
        let mut had_ready = false;
        let live_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.is_up())
            .map(|(&nid, _)| nid)
            .collect();

        loop {
            let ready = {
                // INVARIANT: callers only invoke drain_node_ready with node IDs registered in self.nodes.
                let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                if !n.is_up() {
                    break;
                }
                let Some(raft) = n.raft.as_mut() else {
                    break;
                };
                raft.ready()
            };
            let Some(ready) = ready else {
                break;
            };
            had_ready = true;

            // Step 1: Persist snapshot, entries, and hard state BEFORE messages leave or entries apply.
            let persist_res = {
                let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                let Some(wal) = n.wal.as_mut() else {
                    break;
                };
                wal.persist(Batch {
                    snapshot: ready.snapshot.as_ref(),
                    hard_state: ready.hard_state.as_ref(),
                    entries: &ready.entries,
                })
            };
            match persist_res {
                Ok(()) => {
                    let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                    if let Some(snap) = &ready.snapshot {
                        n.durable_snapshot = Some(snap.clone());
                        n.durable_entries.clear();
                    }
                    if !ready.entries.is_empty() {
                        let first_new = ready.entries[0].index;
                        n.durable_entries.retain(|e| e.index < first_new);
                        n.durable_entries.extend(ready.entries.iter().cloned());
                    }
                    if let Some(hs) = ready.hard_state {
                        n.durable_hard_state = hs;
                    }
                }
                Err(WalError::Io(_)) => {
                    // Fail-stop: halt node permanently for this simulation run.
                    let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                    n.halt_eio();
                    self.record_event(format!("t={} halt_eio({})", self.tick, id));
                    return Ok((all_committed, true));
                }
                Err(other) => {
                    return Err(self.violation(format!("unexpected WAL error on {id}: {other}")));
                }
            }

            // Step 2: Restore snapshot if received from leader.
            if let Some(snap) = &ready.snapshot {
                let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                if let Err(err) = n.kv.restore(snap.meta.index, &snap.data) {
                    return Err(self.violation(format!("restore failed on {id}: {err}")));
                }
                n.last_snapshot_index = snap.meta.index;
                n.last_applied_term = snap.meta.term;
            }

            // Step 3: Deliver outbound messages.
            for msg in ready.messages {
                self.network.send(msg, self.tick, &mut self.rng);
            }

            // Step 4: Apply committed entries in order and notify waiting write clients.
            let mut completed_clients: Vec<(u64, SimOpOutcome)> = Vec::new();
            let mut retry_tokens: Vec<(u64, Option<NodeId>)> = Vec::new();
            let mut apply_err: Option<String> = None;
            {
                let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                for entry in &ready.committed {
                    let applied = match n.kv.apply(entry.index, &entry.payload) {
                        Ok(a) => a,
                        Err(e) => {
                            apply_err = Some(format!("apply failed on {id}: {e}"));
                            break;
                        }
                    };
                    n.last_applied_term = entry.term;

                    if let Some(pw) = n.pending_writes.remove(&entry.index) {
                        if pw.term == entry.term {
                            match applied {
                                Applied::Done(res) | Applied::Duplicate(res) => {
                                    completed_clients
                                        .push((pw.attempt_token, outcome_from_op_result(res)));
                                }
                                Applied::Noop | Applied::Outdated => {
                                    retry_tokens.push((pw.attempt_token, None));
                                }
                            }
                        } else {
                            retry_tokens.push((pw.attempt_token, None));
                        }
                    }
                }
                all_committed.extend(ready.committed.iter().cloned());

                // Step 5 & 6: Register ReadStates and serve any whose index <= applied_index.
                for rs in ready.read_states {
                    if let Some(pr) = n.pending_reads_by_ctx.remove(&rs.ctx) {
                        n.waiting_reads_by_index
                            .entry(rs.index)
                            .or_default()
                            .push(pr);
                    }
                }
                let applied_now = n.kv.applied_index();
                let ready_indices: Vec<Index> = n
                    .waiting_reads_by_index
                    .range(..=applied_now)
                    .map(|(&idx, _)| idx)
                    .collect();
                for ridx in ready_indices {
                    if let Some(reads) = n.waiting_reads_by_index.remove(&ridx) {
                        for r in reads {
                            let val = n.kv.get(&r.key).cloned();
                            completed_clients.push((r.attempt_token, SimOpOutcome::Get(val)));
                        }
                    }
                }

                // Step 7: On leadership loss, release pending writes & reads for client retry.
                if let Some(soft) = ready.soft_state
                    && soft.role != Role::Leader
                {
                    let writes: Vec<PendingSimWrite> = std::mem::take(&mut n.pending_writes)
                        .into_values()
                        .collect();
                    for w in writes {
                        retry_tokens.push((w.attempt_token, soft.leader_id));
                    }
                    let reads_ctx: Vec<PendingSimRead> =
                        std::mem::take(&mut n.pending_reads_by_ctx)
                            .into_values()
                            .collect();
                    for r in reads_ctx {
                        retry_tokens.push((r.attempt_token, soft.leader_id));
                    }
                    let reads_wait: Vec<PendingSimRead> =
                        std::mem::take(&mut n.waiting_reads_by_index)
                            .into_values()
                            .flatten()
                            .collect();
                    for r in reads_wait {
                        retry_tokens.push((r.attempt_token, soft.leader_id));
                    }
                }
            }
            if let Some(err) = apply_err {
                return Err(self.violation(err));
            }

            for (tok, out) in completed_clients {
                self.clients.on_success(tok, self.tick, out);
            }
            for (tok, hint) in retry_tokens {
                self.clients
                    .on_retryable_error(tok, hint, self.tick, &live_ids, &mut self.rng);
            }

            // Step 8: Maybe snapshot & compact.
            let maybe_snap = {
                // INVARIANT: id is a cluster node inserted at startup.
                let n = self.nodes.get(&id).expect("INVARIANT: valid node id");
                let applied = n.kv.applied_index();
                if applied.0.saturating_sub(n.last_snapshot_index.0) >= self.cfg.snapshot_threshold
                {
                    Some(Snapshot {
                        meta: SnapshotMeta {
                            index: applied,
                            term: n.last_applied_term,
                        },
                        data: n.kv.snapshot(),
                    })
                } else {
                    None
                }
            };
            if let Some(snap) = maybe_snap {
                let save_res = {
                    let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                    let Some(wal) = n.wal.as_mut() else {
                        break;
                    };
                    wal.save_snapshot(&snap)
                };
                match save_res {
                    Ok(()) => {
                        let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                        n.durable_snapshot = Some(snap.clone());
                        n.durable_entries.retain(|e| e.index > snap.meta.index);
                        n.last_snapshot_index = snap.meta.index;
                        if let Some(raft) = n.raft.as_mut() {
                            raft.compact(snap);
                        }
                    }
                    Err(WalError::Io(_)) => {
                        let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
                        n.halt_eio();
                        self.record_event(format!("t={} halt_eio_snap({})", self.tick, id));
                        return Ok((all_committed, true));
                    }
                    Err(other) => {
                        return Err(self
                            .violation(format!("unexpected WAL snapshot error on {id}: {other}")));
                    }
                }
            }

            // Step 9: Advance Raft core.
            let n = self.nodes.get_mut(&id).expect("INVARIANT: valid node id");
            if let Some(raft) = n.raft.as_mut() {
                raft.advance();
            }
        }

        Ok((all_committed, had_ready))
    }

    fn verify_liveness(&self) -> Result<(), Violation> {
        let up_nodes: Vec<(&SimNode, &RaftNode)> = self
            .nodes
            .values()
            .filter_map(|n| {
                n.is_up()
                    .then_some(n)
                    .and_then(|n| n.raft.as_ref().map(|r| (n, r)))
            })
            .collect();
        let leaders: Vec<NodeId> = up_nodes
            .iter()
            .filter(|(_, r)| r.status().role == Role::Leader)
            .map(|(_, r)| r.status().id)
            .collect();
        if leaders.len() != 1 {
            let details: Vec<String> = up_nodes
                .iter()
                .map(|(_, r)| format!("{:?}", r.status()))
                .collect();
            return Err(self.violation(format!(
                "Liveness violated after heal: expected 1 leader, found {leaders:?}; statuses: {}",
                details.join(" | ")
            )));
        }

        let (first, first_raft) = up_nodes[0];
        let expected_commit = first_raft.status().commit;
        let expected_applied = first.kv.applied_index();
        let expected_snap = first.kv.snapshot();

        for (node, raft) in &up_nodes[1..] {
            let st = raft.status();
            if st.commit != expected_commit || node.kv.applied_index() != expected_applied {
                return Err(self.violation(format!(
                    "Liveness convergence failed: node {} commit={} applied={} vs node {} commit={} applied={}",
                    node.id, st.commit, node.kv.applied_index(), first.id, expected_commit, expected_applied
                )));
            }
            if node.kv.snapshot() != expected_snap {
                return Err(self.violation(format!(
                    "Liveness state divergence between node {} and node {}",
                    node.id, first.id
                )));
            }
        }
        Ok(())
    }

    fn record_event(&mut self, event: String) {
        if self.trace.len() >= 200 {
            self.trace.pop_front();
        }
        self.trace.push_back(event);
    }

    fn mix_hash(&mut self, values: &[u64]) {
        mix_hash_into(&mut self.trace_hash, values);
    }

    fn violation(&self, message: String) -> Violation {
        Violation {
            seed: self.cfg.seed,
            node_count: self.cfg.node_count,
            tick: self.tick,
            message,
            recent_trace: self.trace.iter().cloned().collect(),
        }
    }
}

fn mix_hash_into(hash: &mut u64, values: &[u64]) {
    for &v in values {
        for b in v.to_le_bytes() {
            *hash ^= u64::from(b);
            *hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
}
