//! Deterministic simulated network with bounded delays, drops, duplication, and partitions.

use std::collections::{BTreeMap, BTreeSet};

use raft_core::{Message, NodeId};
use rand::RngExt;

/// Simulated message delivery network driven by a virtual tick clock.
#[derive(Debug, Clone)]
pub struct SimNetwork {
    min_delay: u64,
    max_delay: u64,
    drop_prob: f64,
    dup_prob: f64,
    next_seq: u64,
    queue: BTreeMap<(u64, u64), Message>,
    partitions: BTreeMap<NodeId, usize>,
    blocked_links: BTreeSet<(NodeId, NodeId)>,
}

impl SimNetwork {
    /// Creates a simulated network for `nodes` with the given baseline delivery parameters.
    pub fn new(
        nodes: &[NodeId],
        min_delay: u64,
        max_delay: u64,
        drop_prob: f64,
        dup_prob: f64,
    ) -> Self {
        let partitions = nodes.iter().map(|&id| (id, 0usize)).collect();
        Self {
            min_delay: min_delay.max(1),
            max_delay: max_delay.max(min_delay.max(1)),
            drop_prob,
            dup_prob,
            next_seq: 1,
            queue: BTreeMap::new(),
            partitions,
            blocked_links: BTreeSet::new(),
        }
    }

    /// Assigns `node` to partition group `group`. Nodes in different groups cannot communicate.
    pub fn set_partition_group(&mut self, node: NodeId, group: usize) {
        self.partitions.insert(node, group);
    }

    /// Blocks one-way delivery from `from` to `to`.
    pub fn block_link(&mut self, from: NodeId, to: NodeId) {
        self.blocked_links.insert((from, to));
    }

    /// Isolates `node` from all other nodes in both directions.
    pub fn isolate_node(&mut self, node: NodeId) {
        let next_group = self.partitions.values().copied().max().unwrap_or(0) + 1;
        self.partitions.insert(node, next_group);
    }

    /// Updates transient packet loss and delay bounds.
    pub fn set_conditions(&mut self, min_delay: u64, max_delay: u64, drop_prob: f64) {
        self.min_delay = min_delay.max(1);
        self.max_delay = max_delay.max(self.min_delay);
        self.drop_prob = drop_prob.clamp(0.0, 1.0);
    }

    /// Removes all partitions and link blocks and restores clean low-latency delivery.
    pub fn heal(&mut self) {
        for group in self.partitions.values_mut() {
            *group = 0;
        }
        self.blocked_links.clear();
        self.drop_prob = 0.0;
        self.dup_prob = 0.0;
        self.min_delay = 1;
        self.max_delay = 2;
    }

    /// Enqueues `msg` for future delivery unless partitioned, blocked, or randomly dropped.
    pub fn send(&mut self, msg: Message, now_tick: u64, rng: &mut impl rand::Rng) {
        let from_group = self.partitions.get(&msg.from).copied().unwrap_or(0);
        let to_group = self.partitions.get(&msg.to).copied().unwrap_or(0);
        if from_group != to_group || self.blocked_links.contains(&(msg.from, msg.to)) {
            return;
        }
        if self.drop_prob > 0.0 && rng.random_bool(self.drop_prob) {
            return;
        }

        let delay = rng.random_range(self.min_delay..=self.max_delay);
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.insert((now_tick + delay, seq), msg.clone());

        if self.dup_prob > 0.0 && rng.random_bool(self.dup_prob) {
            let dup_delay = rng.random_range(self.min_delay..=self.max_delay);
            let dup_seq = self.next_seq;
            self.next_seq += 1;
            self.queue.insert((now_tick + dup_delay, dup_seq), msg);
        }
    }

    /// Drains all messages scheduled for delivery at or before `now_tick`.
    pub fn drain_due(&mut self, now_tick: u64) -> Vec<Message> {
        let mut due = Vec::new();
        while let Some((&(tick, seq), _)) = self.queue.first_key_value() {
            if tick > now_tick {
                break;
            }
            if let Some(msg) = self.queue.remove(&(tick, seq)) {
                due.push(msg);
            }
        }
        due
    }
}
