//! Seeded fault scheduler (nemesis) injecting partitions, crashes, and link shifts.

use raft_core::NodeId;
use rand::RngExt;

/// Fault action chosen by [`Nemesis`] at a simulation tick.
#[derive(Debug, Clone)]
pub enum FaultAction {
    /// Split the cluster into two partition groups.
    PartitionMinority(Vec<NodeId>),
    /// Isolate a specific node (such as the current leader) from all peers.
    IsolateNode(NodeId),
    /// Heal all network partitions and link blocks.
    HealNetwork,
    /// Crash `node` and restart it at `restart_tick`.
    CrashNode {
        /// Node to crash.
        node: NodeId,
        /// Tick when the node may restart.
        restart_tick: u64,
    },
    /// Vary transient network delay bounds and packet drop probability.
    ShiftNetwork {
        /// New minimum delivery delay in ticks.
        min_delay: u64,
        /// New maximum delivery delay in ticks.
        max_delay: u64,
        /// New packet drop probability.
        drop_prob: f64,
    },
    /// Inject a permanent `EIO` failure on the next disk sync of `node`.
    InjectEio(NodeId),
}

/// Seeded scheduler emitting randomized fault events during the non-heal window.
#[derive(Debug)]
pub struct Nemesis {
    next_event_tick: u64,
    heal_start_tick: u64,
    eio_injected: usize,
    max_eio_nodes: usize,
}

impl Nemesis {
    /// Creates a nemesis that operates until `heal_start_tick`.
    pub fn new(heal_start_tick: u64, node_count: usize, allow_eio: bool) -> Self {
        let max_eio_nodes = if allow_eio {
            // Keep at least a quorum of nodes non-halted so liveness remains recoverable.
            node_count - (node_count / 2 + 1)
        } else {
            0
        };
        Self {
            next_event_tick: 60,
            heal_start_tick,
            eio_injected: 0,
            max_eio_nodes,
        }
    }

    /// Optionally emits a fault action for `now_tick` given currently up nodes and known leader.
    pub fn maybe_schedule(
        &mut self,
        now_tick: u64,
        all_nodes: &[NodeId],
        up_nodes: &[NodeId],
        current_leader: Option<NodeId>,
        non_halted_count: usize,
        rng: &mut impl rand::Rng,
    ) -> Option<FaultAction> {
        if now_tick >= self.heal_start_tick || now_tick < self.next_event_tick {
            return None;
        }
        self.next_event_tick = now_tick + rng.random_range(40..=140);

        let quorum = all_nodes.len() / 2 + 1;
        let choice = rng.random_range(0..10u8);

        match choice {
            0..=1 => {
                // Partition minority (1 node in 3-node, 1-2 in 5-node).
                let count = if all_nodes.len() >= 5 && rng.random_bool(0.5) {
                    2
                } else {
                    1
                };
                let mut pool = all_nodes.to_vec();
                let mut minority = Vec::with_capacity(count);
                for _ in 0..count {
                    let idx = rng.random_range(0..pool.len());
                    minority.push(pool.swap_remove(idx));
                }
                Some(FaultAction::PartitionMinority(minority))
            }
            2 => {
                let target =
                    current_leader.unwrap_or(all_nodes[rng.random_range(0..all_nodes.len())]);
                Some(FaultAction::IsolateNode(target))
            }
            3..=4 => Some(FaultAction::HealNetwork),
            5..=7 => {
                if up_nodes.is_empty() {
                    return None;
                }
                // Allow brief quorum loss sometimes, or crash any up node with bounded restart.
                let node = if up_nodes.len() > quorum || rng.random_bool(0.2) {
                    current_leader
                        .filter(|l| up_nodes.contains(l) && rng.random_bool(0.5))
                        .unwrap_or_else(|| up_nodes[rng.random_range(0..up_nodes.len())])
                } else {
                    return Some(FaultAction::HealNetwork);
                };
                let down_len = rng
                    .random_range(15..=80)
                    .min(self.heal_start_tick.saturating_sub(now_tick).max(1));
                Some(FaultAction::CrashNode {
                    node,
                    restart_tick: now_tick + down_len,
                })
            }
            8 => Some(FaultAction::ShiftNetwork {
                min_delay: rng.random_range(1..=3),
                max_delay: rng.random_range(4..=9),
                drop_prob: f64::from(rng.random_range(0..=12)) / 100.0,
            }),
            _ => {
                if self.eio_injected < self.max_eio_nodes
                    && non_halted_count > quorum
                    && !up_nodes.is_empty()
                {
                    let target = up_nodes[rng.random_range(0..up_nodes.len())];
                    self.eio_injected += 1;
                    Some(FaultAction::InjectEio(target))
                } else {
                    Some(FaultAction::HealNetwork)
                }
            }
        }
    }
}
