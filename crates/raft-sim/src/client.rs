//! Simulated clients generating sequential KV workloads with timeouts and retries.

use bytes::Bytes;
use raft_core::NodeId;
use raftkv_kv::{Command, Op, OpResult};
use rand::RngExt;

/// Operation specification tracked for linearizability checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimOpKind {
    /// Insert or overwrite `key` with `value`.
    Put(Bytes),
    /// Delete `key`.
    Delete,
    /// Single-key Compare-And-Swap.
    Cas {
        /// Expected current value (`None` means key must be absent).
        expected: Option<Bytes>,
        /// Replacement value when `expected` matches.
        value: Bytes,
    },
    /// Linearizable read of `key`.
    Get,
}

/// Definitive result observed by a simulated client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimOpOutcome {
    /// Put completed.
    Put,
    /// Delete completed with `existed`.
    Delete {
        /// Whether the key existed prior to deletion.
        existed: bool,
    },
    /// CAS completed with swap outcome and observed post-op value.
    Cas {
        /// Whether the swap occurred.
        swapped: bool,
        /// Observed value after CAS.
        current: Option<Bytes>,
    },
    /// Get completed with optional value.
    Get(Option<Bytes>),
}

/// Complete history record for one logical client operation (coalescing retries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryOp {
    /// Logical operation identifier.
    pub id: usize,
    /// Issuing client identifier.
    pub client_id: u64,
    /// Per-client monotonic request sequence number.
    pub request_id: u64,
    /// Target key.
    pub key: Bytes,
    /// Operation semantics.
    pub kind: SimOpKind,
    /// Tick of the first invocation attempt.
    pub invoke_tick: u64,
    /// Tick of definitive completion (`None` if still indeterminate at run end).
    pub complete_tick: Option<u64>,
    /// Observed definitive outcome (`None` if indeterminate).
    pub outcome: Option<SimOpOutcome>,
}

/// An in-flight client operation awaiting a cluster response or timeout.
#[derive(Debug, Clone)]
pub struct InFlightRequest {
    pub(crate) history_idx: usize,
    pub(crate) client_id: u64,
    pub(crate) request_id: u64,
    pub(crate) key: Bytes,
    pub(crate) kind: SimOpKind,
    pub(crate) target: NodeId,
    pub(crate) sent_tick: u64,
    pub(crate) attempt_token: u64,
}

#[derive(Debug)]
struct SimClientState {
    client_id: u64,
    next_request_id: u64,
    in_flight: Option<InFlightRequest>,
    last_leader_hint: Option<NodeId>,
    cooldown_until: u64,
}

/// Coordinates simulated clients and collects their invocation/completion history.
#[derive(Debug)]
pub struct ClientDriver {
    keys: Vec<Bytes>,
    values: Vec<Bytes>,
    clients: Vec<SimClientState>,
    history: Vec<HistoryOp>,
    next_attempt_token: u64,
    request_timeout_ticks: u64,
}

impl ClientDriver {
    /// Creates `num_clients` sequential clients operating over keys `k0..k4`.
    pub fn new(num_clients: usize, request_timeout_ticks: u64) -> Self {
        let keys = (0..5).map(|i| Bytes::from(format!("k{i}"))).collect();
        let values = (0..4).map(|i| Bytes::from(format!("v{i}"))).collect();
        let clients = (1..=num_clients as u64)
            .map(|client_id| SimClientState {
                client_id,
                next_request_id: 1,
                in_flight: None,
                last_leader_hint: None,
                cooldown_until: 0,
            })
            .collect();
        Self {
            keys,
            values,
            clients,
            history: Vec::new(),
            next_attempt_token: 1,
            request_timeout_ticks,
        }
    }

    /// Advances client state for `now_tick`, emitting new or retried requests to target nodes.
    ///
    /// When `allow_new` is false (at the tail of the heal phase), only in-flight retries continue.
    pub fn step(
        &mut self,
        now_tick: u64,
        live_nodes: &[NodeId],
        allow_new: bool,
        rng: &mut impl rand::Rng,
    ) -> Vec<InFlightRequest> {
        let mut to_dispatch = Vec::new();
        if live_nodes.is_empty() {
            return to_dispatch;
        }

        for client in &mut self.clients {
            if let Some(req) = &mut client.in_flight {
                if now_tick.saturating_sub(req.sent_tick) >= self.request_timeout_ticks {
                    let next_target =
                        pick_next_target(live_nodes, req.target, client.last_leader_hint, rng);
                    req.target = next_target;
                    req.sent_tick = now_tick;
                    req.attempt_token = self.next_attempt_token;
                    self.next_attempt_token += 1;
                    to_dispatch.push(req.clone());
                }
                continue;
            }

            if !allow_new || now_tick < client.cooldown_until {
                continue;
            }

            let key = self.keys[rng.random_range(0..self.keys.len())].clone();
            let kind = match rng.random_range(0..4u8) {
                0 => SimOpKind::Put(self.values[rng.random_range(0..self.values.len())].clone()),
                1 => SimOpKind::Delete,
                2 => {
                    let expected = if rng.random_bool(0.35) {
                        None
                    } else {
                        Some(self.values[rng.random_range(0..self.values.len())].clone())
                    };
                    let value = self.values[rng.random_range(0..self.values.len())].clone();
                    SimOpKind::Cas { expected, value }
                }
                _ => SimOpKind::Get,
            };

            let request_id = client.next_request_id;
            let history_idx = self.history.len();
            self.history.push(HistoryOp {
                id: history_idx,
                client_id: client.client_id,
                request_id,
                key: key.clone(),
                kind: kind.clone(),
                invoke_tick: now_tick,
                complete_tick: None,
                outcome: None,
            });

            let target = client
                .last_leader_hint
                .filter(|id| live_nodes.contains(id))
                .unwrap_or_else(|| live_nodes[rng.random_range(0..live_nodes.len())]);

            let token = self.next_attempt_token;
            self.next_attempt_token += 1;
            let req = InFlightRequest {
                history_idx,
                client_id: client.client_id,
                request_id,
                key,
                kind,
                target,
                sent_tick: now_tick,
                attempt_token: token,
            };
            client.in_flight = Some(req.clone());
            to_dispatch.push(req);
        }

        to_dispatch
    }

    /// Handles a definitive success outcome from the cluster for `attempt_token`.
    pub fn on_success(&mut self, attempt_token: u64, now_tick: u64, outcome: SimOpOutcome) {
        for client in &mut self.clients {
            if let Some(req) = &client.in_flight
                && req.attempt_token == attempt_token
            {
                let h = &mut self.history[req.history_idx];
                h.complete_tick = Some(now_tick);
                h.outcome = Some(outcome);
                client.last_leader_hint = Some(req.target);
                client.next_request_id += 1;
                client.in_flight = None;
                client.cooldown_until = now_tick + 1;
                break;
            }
        }
    }

    /// Handles a retryable failure (`NotLeader` or `Aborted`), immediately moving to another node.
    pub fn on_retryable_error(
        &mut self,
        attempt_token: u64,
        leader_hint: Option<NodeId>,
        now_tick: u64,
        live_nodes: &[NodeId],
        rng: &mut impl rand::Rng,
    ) -> Option<InFlightRequest> {
        for client in &mut self.clients {
            let Some(req) = &mut client.in_flight else {
                continue;
            };
            if req.attempt_token != attempt_token {
                continue;
            }
            if let Some(hint) = leader_hint {
                client.last_leader_hint = Some(hint);
            } else if client.last_leader_hint == Some(req.target) {
                client.last_leader_hint = None;
            }
            if live_nodes.is_empty() {
                return None;
            }
            let next_target = pick_next_target(live_nodes, req.target, leader_hint, rng);
            req.target = next_target;
            req.sent_tick = now_tick;
            req.attempt_token = self.next_attempt_token;
            self.next_attempt_token += 1;
            return Some(req.clone());
        }
        None
    }

    /// Returns the recorded operation history.
    pub fn history(&self) -> &[HistoryOp] {
        &self.history
    }
}

impl InFlightRequest {
    /// Encodes a write request into a [`Command`] byte payload.
    pub fn encode_write(&self) -> Option<Bytes> {
        let op = match &self.kind {
            SimOpKind::Put(value) => Op::Put {
                key: self.key.clone(),
                value: value.clone(),
            },
            SimOpKind::Delete => Op::Delete {
                key: self.key.clone(),
            },
            SimOpKind::Cas { expected, value } => Op::Cas {
                key: self.key.clone(),
                expected: expected.clone(),
                value: value.clone(),
            },
            SimOpKind::Get => return None,
        };
        Some(
            Command {
                client_id: self.client_id,
                request_id: self.request_id,
                op,
            }
            .encode(),
        )
    }
}

/// Converts a [`raftkv_kv::OpResult`] into a [`SimOpOutcome`].
pub fn outcome_from_op_result(res: OpResult) -> SimOpOutcome {
    match res {
        OpResult::Put => SimOpOutcome::Put,
        OpResult::Delete { existed } => SimOpOutcome::Delete { existed },
        OpResult::Cas { swapped, current } => SimOpOutcome::Cas { swapped, current },
    }
}

fn pick_next_target(
    live_nodes: &[NodeId],
    current: NodeId,
    hint: Option<NodeId>,
    rng: &mut impl rand::Rng,
) -> NodeId {
    if let Some(h) = hint
        && h != current
        && live_nodes.contains(&h)
    {
        return h;
    }
    let others: Vec<NodeId> = live_nodes
        .iter()
        .copied()
        .filter(|&id| id != current)
        .collect();
    if others.is_empty() {
        current
    } else {
        others[rng.random_range(0..others.len())]
    }
}
