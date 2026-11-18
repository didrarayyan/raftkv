//! Single-owner Tokio driver loop coordinating Raft consensus, WAL durability, and the KV store.

use std::collections::BTreeMap;

use bytes::Bytes;
use raft_core::{
    Index, Message, NodeId, ProposeError, RaftNode, ReadError, Role, Snapshot, SnapshotMeta, Term,
};
use raftkv_kv::{Applied, Command, KvStore, OpResult};
use raftkv_proto::StatusResponse;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::Status;
use tonic::metadata::MetadataValue;

use crate::codec::status_to_proto;
use crate::wal_thread::{OwnedBatch, WalHandle};

/// Bounded capacity of the inbound driver channel (`DriverInput`).
pub const DRIVER_CHANNEL_CAP: usize = 1024;
/// Bounded capacity of each peer's outbound message channel.
pub const PEER_CHANNEL_CAP: usize = 256;

/// Consistency guarantee requested for read operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Linearizable read guaranteed via quorum heartbeat `ReadIndex`.
    Linearizable,
    /// Stale local read directly against the node's current `KvStore` state.
    Stale,
}

/// Read operation dispatched through the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOp {
    /// Single-key lookup.
    Get {
        /// Key bytes to look up.
        key: Bytes,
    },
    /// Ordered half-open range scan `[start, end)` bounded by `limit`.
    Range {
        /// Inclusive start key bytes.
        start: Bytes,
        /// Exclusive end key bytes (`empty` means unbounded).
        end: Bytes,
        /// Maximum number of entries to return.
        limit: usize,
    },
}

/// Result of a [`ReadOp`] executed against the node's [`KvStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadReply {
    /// Result for [`ReadOp::Get`].
    Get {
        /// Value associated with the key, if present.
        value: Option<Bytes>,
        /// Applied Raft log index reflected by this read.
        applied_index: u64,
    },
    /// Result for [`ReadOp::Range`].
    Range {
        /// Matching key-value pairs in sorted key order.
        kvs: Vec<(Bytes, Bytes)>,
        /// True when additional matching keys exist beyond `limit`.
        more: bool,
        /// Applied Raft log index reflected by this read.
        applied_index: u64,
    },
}

/// Result of a committed client write command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReply {
    /// Raft log index at which the command was committed.
    pub log_index: u64,
    /// Deterministic state-machine result.
    pub result: OpResult,
}

/// Messages delivered into the single-owner [`RaftDriver`] event loop.
#[derive(Debug)]
pub enum DriverInput {
    /// Periodic logical Raft clock tick.
    Tick,
    /// Inbound Raft protocol RPC message from a cluster peer.
    Peer(Message),
    /// Client mutation proposal.
    Write {
        /// Encodable KV command.
        cmd: Command,
        /// Server deadline beyond which the request fails with `DEADLINE_EXCEEDED`.
        deadline: Instant,
        /// Reply channel delivering commit result or gRPC status error.
        reply: oneshot::Sender<Result<WriteReply, Status>>,
    },
    /// Client read request (`Get` or `Range`).
    Read {
        /// Read operation kind and bounds.
        op: ReadOp,
        /// Linearizable or stale consistency mode.
        consistency: ReadConsistency,
        /// Server deadline beyond which the request fails with `DEADLINE_EXCEEDED`.
        deadline: Instant,
        /// Reply channel delivering read result or gRPC status error.
        reply: oneshot::Sender<Result<ReadReply, Status>>,
    },
    /// Admin status query.
    Status {
        /// Reply channel receiving current node metrics.
        reply: oneshot::Sender<StatusResponse>,
    },
    /// Graceful shutdown signal.
    Shutdown {
        /// Acknowledged once pending client callers are failed and the loop exits.
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
struct PendingWrite {
    term: Term,
    deadline: Instant,
    reply: oneshot::Sender<Result<WriteReply, Status>>,
}

#[derive(Debug)]
struct PendingRead {
    ctx: u64,
    op: ReadOp,
    min_index: Option<Index>,
    deadline: Instant,
    reply: oneshot::Sender<Result<ReadReply, Status>>,
}

/// Formats the canonical gRPC error when a non-leader cannot serve a request.
pub fn not_leader_status(leader_id: Option<NodeId>, peers: &BTreeMap<NodeId, String>) -> Status {
    let Some(id) = leader_id else {
        return Status::unavailable("no cluster leader elected");
    };
    let mut status = Status::failed_precondition(format!("not leader; current leader is {id}"));
    if let Ok(val) = MetadataValue::try_from(id.0.to_string()) {
        status.metadata_mut().insert("x-raftkv-leader-id", val);
    }
    if let Some(addr) = peers.get(&id)
        && let Ok(val) = MetadataValue::try_from(addr.as_str())
    {
        status.metadata_mut().insert("x-raftkv-leader-addr", val);
    }
    status
}

/// Single-owner actor owning [`RaftNode`], [`KvStore`], outbound peer senders, and [`WalHandle`].
#[derive(Debug)]
pub struct RaftDriver {
    node_id: NodeId,
    node: RaftNode,
    kv: KvStore,
    wal: WalHandle,
    peer_addrs: BTreeMap<NodeId, String>,
    peer_txs: BTreeMap<NodeId, mpsc::Sender<Message>>,
    snapshot_threshold: u64,
    last_applied_term: Term,
    pending_writes: BTreeMap<u64, PendingWrite>,
    pending_reads: Vec<PendingRead>,
    next_read_ctx: u64,
    last_logged_term: Term,
    last_logged_role: Role,
}

/// Wiring and threshold configuration passed to [`RaftDriver::new`].
#[derive(Debug)]
pub struct DriverConfig {
    /// Local node identifier.
    pub node_id: NodeId,
    /// Map of all cluster member IDs to advertised `host:port` endpoints.
    pub peer_addrs: BTreeMap<NodeId, String>,
    /// Outbound peer sender channels keyed by remote peer ID.
    pub peer_txs: BTreeMap<NodeId, mpsc::Sender<Message>>,
    /// Applied entry interval after which a local snapshot is compacted.
    pub snapshot_threshold: u64,
    /// Term corresponding to the initially restored `KvStore` applied index.
    pub last_applied_term: Term,
}

impl RaftDriver {
    /// Creates a new driver initialized from recovered Raft and KV state.
    pub fn new(node: RaftNode, kv: KvStore, wal: WalHandle, cfg: DriverConfig) -> Self {
        let init = node.status();
        Self {
            node_id: cfg.node_id,
            node,
            kv,
            wal,
            peer_addrs: cfg.peer_addrs,
            peer_txs: cfg.peer_txs,
            snapshot_threshold: cfg.snapshot_threshold,
            last_applied_term: cfg.last_applied_term,
            pending_writes: BTreeMap::new(),
            pending_reads: Vec::new(),
            next_read_ctx: 1,
            last_logged_term: init.term,
            last_logged_role: init.role,
        }
    }

    /// Runs the group-commit driver event loop until `Shutdown` or input channel closure.
    pub async fn run(mut self, mut rx: mpsc::Receiver<DriverInput>) {
        // Replay any committed entries recovered from the WAL on startup.
        self.drain_ready().await;

        while let Some(first) = rx.recv().await {
            let mut shutdown_reply = None;
            if let DriverInput::Shutdown { reply } = first {
                shutdown_reply = Some(reply);
            } else {
                self.handle_input(first);
                // Group commit: drain all immediately buffered inputs before entering Ready().
                while let Ok(next) = rx.try_recv() {
                    if let DriverInput::Shutdown { reply } = next {
                        shutdown_reply = Some(reply);
                        break;
                    }
                    self.handle_input(next);
                }
            }

            if let Some(reply) = shutdown_reply {
                self.fail_all_pending(&Status::unavailable("server is shutting down"));
                let _ = reply.send(());
                break;
            }

            self.drain_ready().await;
            self.expire_timed_out();
        }

        self.wal.shutdown();
    }

    fn handle_input(&mut self, input: DriverInput) {
        match input {
            DriverInput::Tick => self.node.tick(),
            DriverInput::Peer(msg) => self.node.step(msg),
            DriverInput::Write {
                cmd,
                deadline,
                reply,
            } => self.handle_write(&cmd, deadline, reply),
            DriverInput::Read {
                op,
                consistency,
                deadline,
                reply,
            } => self.handle_read(op, consistency, deadline, reply),
            DriverInput::Status { reply } => {
                let _ = reply.send(status_to_proto(self.node.status()));
            }
            DriverInput::Shutdown { .. } => {}
        }
    }

    fn handle_write(
        &mut self,
        cmd: &Command,
        deadline: Instant,
        reply: oneshot::Sender<Result<WriteReply, Status>>,
    ) {
        if reply.is_closed() {
            return;
        }
        if Instant::now() >= deadline {
            let _ = reply.send(Err(Status::deadline_exceeded("request timed out")));
            return;
        }

        match self.node.propose(cmd.encode()) {
            Ok(prop) => {
                if let Some(old) = self.pending_writes.insert(
                    prop.index.0,
                    PendingWrite {
                        term: prop.term,
                        deadline,
                        reply,
                    },
                ) {
                    let _ = old.reply.send(Err(Status::aborted(
                        "proposal index superseded by newer proposal",
                    )));
                }
            }
            Err(ProposeError::NotLeader { leader }) => {
                let _ = reply.send(Err(not_leader_status(leader, &self.peer_addrs)));
            }
        }
    }

    fn handle_read(
        &mut self,
        op: ReadOp,
        consistency: ReadConsistency,
        deadline: Instant,
        reply: oneshot::Sender<Result<ReadReply, Status>>,
    ) {
        if reply.is_closed() {
            return;
        }
        if Instant::now() >= deadline {
            let _ = reply.send(Err(Status::deadline_exceeded("request timed out")));
            return;
        }

        if consistency == ReadConsistency::Stale {
            let res = self.execute_read(&op);
            let _ = reply.send(Ok(res));
            return;
        }

        let ctx = self.alloc_read_ctx();
        match self.node.read_index(ctx) {
            Ok(()) => self.pending_reads.push(PendingRead {
                ctx,
                op,
                min_index: None,
                deadline,
                reply,
            }),
            Err(ReadError::NotLeader { leader }) => {
                let _ = reply.send(Err(not_leader_status(leader, &self.peer_addrs)));
            }
            Err(err) => {
                let _ = reply.send(Err(Status::internal(err.to_string())));
            }
        }
    }

    fn alloc_read_ctx(&mut self) -> u64 {
        loop {
            let ctx = self.next_read_ctx;
            self.next_read_ctx = self.next_read_ctx.wrapping_add(1);
            if self.next_read_ctx == 0 {
                self.next_read_ctx = 1;
            }
            if !self.pending_reads.iter().any(|r| r.ctx == ctx) {
                return ctx;
            }
        }
    }

    async fn drain_ready(&mut self) {
        while let Some(ready) = self.node.ready() {
            let pre_status = self.node.status();
            if let Some(soft) = ready.soft_state
                && (soft.role != self.last_logged_role || pre_status.term != self.last_logged_term)
            {
                tracing::info!(
                    node = %self.node_id,
                    term = %pre_status.term,
                    role = %soft.role,
                    leader = ?soft.leader_id,
                    "raft state updated"
                );
                self.last_logged_role = soft.role;
                self.last_logged_term = pre_status.term;
            } else if pre_status.term != self.last_logged_term {
                tracing::info!(
                    node = %self.node_id,
                    term = %pre_status.term,
                    role = %pre_status.role,
                    "raft term updated"
                );
                self.last_logged_term = pre_status.term;
            }

            // 1. WAL persist and await before sending messages or applying entries.
            if ready.hard_state.is_some() || ready.snapshot.is_some() || !ready.entries.is_empty() {
                let batch = OwnedBatch {
                    snapshot: ready.snapshot.clone(),
                    hard_state: ready.hard_state,
                    entries: ready.entries,
                };
                if self.wal.persist(batch).await.is_err() {
                    tracing::error!(
                        node = %self.node_id,
                        term = %pre_status.term,
                        role = %pre_status.role,
                        "WAL channel closed unexpectedly"
                    );
                    std::process::abort();
                }
            }

            // 2. Restore snapshot into KV state machine if one arrived from leader.
            if let Some(snap) = ready.snapshot {
                if let Err(err) = self.kv.restore(snap.meta.index, &snap.data) {
                    tracing::error!(
                        node = %self.node_id,
                        term = %pre_status.term,
                        role = %pre_status.role,
                        error = %err,
                        "failed to restore KV snapshot after WAL sync"
                    );
                    std::process::abort();
                }
                self.last_applied_term = snap.meta.term;
                tracing::info!(
                    node = %self.node_id,
                    term = %pre_status.term,
                    role = %pre_status.role,
                    index = snap.meta.index.0,
                    "installed snapshot from leader"
                );
            }

            // 3. Send outbound peer messages (dropping if peer channel is full).
            for msg in ready.messages {
                if let Some(tx) = self.peer_txs.get(&msg.to) {
                    let _ = tx.try_send(msg);
                }
            }

            // 4. Apply committed entries and compare proposal terms.
            for entry in ready.committed {
                let res = self.kv.apply(entry.index, &entry.payload);
                self.last_applied_term = entry.term;
                if let Some(pending) = self.pending_writes.remove(&entry.index.0) {
                    if pending.term == entry.term {
                        let outcome = match res {
                            Ok(Applied::Done(op_res) | Applied::Duplicate(op_res)) => {
                                Ok(WriteReply {
                                    log_index: entry.index.0,
                                    result: op_res,
                                })
                            }
                            Ok(Applied::Outdated) => Err(Status::invalid_argument(
                                "request_id older than last applied client request",
                            )),
                            Ok(Applied::Noop) => Err(Status::aborted(
                                "proposal overwritten by leader noop barrier",
                            )),
                            Err(err) => Err(Status::invalid_argument(err.to_string())),
                        };
                        let _ = pending.reply.send(outcome);
                    } else {
                        let _ = pending
                            .reply
                            .send(Err(Status::aborted("proposal overwritten in newer term")));
                    }
                }
            }

            // 5. Register linearizable read indices.
            for rs in ready.read_states {
                if let Some(pending) = self.pending_reads.iter_mut().find(|r| r.ctx == rs.ctx) {
                    pending.min_index = Some(rs.index);
                }
            }

            // 6. Serve waiting linearizable reads whose applied_index caught up.
            self.flush_ready_reads();

            // 7. Fail uncommitted writes and waiting reads on leaving Leader.
            let post_status = self.node.status();
            if post_status.role != Role::Leader
                && (!self.pending_writes.is_empty() || !self.pending_reads.is_empty())
            {
                let err = not_leader_status(post_status.leader_id, &self.peer_addrs);
                self.fail_all_pending(&err);
            }

            // 8. Trigger local snapshot & compaction when threshold reached.
            let applied = self.kv.applied_index();
            let last_snap = self.node.status().snapshot_index;
            if applied.0.saturating_sub(last_snap.0) >= self.snapshot_threshold {
                let snap = Snapshot {
                    meta: SnapshotMeta {
                        index: applied,
                        term: self.last_applied_term,
                    },
                    data: self.kv.snapshot(),
                };
                if self
                    .wal
                    .persist(OwnedBatch {
                        snapshot: Some(snap.clone()),
                        hard_state: None,
                        entries: Vec::new(),
                    })
                    .await
                    .is_err()
                {
                    tracing::error!(
                        node = %self.node_id,
                        term = %post_status.term,
                        role = %post_status.role,
                        "WAL channel closed while saving local snapshot"
                    );
                    std::process::abort();
                }
                self.node.compact(snap);
                tracing::info!(
                    node = %self.node_id,
                    term = %post_status.term,
                    role = %post_status.role,
                    index = applied.0,
                    "compacted local snapshot"
                );
            }

            // 9. Advance Raft state machine to release this Ready.
            self.node.advance();
        }
    }

    fn flush_ready_reads(&mut self) {
        let applied = self.kv.applied_index();
        let mut idx = 0;
        while idx < self.pending_reads.len() {
            if self.pending_reads[idx].reply.is_closed() {
                let _ = self.pending_reads.swap_remove(idx);
                continue;
            }
            if let Some(min_idx) = self.pending_reads[idx].min_index
                && applied >= min_idx
            {
                let req = self.pending_reads.swap_remove(idx);
                let reply = self.execute_read(&req.op);
                let _ = req.reply.send(Ok(reply));
            } else {
                idx += 1;
            }
        }
    }

    fn execute_read(&self, op: &ReadOp) -> ReadReply {
        let applied_index = self.kv.applied_index().0;
        match op {
            ReadOp::Get { key } => ReadReply::Get {
                value: self.kv.get(key).cloned(),
                applied_index,
            },
            ReadOp::Range { start, end, limit } => {
                let (kvs, more) = self.kv.range(start, end, *limit);
                ReadReply::Range {
                    kvs,
                    more,
                    applied_index,
                }
            }
        }
    }

    fn fail_all_pending(&mut self, status: &Status) {
        let writes = std::mem::take(&mut self.pending_writes);
        for (_, pending) in writes {
            let _ = pending.reply.send(Err(status.clone()));
        }
        let reads = std::mem::take(&mut self.pending_reads);
        for pending in reads {
            let _ = pending.reply.send(Err(status.clone()));
        }
    }

    fn expire_timed_out(&mut self) {
        let now = Instant::now();
        let expired_keys: Vec<u64> = self
            .pending_writes
            .iter()
            .filter(|(_, p)| p.deadline <= now || p.reply.is_closed())
            .map(|(&idx, _)| idx)
            .collect();
        for idx in expired_keys {
            if let Some(pending) = self.pending_writes.remove(&idx)
                && !pending.reply.is_closed()
            {
                let _ = pending
                    .reply
                    .send(Err(Status::deadline_exceeded("request timed out")));
            }
        }

        let mut i = 0;
        while i < self.pending_reads.len() {
            let p = &self.pending_reads[i];
            if p.deadline <= now || p.reply.is_closed() {
                let removed = self.pending_reads.swap_remove(i);
                if !removed.reply.is_closed() {
                    let _ = removed
                        .reply
                        .send(Err(Status::deadline_exceeded("request timed out")));
                }
            } else {
                i += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;

    use raft_core::{Config, Recovered};

    use super::*;

    #[tokio::test]
    async fn outbound_messages_wait_for_wal_sync_completion() {
        let cfg = Config::with_defaults(NodeId(1), vec![NodeId(1), NodeId(2), NodeId(3)], 42);
        let node = RaftNode::new(cfg, Recovered::default()).expect("construct node");

        let (wal_tx, wal_rx) = std_mpsc::channel();
        let wal = WalHandle::from_sender(wal_tx);

        let (p2_tx, mut p2_rx) = mpsc::channel(PEER_CHANNEL_CAP);
        let (p3_tx, _p3_rx) = mpsc::channel(PEER_CHANNEL_CAP);
        let mut peer_txs = BTreeMap::new();
        peer_txs.insert(NodeId(2), p2_tx);
        peer_txs.insert(NodeId(3), p3_tx);

        let mut peer_addrs = BTreeMap::new();
        peer_addrs.insert(NodeId(1), "127.0.0.1:7001".to_owned());
        peer_addrs.insert(NodeId(2), "127.0.0.1:7002".to_owned());
        peer_addrs.insert(NodeId(3), "127.0.0.1:7003".to_owned());

        let driver = RaftDriver::new(
            node,
            KvStore::new(),
            wal,
            DriverConfig {
                node_id: NodeId(1),
                peer_addrs,
                peer_txs,
                snapshot_threshold: 10_000,
                last_applied_term: Term(0),
            },
        );

        let (driver_tx, driver_rx) = mpsc::channel(DRIVER_CHANNEL_CAP);
        let handle = tokio::spawn(driver.run(driver_rx));

        // Drive enough ticks for node 1 to start an election / pre-vote; a higher-term
        // RequestVote message guarantees a non-empty HardState WAL write + outbound messages.
        let trigger_vote = Message {
            from: NodeId(2),
            to: NodeId(1),
            term: Term(2),
            body: raft_core::MessageBody::RequestVote {
                last_index: Index(0),
                last_term: Term(0),
            },
        };
        driver_tx
            .send(DriverInput::Peer(trigger_vote))
            .await
            .expect("send peer msg");

        // WAL thread receives the PersistJob before any message is emitted.
        let (job_res, _wal_rx) = tokio::task::spawn_blocking(move || {
            let res = wal_rx.recv_timeout(Duration::from_secs(2));
            (res, wal_rx)
        })
        .await
        .expect("join blocking wal recv");
        let job = job_res.expect("expected PersistJob");
        assert!(job.batch.hard_state.is_some());

        // Outbound vote response MUST NOT be sent until WAL sync acknowledges.
        let premature = tokio::time::timeout(Duration::from_millis(60), p2_rx.recv()).await;
        assert!(
            premature.is_err(),
            "outbound Raft message escaped before WAL sync completed"
        );

        // Complete WAL sync and confirm the outbound message is now delivered.
        job.reply.send(()).expect("complete WAL sync");
        let sent = tokio::time::timeout(Duration::from_secs(2), p2_rx.recv())
            .await
            .expect("peer should receive message after WAL sync")
            .expect("channel open");
        assert_eq!(sent.from, NodeId(1));
        assert_eq!(sent.to, NodeId(2));

        let (sd_tx, sd_rx) = oneshot::channel();
        driver_tx
            .send(DriverInput::Shutdown { reply: sd_tx })
            .await
            .expect("send shutdown");
        sd_rx.await.expect("shutdown ack");
        handle.await.expect("driver join");
    }
}
