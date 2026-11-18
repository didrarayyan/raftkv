//! Tonic gRPC implementations for the `Kv`, `Admin`, and `Raft` services.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use raft_core::NodeId;
use raftkv_kv::{Command, Op, OpResult};
use raftkv_proto::admin_server::Admin;
use raftkv_proto::kv_server::Kv;
use raftkv_proto::raft_server::Raft;
use raftkv_proto::{
    Ack, CasRequest, CasResponse, Consistency, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, KeyValue, PutRequest, PutResponse, RaftMessage, RangeRequest, RangeResponse,
    SnapshotChunk, StatusRequest, StatusResponse,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tonic::{Request, Response, Status, Streaming};

use crate::codec::{SnapshotAssembleError, SnapshotAssembler, decode_step_message};
use crate::config::LimitSettings;
use crate::driver::{DriverInput, ReadConsistency, ReadOp, ReadReply};

const DEFAULT_RANGE_LIMIT: usize = 1000;
const MAX_RANGE_LIMIT: usize = 10_000;

/// Immutable request validation and peer identity metadata shared across service handlers.
#[derive(Debug)]
pub struct ServiceMeta {
    /// Local cluster member identifier.
    pub node_id: NodeId,
    /// Map of all cluster member IDs to advertised endpoints.
    pub peers: BTreeMap<NodeId, String>,
    /// Server request and payload size bounds.
    pub limits: LimitSettings,
}

/// Shared handle cloned into each tonic gRPC service adapter.
#[derive(Debug, Clone)]
pub struct RpcServices {
    meta: Arc<ServiceMeta>,
    driver_tx: mpsc::Sender<DriverInput>,
}

impl RpcServices {
    /// Creates a new set of gRPC service handlers backed by `driver_tx`.
    pub fn new(meta: Arc<ServiceMeta>, driver_tx: mpsc::Sender<DriverInput>) -> Self {
        Self { meta, driver_tx }
    }

    fn validate_key(&self, key: &[u8]) -> Result<(), Status> {
        if key.is_empty() {
            return Err(Status::invalid_argument("key must not be empty"));
        }
        if key.len() > self.meta.limits.max_key_bytes {
            return Err(Status::invalid_argument(format!(
                "key length {} exceeds max_key_bytes {}",
                key.len(),
                self.meta.limits.max_key_bytes
            )));
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<(), Status> {
        if value.len() > self.meta.limits.max_value_bytes {
            return Err(Status::invalid_argument(format!(
                "value length {} exceeds max_value_bytes {}",
                value.len(),
                self.meta.limits.max_value_bytes
            )));
        }
        Ok(())
    }

    fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.meta.limits.request_timeout_ms)
    }

    fn send_peer_message(&self, msg: raft_core::Message) -> Result<(), Status> {
        self.driver_tx
            .try_send(DriverInput::Peer(msg))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => {
                    Status::resource_exhausted("driver input queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    Status::unavailable("server is shutting down")
                }
            })
    }

    async fn call_driver<T>(
        &self,
        make_input: impl FnOnce(Instant, oneshot::Sender<Result<T, Status>>) -> DriverInput,
    ) -> Result<T, Status> {
        let timeout = self.request_timeout();
        let deadline = Instant::now() + timeout;
        let (reply_tx, reply_rx) = oneshot::channel();
        let input = make_input(deadline, reply_tx);

        self.driver_tx.try_send(input).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => {
                Status::resource_exhausted("driver input queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => Status::unavailable("server is shutting down"),
        })?;

        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(Status::unavailable("server is shutting down")),
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }
}

fn parse_consistency(raw: i32) -> Result<ReadConsistency, Status> {
    match Consistency::try_from(raw) {
        Ok(Consistency::Linearizable) => Ok(ReadConsistency::Linearizable),
        Ok(Consistency::Stale) => Ok(ReadConsistency::Stale),
        Err(_) => Err(Status::invalid_argument(format!(
            "unsupported consistency mode {raw}"
        ))),
    }
}

#[tonic::async_trait]
impl Kv for RpcServices {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        self.validate_key(&req.key)?;
        self.validate_value(&req.value)?;
        let cmd = Command {
            client_id: req.client_id,
            request_id: req.request_id,
            op: Op::Put {
                key: req.key,
                value: req.value,
            },
        };
        let reply = self
            .call_driver(|deadline, reply| DriverInput::Write {
                cmd,
                deadline,
                reply,
            })
            .await?;
        Ok(Response::new(PutResponse {
            log_index: reply.log_index,
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        self.validate_key(&req.key)?;
        let consistency = parse_consistency(req.consistency)?;
        let reply = self
            .call_driver(|deadline, reply| DriverInput::Read {
                op: ReadOp::Get { key: req.key },
                consistency,
                deadline,
                reply,
            })
            .await?;
        match reply {
            ReadReply::Get {
                value,
                applied_index,
            } => Ok(Response::new(GetResponse {
                value,
                applied_index,
            })),
            ReadReply::Range { .. } => Err(Status::internal("mismatched read reply variant")),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        self.validate_key(&req.key)?;
        let cmd = Command {
            client_id: req.client_id,
            request_id: req.request_id,
            op: Op::Delete { key: req.key },
        };
        let reply = self
            .call_driver(|deadline, reply| DriverInput::Write {
                cmd,
                deadline,
                reply,
            })
            .await?;
        let OpResult::Delete { existed } = reply.result else {
            return Err(Status::internal("mismatched delete reply variant"));
        };
        Ok(Response::new(DeleteResponse {
            existed,
            log_index: reply.log_index,
        }))
    }

    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();
        if req.start.len() > self.meta.limits.max_key_bytes
            || req.end.len() > self.meta.limits.max_key_bytes
        {
            return Err(Status::invalid_argument(format!(
                "range bound exceeds max_key_bytes {}",
                self.meta.limits.max_key_bytes
            )));
        }
        if !req.end.is_empty() && req.start > req.end {
            return Err(Status::invalid_argument(
                "range start key must not exceed end key",
            ));
        }
        let consistency = parse_consistency(req.consistency)?;
        let limit = if req.limit == 0 {
            DEFAULT_RANGE_LIMIT
        } else {
            (req.limit as usize).min(MAX_RANGE_LIMIT)
        };

        let reply = self
            .call_driver(|deadline, reply| DriverInput::Read {
                op: ReadOp::Range {
                    start: req.start,
                    end: req.end,
                    limit,
                },
                consistency,
                deadline,
                reply,
            })
            .await?;
        match reply {
            ReadReply::Range {
                kvs,
                more,
                applied_index,
            } => Ok(Response::new(RangeResponse {
                kvs: kvs
                    .into_iter()
                    .map(|(key, value)| KeyValue { key, value })
                    .collect(),
                more,
                applied_index,
            })),
            ReadReply::Get { .. } => Err(Status::internal("mismatched read reply variant")),
        }
    }

    async fn compare_and_swap(
        &self,
        request: Request<CasRequest>,
    ) -> Result<Response<CasResponse>, Status> {
        let req = request.into_inner();
        self.validate_key(&req.key)?;
        self.validate_value(&req.value)?;
        if let Some(expected) = &req.expected {
            self.validate_value(expected)?;
        }
        let cmd = Command {
            client_id: req.client_id,
            request_id: req.request_id,
            op: Op::Cas {
                key: req.key,
                expected: req.expected,
                value: req.value,
            },
        };
        let reply = self
            .call_driver(|deadline, reply| DriverInput::Write {
                cmd,
                deadline,
                reply,
            })
            .await?;
        match reply.result {
            OpResult::Cas { swapped, current } => Ok(Response::new(CasResponse {
                swapped,
                current,
                log_index: reply.log_index,
            })),
            _ => Err(Status::internal("mismatched CAS reply variant")),
        }
    }
}

#[tonic::async_trait]
impl Admin for RpcServices {
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let timeout = self.request_timeout();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.driver_tx
            .try_send(DriverInput::Status { reply: reply_tx })
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => {
                    Status::resource_exhausted("driver input queue is full")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    Status::unavailable("server is shutting down")
                }
            })?;

        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(status)) => Ok(Response::new(status)),
            Ok(Err(_)) => Err(Status::unavailable("server is shutting down")),
            Err(_) => Err(Status::deadline_exceeded("status request timed out")),
        }
    }
}

#[tonic::async_trait]
impl Raft for RpcServices {
    async fn step(&self, request: Request<RaftMessage>) -> Result<Response<Ack>, Status> {
        let msg = decode_step_message(request.into_inner())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if !self.meta.peers.contains_key(&msg.from) || msg.to != self.meta.node_id {
            return Err(Status::permission_denied(format!(
                "unexpected sender {} or recipient {}",
                msg.from, msg.to
            )));
        }
        self.send_peer_message(msg)?;
        Ok(Response::new(Ack {}))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<Ack>, Status> {
        let mut stream = request.into_inner();
        let mut assembler = SnapshotAssembler::new(self.meta.limits.max_snapshot_bytes);
        let mut checked_peer = false;

        while let Some(chunk) = stream.message().await? {
            if !checked_peer {
                let from = NodeId(chunk.from);
                let to = NodeId(chunk.to);
                if !self.meta.peers.contains_key(&from) || to != self.meta.node_id {
                    return Err(Status::permission_denied(format!(
                        "unexpected snapshot sender {from} or recipient {to}"
                    )));
                }
                checked_peer = true;
            }
            assembler.push(&chunk).map_err(|err| match err {
                SnapshotAssembleError::TooLarge { .. } => {
                    Status::resource_exhausted(err.to_string())
                }
                SnapshotAssembleError::EmptyStream | SnapshotAssembleError::InconsistentHeader => {
                    Status::invalid_argument(err.to_string())
                }
            })?;
        }

        let msg = assembler
            .finish()
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        self.send_peer_message(msg)?;
        Ok(Response::new(Ack {}))
    }
}
