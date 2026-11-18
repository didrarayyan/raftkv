//! Leader-following gRPC client library with idempotent write retry semantics.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use raftkv_proto::admin_client::AdminClient;
use raftkv_proto::kv_client::KvClient;
use raftkv_proto::{
    CasRequest, Consistency, DeleteRequest, GetRequest, PutRequest, RangeRequest, StatusRequest,
    StatusResponse,
};
use thiserror::Error;
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(25);
const MAX_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// Errors returned by [`Client`] operations.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Endpoint list was empty or malformed.
    #[error("invalid endpoint configuration: {0}")]
    InvalidConfig(String),
    /// Remote RPC rejected the request or failed with a gRPC status error.
    #[error("RPC failed ({code}): {message}")]
    Rpc {
        /// Canonical gRPC status code.
        code: Code,
        /// Server-supplied diagnostic message.
        message: String,
    },
    /// Transport connection or timeout failure prevented completion within `--timeout`.
    #[error("cluster request timed out: {0}")]
    Timeout(String),
}

impl From<Status> for ClientError {
    fn from(status: Status) -> Self {
        Self::Rpc {
            code: status.code(),
            message: status.message().to_owned(),
        }
    }
}

/// Response returned by [`Client::put`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutResult {
    /// Raft log index assigned to the write.
    pub log_index: u64,
}

/// Response returned by [`Client::get`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResult {
    /// Stored value bytes, if key existed.
    pub value: Option<Bytes>,
    /// Applied log index observed on the serving replica.
    pub applied_index: u64,
}

/// Response returned by [`Client::delete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteResult {
    /// Whether the key existed before deletion.
    pub existed: bool,
    /// Raft log index assigned to the delete.
    pub log_index: u64,
}

/// Response returned by [`Client::cas`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasResult {
    /// Whether `expected` matched and `value` was stored.
    pub swapped: bool,
    /// Observed value after evaluating the CAS operation.
    pub current: Option<Bytes>,
    /// Raft log index assigned to the CAS.
    pub log_index: u64,
}

/// Response returned by [`Client::range`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeResult {
    /// Key-value pairs in `[start, end)` ordered lexicographically.
    pub kvs: Vec<(Bytes, Bytes)>,
    /// `true` if additional matching keys exist beyond `limit`.
    pub more: bool,
    /// Applied log index observed on the serving replica.
    pub applied_index: u64,
}

/// Per-endpoint result collected by [`Client::status_all`].
#[derive(Debug, Clone, PartialEq)]
pub struct NodeStatusReport {
    /// Configured `host:port` endpoint string.
    pub endpoint: String,
    /// Successful status snapshot or one-line error string.
    pub status: Result<StatusResponse, String>,
}

/// Stateful, leader-following client for a `raftkv` cluster.
#[derive(Debug)]
pub struct Client {
    endpoints: Vec<String>,
    channels: BTreeMap<String, Option<Channel>>,
    current_idx: usize,
    leader_hint: Option<String>,
    session_id: u64,
    next_request_id: u64,
    timeout: Duration,
}

impl Client {
    /// Constructs a client targeting `endpoints` (`host:port` strings) with a random `client_id`.
    pub fn connect(endpoints: Vec<String>) -> Result<Self, ClientError> {
        if endpoints.is_empty() {
            return Err(ClientError::InvalidConfig(
                "at least one endpoint must be specified".to_owned(),
            ));
        }

        let mut normalized = Vec::with_capacity(endpoints.len());
        let mut channels = BTreeMap::new();
        for raw in endpoints {
            let clean = normalize_endpoint(&raw)?;
            if channels.insert(clean.clone(), None).is_none() {
                normalized.push(clean);
            }
        }

        let mut client_id: u64 = rand::random();
        if client_id == 0 {
            client_id = 1;
        }

        Ok(Self {
            endpoints: normalized,
            channels,
            current_idx: 0,
            leader_hint: None,
            session_id: client_id,
            next_request_id: 1,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Configures the overall retry deadline per client operation (default `5s`).
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Overrides the client session identifier and next request sequence number.
    ///
    /// Primarily useful for verifying idempotent deduplication across client instances.
    #[must_use]
    pub const fn with_session(mut self, client_id: u64, next_request_id: u64) -> Self {
        self.session_id = client_id;
        self.next_request_id = next_request_id;
        self
    }

    /// Returns this client's session identifier.
    pub const fn client_id(&self) -> u64 {
        self.session_id
    }

    /// Returns the request ID that will be attached to the next write operation.
    pub const fn next_request_id(&self) -> u64 {
        self.next_request_id
    }

    /// Inserts or overwrites `key` with `value`, retrying leader redirects with the same `request_id`.
    pub async fn put(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<PutResult, ClientError> {
        let key = key.into();
        let value = value.into();
        let req_id = self.next_request_id;
        let client_id = self.session_id;

        let res = self
            .retry_kv(|mut client, deadline| {
                let req = PutRequest {
                    client_id,
                    request_id: req_id,
                    key: key.clone(),
                    value: value.clone(),
                };
                async move {
                    let mut grpc = Request::new(req);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    grpc.set_timeout(remaining);
                    client.put(grpc).await.map(|r| PutResult {
                        log_index: r.into_inner().log_index,
                    })
                }
            })
            .await;

        self.next_request_id = self.next_request_id.saturating_add(1);
        res
    }

    /// Deletes `key`, retrying transient errors with the exact same `request_id`.
    pub async fn delete(&mut self, key: impl Into<Bytes>) -> Result<DeleteResult, ClientError> {
        let key = key.into();
        let req_id = self.next_request_id;
        let client_id = self.session_id;

        let res = self
            .retry_kv(|mut client, deadline| {
                let req = DeleteRequest {
                    client_id,
                    request_id: req_id,
                    key: key.clone(),
                };
                async move {
                    let mut grpc = Request::new(req);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    grpc.set_timeout(remaining);
                    client.delete(grpc).await.map(|r| {
                        let inner = r.into_inner();
                        DeleteResult {
                            existed: inner.existed,
                            log_index: inner.log_index,
                        }
                    })
                }
            })
            .await;

        self.next_request_id = self.next_request_id.saturating_add(1);
        res
    }

    /// Atomically swaps `key` to `value` iff its current value matches `expected`.
    pub async fn cas(
        &mut self,
        key: impl Into<Bytes>,
        expected: Option<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<CasResult, ClientError> {
        let key = key.into();
        let value = value.into();
        let req_id = self.next_request_id;
        let client_id = self.session_id;

        let res = self
            .retry_kv(|mut client, deadline| {
                let req = CasRequest {
                    client_id,
                    request_id: req_id,
                    key: key.clone(),
                    expected: expected.clone(),
                    value: value.clone(),
                };
                async move {
                    let mut grpc = Request::new(req);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    grpc.set_timeout(remaining);
                    client.compare_and_swap(grpc).await.map(|r| {
                        let inner = r.into_inner();
                        CasResult {
                            swapped: inner.swapped,
                            current: inner.current,
                            log_index: inner.log_index,
                        }
                    })
                }
            })
            .await;

        self.next_request_id = self.next_request_id.saturating_add(1);
        res
    }

    /// Reads the value at `key` using linearizable (`stale == false`) or local stale semantics.
    pub async fn get(
        &mut self,
        key: impl Into<Bytes>,
        stale: bool,
    ) -> Result<GetResult, ClientError> {
        let key = key.into();
        let consistency = consistency_proto(stale);
        self.retry_kv(|mut client, deadline| {
            let req = GetRequest {
                key: key.clone(),
                consistency,
            };
            async move {
                let mut grpc = Request::new(req);
                let remaining = deadline.saturating_duration_since(Instant::now());
                grpc.set_timeout(remaining);
                client.get(grpc).await.map(|r| {
                    let inner = r.into_inner();
                    GetResult {
                        value: inner.value,
                        applied_index: inner.applied_index,
                    }
                })
            }
        })
        .await
    }

    /// Scans ordered keys in `[start, end)` up to `limit`.
    pub async fn range(
        &mut self,
        start: impl Into<Bytes>,
        end: impl Into<Bytes>,
        limit: u32,
        stale: bool,
    ) -> Result<RangeResult, ClientError> {
        let start = start.into();
        let end = end.into();
        let consistency = consistency_proto(stale);
        self.retry_kv(|mut client, deadline| {
            let req = RangeRequest {
                start: start.clone(),
                end: end.clone(),
                limit,
                consistency,
            };
            async move {
                let mut grpc = Request::new(req);
                let remaining = deadline.saturating_duration_since(Instant::now());
                grpc.set_timeout(remaining);
                client.range(grpc).await.map(|r| {
                    let inner = r.into_inner();
                    RangeResult {
                        kvs: inner.kvs.into_iter().map(|kv| (kv.key, kv.value)).collect(),
                        more: inner.more,
                        applied_index: inner.applied_index,
                    }
                })
            }
        })
        .await
    }

    /// Queries `Admin::Status` on every configured endpoint, returning per-endpoint results.
    pub async fn status_all(&mut self) -> Vec<NodeStatusReport> {
        let endpoints = self.endpoints.clone();
        let per_node_timeout = self.timeout;
        let mut reports = Vec::with_capacity(endpoints.len());

        for ep in endpoints {
            let status = match self.channel_for(&ep, per_node_timeout).await {
                Ok(channel) => {
                    let mut admin = AdminClient::new(channel);
                    let mut req = Request::new(StatusRequest {});
                    req.set_timeout(per_node_timeout);
                    match admin.status(req).await {
                        Ok(resp) => Ok(resp.into_inner()),
                        Err(status) => Err(format!("{} ({})", status.message(), status.code())),
                    }
                }
                Err(err) => {
                    self.clear_channel(&ep);
                    Err(err)
                }
            };
            reports.push(NodeStatusReport {
                endpoint: ep,
                status,
            });
        }
        reports
    }

    async fn retry_kv<F, Fut, T>(&mut self, mut call: F) -> Result<T, ClientError>
    where
        F: FnMut(KvClient<Channel>, Instant) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let deadline = Instant::now() + self.timeout;
        let mut backoff = INITIAL_RETRY_BACKOFF;
        let mut last_err: Option<ClientError> = None;

        loop {
            let target = self.pick_endpoint();
            let now = Instant::now();
            if now >= deadline {
                return Err(last_err.unwrap_or_else(|| {
                    ClientError::Timeout("deadline elapsed before RPC completed".to_owned())
                }));
            }

            let remaining = deadline.saturating_duration_since(now);
            let channel = match self.channel_for(&target, remaining).await {
                Ok(ch) => ch,
                Err(err_msg) => {
                    self.clear_channel(&target);
                    self.advance_after_failure(&target);
                    last_err = Some(ClientError::Rpc {
                        code: Code::Unavailable,
                        message: err_msg,
                    });
                    if !sleep_backoff(&mut backoff, deadline).await {
                        return Err(last_err.unwrap_or_else(|| {
                            ClientError::Timeout("connect attempts timed out".to_owned())
                        }));
                    }
                    continue;
                }
            };

            let kv_client = KvClient::new(channel);
            match call(kv_client, deadline).await {
                Ok(value) => return Ok(value),
                Err(status) => {
                    let code = status.code();
                    tracing::debug!(endpoint = %target, code = ?code, msg = %status.message(), "KV RPC error");

                    let leader_addr = extract_leader_addr(&status);
                    let retryable = is_retryable(code);
                    if !retryable {
                        return Err(ClientError::from(status));
                    }

                    last_err = Some(ClientError::from(status));

                    if code == Code::FailedPrecondition
                        && let Some(addr) = leader_addr
                    {
                        self.register_leader_hint(&addr);
                        if addr != target && Instant::now() < deadline {
                            // Immediate hop when redirected to a different node.
                            continue;
                        }
                    } else {
                        if code == Code::Unavailable {
                            self.clear_channel(&target);
                        }
                        self.advance_after_failure(&target);
                    }

                    if !sleep_backoff(&mut backoff, deadline).await {
                        return Err(last_err.unwrap_or_else(|| {
                            ClientError::Timeout("cluster request timed out".to_owned())
                        }));
                    }
                }
            }
        }
    }

    fn pick_endpoint(&self) -> String {
        if let Some(hint) = &self.leader_hint {
            return hint.clone();
        }
        self.endpoints[self.current_idx].clone()
    }

    fn register_leader_hint(&mut self, addr: &str) {
        if let Ok(clean) = normalize_endpoint(addr) {
            if !self.channels.contains_key(&clean) {
                self.channels.insert(clean.clone(), None);
                self.endpoints.push(clean.clone());
            }
            if let Some(pos) = self.endpoints.iter().position(|e| e == &clean) {
                self.current_idx = pos;
            }
            self.leader_hint = Some(clean);
        }
    }

    fn advance_after_failure(&mut self, failed_endpoint: &str) {
        if self.leader_hint.as_deref() == Some(failed_endpoint) {
            self.leader_hint = None;
        }
        if !self.endpoints.is_empty() {
            self.current_idx = (self.current_idx + 1) % self.endpoints.len();
        }
    }

    fn clear_channel(&mut self, endpoint: &str) {
        if let Some(slot) = self.channels.get_mut(endpoint) {
            *slot = None;
        }
    }

    async fn channel_for(
        &mut self,
        endpoint: &str,
        remaining: Duration,
    ) -> Result<Channel, String> {
        if let Some(Some(ch)) = self.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let uri = format!("http://{endpoint}");
        let ep = Endpoint::from_shared(uri)
            .map_err(|err| err.to_string())?
            .connect_timeout(CONNECT_TIMEOUT.min(remaining))
            .tcp_nodelay(true);
        let ch = ep.connect().await.map_err(|err| err.to_string())?;
        self.channels.insert(endpoint.to_owned(), Some(ch.clone()));
        Ok(ch)
    }
}

fn normalize_endpoint(raw: &str) -> Result<String, ClientError> {
    let trimmed = raw.trim().strip_prefix("http://").unwrap_or(raw.trim());
    if trimmed.is_empty() || trimmed.contains("://") || trimmed.contains('/') {
        return Err(ClientError::InvalidConfig(format!(
            "invalid endpoint '{raw}' (expected host:port)"
        )));
    }
    let test_uri = format!("http://{trimmed}");
    test_uri
        .parse::<tonic::transport::Uri>()
        .map_err(|err| ClientError::InvalidConfig(format!("invalid endpoint '{raw}': {err}")))?;
    Ok(trimmed.to_owned())
}

fn consistency_proto(stale: bool) -> i32 {
    if stale {
        Consistency::Stale as i32
    } else {
        Consistency::Linearizable as i32
    }
}

fn is_retryable(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::Aborted
            | Code::DeadlineExceeded
            | Code::FailedPrecondition
            | Code::ResourceExhausted
    )
}

fn extract_leader_addr(status: &Status) -> Option<String> {
    status
        .metadata()
        .get("x-raftkv-leader-addr")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

async fn sleep_backoff(backoff: &mut Duration, deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }
    let remaining = deadline.saturating_duration_since(now);
    let wait = (*backoff).min(remaining);
    if wait.is_zero() {
        return false;
    }
    tokio::time::sleep(wait).await;
    *backoff = (*backoff * 2).min(MAX_RETRY_BACKOFF);
    Instant::now() < deadline
}
