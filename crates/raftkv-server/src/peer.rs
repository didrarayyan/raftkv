//! Per-peer asynchronous gRPC sender tasks with exponential backoff and drop-on-overload.

use std::time::Duration;

use raft_core::{Message, MessageBody, NodeId};
use raftkv_proto::raft_client::RaftClient;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use crate::codec::{encode_snapshot_chunks, encode_step_message};

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const STEP_RPC_TIMEOUT: Duration = Duration::from_millis(1000);
const SNAPSHOT_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF_MS: u64 = 50;
const MAX_BACKOFF_MS: u64 = 2000;

/// Runs the outbound RPC loop for a single remote peer until the channel closes.
pub async fn run_peer_sender(
    self_id: NodeId,
    peer_id: NodeId,
    peer_addr: String,
    seed: u64,
    mut rx: mpsc::Receiver<Message>,
) {
    let uri = format!("http://{peer_addr}");
    let mut state = PeerConnState::new(self_id, peer_id, uri, seed);

    while let Some(msg) = rx.recv().await {
        state.handle_message(msg).await;
    }
}

struct PeerConnState {
    self_id: NodeId,
    peer_id: NodeId,
    uri: String,
    client: Option<RaftClient<Channel>>,
    backoff_ms: u64,
    next_retry_at: Option<Instant>,
    rng: ChaCha8Rng,
}

impl PeerConnState {
    fn new(self_id: NodeId, peer_id: NodeId, uri: String, seed: u64) -> Self {
        Self {
            self_id,
            peer_id,
            uri,
            client: None,
            backoff_ms: INITIAL_BACKOFF_MS,
            next_retry_at: None,
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    async fn handle_message(&mut self, msg: Message) {
        if let Some(retry_at) = self.next_retry_at {
            if Instant::now() < retry_at {
                // Peer recently failed; drop queued outbound messages rather than backing up Raft.
                return;
            }
            self.next_retry_at = None;
        }

        let client = match self.ensure_connected().await {
            Ok(client) => client,
            Err(err) => {
                tracing::debug!(
                    node = %self.self_id,
                    peer = %self.peer_id,
                    uri = %self.uri,
                    error = %err,
                    "failed to connect to peer"
                );
                self.record_failure();
                return;
            }
        };

        let result = send_rpc(client, self.self_id, msg).await;
        match result {
            Ok(()) => {
                self.backoff_ms = INITIAL_BACKOFF_MS;
                self.next_retry_at = None;
            }
            Err(err) => {
                tracing::debug!(
                    node = %self.self_id,
                    peer = %self.peer_id,
                    error = %err,
                    "peer RPC failed"
                );
                self.client = None;
                self.record_failure();
            }
        }
    }

    async fn ensure_connected(&mut self) -> Result<RaftClient<Channel>, tonic::transport::Error> {
        if let Some(client) = &self.client {
            return Ok(client.clone());
        }
        let endpoint = Endpoint::from_shared(self.uri.clone())?
            .connect_timeout(PEER_CONNECT_TIMEOUT)
            .tcp_nodelay(true);
        let channel = endpoint.connect().await?;
        let client = RaftClient::new(channel);
        self.client = Some(client.clone());
        Ok(client)
    }

    fn record_failure(&mut self) {
        let base = self.backoff_ms.min(MAX_BACKOFF_MS);
        let max_jitter = (base / 4).max(1);
        let jitter = self.rng.random_range(0..=max_jitter);
        let delay = Duration::from_millis((base + jitter).min(MAX_BACKOFF_MS + max_jitter));
        self.next_retry_at = Some(Instant::now() + delay);
        self.backoff_ms = (self.backoff_ms.saturating_mul(2)).min(MAX_BACKOFF_MS);
    }
}

async fn send_rpc(
    mut client: RaftClient<Channel>,
    self_id: NodeId,
    msg: Message,
) -> Result<(), String> {
    if let MessageBody::InstallSnapshot { snapshot } = msg.body {
        let chunks = encode_snapshot_chunks(self_id, msg.to, msg.term, &snapshot);
        let mut req = Request::new(tokio_stream::iter(chunks));
        req.set_timeout(SNAPSHOT_RPC_TIMEOUT);
        client
            .install_snapshot(req)
            .await
            .map(|_| ())
            .map_err(|status| status.to_string())
    } else {
        let wire = encode_step_message(&msg).map_err(|err| err.to_string())?;
        let mut req = Request::new(wire);
        req.set_timeout(STEP_RPC_TIMEOUT);
        client
            .step(req)
            .await
            .map(|_| ())
            .map_err(|status| status.to_string())
    }
}
