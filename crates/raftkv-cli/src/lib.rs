//! Leader-following gRPC client library and `raftkvctl` CLI helpers.
//!
//! Role: connects to `raftkv` clusters, follows `x-raftkv-leader-addr` redirects,
//! retries transient write errors while preserving `(client_id, request_id)` for
//! idempotency, and formats human-readable or JSON CLI outputs.
//!
//! Invariants:
//! - Same `(client_id, request_id)` is reused across every retry of one write.
//! - `INVALID_ARGUMENT` is never retried.
//!
//! Deliberately omitted:
//! - Cluster configuration mutations or server-side consensus state.

#![forbid(unsafe_code)]

pub mod client;
pub mod format;

pub use client::{
    CasResult, Client, ClientError, DeleteResult, GetResult, NodeStatusReport, PutResult,
    RangeResult,
};
pub use format::{
    HealthSummary, OutputFormat, compute_prefix_end, evaluate_health, format_bytes, format_cas,
    format_delete, format_get, format_health, format_put, format_range, format_status,
    parse_duration,
};

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use raftkv_proto::kv_server::{Kv, KvServer};
    use raftkv_proto::{
        CasRequest, CasResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
        PutRequest, PutResponse, RangeRequest, RangeResponse,
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::metadata::MetadataValue;
    use tonic::{Code, Request, Response, Status};

    use super::*;

    struct RedirectingKv {
        leader_addr: String,
    }

    #[tonic::async_trait]
    impl Kv for RedirectingKv {
        async fn put(
            &self,
            _request: Request<PutRequest>,
        ) -> Result<Response<PutResponse>, Status> {
            let mut status = Status::failed_precondition("not leader; current leader is 2");
            if let Ok(v) = MetadataValue::try_from("2") {
                status.metadata_mut().insert("x-raftkv-leader-id", v);
            }
            if let Ok(v) = MetadataValue::try_from(self.leader_addr.as_str()) {
                status.metadata_mut().insert("x-raftkv-leader-addr", v);
            }
            Err(status)
        }

        async fn get(
            &self,
            _request: Request<GetRequest>,
        ) -> Result<Response<GetResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn delete(
            &self,
            _request: Request<DeleteRequest>,
        ) -> Result<Response<DeleteResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn range(
            &self,
            _request: Request<RangeRequest>,
        ) -> Result<Response<RangeResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn compare_and_swap(
            &self,
            _request: Request<CasRequest>,
        ) -> Result<Response<CasResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    struct AbortingThenOkKv {
        attempts: Arc<AtomicUsize>,
        seen_ids: Arc<tokio::sync::mpsc::UnboundedSender<(u64, u64)>>,
        fail_invalid: bool,
    }

    #[tonic::async_trait]
    impl Kv for AbortingThenOkKv {
        async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
            let inner = request.into_inner();
            let _ = self.seen_ids.send((inner.client_id, inner.request_id));
            if self.fail_invalid {
                return Err(Status::invalid_argument("empty key"));
            }
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                return Err(Status::aborted("proposal overwritten in newer term"));
            }
            Ok(Response::new(PutResponse { log_index: 7 }))
        }

        async fn get(
            &self,
            _request: Request<GetRequest>,
        ) -> Result<Response<GetResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn delete(
            &self,
            _request: Request<DeleteRequest>,
        ) -> Result<Response<DeleteResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn range(
            &self,
            _request: Request<RangeRequest>,
        ) -> Result<Response<RangeResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }

        async fn compare_and_swap(
            &self,
            _request: Request<CasRequest>,
        ) -> Result<Response<CasResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }

    async fn spawn_kv<K: Kv>(svc: K) -> (SocketAddr, oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let incoming = TcpListenerStream::new(listener);
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(KvServer::new(svc))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = rx.await;
                })
                .await;
        });
        (addr, tx)
    }

    #[tokio::test]
    async fn follows_leader_redirect_and_preserves_request_id_across_aborted_retry() {
        let _ = std::marker::PhantomData::<fn()>;
        let attempts = Arc::new(AtomicUsize::new(0));
        let (id_tx, mut id_rx) = tokio::sync::mpsc::unbounded_channel();

        let (leader_addr, stop_leader) = spawn_kv(AbortingThenOkKv {
            attempts: Arc::clone(&attempts),
            seen_ids: Arc::new(id_tx),
            fail_invalid: false,
        })
        .await;

        let (follower_addr, stop_follower) = spawn_kv(RedirectingKv {
            leader_addr: leader_addr.to_string(),
        })
        .await;

        let mut client = Client::connect(vec![follower_addr.to_string()])
            .expect("connect")
            .with_timeout(Duration::from_secs(3))
            .with_session(99, 1);

        let res = client.put("k", "v").await.expect("put succeeds on retry");
        assert_eq!(res.log_index, 7);
        assert_eq!(client.next_request_id(), 2);

        let first = id_rx.recv().await.expect("first attempt recorded");
        let second = id_rx.recv().await.expect("second attempt recorded");
        assert_eq!(first, (99, 1));
        assert_eq!(second, (99, 1));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let _ = stop_leader.send(());
        let _ = stop_follower.send(());
    }

    #[tokio::test]
    async fn does_not_retry_invalid_argument() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (id_tx, mut id_rx) = tokio::sync::mpsc::unbounded_channel();
        let (addr, stop) = spawn_kv(AbortingThenOkKv {
            attempts: Arc::clone(&attempts),
            seen_ids: Arc::new(id_tx),
            fail_invalid: true,
        })
        .await;

        let mut client = Client::connect(vec![addr.to_string()])
            .expect("connect")
            .with_timeout(Duration::from_secs(2));
        let err = client.put("bad", "v").await.expect_err("must fail fast");
        match err {
            ClientError::Rpc { code, .. } => assert_eq!(code, Code::InvalidArgument),
            other => panic!("unexpected error variant: {other}"),
        }
        let first = id_rx.recv().await.expect("one attempt");
        assert_eq!(first.1, 1);
        assert!(id_rx.try_recv().is_err(), "must not retry INVALID_ARGUMENT");

        let _ = stop.send(());
    }
}
