//! Multi-node Raft KV daemon runtime (`raftkvd`).
//!
//! Role: loads TOML server configuration, recovers durable state via [`raftkv_wal`],
//! drives [`raft_core::RaftNode`] and [`raftkv_kv::KvStore`] on a single-owner Tokio
//! task, persists batches on a dedicated OS WAL thread, and exposes gRPC `Kv`,
//! `Admin`, and `Raft` endpoints.
//!
//! Invariants:
//! - Zero shared-memory mutexes or read-write locks; state machines are exclusively owned.
//! - Outbound Raft RPCs and state-machine applies wait for synchronous WAL sync.
//! - Any WAL I/O failure logs an error and aborts the process (`std::process::abort`).

#![forbid(unsafe_code)]

pub mod codec;
pub mod config;
pub mod driver;
pub mod peer;
pub mod service;
pub mod wal_thread;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use raft_core::{RaftNode, Term};
use raftkv_kv::KvStore;
use raftkv_proto::admin_server::AdminServer;
use raftkv_proto::kv_server::KvServer;
use raftkv_proto::raft_server::RaftServer;
use raftkv_wal::{StdFs, Wal, WalOptions};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tonic::transport::Server;

use crate::config::ServerConfig;
use crate::driver::{DRIVER_CHANNEL_CAP, DriverConfig, DriverInput, PEER_CHANNEL_CAP, RaftDriver};
use crate::peer::run_peer_sender;
use crate::service::{RpcServices, ServiceMeta};
use crate::wal_thread::WalHandle;

const WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Acquires the exclusive `<data_dir>/LOCK` file for the lifetime of the server process.
pub fn acquire_data_lock(data_dir: &Path) -> anyhow::Result<File> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data_dir {}", data_dir.display()))?;
    let lock_path = data_dir.join("LOCK");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("failed to open lock file {}", lock_path.display()))?;

    match lock_file.try_lock() {
        Ok(()) => Ok(lock_file),
        Err(TryLockError::WouldBlock) => {
            bail!(
                "data directory lock {} is already held by another raftkvd process",
                lock_path.display()
            );
        }
        Err(TryLockError::Error(err)) => {
            Err(err).with_context(|| format!("failed to lock {}", lock_path.display()))
        }
    }
}

/// Starts `raftkvd` from `cfg` and runs until `shutdown` resolves.
pub async fn run_with_shutdown<F>(cfg: ServerConfig, shutdown: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    cfg.validate()?;
    let listen_addr = cfg.listen_socket_addr()?;
    let _lock_file = acquire_data_lock(&cfg.data_dir)?;

    let wal_opts = WalOptions {
        segment_bytes: WAL_SEGMENT_BYTES,
        retain_entries: cfg.raft.retain_entries,
    };
    let (wal, recovered) = Wal::open(StdFs, &cfg.data_dir, wal_opts)
        .with_context(|| format!("failed to recover WAL in {}", cfg.data_dir.display()))?;

    let mut kv = KvStore::new();
    let last_applied_term = if let Some(snap) = &recovered.snapshot {
        kv.restore(snap.meta.index, &snap.data)
            .context("failed to restore KV snapshot from WAL")?;
        snap.meta.term
    } else {
        Term(0)
    };

    let entropy: u64 = rand::random();
    let raft_seed = entropy ^ cfg.node_id.rotate_left(17);
    let node = RaftNode::new(cfg.to_raft_config(raft_seed), recovered)
        .context("failed to initialize Raft node")?;
    let initial_status = node.status();

    let wal_handle = WalHandle::spawn(wal).context("failed to spawn WAL persistence thread")?;
    let (driver_tx, driver_rx) = mpsc::channel::<DriverInput>(DRIVER_CHANNEL_CAP);

    let self_id = cfg.local_node_id();
    let peer_addrs = cfg.peer_map();
    let mut peer_txs = BTreeMap::new();
    let mut peer_handles = Vec::new();
    for (&peer_id, peer_addr) in &peer_addrs {
        if peer_id == self_id {
            continue;
        }
        let (tx, rx) = mpsc::channel(PEER_CHANNEL_CAP);
        peer_txs.insert(peer_id, tx);
        let peer_seed = entropy ^ peer_id.0.rotate_left(29);
        let addr = peer_addr.clone();
        peer_handles.push(tokio::spawn(run_peer_sender(
            self_id, peer_id, addr, peer_seed, rx,
        )));
    }

    let driver = RaftDriver::new(
        node,
        kv,
        wal_handle,
        DriverConfig {
            node_id: self_id,
            peer_addrs: peer_addrs.clone(),
            peer_txs,
            snapshot_threshold: cfg.raft.snapshot_threshold,
            last_applied_term,
        },
    );
    let driver_handle = tokio::spawn(driver.run(driver_rx));

    let (stop_tx, mut stop_rx) = watch::channel(false);
    let tick_tx = driver_tx.clone();
    let tick_interval = Duration::from_millis(cfg.raft.tick_ms);
    let ticker_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(mpsc::error::TrySendError::Closed(_)) = tick_tx.try_send(DriverInput::Tick) {
                        break;
                    }
                }
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    let services = RpcServices::new(
        Arc::new(ServiceMeta {
            node_id: self_id,
            peers: peer_addrs,
            limits: cfg.limits,
        }),
        driver_tx.clone(),
    );

    tracing::info!(
        node = %self_id,
        term = %initial_status.term,
        role = %initial_status.role,
        listen_addr = %listen_addr,
        "raftkvd listening"
    );

    let max_frame_bytes = cfg
        .limits
        .max_value_bytes
        .max(2 * 1024 * 1024)
        .saturating_add(64 * 1024);

    let serve_result = Server::builder()
        .tcp_nodelay(true)
        .add_service(
            KvServer::new(services.clone())
                .max_decoding_message_size(max_frame_bytes)
                .max_encoding_message_size(max_frame_bytes),
        )
        .add_service(AdminServer::new(services.clone()))
        .add_service(
            RaftServer::new(services)
                .max_decoding_message_size(64 * 1024 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_shutdown(listen_addr, shutdown)
        .await;

    let _ = stop_tx.send(true);
    let _ = ticker_handle.await;

    let (sd_tx, sd_rx) = oneshot::channel();
    if driver_tx
        .send(DriverInput::Shutdown { reply: sd_tx })
        .await
        .is_ok()
    {
        let _ = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, sd_rx).await;
    }
    let _ = driver_handle.await;

    for handle in peer_handles {
        handle.abort();
    }

    serve_result.context("gRPC server exited with error")
}
