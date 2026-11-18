//! Dedicated OS thread executing synchronous WAL writes and snapshot persistence.

use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use raft_core::{Entry, HardState, Snapshot};
use raftkv_wal::{Batch, StdFs, Wal};
use tokio::sync::oneshot;

/// Owned WAL write batch transferred from the Tokio driver to the WAL OS thread.
#[derive(Debug, Clone)]
pub struct OwnedBatch {
    /// Optional snapshot to save atomically before appending entries.
    pub snapshot: Option<Snapshot>,
    /// Optional updated durable hard state.
    pub hard_state: Option<HardState>,
    /// Contiguous log entries to persist (or suffix replacement starting at `entries[0].index`).
    pub entries: Vec<Entry>,
}

/// Single persistence request acknowledged once `Wal::persist` syncs to disk.
#[derive(Debug)]
pub struct PersistJob {
    /// Owned batch to persist.
    pub batch: OwnedBatch,
    /// Completion signal sent back to the awaiting driver task.
    pub reply: oneshot::Sender<()>,
}

/// Handle held exclusively by the driver to submit persistence jobs to the WAL thread.
#[derive(Debug)]
pub struct WalHandle {
    tx: Option<mpsc::Sender<PersistJob>>,
    join: Option<JoinHandle<()>>,
}

impl WalHandle {
    /// Spawns a dedicated OS thread that owns `wal` and aborts the process on any [`raftkv_wal::WalError`].
    pub fn spawn(mut wal: Wal<StdFs>) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::channel::<PersistJob>();
        let join = thread::Builder::new()
            .name("raftkv-wal".to_owned())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    let batch = Batch {
                        snapshot: job.batch.snapshot.as_ref(),
                        hard_state: job.batch.hard_state.as_ref(),
                        entries: &job.batch.entries,
                    };
                    if let Err(err) = wal.persist(batch) {
                        tracing::error!(error = %err, "fatal WAL persistence error; aborting");
                        std::process::abort();
                    }
                    let _ = job.reply.send(());
                }
            })?;

        Ok(Self {
            tx: Some(tx),
            join: Some(join),
        })
    }

    /// Wraps an external channel sender (used by driver unit tests with a simulated WAL).
    pub fn from_sender(tx: mpsc::Sender<PersistJob>) -> Self {
        Self {
            tx: Some(tx),
            join: None,
        }
    }

    /// Enqueues `batch` for synchronous persistence and resolves once synced on disk.
    pub async fn persist(&self, batch: OwnedBatch) -> Result<(), WalClosed> {
        let Some(tx) = self.tx.as_ref() else {
            return Err(WalClosed);
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(PersistJob {
            batch,
            reply: reply_tx,
        })
        .map_err(|_| WalClosed)?;
        reply_rx.await.map_err(|_| WalClosed)
    }

    /// Closes the job channel and waits for the WAL OS thread to flush and exit.
    pub fn shutdown(mut self) {
        self.tx.take();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Error returned if the WAL thread exited unexpectedly during shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalClosed;
