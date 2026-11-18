//! Leader's per-peer replication state machine (`Probe`, `Replicate`, `Snapshot`).

use crate::types::Index;

/// Replication mode for a remote follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressState {
    /// Slow probing: at most one append in flight, reset on heartbeat.
    Probe {
        in_flight: bool,
        last_probe_prev_index: Option<Index>,
    },
    /// Optimistic pipelined replication: `next_index` advances as batches are queued.
    Replicate,
    /// Waiting for `InstallSnapshotResponse` covering `index`; log appends are paused.
    Snapshot { index: Index, ticks: u32 },
}

/// Replication and liveness progress tracked by a leader for one remote peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Progress {
    pub(crate) match_index: Index,
    pub(crate) next_index: Index,
    pub(crate) state: ProgressState,
    pub(crate) recently_active: bool,
}

impl Progress {
    pub(crate) fn new(next_index: Index) -> Self {
        Self {
            match_index: Index(0),
            next_index,
            state: ProgressState::Probe {
                in_flight: false,
                last_probe_prev_index: None,
            },
            recently_active: false,
        }
    }

    pub(crate) fn become_probe(&mut self) {
        self.state = ProgressState::Probe {
            in_flight: false,
            last_probe_prev_index: None,
        };
    }

    pub(crate) fn become_replicate(&mut self) {
        self.state = ProgressState::Replicate;
    }

    pub(crate) fn become_snapshot(&mut self, index: Index) {
        self.state = ProgressState::Snapshot { index, ticks: 0 };
    }
}
