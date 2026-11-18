//! Leader's per-peer replication and liveness tracking state.

use crate::types::Index;

/// Replication and liveness progress tracked by a leader for one remote peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Progress {
    pub(crate) match_index: Index,
    pub(crate) next_index: Index,
    pub(crate) recently_active: bool,
}

impl Progress {
    pub(crate) fn new(next_index: Index) -> Self {
        Self {
            match_index: Index(0),
            next_index,
            recently_active: false,
        }
    }
}
