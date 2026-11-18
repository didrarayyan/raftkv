//! Per-key Wing & Gong linearizability checker with memoization.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::client::{HistoryOp, SimOpKind, SimOpOutcome};

/// Verifies linearizability independently for every key in `history`.
pub fn verify_linearizability(history: &[HistoryOp]) -> Result<(), String> {
    let mut by_key: BTreeMap<Bytes, Vec<HistoryOp>> = BTreeMap::new();
    for op in history {
        by_key.entry(op.key.clone()).or_default().push(op.clone());
    }

    for (key, mut ops) in by_key {
        ops.sort_by_key(|o| (o.invoke_tick, o.id));
        check_key_history(&key, &ops)?;
    }
    Ok(())
}

fn check_key_history(key: &[u8], ops: &[HistoryOp]) -> Result<(), String> {
    let mut done = vec![false; ops.len()];
    // Memoize by (first index after last done op, undone indices before it, register value).
    let mut visited: BTreeSet<(usize, Vec<usize>, Option<Bytes>)> = BTreeSet::new();
    let total_completed = ops.iter().filter(|o| o.complete_tick.is_some()).count();

    if dfs(ops, &mut done, 0, total_completed, None, &mut visited) {
        return Ok(());
    }

    let summary: Vec<String> = ops
        .iter()
        .take(12)
        .map(|o| {
            format!(
                "op#{}(c={} r={} {:?} [{}..{:?}] -> {:?})",
                o.id, o.client_id, o.request_id, o.kind, o.invoke_tick, o.complete_tick, o.outcome
            )
        })
        .collect();
    Err(format!(
        "Linearizability violated on key {:?} (total ops={}); sample: {}",
        String::from_utf8_lossy(key),
        ops.len(),
        summary.join(", ")
    ))
}

enum NextState {
    Invalid,
    Valid(Option<Bytes>),
}

fn dfs(
    ops: &[HistoryOp],
    done: &mut [bool],
    completed_done: usize,
    total_completed: usize,
    state: Option<&Bytes>,
    visited: &mut BTreeSet<(usize, Vec<usize>, Option<Bytes>)>,
) -> bool {
    if completed_done == total_completed {
        return true;
    }

    let last_true = done.iter().rposition(|&d| d).map_or(0, |p| p + 1);
    let prefix_undone: Vec<usize> = (0..last_true).filter(|&i| !done[i]).collect();
    if !visited.insert((last_true, prefix_undone, state.cloned())) {
        return false;
    }

    // Real-time precedence constraint: no eligible op can be invoked strictly after any
    // un-linearized completed op's completion tick.
    let min_complete = ops
        .iter()
        .enumerate()
        .filter(|(i, o)| !done[*i] && o.complete_tick.is_some())
        .filter_map(|(_, o)| o.complete_tick)
        .min()
        .unwrap_or(u64::MAX);

    for (i, op) in ops.iter().enumerate() {
        if done[i] {
            continue;
        }
        if op.invoke_tick > min_complete {
            break;
        }
        // Only try mutating indeterminate ops (indeterminate Get has no observable state effect).
        if op.complete_tick.is_none() && matches!(op.kind, SimOpKind::Get) {
            continue;
        }

        if let NextState::Valid(next_state) = apply_if_valid(state, op) {
            done[i] = true;
            let inc = usize::from(op.complete_tick.is_some());
            if dfs(
                ops,
                done,
                completed_done + inc,
                total_completed,
                next_state.as_ref(),
                visited,
            ) {
                return true;
            }
            done[i] = false;
        }
    }

    false
}

fn apply_if_valid(state: Option<&Bytes>, op: &HistoryOp) -> NextState {
    match (&op.kind, &op.outcome) {
        (SimOpKind::Put(val), Some(SimOpOutcome::Put) | None) => {
            NextState::Valid(Some(val.clone()))
        }
        (SimOpKind::Delete, Some(SimOpOutcome::Delete { existed })) => {
            if *existed == state.is_some() {
                NextState::Valid(None)
            } else {
                NextState::Invalid
            }
        }
        (SimOpKind::Delete, None) => NextState::Valid(None),
        (SimOpKind::Cas { expected, value }, Some(SimOpOutcome::Cas { swapped, current })) => {
            let should_swap = state == expected.as_ref();
            if *swapped != should_swap {
                return NextState::Invalid;
            }
            if should_swap {
                if current.as_ref() == Some(value) {
                    NextState::Valid(Some(value.clone()))
                } else {
                    NextState::Invalid
                }
            } else if current.as_ref() == state {
                NextState::Valid(state.cloned())
            } else {
                NextState::Invalid
            }
        }
        (SimOpKind::Cas { expected, value }, None) => {
            if state == expected.as_ref() {
                NextState::Valid(Some(value.clone()))
            } else {
                NextState::Valid(state.cloned())
            }
        }
        (SimOpKind::Get, Some(SimOpOutcome::Get(observed))) => {
            if observed.as_ref() == state {
                NextState::Valid(state.cloned())
            } else {
                NextState::Invalid
            }
        }
        _ => NextState::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_op(
        id: usize,
        key: &'static [u8],
        kind: SimOpKind,
        invoke: u64,
        complete: Option<u64>,
        outcome: Option<SimOpOutcome>,
    ) -> HistoryOp {
        HistoryOp {
            id,
            client_id: id as u64 + 1,
            request_id: 1,
            key: Bytes::from_static(key),
            kind,
            invoke_tick: invoke,
            complete_tick: complete,
            outcome,
        }
    }

    #[test]
    fn accepts_concurrent_linearizable_history_and_indeterminate_write() {
        let hist = vec![
            mk_op(
                0,
                b"k0",
                SimOpKind::Put(Bytes::from_static(b"v1")),
                1,
                None,
                None,
            ),
            mk_op(
                1,
                b"k0",
                SimOpKind::Get,
                5,
                Some(10),
                Some(SimOpOutcome::Get(Some(Bytes::from_static(b"v1")))),
            ),
        ];
        assert!(verify_linearizability(&hist).is_ok());
    }

    #[test]
    fn rejects_stale_read_after_completed_put() {
        let hist = vec![
            mk_op(
                0,
                b"k0",
                SimOpKind::Put(Bytes::from_static(b"v1")),
                1,
                Some(5),
                Some(SimOpOutcome::Put),
            ),
            mk_op(
                1,
                b"k0",
                SimOpKind::Get,
                6,
                Some(10),
                Some(SimOpOutcome::Get(None)),
            ),
        ];
        assert!(verify_linearizability(&hist).is_err());
    }
}
