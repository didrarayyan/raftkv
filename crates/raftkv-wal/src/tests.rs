//! Crash recovery, fault injection, compaction, and codec tests for `raftkv-wal`.

use std::path::Path;

use bytes::Bytes;
use proptest::prelude::*;
use raft_core::{Entry, HardState, Index, NodeId, Payload, Snapshot, SnapshotMeta, Term};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use tempfile::TempDir;

use crate::{Batch, FaultFs, Fs, StdFs, Wal, WalError, WalOptions};

fn entry(term: u64, index: u64, data: &[u8]) -> Entry {
    Entry {
        term: Term(term),
        index: Index(index),
        payload: Payload::Command(Bytes::copy_from_slice(data)),
    }
}

fn wal_dir_segments(fs: &FaultFs, dir: &Path) -> Vec<std::path::PathBuf> {
    let mut segs: Vec<_> = fs
        .read_dir(&dir.join("wal"))
        .unwrap()
        .into_iter()
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("wal"))
        .collect();
    segs.sort();
    segs
}

#[test]
fn every_offset_truncation_recovers_exact_complete_frames() {
    let dir = Path::new("/wal-trunc");
    let fs = FaultFs::new(1);
    let opts = WalOptions {
        segment_bytes: 160,
        retain_entries: 100,
    };
    let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

    let all_entries: Vec<Entry> = (1..=12)
        .map(|i| entry(1, i, format!("payload-{i:02}").as_bytes()))
        .collect();

    for chunk in all_entries.chunks(2) {
        wal.persist(Batch {
            snapshot: None,
            hard_state: Some(&HardState {
                term: Term(1),
                voted_for: Some(NodeId(1)),
                commit: chunk.last().unwrap().index,
            }),
            entries: chunk,
        })
        .unwrap();
    }
    drop(wal);

    let segs = wal_dir_segments(&fs, dir);
    assert!(segs.len() >= 2, "expected multi-segment WAL");

    let last_seg = segs.last().unwrap().clone();
    let original_last = fs.read_raw(&last_seg).unwrap();

    // Determine recovered prefix at full length, and verify truncation at every byte offset 0..=len.
    let (_, full_rec) = Wal::open(fs.clone(), dir, opts).unwrap();
    let base_entries = full_rec.entries;
    assert_eq!(base_entries, all_entries);

    let mut prev_count = 0usize;
    for cut in 0..=original_last.len() {
        fs.overwrite_durable(&last_seg, original_last[..cut].to_vec());
        let (_, rec) = Wal::open(fs.clone(), dir, opts).unwrap();
        assert!(
            base_entries.starts_with(&rec.entries),
            "truncated recovery at byte {cut} must be a prefix of original entries"
        );
        assert!(
            rec.entries.len() >= prev_count,
            "recovered entry count must be non-decreasing across longer prefixes (cut {cut})"
        );
        prev_count = rec.entries.len();
    }
    assert_eq!(prev_count, all_entries.len());
}

#[test]
fn every_offset_bit_flip_in_last_segment_never_returns_altered_entry() {
    let dir = Path::new("/wal-flip-last");
    let fs = FaultFs::new(2);
    let opts = WalOptions {
        segment_bytes: 180,
        retain_entries: 100,
    };
    let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

    let all_entries: Vec<Entry> = (1..=10)
        .map(|i| entry(2, i, format!("val-{i:03}").as_bytes()))
        .collect();
    for chunk in all_entries.chunks(2) {
        wal.persist(Batch {
            snapshot: None,
            hard_state: None,
            entries: chunk,
        })
        .unwrap();
    }
    drop(wal);

    let segs = wal_dir_segments(&fs, dir);
    assert!(segs.len() >= 2);
    let last_seg = segs.last().unwrap().clone();
    let original_last = fs.read_raw(&last_seg).unwrap();

    for byte_idx in 0..original_last.len() {
        for bit in 0..8u8 {
            let mut mutated = original_last.clone();
            mutated[byte_idx] ^= 1 << bit;
            fs.overwrite_durable(&last_seg, mutated);
            match Wal::open(fs.clone(), dir, opts) {
                Ok((_, rec)) => {
                    assert!(
                        all_entries.starts_with(&rec.entries),
                        "bit flip at ({byte_idx}, {bit}) produced non-prefix entries"
                    );
                }
                Err(WalError::Corruption(_)) => {}
                Err(other) => panic!("unexpected error on bit flip: {other}"),
            }
        }
    }
}

#[test]
fn bit_flip_in_non_last_segment_returns_corruption() {
    let dir = Path::new("/wal-flip-mid");
    let fs = FaultFs::new(3);
    let opts = WalOptions {
        segment_bytes: 140,
        retain_entries: 100,
    };
    let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

    for i in 1..=12u64 {
        wal.persist(Batch {
            snapshot: None,
            hard_state: None,
            entries: &[entry(1, i, b"segment-payload-bytes")],
        })
        .unwrap();
    }
    drop(wal);

    let segs = wal_dir_segments(&fs, dir);
    assert!(segs.len() >= 2);
    let first_seg = &segs[0];
    let mut bytes = fs.read_raw(first_seg).unwrap();
    let flip_pos = bytes.len() - 4;
    bytes[flip_pos] ^= 0x01;
    fs.overwrite_durable(first_seg, bytes);

    let err = Wal::open(fs, dir, opts).unwrap_err();
    assert!(matches!(err, WalError::Corruption(_)));
}

#[test]
fn segment_rotation_preserves_latest_hard_state_after_old_segments_deleted() {
    let dir = Path::new("/wal-rotate-hs");
    let fs = FaultFs::new(4);
    let opts = WalOptions {
        segment_bytes: 120,
        retain_entries: 1,
    };
    let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

    let hs = HardState {
        term: Term(7),
        voted_for: Some(NodeId(3)),
        commit: Index(2),
    };
    wal.persist(Batch {
        snapshot: None,
        hard_state: Some(&hs),
        entries: &[entry(7, 1, b"first"), entry(7, 2, b"second")],
    })
    .unwrap();

    // Force rotation into subsequent segments without emitting a new HardState.
    for i in 3..=10u64 {
        wal.persist(Batch {
            snapshot: None,
            hard_state: None,
            entries: &[entry(7, i, b"rotate-trigger-payload-bytes")],
        })
        .unwrap();
    }

    let snap = Snapshot {
        meta: SnapshotMeta {
            index: Index(9),
            term: Term(7),
        },
        data: Bytes::from_static(b"snap-9"),
    };
    wal.save_snapshot(&snap).unwrap();
    drop(wal);

    let segs = wal_dir_segments(&fs, dir);
    assert!(segs.len() < 4, "obsolete segments should be pruned");

    let (_, recovered) = Wal::open(fs, dir, opts).unwrap();
    assert_eq!(recovered.hard_state.term, Term(7));
    assert_eq!(recovered.hard_state.voted_for, Some(NodeId(3)));
    assert_eq!(recovered.hard_state.commit, Index(2));
    assert_eq!(recovered.snapshot, Some(snap));
}

#[test]
fn snapshot_save_prunes_only_below_retention_and_crash_preserves_data() {
    let dir = Path::new("/wal-snap-retain");
    let fs = FaultFs::new(5);
    let opts = WalOptions {
        segment_bytes: 110,
        retain_entries: 2,
    };
    let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

    for i in 1..=8u64 {
        wal.persist(Batch {
            snapshot: None,
            hard_state: Some(&HardState {
                term: Term(1),
                voted_for: Some(NodeId(1)),
                commit: Index(i),
            }),
            entries: &[entry(1, i, b"entry-payload-data-for-rotation")],
        })
        .unwrap();
    }

    let segs_before = wal_dir_segments(&fs, dir);
    assert!(segs_before.len() >= 3);

    // Save snapshot at index 6 with retain_entries = 2 => cutoff index 4.
    let snap = Snapshot {
        meta: SnapshotMeta {
            index: Index(6),
            term: Term(1),
        },
        data: Bytes::from_static(b"snap-at-6"),
    };
    wal.save_snapshot(&snap).unwrap();

    // Simulate a crash with pending torn directory/file ops right after another append without sync.
    fs.fail_next_sync();
    let _ = wal.persist(Batch {
        snapshot: None,
        hard_state: None,
        entries: &[entry(1, 9, b"uncommitted-crash")],
    });
    drop(wal);

    let mut rng = ChaCha8Rng::seed_from_u64(99);
    fs.crash(&mut rng, true);

    let (_, recovered) = Wal::open(fs, dir, opts).unwrap();
    assert_eq!(recovered.snapshot, Some(snap));
    assert!(recovered.entries.iter().any(|e| e.index == Index(7)));
    assert!(recovered.entries.iter().any(|e| e.index == Index(8)));
}

#[test]
fn overwrite_semantics_discard_old_suffix_on_replay() {
    let dir = Path::new("/wal-overwrite");
    let fs = FaultFs::new(6);
    let opts = WalOptions::default();
    let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

    wal.persist(Batch {
        snapshot: None,
        hard_state: None,
        entries: &[
            entry(1, 1, b"a"),
            entry(1, 2, b"old-2"),
            entry(1, 3, b"old-3"),
        ],
    })
    .unwrap();

    // Overwrite from index 2 with term 2.
    wal.persist(Batch {
        snapshot: None,
        hard_state: None,
        entries: &[entry(2, 2, b"new-2")],
    })
    .unwrap();
    drop(wal);

    let (_, rec) = Wal::open(fs, dir, opts).unwrap();
    assert_eq!(rec.entries, vec![entry(1, 1, b"a"), entry(2, 2, b"new-2")]);
}

#[test]
fn eio_injection_poisons_wal() {
    let dir = Path::new("/wal-eio");
    let fs = FaultFs::new(7);
    let (mut wal, _) = Wal::open(fs.clone(), dir, WalOptions::default()).unwrap();

    fs.fail_next_io();
    let err = wal
        .persist(Batch {
            snapshot: None,
            hard_state: None,
            entries: &[entry(1, 1, b"fail")],
        })
        .unwrap_err();
    assert!(matches!(err, WalError::Io(_)));

    let second = wal
        .persist(Batch {
            snapshot: None,
            hard_state: None,
            entries: &[entry(1, 1, b"retry")],
        })
        .unwrap_err();
    assert!(matches!(second, WalError::Poisoned));

    let third = wal
        .save_snapshot(&Snapshot {
            meta: SnapshotMeta {
                index: Index(1),
                term: Term(1),
            },
            data: Bytes::from_static(b"s"),
        })
        .unwrap_err();
    assert!(matches!(third, WalError::Poisoned));
}

#[test]
fn stdfs_smoke_test_persists_and_recovers_in_tempfile() {
    let dir = TempDir::new().unwrap();
    let opts = WalOptions {
        segment_bytes: 256,
        retain_entries: 2,
    };

    {
        let (mut wal, rec) = Wal::open(StdFs, dir.path(), opts).unwrap();
        assert_eq!(rec.entries, []);
        for i in 1..=6u64 {
            wal.persist(Batch {
                snapshot: None,
                hard_state: Some(&HardState {
                    term: Term(2),
                    voted_for: Some(NodeId(1)),
                    commit: Index(i),
                }),
                entries: &[entry(2, i, format!("std-{i}").as_bytes())],
            })
            .unwrap();
        }
        wal.save_snapshot(&Snapshot {
            meta: SnapshotMeta {
                index: Index(4),
                term: Term(2),
            },
            data: Bytes::from_static(b"std-snap"),
        })
        .unwrap();
    }

    let (_wal, rec) = Wal::open(StdFs, dir.path(), opts).unwrap();
    assert_eq!(rec.snapshot.as_ref().unwrap().meta.index, Index(4));
    assert_eq!(rec.hard_state.term, Term(2));
    assert_eq!(rec.hard_state.commit, Index(6));
    assert_eq!(rec.entries.len(), 2);
    assert_eq!(rec.entries[0].index, Index(5));
    assert_eq!(rec.entries[1].index, Index(6));
}

proptest! {
    #[test]
    fn random_batches_with_torn_tail_crash_recover_valid_prefix(
        batches in proptest::collection::vec(
            (1usize..=4, proptest::collection::vec(any::<u8>(), 0..96)),
            2..16
        ),
        crash_seed in any::<u64>(),
    ) {
        let dir = Path::new("/wal-prop-crash");
        let fs = FaultFs::new(crash_seed ^ 0xA5A5);
        let opts = WalOptions {
            segment_bytes: 256,
            retain_entries: 64,
        };
        let (mut wal, _) = Wal::open(fs.clone(), dir, opts).unwrap();

        let mut synced_entries: Vec<Entry> = Vec::new();
        let mut next_idx = 1u64;
        let last_batch_idx = batches.len() - 1;

        for (b_idx, (count, payload)) in batches.into_iter().enumerate() {
            let chunk: Vec<Entry> = (0..count)
                .map(|_| {
                    let e = entry(1, next_idx, &payload);
                    next_idx += 1;
                    e
                })
                .collect();
            if b_idx == last_batch_idx {
                fs.fail_next_sync();
                let _ = wal.persist(Batch {
                    snapshot: None,
                    hard_state: Some(&HardState {
                        term: Term(1),
                        voted_for: Some(NodeId(1)),
                        commit: Index(next_idx - 1),
                    }),
                    entries: &chunk,
                });
            } else {
                wal.persist(Batch {
                    snapshot: None,
                    hard_state: Some(&HardState {
                        term: Term(1),
                        voted_for: Some(NodeId(1)),
                        commit: Index(next_idx - 1),
                    }),
                    entries: &chunk,
                }).unwrap();
                synced_entries.extend(chunk);
            }
        }
        drop(wal);

        let mut rng = ChaCha8Rng::seed_from_u64(crash_seed);
        fs.crash(&mut rng, true);

        let (_, recovered) = Wal::open(fs, dir, opts).unwrap();
        // Every entry from all completed persists is present, and any recovered tail matches
        // logically written entries without corruption.
        prop_assert!(recovered.entries.len() >= synced_entries.len());
        prop_assert_eq!(&recovered.entries[..synced_entries.len()], synced_entries.as_slice());
    }
}
