//! Criterion throughput benchmark for `raftkv-wal` across batch sizes 1, 16, and 128.

#![forbid(unsafe_code)]
#![allow(
    missing_docs,
    reason = "criterion_group! generates undocumented harness symbols"
)]

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use raft_core::{Entry, Index, Payload, Term};
use raftkv_wal::{Batch, StdFs, Wal, WalOptions};
use tempfile::TempDir;

fn make_entries(start: u64, count: usize) -> Vec<Entry> {
    (0..count)
        .map(|i| Entry {
            term: Term(1),
            index: Index(start + i as u64),
            payload: Payload::Command(Bytes::from(vec![0x5Au8; 128])),
        })
        .collect()
}

fn bench_wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    for batch_size in [1usize, 16, 128] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let dir = TempDir::new().expect("INVARIANT: temp benchmark dir creation");
                let (mut wal, _) =
                    Wal::open(StdFs, dir.path(), WalOptions::default()).expect("INVARIANT: open");
                let mut next_idx = 1u64;
                b.iter(|| {
                    let entries = make_entries(next_idx, size);
                    next_idx += size as u64;
                    wal.persist(Batch {
                        snapshot: None,
                        hard_state: None,
                        entries: &entries,
                    })
                    .expect("INVARIANT: persist");
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_wal_append);
criterion_main!(benches);
