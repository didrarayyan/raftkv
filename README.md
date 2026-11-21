# raftkv (Mini-etcd)

[![CI](https://github.com/didrarayyan/Mini-etcd/actions/workflows/ci.yml/badge.svg)](https://github.com/didrarayyan/Mini-etcd/actions/workflows/ci.yml)

`raftkv` is a Raft-backed distributed key-value store written in Rust, designed around a pure deterministic consensus state machine (`raft-core`), a crash-safe write-ahead log (`raftkv-wal`), a deterministic fault-injecting simulator (`raft-sim`), and multi-process gRPC binaries (`raftkvd`, `raftkvctl`). It is built as a distributed systems engineering and portfolio project to study consensus correctness, storage durability ordering, and simulation testing rather than as a production replacement for etcd.

## Features

- **Consensus core (`raft-core`)**: Leader election with `PreVote` and `CheckQuorum`, fast conflict-term backtracking, `ReadIndex` quorum-verified reads, and snapshot compaction, with zero async, I/O, clock, or `HashMap`/`HashSet` usage.
- **Replicated KV store (`raftkv-kv`)**: Ordered `BTreeMap<Bytes, Bytes>` supporting `Put`, `Delete`, `Cas`, and bounded half-open `[start, end)` `Range`, plus a 4,096-entry deterministic session deduplication table.
- **Crash-safe WAL (`raftkv-wal`)**: IEEE CRC-32 framed segments (`RKVWAL01`), atomic temp-sync-rename snapshots (`RKVSNP01`), crash tail truncation, and pluggable `Fs` (`StdFs` and torn-write/`EIO` `FaultFs`).
- **Deterministic simulation (`raft-sim`)**: Discrete-event scheduler running clusters under packet drops, reordering, partitions, crashes, torn writes, and `EIO`, checked against invariants `I1`-`I8` and linearizability.
- **Daemon & CLI (`raftkvd`, `raftkvctl`)**: Single-owner group-commit Tokio driver, dedicated synchronous WAL OS thread, leader-redirect metadata (`x-raftkv-leader-addr`), idempotent retries, and table/JSON output.

## Quick Start

Prerequisites: Rust toolchain pinned by [rust-toolchain.toml](rust-toolchain.toml) (`rustc 1.100.0`, `edition = "2024"`). Run [scripts/dev-cluster.sh](scripts/dev-cluster.sh) under bash (Linux/macOS/Git Bash) or build both binaries directly:

```bash
cargo build --release -p raftkv-server -p raftkv-cli
./scripts/dev-cluster.sh
```

Recorded session against a live 3-node release cluster on `127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003`:

```text
$ raftkvctl status
ENDPOINT               NODE   ROLE           TERM   LEADER   COMMIT   APPLIED  SNAP     LAST_LOG
127.0.0.1:7001         1      follower       1      2        0        0        0        1
127.0.0.1:7002         2      leader         1      2        1        1        0        1
127.0.0.1:7003         3      follower       1      2        0        0        0        1

Leader 2 peer replication progress:
PEER     MATCH_INDEX  NEXT_INDEX   RECENTLY_ACTIVE
1        1            2            true
3        1            2            true

$ raftkvctl put cluster/name prod-east
OK log_index=2

$ raftkvctl get cluster/name
prod-east
applied_index=2

$ raftkvctl cas cluster/epoch --absent 1
swapped=true current=1 log_index=3

$ raftkvctl range --prefix cluster/
KEY                      VALUE
cluster/epoch            1
cluster/name             prod-east
count=2 more=false applied_index=3

# After terminating leader node 2 and waiting for Term 2 election:
$ raftkvctl status
ENDPOINT               NODE   ROLE           TERM   LEADER   COMMIT   APPLIED  SNAP     LAST_LOG
127.0.0.1:7001         1      follower       2      3        4        4        0        4
127.0.0.1:7002         ERROR: transport error
127.0.0.1:7003         3      leader         2      3        4        4        0        4

Leader 3 peer replication progress:
PEER     MATCH_INDEX  NEXT_INDEX   RECENTLY_ACTIVE
1        4            5            true
2        0            4            false

$ raftkvctl get cluster/name
prod-east
applied_index=4

$ raftkvctl health
healthy term=2 leader=3 reachable=2/3
```

## Architecture

```text
+-----------------------------------------------------------------------------+
| Workspace Crates (`#![forbid(unsafe_code)]`, 0 Mutex / 0 RwLock)            |
|                                                                             |
|  raftkv-cli (raftkvctl) ---gRPC (Kv, Admin)---> raftkv-server (raftkvd)     |
|  raftkv-proto (prost/tonic) <------------------+    |         |            |
|                                                      v         v            |
|  raft-sim (discrete-event) -------------------> raft-core + raftkv-kv       |
|                                                              |              |
|                                                         raftkv-wal (Fs)     |
+-----------------------------------------------------------------------------+

Write / Read Path inside `raftkvd`:
  Client gRPC ---> bounded mpsc(1024) ---> [Single-Owner Driver]
  Tick timer  ---> try_send           --->   1. `propose` / `read_index` on RaftNode
                                             2. `wal.persist(Ready)` -> WAL OS thread (`sync_data`)
                                             3. send `Ready.messages` -> Peer `mpsc(256)` -> gRPC
                                             4. apply `Ready.committed` -> `KvStore` -> reply client
```

Isolating consensus transitions in `raft-core` keeps every state transition synchronous and reproducible from `(Config, seed, inputs)`. In `raftkvd`, one driver task owns `RaftNode` and `KvStore` exclusively without locks, drains all queued inputs into a group-commit `Ready`, and awaits the dedicated WAL OS thread before releasing network messages or applying committed entries.

## Guarantees and Storage Format

- **Linearizable writes and reads**: Mutations replicate through Raft before commit; non-stale `get` and `range` confirm leadership via a quorum `ReadIndex` round and block until `KvStore::applied_index >= read_state.index`. Passing `--stale` reads local state directly.
- **Exactly-once session semantics**: Each write carries `(client_id, request_id)` and deduplicates re-transmissions across leader failovers. `KvStore` retains up to `MAX_SESSIONS = 4096` entries and evicts deterministically by `(applied_index, client_id)`; once evicted, stale retries fail safely as `INVALID_ARGUMENT` if replayed with an older `request_id`.
- **Durability ordering and binary layout**: WAL records (`RKVWAL01`, 64 MiB rotation) frame IEEE CRC-32 over `[len .. payload_end]`, writing entries before hard state; snapshots (`RKVSNP01`) write to a temp file, `sync_data()`, rename atomically, and `sync_dir()`. Corrupted final-segment tails truncate on startup, while non-final corruption returns `WalError::Corruption` and any write/sync `EIO` poisons the WAL and aborts `raftkvd`.

```text
WAL Segment:  | "RKVWAL01" (8B) | seq u64 LE | [ crc32 u32 | len u32 | kind u8 (Entry|HardState) | payload ] ... |
Snapshot:     | "RKVSNP01" (8B) | index u64  | term u64  | data_len u64 | protobuf KvStore+sessions | crc32 u32 |
```

## Testing and Deterministic Simulation

| Layer | Technique | Crate | What It Verifies |
| --- | --- | --- | --- |
| Log model | `proptest` reference model | `raft-core` | `RaftLog` truncation, slicing, compaction vs `Vec` |
| Protocol rules | Named unit + doc tests | `raft-core` | Election safety, `PreVote`, `CheckQuorum`, Figure 8, `ReadIndex` |
| Wire/KV codecs | `proptest` round-trips | `raftkv-kv`, `raftkv-wal`, `raftkv-server` | Encode/decode identity across variants and snapshots |
| Crash recovery | Exhaustive offset mutation | `raftkv-wal` | Every-offset truncation/bit-flip on `FaultFs` & `EIO` poison |
| Cluster safety | Seeded discrete simulation | `raft-sim` | Invariants `I1`-`I8` across partitions, crashes, torn writes |
| Linearizability | Single-register history check | `raft-sim` | Concurrent retries and indeterminate outcomes stay linearizable |
| Determinism | Cross-run trace hash | `raft-sim` | Identical seed yields bit-identical execution trace |
| Real processes | Multi-process `SIGKILL` tests | `raftkv-server` | Real gRPC election, log/snapshot catchup, CAS contention |

Simulation caught two real bugs that are now locked with regression tests: `PreVote` livelocking between a higher-term shorter-log node and lower-term longer-log node (seed `146`, covered by `prevote_succeeds_when_voter_has_higher_term_but_older_log`), and heartbeats skipping lagging peers in `ProgressState::Replicate` during write-idle catchup (caught by `follower_kill_and_rejoin_log_catchup`, covered by `replicate_follower_catches_up_via_heartbeat_without_new_proposals`).

```bash
# Reproduce any failing simulation seed deterministically:
cargo run -p raft-sim --release -- --seed 146 --nodes 3

# Verified release sweeps on this repository:
$ cargo run -p raft-sim --release -- --seeds 0..2000 --nodes 3
PASS: 2000 seeds (0..2000), nodes=3, ticks=20000, total_ops=10437539

$ cargo run -p raft-sim --release -- --seeds 0..500 --nodes 5
PASS: 500 seeds (0..500), nodes=5, ticks=20000, total_ops=1248047
```

## Configuration

`raftkvd --config <path>` validates a strict TOML file (`deny_unknown_fields`). Environment variables: `RUST_LOG` (`info` default), `RAFTKV_ENDPOINTS` (`raftkvctl` fallback), and `SIM_SEEDS` (`raft-sim` test sweep count, default `200`).

| Key | Default | Meaning |
| --- | --- | --- |
| `node_id`, `listen_addr`, `data_dir` | required | Non-zero node ID, `ip:port` bind address, WAL/lock directory |
| `cluster.peers` | required | 1-7 `{ id, addr }` cluster members |
| `raft.tick_ms` / `election_ticks` / `heartbeat_ticks` | `50` / `10` / `2` | Clock tick period, randomized election window `[N, 2N)`, heartbeat |
| `raft.max_append_entries` / `max_append_bytes` | `64` / `1048576` | Max entries and soft byte limit per `AppendEntries` RPC |
| `raft.snapshot_threshold` / `retain_entries` | `10000` / `1024` | Local snapshot cadence and retained trailing WAL entries |
| `limits.max_key_bytes` / `max_value_bytes` | `1024` / `1048576` | Client key and value size limits enforced at gRPC ingress |
| `limits.max_snapshot_bytes` / `request_timeout_ms` | `1073741824` / `5000` | Max incoming snapshot stream size and server RPC deadline |

## Benchmarks

Measured with `cargo bench -p raftkv-wal` (128-byte `Payload::Command` entries, synchronous `StdFs::sync_data()` per batch) on Microsoft Windows 10 Pro, Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz (4 cores / 8 logical processors), 15.8 GB RAM, `rustc 1.100.0 (0ef1bc386 2026-11-09)`:

| Benchmark (`wal_append/<batch>`) | Time (`[low, est, high]`) | Throughput (`[low, est, high]`) |
| --- | --- | --- |
| `wal_append/1` | `[2.4291 ms, 2.5367 ms, 2.7147 ms]` | `[368.37 elem/s, 394.22 elem/s, 411.68 elem/s]` |
| `wal_append/16` | `[2.2609 ms, 2.3930 ms, 2.6106 ms]` | `[6.1289 Kelem/s, 6.6862 Kelem/s, 7.0768 Kelem/s]` |
| `wal_append/128` | `[2.4457 ms, 2.5789 ms, 2.7905 ms]` | `[45.870 Kelem/s, 49.633 Kelem/s, 52.336 Kelem/s]` |

Because every batch pays one disk flush, group-committing 128 entries amortizes `sync_data()` latency and scales WAL throughput by roughly 126x over single-entry flushes.

## Limitations

- **Static cluster membership**: Configured peer lists (1-7 members) are fixed at boot; online reconfiguration (`ConfChange`) and multi-Raft sharding are omitted.
- **In-memory state machine & snapshots**: All keys fit in a single `BTreeMap` serialized synchronously inside `RaftDriver` on snapshot compaction.
- **Unauthenticated plaintext gRPC**: Internal and client RPCs validate peer IDs against `cluster.peers` without TLS, mTLS, or token auth.
- **Directory fsync portability**: `StdFs::sync_dir` calls `File::sync_all()` on Unix; on native Windows it verifies directory metadata while relying on file-level `FlushFileBuffers` and exclusive `<data_dir>/LOCK`.

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice*, PhD Dissertation, Stanford University, 2014 (Section 4.2.3 CheckQuorum; Section 9.6 PreVote).

## License

MIT License; see [LICENSE](LICENSE).
