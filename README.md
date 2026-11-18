# raftkv (Mini-etcd)

[![CI](https://github.com/AYASSEU/Mini-etcd/actions/workflows/ci.yml/badge.svg)](https://github.com/AYASSEU/Mini-etcd/actions/workflows/ci.yml)

A deterministic Raft consensus engine and replicated key-value store written in Rust, structured around a pure state machine (`raft-core`), a crash-safe write-ahead log (`raftkv-wal`), a deterministic fault-injecting simulator (`raft-sim`), and gRPC daemon and CLI binaries (`raftkvd`, `raftkvctl`).

## Overview

Distributed consensus bugs often hide in timing races between disk persistence, network retries, and leadership changes. `raftkv` isolates all protocol logic inside a synchronous, allocation-ordered state machine with zero I/O, networking, timers, or shared locks, so every state transition is reproducible from an explicit `Config`, random seed, and input stream. The same `raft-core`, `raftkv-kv`, and `raftkv-wal` crates run inside both the in-process fault simulator (`raft-sim`) and the multi-process Tokio/gRPC server (`raftkvd`).

## Architecture

```text
+-------------------------------------------------------------------------+
| Workspace Crates                                                        |
|                                                                         |
|  raftkv-cli (raftkvctl) ----gRPC (Kv, Admin)----> raftkv-server (raftkvd)|
|                                                      |    |    |        |
|  raftkv-proto (prost/tonic) <------------------------+    |    |        |
|                                                           v    v        |
|  raft-sim (deterministic sim) ----> raft-core + raftkv-kv + raftkv-wal  |
+-------------------------------------------------------------------------+

raftkvd Node Runtime Pipeline (zero Mutex / RwLock):
  [Kv / Admin / Raft gRPC] ---> bounded mpsc(1024) ---> [Single-Owner Driver]
  [Ticker (tick_ms)]       ---> try_send           --->   |-- WalHandle (OS thread sync)
                                                          |-- KvStore (BTreeMap + sessions)
                                                          '-- Peer mpsc(256) -> gRPC senders
```

- `raft-core`: Pure `RaftNode` state machine (`tick`, `step`, `propose`, `read_index`, `ready`, `advance`, `compact`), verified free of `tokio`, `tonic`, wall clocks, and unordered maps.
- `raftkv-kv`: Ordered `BTreeMap<Bytes, Bytes>` state machine supporting `Put`, `Delete`, `Cas`, and half-open `[start, end)` `Range`, plus a 4,096-entry client session deduplication table.
- `raftkv-wal`: Segmented WAL and atomic snapshot store parameterized over an explicit `Fs` trait (`StdFs` in production, `FaultFs` with torn writes and `EIO` injection in tests).
- `raftkv-proto`: Protobuf and tonic gRPC definitions (`Kv`, `Admin`, `Raft`) compiled at build time via `protoc-bin-vendored`.
- `raft-sim`: Single-threaded discrete-event simulator with packet reorder/drop/partition injection (`NetworkSim`), crash recovery, linearizability checking, and cross-tick invariants `I1`-`I8`.
- `raftkv-server` (`raftkvd`): Group-commit Tokio driver, dedicated synchronous WAL OS thread (fail-stop abort on `WalError`), peer reconnect senders, and server deadlines.
- `raftkv-cli` (`raftkvctl`): Leader-following client library (`x-raftkv-leader-addr` redirect cache, fixed `(client_id, request_id)` retries) and table/JSON CLI.

## Protocol Guarantees

- **Election Safety & Leader Completeness**: Single leader per term with `PreVote` enabled by default so partitioned nodes do not inflate cluster terms, paired with `CheckQuorum` so isolated leaders step down automatically.
- **Durability Ordering**: Outbound Raft RPCs (`Ready::messages`) and state-machine applies (`Ready::committed`) strictly wait until the dedicated WAL thread finishes `sync_data()` for `Ready::snapshot`, `Ready::entries`, and `Ready::hard_state`.
- **Linearizable Reads (`ReadIndex`)**: Non-stale `Get` and `Range` queries confirm leadership via a quorum heartbeat round and wait until `KvStore::applied_index >= read_state.index`. `--stale` serves directly from the local replica without quorum round-trips.
- **Idempotent Retries**: Each `Put`, `Delete`, and `Cas` carries `(client_id, request_id)`. `KvStore` retains up to `MAX_SESSIONS = 4096` latest entries (evicting deterministically by `(applied_index, client_id)`) and returns cached results on duplicate `(client_id, request_id)` across leader failovers while rejecting strictly older IDs (`Applied::Outdated` -> `INVALID_ARGUMENT`).

## Storage Format

WAL segments (`wal/<seq>.wal`) and snapshots (`snap/<index>.snap`) use little-endian headers checksummed with IEEE CRC-32 (`crc32fast`):

```text
WAL Segment File (64 MiB rotation threshold):
  +------------------+---------+--------------+----------+-------------------+
  | "RKVWAL01" (8 B) | seq u64 | crc32 u32 LE | len u32  | kind u8 + payload | ...
  +------------------+---------+--------------+----------+-------------------+
  crc32 covers [len .. payload_end]; kind=1 Entry(term, index, noop|cmd), kind=2 HardState

Snapshot File (temp file -> sync_data -> atomic rename -> sync_dir):
  +------------------+-----------+----------+-------------+-----------------+------------+
  | "RKVSNP01" (8 B) | index u64 | term u64 | data_len u64| protobuf bytes  | crc32 u32  |
  +------------------+-----------+----------+-------------+-----------------+------------+
```

- Truncated or corrupted tails in the **final** WAL segment are truncated cleanly to the last valid record offset during `Wal::open`.
- Bit flips in **non-final** WAL segments or snapshot files fail fast with `WalError::Corruption`.
- Any write or sync `EIO` permanently poisons the `Wal` instance (`WalError::Poisoned`); `raftkvd` logs the failure and calls `std::process::abort()`.

## Deterministic Simulation

`raft-sim` runs multi-node clusters under a seeded discrete-event scheduler combining `NetworkSim` (bounded message delays, reordering, drops, and partitions), `FaultFs` (byte-level torn writes on crash and probabilistic `EIO`), and `Nemesis` fault schedules (`PartitionMinority`, `IsolateNode`, `CrashNode`, `ShiftNetwork`, `InjectEio`), healing before end-of-run liveness convergence checks.

Checked on every state transition:
- `I1` Election Safety, `I2` Leader Append-Only, `I3` Log Matching, `I4` Leader Completeness, `I5` State Machine Safety, `I6` Monotonic Durable/Applied State, `I7` Vote Stability, `I8` Quorum Durability, plus single-register linearizability across concurrent `Put`/`Cas`/`Get` clients.

Release sweep runs verified on this repository:

```text
$ cargo run -p raft-sim --release -- --seeds 0..2000 --nodes 3
PASS: 2000 seeds (0..2000), nodes=3, ticks=20000, total_ops=10437539

$ cargo run -p raft-sim --release -- --seeds 0..500 --nodes 5
PASS: 500 seeds (0..500), nodes=5, ticks=20000, total_ops=1248047
```

## Quick Start

Build release binaries and run the local 3-node dev cluster helper (`./scripts/dev-cluster.sh` on bash, or pass `--config` per node directly):

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

## Configuration Reference

`raftkvd --config <path>` reads a strict TOML file (`deny_unknown_fields`):

| Key | Default | Meaning |
| --- | --- | --- |
| `node_id` | required | Non-zero cluster member ID (`u64`) |
| `listen_addr` | required | gRPC bind address (`ip:port`) |
| `data_dir` | required | Directory for `LOCK`, `wal/`, and `snap/` |
| `cluster.peers` | required | 1-7 `{ id, addr }` member definitions |
| `raft.tick_ms` | `50` | Logical Raft clock period in milliseconds |
| `raft.election_ticks` | `10` | Baseline election timeout (`[N, 2N)` ticks) |
| `raft.heartbeat_ticks` | `2` | Leader heartbeat interval in ticks |
| `raft.max_append_entries` | `64` | Max entries per `AppendEntries` RPC |
| `raft.max_append_bytes` | `1048576` | Soft byte cap per `AppendEntries` RPC |
| `raft.snapshot_threshold` | `10000` | Applied entries between local compactions |
| `raft.retain_entries` | `1024` | Tail entries kept below snapshot before WAL pruning |
| `limits.max_key_bytes` | `1024` | Maximum allowed key byte length (`1..=1024`) |
| `limits.max_value_bytes` | `1048576` | Maximum allowed value length in bytes |
| `limits.max_snapshot_bytes` | `1073741824` | Max streamed `InstallSnapshot` payload bytes |
| `limits.request_timeout_ms` | `5000` | Server-side client RPC deadline in ms |

## Benchmarks

Measured with `cargo bench -p raftkv-wal` (128-byte command entries, `StdFs` synchronous `sync_data()` per batch) on Microsoft Windows 10 Pro, Intel(R) Core(TM) i7-6820HQ CPU @ 2.70GHz (4 cores / 8 logical processors), 15.8 GB RAM, `rustc 1.100.0 (0ef1bc386 2026-11-09)`:

| Benchmark (`wal_append/<batch>`) | Time (`[low, est, high]`) | Throughput (`[low, est, high]`) |
| --- | --- | --- |
| `wal_append/1` | `[2.4291 ms, 2.5367 ms, 2.7147 ms]` | `[368.37 elem/s, 394.22 elem/s, 411.68 elem/s]` |
| `wal_append/16` | `[2.2609 ms, 2.3930 ms, 2.6106 ms]` | `[6.1289 Kelem/s, 6.6862 Kelem/s, 7.0768 Kelem/s]` |
| `wal_append/128` | `[2.4457 ms, 2.5789 ms, 2.7905 ms]` | `[45.870 Kelem/s, 49.633 Kelem/s, 52.336 Kelem/s]` |

Because each batch ends with one filesystem `sync_data()` call, group-committing 128 entries amortizes disk flush latency and increases WAL throughput by roughly 126x compared to single-entry flushes.

## Non-Goals and Trade-offs

- **Static cluster membership**: Voting members are fixed in config (1-7 nodes); dynamic reconfiguration (`ConfChange`), learner nodes, and multi-Raft sharding are omitted.
- **In-memory state machine**: All live keys reside in a single ordered `BTreeMap` and snapshots serialize the entire map, targeting metadata workloads that fit comfortably in RAM.
- **Plaintext internal/client gRPC**: mTLS, authentication tokens, and RBAC are omitted; network peers are checked only against the configured `node_id` allowlist.
- **Fail-stop storage policy**: Tail truncation repairs crash-torn appends on startup, whereas mid-log corruption or write/sync `EIO` aborts `raftkvd` rather than risking silent divergence.

## License

Licensed under the MIT License; see [LICENSE](LICENSE).
