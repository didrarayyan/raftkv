//! Scripted regression scenarios and seeded cluster simulation sweeps.

use std::path::Path;
use std::thread;

use bytes::Bytes;
use raft_core::{Entry, Index, NodeId, Payload, Role, Term};
use raft_sim::{Sim, SimConfig};
use raftkv_kv::{Command, Op};
use raftkv_wal::{Batch, Wal, WalOptions};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;

fn clean_cfg(seed: u64, node_count: usize) -> SimConfig {
    SimConfig {
        seed,
        node_count,
        total_ticks: 200,
        heal_ticks: 50,
        min_delay: 1,
        max_delay: 3,
        drop_prob: 0.0,
        dup_prob: 0.0,
        snapshot_threshold: 500,
        wal_segment_bytes: 64 * 1024,
        retain_entries: 128,
        nemesis_enabled: false,
        clients_enabled: false,
        allow_eio: false,
    }
}

fn encode_put(client_id: u64, request_id: u64, key: &'static [u8], val: &'static [u8]) -> Bytes {
    Command {
        client_id,
        request_id,
        op: Op::Put {
            key: Bytes::from_static(key),
            value: Bytes::from_static(val),
        },
    }
    .encode()
}

fn encode_del(client_id: u64, request_id: u64, key: &'static [u8]) -> Bytes {
    Command {
        client_id,
        request_id,
        op: Op::Delete {
            key: Bytes::from_static(key),
        },
    }
    .encode()
}

#[test]
fn elects_single_leader_on_boot() {
    let mut sim = Sim::new(clean_cfg(1, 3));
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().expect("leader must be elected");
    let statuses = sim.statuses();
    assert_eq!(statuses[&leader].role, Role::Leader);
    for st in statuses.values() {
        assert_eq!(st.leader_id, Some(leader));
    }
}

#[test]
fn leader_crash_triggers_reelection() {
    let mut sim = Sim::new(clean_cfg(2, 3));
    sim.step_ticks(40).unwrap();
    let old_leader = sim.leader().unwrap();
    let old_term = sim.statuses()[&old_leader].term;

    sim.crash_node(old_leader, 200, false);
    sim.step_ticks(80).unwrap();

    let new_leader = sim.leader().expect("remaining quorum must elect a leader");
    assert_ne!(new_leader, old_leader);
    assert!(sim.statuses()[&new_leader].term > old_term);
}

#[test]
fn partitioned_leader_steps_down_and_cannot_commit() {
    let mut sim = Sim::new(clean_cfg(3, 3));
    sim.step_ticks(40).unwrap();
    let old_leader = sim.leader().unwrap();

    sim.isolate_node(old_leader);
    let idx = sim
        .propose_on(old_leader, encode_put(1, 1, b"k", b"v"))
        .unwrap();

    sim.step_ticks(30).unwrap();

    let statuses = sim.statuses();
    assert_eq!(statuses[&old_leader].role, Role::Follower);
    assert!(statuses[&old_leader].commit < idx);
    assert_eq!(sim.kv_get(old_leader, b"k"), None);
}

#[test]
fn minority_partition_cannot_elect() {
    let mut sim = Sim::new(clean_cfg(4, 3));
    sim.isolate_node(NodeId(1));
    sim.crash_node(NodeId(2), 500, false);
    sim.step_ticks(80).unwrap();

    assert_ne!(sim.statuses()[&NodeId(1)].role, Role::Leader);
    assert_ne!(sim.statuses()[&NodeId(3)].role, Role::Leader);
}

#[test]
fn figure_8_old_term_entries_are_not_committed_by_counting() {
    let mut cfg = clean_cfg(5, 5);
    cfg.nemesis_enabled = true;
    cfg.clients_enabled = true;
    cfg.total_ticks = 2000;
    cfg.heal_ticks = 300;
    let mut sim = Sim::new(cfg);
    let report = sim.run().unwrap();
    assert!(report.max_commit >= Index(1));
}

#[test]
fn lagging_follower_catches_up_via_log() {
    let mut sim = Sim::new(clean_cfg(6, 3));
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().unwrap();
    let follower = [NodeId(1), NodeId(2), NodeId(3)]
        .into_iter()
        .find(|&id| id != leader)
        .unwrap();

    sim.crash_node(follower, 500, false);
    for rid in 1..=5u64 {
        sim.propose_on(leader, encode_put(1, rid, b"seq", b"ok"))
            .unwrap();
    }
    sim.step_ticks(20).unwrap();
    assert_eq!(sim.kv_get(leader, b"seq"), Some(Bytes::from_static(b"ok")));

    sim.restart_node(follower).unwrap();
    sim.step_ticks(25).unwrap();
    assert_eq!(
        sim.kv_get(follower, b"seq"),
        Some(Bytes::from_static(b"ok"))
    );
}

#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let mut cfg = clean_cfg(7, 3);
    cfg.snapshot_threshold = 4;
    cfg.retain_entries = 1;
    let mut sim = Sim::new(cfg);
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().unwrap();
    let lagging = [NodeId(1), NodeId(2), NodeId(3)]
        .into_iter()
        .find(|&id| id != leader)
        .unwrap();

    sim.isolate_node(lagging);
    for rid in 1..=10u64 {
        sim.propose_on(leader, encode_put(1, rid, b"snap_k", b"snap_v"))
            .unwrap();
    }
    sim.step_ticks(30).unwrap();
    sim.force_compact_on(leader).unwrap();
    assert!(sim.statuses()[&leader].snapshot_index >= Index(5));

    sim.heal_network();
    sim.step_ticks(40).unwrap();

    assert!(sim.statuses()[&lagging].snapshot_index >= Index(5));
    assert_eq!(
        sim.kv_get(lagging, b"snap_k"),
        Some(Bytes::from_static(b"snap_v"))
    );
}

#[test]
fn prevote_stops_term_inflation_from_isolated_node() {
    let mut sim = Sim::new(clean_cfg(8, 3));
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().unwrap();
    let isolated = [NodeId(1), NodeId(2), NodeId(3)]
        .into_iter()
        .find(|&id| id != leader)
        .unwrap();
    let term_before = sim.statuses()[&leader].term;

    sim.isolate_node(isolated);
    sim.step_ticks(120).unwrap();
    assert_eq!(sim.statuses()[&isolated].term, term_before);

    sim.heal_network();
    sim.step_ticks(20).unwrap();
    assert_eq!(sim.statuses()[&leader].term, term_before);
    assert_eq!(sim.statuses()[&leader].role, Role::Leader);
}

#[test]
fn torn_wal_tail_is_discarded_on_restart() {
    let mut sim = Sim::new(clean_cfg(9, 3));
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().unwrap();
    let target = [NodeId(1), NodeId(2), NodeId(3)]
        .into_iter()
        .find(|&id| id != leader)
        .unwrap();

    sim.propose_on(leader, encode_put(1, 1, b"durable", b"yes"))
        .unwrap();
    sim.step_ticks(20).unwrap();
    assert_eq!(
        sim.kv_get(target, b"durable"),
        Some(Bytes::from_static(b"yes"))
    );

    sim.crash_node(target, 10_000, false);
    let fs = sim.node_fs(target);
    {
        let (mut wal, rec) =
            Wal::open(fs.clone(), Path::new("/data"), WalOptions::default()).unwrap();
        let next_idx = rec.entries.last().map_or(Index(1), |e| e.index.next());
        fs.fail_next_sync();
        let _ = wal.persist(Batch {
            snapshot: None,
            hard_state: None,
            entries: &[Entry {
                term: Term(9),
                index: next_idx,
                payload: Payload::Command(Bytes::from_static(b"unsynced-torn-payload")),
            }],
        });
    }
    let mut rng = ChaCha8Rng::seed_from_u64(404);
    fs.crash(&mut rng, true);

    sim.restart_node(target).unwrap();
    sim.step_ticks(20).unwrap();
    assert_eq!(
        sim.kv_get(target, b"durable"),
        Some(Bytes::from_static(b"yes"))
    );
}

#[test]
fn acknowledged_writes_survive_full_cluster_crash() {
    let mut sim = Sim::new(clean_cfg(10, 3));
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().unwrap();

    sim.propose_on(leader, encode_put(1, 1, b"alpha", b"1"))
        .unwrap();
    sim.propose_on(leader, encode_put(1, 2, b"beta", b"2"))
        .unwrap();
    sim.step_ticks(25).unwrap();

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        sim.crash_node(id, 10_000, true);
    }
    sim.step_ticks(5).unwrap();
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        sim.restart_node(id).unwrap();
    }
    sim.step_ticks(40).unwrap();

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        assert_eq!(sim.kv_get(id, b"alpha"), Some(Bytes::from_static(b"1")));
        assert_eq!(sim.kv_get(id, b"beta"), Some(Bytes::from_static(b"2")));
    }
}

#[test]
fn duplicate_client_request_applies_once() {
    let mut sim = Sim::new(clean_cfg(11, 3));
    sim.step_ticks(40).unwrap();
    let leader = sim.leader().unwrap();

    sim.propose_on(leader, encode_put(1, 1, b"k", b"first"))
        .unwrap();
    sim.propose_on(leader, encode_del(1, 2, b"k")).unwrap();
    sim.propose_on(leader, encode_put(2, 1, b"k", b"second"))
        .unwrap();
    sim.propose_on(leader, encode_del(1, 2, b"k")).unwrap();
    sim.step_ticks(25).unwrap();

    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        assert_eq!(sim.kv_get(id, b"k"), Some(Bytes::from_static(b"second")));
    }
}

#[test]
fn read_index_is_never_stale_after_leader_change() {
    let mut cfg = clean_cfg(12, 3);
    cfg.clients_enabled = true;
    cfg.nemesis_enabled = true;
    cfg.total_ticks = 3000;
    cfg.heal_ticks = 400;
    let mut sim = Sim::new(cfg);
    let report = sim.run().unwrap();
    assert!(report.completed_ops > 10);
}

#[test]
fn same_seed_produces_identical_trace_hash() {
    let mut a = Sim::new(SimConfig::standard(1337, 3));
    let rep_a = a.run().unwrap();

    let mut b = Sim::new(SimConfig::standard(1337, 3));
    let rep_b = b.run().unwrap();

    assert_eq!(rep_a, rep_b);
    assert_eq!(a.trace_hash(), b.trace_hash());
}

fn run_sweep(node_count: usize, total_seeds: u64, ticks: u64) {
    let seeds: Vec<u64> = (0..total_seeds).collect();
    let workers = thread::available_parallelism()
        .map_or(4, std::num::NonZero::get)
        .min(seeds.len().max(1));
    let chunk_size = seeds.len().div_ceil(workers);

    thread::scope(|scope| {
        for chunk in seeds.chunks(chunk_size) {
            scope.spawn(move || {
                for &seed in chunk {
                    let mut cfg = SimConfig::standard(seed, node_count);
                    cfg.total_ticks = ticks;
                    cfg.heal_ticks = 400;
                    let mut sim = Sim::new(cfg);
                    if let Err(v) = sim.run() {
                        panic!(
                            "{node_count}-node simulation failed at seed={} tick={}: {}",
                            v.seed, v.tick, v.message
                        );
                    }
                }
            });
        }
    });
}

#[test]
fn seeded_simulation_sweep() {
    let three_node_seeds: u64 = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let five_node_seeds = (three_node_seeds / 4).max(50).min(three_node_seeds);

    run_sweep(3, three_node_seeds, 2500);
    run_sweep(5, five_node_seeds, 2500);
}
