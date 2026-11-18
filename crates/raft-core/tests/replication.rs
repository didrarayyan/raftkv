//! Replication, commit, `ReadIndex`, and snapshot tests for `raft-core`.

use std::collections::BTreeMap;

use bytes::Bytes;
use raft_core::{
    AppendResult, Config, Entry, HardState, Index, Message, MessageBody, NodeId, Payload,
    ProposeError, RaftNode, ReadError, ReadState, Recovered, Role, Snapshot, SnapshotMeta, Term,
};

fn cmd(s: &'static [u8]) -> Payload {
    Payload::Command(Bytes::from_static(s))
}

fn elect_three_node_leader() -> BTreeMap<NodeId, RaftNode> {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut nodes: BTreeMap<NodeId, RaftNode> = [1u64, 2, 3]
        .into_iter()
        .map(|i| {
            let id = NodeId(i);
            let node = RaftNode::new(
                Config::with_defaults(id, peers.clone(), i * 17),
                Recovered::default(),
            )
            .unwrap();
            (id, node)
        })
        .collect();

    for _ in 0..40 {
        let ids: Vec<NodeId> = nodes.keys().copied().collect();
        for &id in &ids {
            nodes.get_mut(&id).unwrap().tick();
        }
        pump_messages(&mut nodes, &[]);
        if nodes.values().any(|n| n.status().role == Role::Leader) {
            break;
        }
    }
    pump_messages(&mut nodes, &[]);
    nodes
}

fn pump_messages(nodes: &mut BTreeMap<NodeId, RaftNode>, blocked: &[(NodeId, NodeId)]) {
    let ids: Vec<NodeId> = nodes.keys().copied().collect();
    let mut inboxes: BTreeMap<NodeId, Vec<Message>> =
        ids.iter().map(|&id| (id, Vec::new())).collect();

    for _ in 0..16 {
        let mut any = false;
        for &id in &ids {
            let node = nodes.get_mut(&id).unwrap();
            while let Some(ready) = node.ready() {
                for msg in ready.messages {
                    if !blocked.contains(&(msg.from, msg.to)) {
                        inboxes.get_mut(&msg.to).unwrap().push(msg);
                        any = true;
                    }
                }
                node.advance();
            }
        }
        if !any {
            break;
        }
        for &id in &ids {
            let msgs = std::mem::take(inboxes.get_mut(&id).unwrap());
            let node = nodes.get_mut(&id).unwrap();
            for msg in msgs {
                node.step(msg);
            }
        }
    }
}

fn leader_id(nodes: &BTreeMap<NodeId, RaftNode>) -> NodeId {
    nodes
        .iter()
        .find(|(_, n)| n.status().role == Role::Leader)
        .map(|(&id, _)| id)
        .unwrap()
}

#[test]
fn stale_append_entries_does_not_truncate_committed_entries() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let entries = vec![
        Entry {
            term: Term(1),
            index: Index(1),
            payload: Payload::Noop,
        },
        Entry {
            term: Term(1),
            index: Index(2),
            payload: cmd(b"a"),
        },
        Entry {
            term: Term(1),
            index: Index(3),
            payload: cmd(b"b"),
        },
    ];
    let mut follower = RaftNode::new(
        Config::with_defaults(NodeId(2), peers, 1),
        Recovered {
            hard_state: HardState {
                term: Term(1),
                voted_for: Some(NodeId(1)),
                commit: Index(3),
            },
            snapshot: None,
            entries: entries.clone(),
        },
    )
    .unwrap();

    // Leader re-sends a duplicate prefix [1, 2] as might happen after reordering.
    follower.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(1),
        body: MessageBody::AppendEntries {
            prev_index: Index(0),
            prev_term: Term(0),
            entries: entries[..2].to_vec(),
            leader_commit: Index(2),
            read_ctx: 0,
        },
    });
    let ready = follower.ready().unwrap();
    assert_eq!(ready.entries, []);
    assert_eq!(follower.status().last_log_index, Index(3));
    assert_eq!(follower.status().commit, Index(3));
    follower.advance();
}

#[test]
fn conflicting_suffix_is_replaced() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut follower = RaftNode::new(
        Config::with_defaults(NodeId(2), peers, 2),
        Recovered {
            hard_state: HardState {
                term: Term(1),
                voted_for: None,
                commit: Index(1),
            },
            snapshot: None,
            entries: vec![
                Entry {
                    term: Term(1),
                    index: Index(1),
                    payload: Payload::Noop,
                },
                Entry {
                    term: Term(1),
                    index: Index(2),
                    payload: cmd(b"old2"),
                },
                Entry {
                    term: Term(1),
                    index: Index(3),
                    payload: cmd(b"old3"),
                },
            ],
        },
    )
    .unwrap();

    follower.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(2),
        body: MessageBody::AppendEntries {
            prev_index: Index(1),
            prev_term: Term(1),
            entries: vec![Entry {
                term: Term(2),
                index: Index(2),
                payload: cmd(b"new2"),
            }],
            leader_commit: Index(2),
            read_ctx: 0,
        },
    });
    let ready = follower.ready().unwrap();
    assert_eq!(ready.entries.len(), 1);
    assert_eq!(ready.entries[0].index, Index(2));
    assert_eq!(ready.entries[0].term, Term(2));
    assert_eq!(follower.status().last_log_index, Index(2));
    assert_eq!(follower.status().commit, Index(2));
    follower.advance();
}

#[test]
fn fast_backtracking_jumps_by_term() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    // Follower has a long stale suffix of 20 entries in term 2 starting at index 2.
    let mut follower_entries = vec![Entry {
        term: Term(1),
        index: Index(1),
        payload: Payload::Noop,
    }];
    for i in 2..=21u64 {
        follower_entries.push(Entry {
            term: Term(2),
            index: Index(i),
            payload: cmd(b"stale"),
        });
    }
    let mut follower = RaftNode::new(
        Config::with_defaults(NodeId(2), peers.clone(), 2),
        Recovered {
            hard_state: HardState {
                term: Term(2),
                voted_for: None,
                commit: Index(1),
            },
            snapshot: None,
            entries: follower_entries,
        },
    )
    .unwrap();

    // Leader at term 4 only had term 1 at index 1 and 20 entries in term 3..4.
    let mut leader_entries = vec![Entry {
        term: Term(1),
        index: Index(1),
        payload: Payload::Noop,
    }];
    for i in 2..=21u64 {
        leader_entries.push(Entry {
            term: Term(4),
            index: Index(i),
            payload: cmd(b"new"),
        });
    }
    let mut leader = RaftNode::new(
        Config::with_defaults(NodeId(1), peers, 1),
        Recovered {
            hard_state: HardState {
                term: Term(3),
                voted_for: Some(NodeId(1)),
                commit: Index(1),
            },
            snapshot: None,
            entries: leader_entries,
        },
    )
    .unwrap();

    // Drive leader election against node 3 (simulated directly via RequestVoteResponse).
    for _ in 0..25 {
        leader.tick();
        if let Some(r) = leader.ready() {
            for msg in r.messages {
                if msg.to == NodeId(3) {
                    match msg.body {
                        MessageBody::PreVote { .. } => leader.step(Message {
                            from: NodeId(3),
                            to: NodeId(1),
                            term: msg.term,
                            body: MessageBody::PreVoteResponse { granted: true },
                        }),
                        MessageBody::RequestVote { .. } => leader.step(Message {
                            from: NodeId(3),
                            to: NodeId(1),
                            term: msg.term,
                            body: MessageBody::RequestVoteResponse { granted: true },
                        }),
                        _ => {}
                    }
                }
            }
            leader.advance();
        }
        if leader.status().role == Role::Leader {
            break;
        }
    }
    assert_eq!(leader.status().role, Role::Leader);

    // One round of AppendEntries from leader (prev_index = 21, term = 4) fails on follower
    // and jumps straight back to conflict_index = 2 in a single rejection!
    let mut rejections = 0usize;
    for _ in 0..6 {
        if let Some(r) = leader.ready() {
            for msg in r.messages {
                if msg.to == NodeId(2) {
                    follower.step(msg);
                    if let Some(fr) = follower.ready() {
                        for resp in fr.messages {
                            if let MessageBody::AppendEntriesResponse {
                                result:
                                    AppendResult::Rejected {
                                        conflict_index,
                                        conflict_term,
                                    },
                                ..
                            } = resp.body
                            {
                                rejections += 1;
                                assert_eq!(conflict_index, Index(2));
                                assert_eq!(conflict_term, Term(2));
                            }
                            leader.step(resp);
                        }
                        follower.advance();
                    }
                }
            }
            leader.advance();
        }
        if follower.status().last_log_index == Index(22) && follower.status().commit >= Index(1) {
            break;
        }
    }
    assert_eq!(rejections, 1);
    assert_eq!(follower.status().last_log_index, Index(22));
}

#[test]
fn figure_8_old_term_entries_are_not_committed_by_counting_replicas() {
    // Leader in term 4 has entry 2 from term 2 replicated on a quorum (self + node 2),
    // but entry 2 MUST NOT commit until an entry from term 4 commits (the leader's Noop).
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut cfg = Config::with_defaults(NodeId(1), peers, 1);
    cfg.pre_vote = false;

    let mut leader = RaftNode::new(
        cfg,
        Recovered {
            hard_state: HardState {
                term: Term(3),
                voted_for: Some(NodeId(1)),
                commit: Index(1),
            },
            snapshot: None,
            entries: vec![
                Entry {
                    term: Term(1),
                    index: Index(1),
                    payload: Payload::Noop,
                },
                Entry {
                    term: Term(2),
                    index: Index(2),
                    payload: cmd(b"old"),
                },
            ],
        },
    )
    .unwrap();

    // Elect node 1 in term 4 via node 3 vote, while node 1 only receives AppendEntriesResponse
    // up to index 2 from node 2 before index 3 (term 4 Noop) is replicated.
    for _ in 0..25 {
        leader.tick();
        if let Some(r) = leader.ready() {
            for msg in r.messages {
                if msg.to == NodeId(3) && matches!(msg.body, MessageBody::RequestVote { .. }) {
                    leader.step(Message {
                        from: NodeId(3),
                        to: NodeId(1),
                        term: msg.term,
                        body: MessageBody::RequestVoteResponse { granted: true },
                    });
                }
            }
            leader.advance();
        }
        if leader.status().role == Role::Leader {
            break;
        }
    }
    assert_eq!(leader.status().role, Role::Leader);
    assert_eq!(leader.status().term, Term(4));
    // Drain the Noop ready so persisted_index advances to 3.
    while let Some(r) = leader.ready() {
        leader.advance();
        let _ = r;
    }

    // Node 2 acknowledges ONLY up to index 2 (term 2), NOT index 3 (term 4).
    leader.step(Message {
        from: NodeId(2),
        to: NodeId(1),
        term: Term(4),
        body: MessageBody::AppendEntriesResponse {
            result: AppendResult::Accepted {
                match_index: Index(2),
            },
            read_ctx: 0,
        },
    });
    assert_eq!(leader.status().commit, Index(1));

    // Once node 2 acknowledges index 3 (term 4), commit jumps to 3 (indirectly committing index 2).
    leader.step(Message {
        from: NodeId(2),
        to: NodeId(1),
        term: Term(4),
        body: MessageBody::AppendEntriesResponse {
            result: AppendResult::Accepted {
                match_index: Index(3),
            },
            read_ctx: 0,
        },
    });
    assert_eq!(leader.status().commit, Index(3));
}

#[test]
fn commit_never_decreases() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut follower = RaftNode::new(
        Config::with_defaults(NodeId(2), peers, 2),
        Recovered {
            hard_state: HardState {
                term: Term(2),
                voted_for: None,
                commit: Index(3),
            },
            snapshot: None,
            entries: vec![
                Entry {
                    term: Term(1),
                    index: Index(1),
                    payload: Payload::Noop,
                },
                Entry {
                    term: Term(2),
                    index: Index(2),
                    payload: cmd(b"k1"),
                },
                Entry {
                    term: Term(2),
                    index: Index(3),
                    payload: cmd(b"k2"),
                },
            ],
        },
    )
    .unwrap();

    follower.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(2),
        body: MessageBody::AppendEntries {
            prev_index: Index(0),
            prev_term: Term(0),
            entries: vec![],
            leader_commit: Index(1),
            read_ctx: 0,
        },
    });
    assert_eq!(follower.status().commit, Index(3));
}

#[test]
fn propose_on_follower_returns_not_leader() {
    let mut nodes = elect_three_node_leader();
    let lid = leader_id(&nodes);
    let fid = nodes.keys().copied().find(|&id| id != lid).unwrap();
    let err = nodes
        .get_mut(&fid)
        .unwrap()
        .propose(Bytes::from_static(b"x"))
        .unwrap_err();
    assert_eq!(err, ProposeError::NotLeader { leader: Some(lid) });
}

#[test]
fn read_index_waits_for_election_noop_to_commit() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut cfg = Config::with_defaults(NodeId(1), peers, 1);
    cfg.pre_vote = false;
    let mut leader = RaftNode::new(cfg, Recovered::default()).unwrap();

    for _ in 0..25 {
        leader.tick();
        if let Some(r) = leader.ready() {
            for msg in r.messages {
                if msg.to == NodeId(2) && matches!(msg.body, MessageBody::RequestVote { .. }) {
                    leader.step(Message {
                        from: NodeId(2),
                        to: NodeId(1),
                        term: msg.term,
                        body: MessageBody::RequestVoteResponse { granted: true },
                    });
                }
            }
            leader.advance();
        }
        if leader.status().role == Role::Leader {
            break;
        }
    }
    assert_eq!(leader.status().role, Role::Leader);
    assert_eq!(leader.status().commit, Index(0));

    // Calling read_index before the term's Noop commits queues the read instead of emitting immediately.
    assert!(leader.read_index(77).is_ok());
    assert_eq!(leader.read_index(77), Err(ReadError::DuplicateContext));
    assert_eq!(leader.read_index(0), Err(ReadError::InvalidContext));

    while let Some(r) = leader.ready() {
        assert_eq!(r.read_states, []);
        leader.advance();
    }

    // Replicating the election Noop (index 1) promotes queued read 77 and sends heartbeat with read_ctx=77.
    leader.step(Message {
        from: NodeId(2),
        to: NodeId(1),
        term: Term(1),
        body: MessageBody::AppendEntriesResponse {
            result: AppendResult::Accepted {
                match_index: Index(1),
            },
            read_ctx: 0,
        },
    });
    assert_eq!(leader.status().commit, Index(1));

    let r_hb = leader.ready().unwrap();
    assert!(
        r_hb.messages
            .iter()
            .any(|m| { matches!(m.body, MessageBody::AppendEntries { read_ctx: 77, .. }) })
    );
    assert_eq!(r_hb.read_states, []);
    leader.advance();

    leader.step(Message {
        from: NodeId(2),
        to: NodeId(1),
        term: Term(1),
        body: MessageBody::AppendEntriesResponse {
            result: AppendResult::Accepted {
                match_index: Index(1),
            },
            read_ctx: 77,
        },
    });
    let r_done = leader.ready().unwrap();
    assert_eq!(
        r_done.read_states,
        vec![ReadState {
            ctx: 77,
            index: Index(1)
        }]
    );
    leader.advance();
}

#[test]
fn read_index_requires_quorum_echo() {
    let mut nodes = elect_three_node_leader();
    let lid = leader_id(&nodes);
    let followers: Vec<NodeId> = nodes.keys().copied().filter(|&id| id != lid).collect();

    // Multiple reads queued before ready() share one heartbeat round.
    nodes.get_mut(&lid).unwrap().read_index(10).unwrap();
    nodes.get_mut(&lid).unwrap().read_index(11).unwrap();

    let hb = nodes.get_mut(&lid).unwrap().ready().unwrap();
    assert_eq!(hb.read_states, []);
    let ctx_msgs: Vec<Message> = hb
        .messages
        .into_iter()
        .filter(|m| matches!(m.body, MessageBody::AppendEntries { read_ctx: 10, .. }))
        .collect();
    assert_eq!(ctx_msgs.len(), 2);
    nodes.get_mut(&lid).unwrap().advance();

    // Only one follower echo is needed alongside self (2 of 3).
    let f_msg = ctx_msgs.into_iter().find(|m| m.to == followers[0]).unwrap();
    nodes.get_mut(&followers[0]).unwrap().step(f_msg);
    let f_ready = nodes.get_mut(&followers[0]).unwrap().ready().unwrap();
    for resp in f_ready.messages {
        nodes.get_mut(&lid).unwrap().step(resp);
    }
    nodes.get_mut(&followers[0]).unwrap().advance();

    let done = nodes.get_mut(&lid).unwrap().ready().unwrap();
    assert_eq!(
        done.read_states,
        vec![
            ReadState {
                ctx: 10,
                index: Index(1)
            },
            ReadState {
                ctx: 11,
                index: Index(1)
            },
        ]
    );
    nodes.get_mut(&lid).unwrap().advance();
}

#[test]
fn snapshot_install_replaces_divergent_log() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut follower = RaftNode::new(
        Config::with_defaults(NodeId(2), peers, 2),
        Recovered {
            hard_state: HardState {
                term: Term(1),
                voted_for: None,
                commit: Index(1),
            },
            snapshot: None,
            entries: vec![
                Entry {
                    term: Term(1),
                    index: Index(1),
                    payload: Payload::Noop,
                },
                Entry {
                    term: Term(1),
                    index: Index(2),
                    payload: cmd(b"divergent"),
                },
            ],
        },
    )
    .unwrap();

    let snap = Snapshot {
        meta: SnapshotMeta {
            index: Index(5),
            term: Term(3),
        },
        data: Bytes::from_static(b"kv-image"),
    };
    follower.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(3),
        body: MessageBody::InstallSnapshot {
            snapshot: snap.clone(),
        },
    });

    let ready = follower.ready().unwrap();
    assert_eq!(ready.snapshot, Some(snap));
    assert_eq!(
        ready.messages[0].body,
        MessageBody::InstallSnapshotResponse {
            match_index: Index(5)
        }
    );
    follower.advance();

    let status = follower.status();
    assert_eq!(status.snapshot_index, Index(5));
    assert_eq!(status.last_log_index, Index(5));
    assert_eq!(status.commit, Index(5));
    assert_eq!(status.applied, Index(5));
}

#[test]
fn snapshot_install_on_matching_log_only_compacts() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut follower = RaftNode::new(
        Config::with_defaults(NodeId(2), peers, 2),
        Recovered {
            hard_state: HardState {
                term: Term(2),
                voted_for: None,
                commit: Index(5),
            },
            snapshot: None,
            entries: vec![
                Entry {
                    term: Term(1),
                    index: Index(1),
                    payload: Payload::Noop,
                },
                Entry {
                    term: Term(2),
                    index: Index(2),
                    payload: cmd(b"k2"),
                },
                Entry {
                    term: Term(2),
                    index: Index(3),
                    payload: cmd(b"k3"),
                },
                Entry {
                    term: Term(2),
                    index: Index(4),
                    payload: cmd(b"k4"),
                },
                Entry {
                    term: Term(2),
                    index: Index(5),
                    payload: cmd(b"k5"),
                },
            ],
        },
    )
    .unwrap();

    // Drain initial committed entries so applied reaches 5.
    while let Some(r) = follower.ready() {
        follower.advance();
        let _ = r;
    }
    assert_eq!(follower.status().applied, Index(5));

    let snap = Snapshot {
        meta: SnapshotMeta {
            index: Index(3),
            term: Term(2),
        },
        data: Bytes::from_static(b"snap3"),
    };
    follower.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(2),
        body: MessageBody::InstallSnapshot { snapshot: snap },
    });

    let ready = follower.ready().unwrap();
    assert_eq!(ready.snapshot, None);
    assert_eq!(
        ready.messages[0].body,
        MessageBody::InstallSnapshotResponse {
            match_index: Index(3)
        }
    );
    follower.advance();

    assert_eq!(follower.status().snapshot_index, Index(3));
    assert_eq!(follower.status().last_log_index, Index(5));
}

#[test]
fn leader_sends_install_snapshot_when_follower_needs_compacted_entry() {
    let mut nodes = elect_three_node_leader();
    let lid = leader_id(&nodes);
    let followers: Vec<NodeId> = nodes.keys().copied().filter(|&id| id != lid).collect();
    let slow_follower = followers[1];

    // Isolate slow_follower while leader and fast follower commit and compact entries 2..=4.
    let blocked = vec![(lid, slow_follower), (slow_follower, lid)];
    for payload in [b"a".as_slice(), b"b", b"c"] {
        nodes
            .get_mut(&lid)
            .unwrap()
            .propose(Bytes::copy_from_slice(payload))
            .unwrap();
        pump_messages(&mut nodes, &blocked);
    }
    assert_eq!(nodes[&lid].status().commit, Index(4));

    let snap = Snapshot {
        meta: SnapshotMeta {
            index: Index(4),
            term: nodes[&lid].status().term,
        },
        data: Bytes::from_static(b"compacted-at-4"),
    };
    nodes.get_mut(&lid).unwrap().compact(snap);

    // Propose index 5 and un-partition slow_follower.
    nodes
        .get_mut(&lid)
        .unwrap()
        .propose(Bytes::from_static(b"d"))
        .unwrap();
    pump_messages(&mut nodes, &[]);

    assert_eq!(nodes[&slow_follower].status().snapshot_index, Index(4));
    assert_eq!(nodes[&slow_follower].status().last_log_index, Index(5));
    assert_eq!(nodes[&slow_follower].status().commit, Index(5));
}
