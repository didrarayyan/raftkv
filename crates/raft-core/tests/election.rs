//! Election unit tests covering `raft-core` rules E1 through E7.

use std::collections::BTreeMap;

use bytes::Bytes;
use raft_core::{
    Config, ConfigError, Entry, HardState, Index, Message, MessageBody, NodeId, Payload, RaftNode,
    Recovered, Role, Term,
};

struct ClusterHarness {
    nodes: BTreeMap<NodeId, RaftNode>,
    inboxes: BTreeMap<NodeId, Vec<Message>>,
    blocked_links: Vec<(NodeId, NodeId)>,
}

impl ClusterHarness {
    fn new(configs: Vec<Config>) -> Self {
        let mut nodes = BTreeMap::new();
        let mut inboxes = BTreeMap::new();
        for cfg in configs {
            let id = cfg.id;
            let node = RaftNode::new(cfg, Recovered::default()).unwrap();
            nodes.insert(id, node);
            inboxes.insert(id, Vec::new());
        }
        Self {
            nodes,
            inboxes,
            blocked_links: Vec::new(),
        }
    }

    fn isolate(&mut self, isolated: NodeId) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            if id != isolated {
                self.blocked_links.push((isolated, id));
                self.blocked_links.push((id, isolated));
            }
        }
    }

    fn heal(&mut self) {
        self.blocked_links.clear();
    }

    fn drain_ready_on(&mut self, id: NodeId) -> Vec<Entry> {
        let mut applied = Vec::new();
        let node = self.nodes.get_mut(&id).unwrap();
        while let Some(ready) = node.ready() {
            for msg in ready.messages {
                if !self.blocked_links.contains(&(msg.from, msg.to))
                    && let Some(q) = self.inboxes.get_mut(&msg.to)
                {
                    q.push(msg);
                }
            }
            applied.extend(ready.committed);
            node.advance();
        }
        applied
    }

    fn step_tick(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for &id in &ids {
            self.nodes.get_mut(&id).unwrap().tick();
            self.drain_ready_on(id);
        }
        for _ in 0..8 {
            let mut delivered = false;
            for &id in &ids {
                let msgs = std::mem::take(self.inboxes.get_mut(&id).unwrap());
                if !msgs.is_empty() {
                    delivered = true;
                }
                for msg in msgs {
                    self.nodes.get_mut(&id).unwrap().step(msg);
                }
                self.drain_ready_on(id);
            }
            if !delivered {
                break;
            }
        }
    }

    fn run_ticks(&mut self, ticks: usize) {
        for _ in 0..ticks {
            self.step_tick();
        }
    }
}

#[test]
fn new_rejects_invalid_configs() {
    let mut cfg = Config::with_defaults(NodeId(0), vec![NodeId(1)], 1);
    assert_eq!(
        RaftNode::new(cfg.clone(), Recovered::default()).unwrap_err(),
        ConfigError::InvalidNodeId
    );

    cfg = Config::with_defaults(NodeId(1), vec![NodeId(1), NodeId(2)], 1);
    cfg.election_ticks = 4;
    cfg.heartbeat_ticks = 2;
    assert!(matches!(
        RaftNode::new(cfg, Recovered::default()),
        Err(ConfigError::InvalidTimers { .. })
    ));
}

#[test]
fn single_node_elects_itself_and_commits_immediately() {
    let cfg = Config::with_defaults(NodeId(1), vec![NodeId(1)], 42);
    let mut node = RaftNode::new(cfg, Recovered::default()).unwrap();

    for _ in 0..25 {
        node.tick();
        if node.status().role == Role::Leader {
            break;
        }
    }
    assert_eq!(node.status().role, Role::Leader);
    assert_eq!(node.status().term, Term(1));

    let mut committed = Vec::new();
    while let Some(ready) = node.ready() {
        committed.extend(ready.committed);
        node.advance();
    }

    assert_eq!(node.status().commit, Index(1));
    assert_eq!(node.status().applied, Index(1));
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].payload, Payload::Noop);
}

#[test]
fn three_node_election_on_clean_network() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = ClusterHarness::new(vec![
        Config::with_defaults(NodeId(1), peers.clone(), 1),
        Config::with_defaults(NodeId(2), peers.clone(), 2),
        Config::with_defaults(NodeId(3), peers, 3),
    ]);

    cluster.run_ticks(40);

    let leaders: Vec<NodeId> = cluster
        .nodes
        .iter()
        .filter(|(_, n)| n.status().role == Role::Leader)
        .map(|(&id, _)| id)
        .collect();
    assert_eq!(leaders.len(), 1);

    let leader_id = leaders[0];
    let leader_term = cluster.nodes[&leader_id].status().term;
    assert!(leader_term >= Term(1));
    for node in cluster.nodes.values() {
        assert_eq!(node.status().leader_id, Some(leader_id));
        assert_eq!(node.status().term, leader_term);
    }
}

#[test]
fn candidate_with_stale_log_cannot_win() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut voter = RaftNode::new(
        Config::with_defaults(NodeId(2), peers.clone(), 10),
        Recovered {
            hard_state: HardState {
                term: Term(2),
                voted_for: None,
                commit: Index(0),
            },
            snapshot: None,
            entries: vec![Entry {
                term: Term(2),
                index: Index(1),
                payload: Payload::Command(Bytes::from_static(b"newer")),
            }],
        },
    )
    .unwrap();

    voter.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(3),
        body: MessageBody::PreVote {
            last_index: Index(0),
            last_term: Term(0),
        },
    });
    let pre_ready = voter.ready().unwrap();
    assert_eq!(
        pre_ready.messages[0].body,
        MessageBody::PreVoteResponse { granted: false }
    );
    voter.advance();

    voter.step(Message {
        from: NodeId(1),
        to: NodeId(2),
        term: Term(3),
        body: MessageBody::RequestVote {
            last_index: Index(1),
            last_term: Term(1),
        },
    });
    let vote_ready = voter.ready().unwrap();
    assert_eq!(
        vote_ready.messages[0].body,
        MessageBody::RequestVoteResponse { granted: false }
    );
    voter.advance();

    // Even when node 1 proposes a full term 3 election against a 3-node cluster with newer logs on 2 & 3,
    // node 1 stays stuck without a quorum.
    let mut c1 = Config::with_defaults(NodeId(1), peers, 1);
    c1.pre_vote = false;
    let mut stale = RaftNode::new(c1, Recovered::default()).unwrap();
    for _ in 0..25 {
        stale.tick();
        if let Some(r) = stale.ready() {
            for msg in r.messages {
                if msg.to == NodeId(2) {
                    voter.step(msg);
                    if let Some(vr) = voter.ready() {
                        for resp in vr.messages {
                            stale.step(resp);
                        }
                        voter.advance();
                    }
                }
            }
            stale.advance();
        }
    }
    assert_ne!(stale.status().role, Role::Leader);
}

#[test]
fn vote_is_granted_once_per_term() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut voter = RaftNode::new(
        Config::with_defaults(NodeId(3), peers, 5),
        Recovered::default(),
    )
    .unwrap();

    voter.step(Message {
        from: NodeId(1),
        to: NodeId(3),
        term: Term(2),
        body: MessageBody::RequestVote {
            last_index: Index(1),
            last_term: Term(1),
        },
    });
    let r1 = voter.ready().unwrap();
    assert_eq!(
        r1.messages[0].body,
        MessageBody::RequestVoteResponse { granted: true }
    );
    assert_eq!(r1.hard_state.unwrap().voted_for, Some(NodeId(1)));
    voter.advance();

    voter.step(Message {
        from: NodeId(2),
        to: NodeId(3),
        term: Term(2),
        body: MessageBody::RequestVote {
            last_index: Index(2),
            last_term: Term(2),
        },
    });
    let r2 = voter.ready().unwrap();
    assert_eq!(
        r2.messages[0].body,
        MessageBody::RequestVoteResponse { granted: false }
    );
    voter.advance();

    // Re-requesting from the same candidate in the same term stays idempotent.
    voter.step(Message {
        from: NodeId(1),
        to: NodeId(3),
        term: Term(2),
        body: MessageBody::RequestVote {
            last_index: Index(1),
            last_term: Term(1),
        },
    });
    let r3 = voter.ready().unwrap();
    assert_eq!(
        r3.messages[0].body,
        MessageBody::RequestVoteResponse { granted: true }
    );
    voter.advance();
}

#[test]
fn pre_vote_does_not_bump_term() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut node = RaftNode::new(
        Config::with_defaults(NodeId(1), peers, 1),
        Recovered::default(),
    )
    .unwrap();

    for _ in 0..40 {
        node.tick();
        while let Some(r) = node.ready() {
            assert_eq!(r.hard_state, None);
            node.advance();
        }
    }
    assert_eq!(node.status().term, Term(0));
    assert_eq!(node.status().role, Role::PreCandidate);
}

#[test]
fn isolated_node_rejoining_does_not_depose_healthy_leader() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = ClusterHarness::new(vec![
        Config::with_defaults(NodeId(1), peers.clone(), 11),
        Config::with_defaults(NodeId(2), peers.clone(), 22),
        Config::with_defaults(NodeId(3), peers, 33),
    ]);

    cluster.run_ticks(35);
    let leader_id = cluster
        .nodes
        .iter()
        .find(|(_, n)| n.status().role == Role::Leader)
        .map(|(&id, _)| id)
        .unwrap();
    let follower_to_isolate = cluster
        .nodes
        .keys()
        .copied()
        .find(|&id| id != leader_id)
        .unwrap();
    let initial_term = cluster.nodes[&leader_id].status().term;

    cluster.isolate(follower_to_isolate);
    cluster.run_ticks(50);

    // Healthy majority kept the exact same leader and term.
    assert_eq!(cluster.nodes[&leader_id].status().role, Role::Leader);
    assert_eq!(cluster.nodes[&leader_id].status().term, initial_term);
    assert_eq!(
        cluster.nodes[&follower_to_isolate].status().term,
        initial_term
    );

    // Rejoin and deliver any delayed RequestVote at a higher term directly to check E4 guard too.
    let follower_peer = cluster
        .nodes
        .keys()
        .copied()
        .find(|&id| id != leader_id && id != follower_to_isolate)
        .unwrap();
    cluster
        .nodes
        .get_mut(&follower_peer)
        .unwrap()
        .step(Message {
            from: follower_to_isolate,
            to: follower_peer,
            term: initial_term.next().next(),
            body: MessageBody::RequestVote {
                last_index: Index(1),
                last_term: initial_term,
            },
        });
    assert_eq!(cluster.nodes[&follower_peer].status().term, initial_term);

    cluster.heal();
    cluster.run_ticks(15);

    assert_eq!(cluster.nodes[&leader_id].status().role, Role::Leader);
    assert_eq!(cluster.nodes[&leader_id].status().term, initial_term);
    assert_eq!(
        cluster.nodes[&follower_to_isolate].status().leader_id,
        Some(leader_id)
    );
}

#[test]
fn leader_steps_down_without_quorum() {
    let peers = vec![NodeId(1), NodeId(2), NodeId(3)];
    let mut cluster = ClusterHarness::new(vec![
        Config::with_defaults(NodeId(1), peers.clone(), 101),
        Config::with_defaults(NodeId(2), peers.clone(), 102),
        Config::with_defaults(NodeId(3), peers, 103),
    ]);

    cluster.run_ticks(35);
    let leader_id = cluster
        .nodes
        .iter()
        .find(|(_, n)| n.status().role == Role::Leader)
        .map(|(&id, _)| id)
        .unwrap();
    let term_before = cluster.nodes[&leader_id].status().term;

    cluster.isolate(leader_id);
    cluster.run_ticks(22);

    let isolated_leader = &cluster.nodes[&leader_id];
    assert_eq!(isolated_leader.status().role, Role::Follower);
    assert_eq!(isolated_leader.status().term, term_before);
    assert_eq!(isolated_leader.status().leader_id, None);
}
