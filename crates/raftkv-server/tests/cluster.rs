//! Multi-process cluster integration tests spawning `raftkvd` binaries over loopback gRPC.

#![forbid(unsafe_code)]

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use raftkv_cli::{Client, ClientError};
use raftkv_proto::Role;
use tempfile::TempDir;
use tonic::Code;

struct NodeHandle {
    id: u64,
    addr: String,
    cfg_path: PathBuf,
    log_path: PathBuf,
    child: Option<Child>,
}

struct TestCluster {
    nodes: Vec<NodeHandle>,
    _temp_dir: TempDir,
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn pick_free_ports(count: usize) -> Vec<SocketAddr> {
    let listeners: Vec<TcpListener> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("local_addr"))
        .collect()
}

fn toml_escaped_path(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

impl TestCluster {
    fn start(snapshot_threshold: u64, retain_entries: u64) -> Self {
        let temp_dir = tempfile::tempdir().expect("create tempdir");
        let addrs = pick_free_ports(3);
        let peer_strs: Vec<String> = addrs.iter().map(ToString::to_string).collect();
        let peers_toml = peer_strs
            .iter()
            .enumerate()
            .map(|(i, addr)| format!("{{ id = {}, addr = \"{addr}\" }}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");

        let mut nodes = Vec::with_capacity(3);
        for (idx, addr) in peer_strs.into_iter().enumerate() {
            let id = (idx + 1) as u64;
            let data_dir = temp_dir.path().join(format!("data-{id}"));
            fs::create_dir_all(&data_dir).expect("create node data_dir");
            let cfg_path = temp_dir.path().join(format!("node-{id}.toml"));
            let log_path = temp_dir.path().join(format!("node-{id}.log"));

            let toml_body = format!(
                "node_id = {id}\n\
                 listen_addr = \"{addr}\"\n\
                 data_dir = \"{}\"\n\
                 \n\
                 [cluster]\n\
                 peers = [{peers_toml}]\n\
                 \n\
                 [raft]\n\
                 tick_ms = 50\n\
                 election_ticks = 10\n\
                 heartbeat_ticks = 2\n\
                 snapshot_threshold = {snapshot_threshold}\n\
                 retain_entries = {retain_entries}\n",
                toml_escaped_path(&data_dir)
            );
            fs::write(&cfg_path, toml_body).expect("write node config");

            let child = spawn_raftkvd(&cfg_path, &log_path);
            nodes.push(NodeHandle {
                id,
                addr,
                cfg_path,
                log_path,
                child: Some(child),
            });
        }

        Self {
            nodes,
            _temp_dir: temp_dir,
        }
    }

    fn endpoints(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.addr.clone()).collect()
    }

    fn client(&self) -> Client {
        Client::connect(self.endpoints())
            .expect("connect cluster client")
            .with_timeout(Duration::from_secs(10))
    }

    fn kill_node(&mut self, idx: usize) {
        if let Some(mut child) = self.nodes[idx].child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn restart_node(&mut self, idx: usize) {
        assert!(
            self.nodes[idx].child.is_none(),
            "node {} still running",
            self.nodes[idx].id
        );
        let child = spawn_raftkvd(&self.nodes[idx].cfg_path, &self.nodes[idx].log_path);
        self.nodes[idx].child = Some(child);
    }

    fn kill_all(&mut self) {
        for idx in 0..self.nodes.len() {
            self.kill_node(idx);
        }
    }

    fn restart_all(&mut self) {
        for idx in 0..self.nodes.len() {
            self.restart_node(idx);
        }
    }

    async fn wait_for_leader(&self, min_term: u64, timeout: Duration) -> usize {
        let mut client = self.client().with_timeout(Duration::from_millis(800));
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            let reports = client.status_all().await;
            let mut leaders = Vec::new();
            for (idx, report) in reports.iter().enumerate() {
                if let Ok(st) = &report.status
                    && st.role == Role::Leader as i32
                    && st.term >= min_term
                {
                    leaders.push((idx, st.term));
                }
            }
            if leaders.len() == 1 {
                return leaders[0].0;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "timed out waiting for single leader (min_term={min_term}); logs: {:?}",
            self.nodes
                .iter()
                .map(|n| (&n.log_path, fs::read_to_string(&n.log_path).ok()))
                .collect::<Vec<_>>()
        );
    }

    async fn wait_until_node_applied(&self, node_idx: usize, min_applied: u64, timeout: Duration) {
        let mut client = Client::connect(vec![self.nodes[node_idx].addr.clone()])
            .expect("single-node client")
            .with_timeout(Duration::from_millis(800));
        let deadline = Instant::now() + timeout;

        while Instant::now() < deadline {
            let reports = client.status_all().await;
            if let Some(Ok(st)) = reports.first().map(|r| &r.status)
                && st.applied_index >= min_applied
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "node {} did not reach applied_index {min_applied} within {timeout:?}",
            self.nodes[node_idx].id
        );
    }
}

fn spawn_raftkvd(cfg_path: &std::path::Path, log_path: &std::path::Path) -> Child {
    let bin = env!("CARGO_BIN_EXE_raftkvd");
    let stderr_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("open raftkvd log file");
    Command::new(bin)
        .arg("--config")
        .arg(cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .expect("spawn raftkvd process")
}

#[tokio::test]
async fn three_node_boot_and_elect() {
    let cluster = TestCluster::start(10_000, 1024);
    let leader_idx = cluster.wait_for_leader(1, Duration::from_secs(10)).await;

    let mut client = cluster.client();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let reports = client.status_all().await;
        let leader_count = reports
            .iter()
            .filter(|r| matches!(&r.status, Ok(st) if st.role == Role::Leader as i32))
            .count();
        let follower_count = reports
            .iter()
            .filter(|r| matches!(&r.status, Ok(st) if st.role == Role::Follower as i32))
            .count();
        let term = reports
            .get(leader_idx)
            .and_then(|r| r.status.as_ref().ok())
            .map_or(0, |st| st.term);

        if leader_count == 1
            && follower_count == 2
            && term >= 1
            && reports
                .iter()
                .all(|r| matches!(&r.status, Ok(st) if st.term == term))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not converge on term"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn put_get_delete_cas_range_round_trip() {
    let cluster = TestCluster::start(10_000, 1024);
    let leader_idx = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    let follower_idx = (leader_idx + 1) % 3;

    let mut client = cluster.client();
    client.put("alpha", "1").await.expect("put alpha");
    client.put("beta", "2").await.expect("put beta");
    client.put("gamma", "3").await.expect("put gamma");

    let g = client.get("alpha", false).await.expect("get alpha");
    assert_eq!(g.value.as_deref(), Some(b"1".as_slice()));

    let cas_absent = client
        .cas("delta", None, b"4".as_slice())
        .await
        .expect("cas absent");
    assert!(cas_absent.swapped);

    let cas_fail = client
        .cas("delta", Some(b"wrong".as_slice().into()), b"5".as_slice())
        .await
        .expect("cas mismatch");
    assert!(!cas_fail.swapped);
    assert_eq!(cas_fail.current.as_deref(), Some(b"4".as_slice()));

    let cas_ok = client
        .cas("delta", Some(b"4".as_slice().into()), b"40".as_slice())
        .await
        .expect("cas match");
    assert!(cas_ok.swapped);

    let del = client.delete("beta").await.expect("delete beta");
    assert!(del.existed);

    let page = client
        .range("a", "z", 2, false)
        .await
        .expect("range [a, z)");
    assert!(page.more);
    assert_eq!(
        page.kvs
            .iter()
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
            .collect::<Vec<_>>(),
        vec![
            (b"alpha".as_slice(), b"1".as_slice()),
            (b"delta".as_slice(), b"40".as_slice()),
        ]
    );

    // Wait for follower to catch up and serve a stale local read directly.
    cluster
        .wait_until_node_applied(follower_idx, del.log_index, Duration::from_secs(5))
        .await;
    let mut follower_client = Client::connect(vec![cluster.nodes[follower_idx].addr.clone()])
        .expect("follower client")
        .with_timeout(Duration::from_secs(3));
    let stale_res = follower_client
        .get("delta", true)
        .await
        .expect("stale read on follower");
    assert_eq!(stale_res.value.as_deref(), Some(b"40".as_slice()));
}

#[tokio::test]
async fn leader_kill_and_failover() {
    let mut cluster = TestCluster::start(10_000, 1024);
    let old_leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    let mut client = cluster.client();

    for i in 0..50 {
        client
            .put(format!("k{i:02}"), format!("v{i:02}"))
            .await
            .expect("put initial key");
    }

    cluster.kill_node(old_leader);
    let new_leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    assert_ne!(new_leader, old_leader);

    for i in 0..50 {
        let got = client
            .get(format!("k{i:02}"), false)
            .await
            .expect("get after failover");
        assert_eq!(got.value.as_deref(), Some(format!("v{i:02}").as_bytes()));
    }
    client
        .put("post-failover", "ok")
        .await
        .expect("write after failover");
}

#[tokio::test]
async fn follower_kill_and_rejoin_log_catchup() {
    let mut cluster = TestCluster::start(10_000, 1024);
    let leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    let follower = (leader + 1) % 3;
    cluster.kill_node(follower);

    let mut client = cluster.client();
    let mut last_idx = 0;
    for i in 0..50 {
        let put = client
            .put(format!("log-{i:02}"), format!("val-{i:02}"))
            .await
            .expect("write while follower down");
        last_idx = put.log_index;
    }

    cluster.restart_node(follower);
    cluster
        .wait_until_node_applied(follower, last_idx, Duration::from_secs(10))
        .await;

    let mut follower_client = Client::connect(vec![cluster.nodes[follower].addr.clone()])
        .expect("follower client")
        .with_timeout(Duration::from_secs(3));
    for i in 0..50 {
        let got = follower_client
            .get(format!("log-{i:02}"), true)
            .await
            .expect("stale read on rejoined follower");
        assert_eq!(got.value.as_deref(), Some(format!("val-{i:02}").as_bytes()));
    }
}

#[tokio::test]
async fn follower_kill_and_rejoin_snapshot_catchup() {
    let mut cluster = TestCluster::start(50, 16);
    let leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    let follower = (leader + 1) % 3;
    cluster.kill_node(follower);

    let endpoints = cluster.endpoints();
    let mut handles = Vec::new();
    for worker in 0..4 {
        let eps = endpoints.clone();
        handles.push(tokio::spawn(async move {
            let mut cli = Client::connect(eps)
                .expect("worker client")
                .with_timeout(Duration::from_secs(15));
            for j in 0..50 {
                let idx = worker * 50 + j;
                cli.put(format!("snap-{idx:03}"), format!("v-{idx:03}"))
                    .await
                    .expect("put snapshot entry");
            }
        }));
    }
    for h in handles {
        h.await.expect("join writer worker");
    }

    // Confirm leader has compacted at snapshot_index >= 50 before restarting follower.
    let mut leader_cli = Client::connect(vec![cluster.nodes[leader].addr.clone()])
        .expect("leader client")
        .with_timeout(Duration::from_secs(2));
    let st = &leader_cli.status_all().await[0].status;
    let snap_idx = st.as_ref().expect("leader status").snapshot_index;
    assert!(
        snap_idx >= 50,
        "expected leader snapshot >= 50, got {snap_idx}"
    );

    cluster.restart_node(follower);
    let mut follower_cli = Client::connect(vec![cluster.nodes[follower].addr.clone()])
        .expect("follower client")
        .with_timeout(Duration::from_secs(2));
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let reports = follower_cli.status_all().await;
        if let Some(Ok(st)) = reports.first().map(|r| &r.status)
            && st.snapshot_index >= 50
            && st.applied_index >= 200
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "follower did not catch up via InstallSnapshot"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for idx in 0..200 {
        let got = follower_cli
            .get(format!("snap-{idx:03}"), true)
            .await
            .expect("stale read from follower after InstallSnapshot");
        assert_eq!(got.value.as_deref(), Some(format!("v-{idx:03}").as_bytes()));
    }
}

#[tokio::test]
async fn full_cluster_restart_preserves_acknowledged_writes() {
    let mut cluster = TestCluster::start(10_000, 1024);
    let _leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    let mut client = cluster.client();

    for i in 0..50 {
        client
            .put(format!("persist-{i:02}"), format!("value-{i:02}"))
            .await
            .expect("put persistent key");
    }

    cluster.kill_all();
    cluster.restart_all();
    let _new_leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;

    for i in 0..50 {
        let got = client
            .get(format!("persist-{i:02}"), false)
            .await
            .expect("read after full restart");
        assert_eq!(
            got.value.as_deref(),
            Some(format!("value-{i:02}").as_bytes())
        );
    }
}

#[tokio::test]
async fn duplicate_client_id_request_id_is_idempotent_across_failover() {
    let mut cluster = TestCluster::start(10_000, 1024);
    let old_leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;

    let mut setup_cli = cluster.client();
    setup_cli.put("counter", "0").await.expect("init counter=0");

    let mut c1 = cluster.client().with_session(9, 1);
    let first_cas = c1
        .cas("counter", Some(b"0".as_slice().into()), "1")
        .await
        .expect("first CAS succeeds");
    assert!(first_cas.swapped);

    cluster.kill_node(old_leader);
    let new_leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    assert_ne!(new_leader, old_leader);

    let mut c2 = Client::connect(vec![cluster.nodes[new_leader].addr.clone()])
        .expect("connect to new leader")
        .with_timeout(Duration::from_secs(10))
        .with_session(9, 1);
    let replay_cas = c2
        .cas("counter", Some(b"0".as_slice().into()), "1")
        .await
        .expect("re-submitted CAS returns cached result");
    assert!(
        replay_cas.swapped,
        "replayed (client_id=9, request_id=1) should return cached swapped=true"
    );

    let check = setup_cli.get("counter", false).await.expect("get counter");
    assert_eq!(check.value.as_deref(), Some(b"1".as_slice()));
}

#[tokio::test]
async fn concurrent_clients_preserve_linearizability() {
    let cluster = TestCluster::start(10_000, 1024);
    let _leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;

    let mut setup_cli = cluster.client();
    setup_cli.put("counter", "0").await.expect("init counter");

    let endpoints = cluster.endpoints();
    let mut workers = Vec::new();
    for _ in 0..4 {
        let eps = endpoints.clone();
        workers.push(tokio::spawn(async move {
            let mut cli = Client::connect(eps)
                .expect("worker client")
                .with_timeout(Duration::from_secs(10));
            let mut expected = None;
            let mut increments = 0usize;
            while increments < 50 {
                let cur = if let Some(v) = expected.take() {
                    v
                } else {
                    let g = cli.get("counter", false).await.expect("read counter");
                    g.value.expect("counter exists")
                };
                let n: u64 = std::str::from_utf8(&cur)
                    .expect("utf8 counter")
                    .parse()
                    .expect("numeric counter");
                let next = (n + 1).to_string();
                let res = cli
                    .cas("counter", Some(cur), next.into_bytes())
                    .await
                    .expect("cas increment");
                if res.swapped {
                    increments += 1;
                } else {
                    expected = res.current;
                }
            }
        }));
    }

    for w in workers {
        w.await.expect("join concurrent CAS worker");
    }

    let final_val = setup_cli
        .get("counter", false)
        .await
        .expect("final counter");
    assert_eq!(final_val.value.as_deref(), Some(b"200".as_slice()));
}

#[tokio::test]
async fn oversized_key_and_value_rejected_with_invalid_argument() {
    let cluster = TestCluster::start(10_000, 1024);
    let _leader = cluster.wait_for_leader(1, Duration::from_secs(10)).await;
    let mut client = cluster.client();

    let empty_err = client.put("", "v").await.expect_err("empty key must fail");
    assert!(matches!(
        empty_err,
        ClientError::Rpc {
            code: Code::InvalidArgument,
            ..
        }
    ));

    let big_key = vec![b'k'; 1025];
    let big_key_err = client
        .put(big_key, "v")
        .await
        .expect_err("oversized key must fail");
    assert!(matches!(
        big_key_err,
        ClientError::Rpc {
            code: Code::InvalidArgument,
            ..
        }
    ));

    let big_val = vec![b'v'; 1024 * 1024 + 1];
    let big_val_err = client
        .put("ok-key", big_val)
        .await
        .expect_err("oversized value must fail");
    assert!(matches!(
        big_val_err,
        ClientError::Rpc {
            code: Code::InvalidArgument,
            ..
        }
    ));
}
