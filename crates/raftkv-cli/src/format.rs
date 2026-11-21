//! Byte formatting, key prefix bounds, duration parsing, and table/JSON output renderers.

use std::time::Duration;

use raftkv_proto::{Role, StatusResponse};
use serde::Serialize;

use crate::client::{CasResult, DeleteResult, GetResult, NodeStatusReport, PutResult, RangeResult};

/// Output serialization format selected by `--output`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Human-readable aligned text output.
    #[default]
    Table,
    /// Structured JSON output via `serde_json`.
    Json,
}

/// Computes the exclusive upper bound `[prefix, end)` for `--prefix` range scans.
///
/// Returns `None` when `prefix` is empty or consists solely of `0xff` bytes
/// (indicating an unbounded upper range).
pub fn compute_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Renders raw key/value bytes safely, preserving valid non-control UTF-8 and escaping binary data.
pub fn format_bytes(bytes: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(bytes)
        && !text.bytes().any(|b| b.is_ascii_control())
    {
        return text.to_owned();
    }
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        for ch in std::ascii::escape_default(b) {
            out.push(char::from(ch));
        }
    }
    out
}

/// Parses human-friendly duration strings (`ms`, `s`, `m`) into [`Duration`].
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("timeout duration must not be empty".to_owned());
    }
    let (num_str, multiplier_ms) = if let Some(rest) = trimmed.strip_suffix("ms") {
        (rest, 1u64)
    } else if let Some(rest) = trimmed.strip_suffix('s') {
        (rest, 1000u64)
    } else if let Some(rest) = trimmed.strip_suffix('m') {
        (rest, 60_000u64)
    } else {
        return Err(format!(
            "unsupported duration '{input}' (expected e.g. 500ms, 5s, 1m)"
        ));
    };
    let val: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration number in '{input}'"))?;
    if val == 0 {
        return Err("timeout duration must be greater than zero".to_owned());
    }
    val.checked_mul(multiplier_ms)
        .map(Duration::from_millis)
        .ok_or_else(|| format!("duration '{input}' overflows"))
}

/// Summary produced when validating cluster quorum health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthSummary {
    /// Highest election term observed among healthy quorum responders.
    pub term: u64,
    /// Node ID of the single agreed leader in `term`.
    pub leader_id: u64,
    /// Number of endpoints that responded.
    pub reachable: usize,
    /// Total endpoints checked.
    pub total: usize,
}

/// Evaluates whether a quorum of endpoints responded and agreed on one leader in the highest term.
pub fn evaluate_health(
    total_endpoints: usize,
    reports: &[NodeStatusReport],
) -> Result<HealthSummary, String> {
    let quorum = total_endpoints / 2 + 1;
    let reachable: Vec<&StatusResponse> = reports
        .iter()
        .filter_map(|r| r.status.as_ref().ok())
        .collect();

    if reachable.len() < quorum {
        return Err(format!(
            "unhealthy cluster: {}/{} endpoints reachable (need quorum {})",
            reachable.len(),
            total_endpoints,
            quorum
        ));
    }

    let highest_term = reachable.iter().map(|s| s.term).max().unwrap_or(0);
    let in_highest: Vec<&StatusResponse> = reachable
        .into_iter()
        .filter(|s| s.term == highest_term)
        .collect();

    if in_highest.len() < quorum {
        return Err(format!(
            "unhealthy cluster: only {}/{} reachable nodes on highest term {}",
            in_highest.len(),
            total_endpoints,
            highest_term
        ));
    }

    let mut agreed_leader: Option<u64> = None;
    let mut agreeing_count = 0usize;
    for st in &in_highest {
        if st.leader_id == 0 {
            continue;
        }
        match agreed_leader {
            None => {
                agreed_leader = Some(st.leader_id);
                agreeing_count = 1;
            }
            Some(prev) if prev == st.leader_id => {
                agreeing_count += 1;
            }
            Some(prev) => {
                return Err(format!(
                    "unhealthy cluster: conflicting leaders {} and {} in term {}",
                    prev, st.leader_id, highest_term
                ));
            }
        }
    }

    let Some(leader_id) = agreed_leader else {
        return Err(format!(
            "unhealthy cluster: no leader elected in term {highest_term}"
        ));
    };

    if agreeing_count < quorum {
        return Err(format!(
            "unhealthy cluster: only {agreeing_count} node(s) recognize leader {leader_id} in term {highest_term} (need quorum {quorum})"
        ));
    }

    let max_applied = reports
        .iter()
        .filter_map(|r| r.status.as_ref().ok())
        .map(|s| s.applied_index)
        .max()
        .unwrap_or(0);
    if let Some(leader_status) = in_highest.iter().find(|s| s.node_id == leader_id)
        && leader_status.commit_index < max_applied
    {
        return Err(format!(
            "unhealthy cluster: leader {leader_id} commit_index ({}) lags behind max applied_index ({max_applied})",
            leader_status.commit_index
        ));
    }

    Ok(HealthSummary {
        term: highest_term,
        leader_id,
        reachable: reports.iter().filter(|r| r.status.is_ok()).count(),
        total: total_endpoints,
    })
}

/// Formats `put` command output.
pub fn format_put(res: &PutResult, fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => format!("OK log_index={}", res.log_index),
        OutputFormat::Json => serialize_json(&JsonPut {
            ok: true,
            log_index: res.log_index,
        }),
    }
}

/// Formats `get` command output.
pub fn format_get(res: &GetResult, fmt: OutputFormat) -> String {
    let val_str = res.value.as_deref().map(format_bytes);
    match fmt {
        OutputFormat::Table => match val_str {
            Some(v) => format!("{v}\napplied_index={}", res.applied_index),
            None => format!("(not found)\napplied_index={}", res.applied_index),
        },
        OutputFormat::Json => serialize_json(&JsonGet {
            found: val_str.is_some(),
            value: val_str,
            applied_index: res.applied_index,
        }),
    }
}

/// Formats `del` command output.
pub fn format_delete(res: &DeleteResult, fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => {
            format!("existed={} log_index={}", res.existed, res.log_index)
        }
        OutputFormat::Json => serialize_json(&JsonDelete {
            existed: res.existed,
            log_index: res.log_index,
        }),
    }
}

/// Formats `cas` command output.
pub fn format_cas(res: &CasResult, fmt: OutputFormat) -> String {
    let current = res.current.as_deref().map(format_bytes);
    match fmt {
        OutputFormat::Table => {
            let cur_disp = current.as_deref().unwrap_or("(absent)");
            format!(
                "swapped={} current={} log_index={}",
                res.swapped, cur_disp, res.log_index
            )
        }
        OutputFormat::Json => serialize_json(&JsonCas {
            swapped: res.swapped,
            current,
            log_index: res.log_index,
        }),
    }
}

/// Formats `range` command output.
pub fn format_range(res: &RangeResult, fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => {
            let mut lines = Vec::with_capacity(res.kvs.len() + 2);
            lines.push(format!("{:<24} {}", "KEY", "VALUE"));
            for (k, v) in &res.kvs {
                lines.push(format!("{:<24} {}", format_bytes(k), format_bytes(v)));
            }
            lines.push(format!(
                "count={} more={} applied_index={}",
                res.kvs.len(),
                res.more,
                res.applied_index
            ));
            lines.join("\n")
        }
        OutputFormat::Json => {
            let items = res
                .kvs
                .iter()
                .map(|(k, v)| JsonKv {
                    key: format_bytes(k),
                    value: format_bytes(v),
                })
                .collect();
            serialize_json(&JsonRange {
                kvs: items,
                more: res.more,
                applied_index: res.applied_index,
            })
        }
    }
}

fn role_label(raw: i32) -> &'static str {
    match Role::try_from(raw) {
        Ok(Role::Follower) => "follower",
        Ok(Role::PreCandidate) => "pre_candidate",
        Ok(Role::Candidate) => "candidate",
        Ok(Role::Leader) => "leader",
        Err(_) => "unknown",
    }
}

/// Formats `status` cluster report output.
pub fn format_status(reports: &[NodeStatusReport], fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => {
            let mut lines = Vec::new();
            lines.push(format!(
                "{:<22} {:<6} {:<14} {:<6} {:<8} {:<8} {:<8} {:<8} {}",
                "ENDPOINT",
                "NODE",
                "ROLE",
                "TERM",
                "LEADER",
                "COMMIT",
                "APPLIED",
                "SNAP",
                "LAST_LOG"
            ));
            let mut leader_peers: Option<(u64, &Vec<raftkv_proto::PeerProgress>)> = None;

            for report in reports {
                match &report.status {
                    Ok(st) => {
                        let role = role_label(st.role);
                        if st.role == Role::Leader as i32 && !st.peers.is_empty() {
                            leader_peers = Some((st.node_id, &st.peers));
                        }
                        let leader = if st.leader_id == 0 {
                            "-".to_owned()
                        } else {
                            st.leader_id.to_string()
                        };
                        lines.push(format!(
                            "{:<22} {:<6} {:<14} {:<6} {:<8} {:<8} {:<8} {:<8} {}",
                            report.endpoint,
                            st.node_id,
                            role,
                            st.term,
                            leader,
                            st.commit_index,
                            st.applied_index,
                            st.snapshot_index,
                            st.last_log_index,
                        ));
                    }
                    Err(err) => {
                        lines.push(format!("{:<22} ERROR: {}", report.endpoint, err));
                    }
                }
            }

            if let Some((leader_id, peers)) = leader_peers {
                lines.push(String::new());
                lines.push(format!("Leader {leader_id} peer replication progress:"));
                lines.push(format!(
                    "{:<8} {:<12} {:<12} {}",
                    "PEER", "MATCH_INDEX", "NEXT_INDEX", "RECENTLY_ACTIVE"
                ));
                for p in peers {
                    lines.push(format!(
                        "{:<8} {:<12} {:<12} {}",
                        p.node_id, p.match_index, p.next_index, p.recently_active
                    ));
                }
            }

            lines.join("\n")
        }
        OutputFormat::Json => {
            let nodes = reports
                .iter()
                .map(|r| match &r.status {
                    Ok(st) => JsonNodeReport {
                        endpoint: r.endpoint.clone(),
                        reachable: true,
                        error: None,
                        node_id: Some(st.node_id),
                        role: Some(role_label(st.role)),
                        term: Some(st.term),
                        leader_id: Some(st.leader_id),
                        commit_index: Some(st.commit_index),
                        applied_index: Some(st.applied_index),
                        last_log_index: Some(st.last_log_index),
                        snapshot_index: Some(st.snapshot_index),
                        peers: st
                            .peers
                            .iter()
                            .map(|p| JsonPeerProgress {
                                node_id: p.node_id,
                                match_index: p.match_index,
                                next_index: p.next_index,
                                recently_active: p.recently_active,
                            })
                            .collect(),
                    },
                    Err(err) => JsonNodeReport {
                        endpoint: r.endpoint.clone(),
                        reachable: false,
                        error: Some(err.clone()),
                        node_id: None,
                        role: None,
                        term: None,
                        leader_id: None,
                        commit_index: None,
                        applied_index: None,
                        last_log_index: None,
                        snapshot_index: None,
                        peers: Vec::new(),
                    },
                })
                .collect();
            serialize_json(&JsonStatusList { nodes })
        }
    }
}

/// Formats `health` summary output.
pub fn format_health(summary: &HealthSummary, fmt: OutputFormat) -> String {
    match fmt {
        OutputFormat::Table => format!(
            "healthy term={} leader={} reachable={}/{}",
            summary.term, summary.leader_id, summary.reachable, summary.total
        ),
        OutputFormat::Json => serialize_json(summary),
    }
}

fn serialize_json<T: Serialize>(value: &T) -> String {
    // INVARIANT: All Json* structs consist solely of UTF-8 strings, integers, booleans, and vecs.
    serde_json::to_string_pretty(value).expect("JSON serialization of CLI structs cannot fail")
}

#[derive(Serialize)]
struct JsonPut {
    ok: bool,
    log_index: u64,
}

#[derive(Serialize)]
struct JsonGet {
    found: bool,
    value: Option<String>,
    applied_index: u64,
}

#[derive(Serialize)]
struct JsonDelete {
    existed: bool,
    log_index: u64,
}

#[derive(Serialize)]
struct JsonCas {
    swapped: bool,
    current: Option<String>,
    log_index: u64,
}

#[derive(Serialize)]
struct JsonKv {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct JsonRange {
    kvs: Vec<JsonKv>,
    more: bool,
    applied_index: u64,
}

#[derive(Serialize)]
struct JsonPeerProgress {
    node_id: u64,
    match_index: u64,
    next_index: u64,
    recently_active: bool,
}

#[derive(Serialize)]
struct JsonNodeReport {
    endpoint: String,
    reachable: bool,
    error: Option<String>,
    node_id: Option<u64>,
    role: Option<&'static str>,
    term: Option<u64>,
    leader_id: Option<u64>,
    commit_index: Option<u64>,
    applied_index: Option<u64>,
    last_log_index: Option<u64>,
    snapshot_index: Option<u64>,
    peers: Vec<JsonPeerProgress>,
}

#[derive(Serialize)]
struct JsonStatusList {
    nodes: Vec<JsonNodeReport>,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use raftkv_proto::PeerProgress;

    use super::*;

    #[test]
    fn compute_prefix_end_handles_boundaries() {
        assert_eq!(compute_prefix_end(b"a"), Some(b"b".to_vec()));
        assert_eq!(compute_prefix_end(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(compute_prefix_end(b"\xff\xff"), None);
        assert_eq!(compute_prefix_end(b""), None);
    }

    #[test]
    fn escapes_non_utf8_bytes_safely() {
        assert_eq!(format_bytes(b"hello-world"), "hello-world");
        assert_eq!(format_bytes(b"\xff\x00a"), r"\xff\x00a");
    }

    #[test]
    fn table_and_json_snapshots_match_expected_layouts() {
        let range_res = RangeResult {
            kvs: vec![
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"k2"), Bytes::from_static(b"\xff")),
            ],
            more: true,
            applied_index: 9,
        };
        let table = format_range(&range_res, OutputFormat::Table);
        assert_eq!(
            table,
            "KEY                      VALUE\nk1                       v1\nk2                       \\xff\ncount=2 more=true applied_index=9"
        );
        let json = format_range(&range_res, OutputFormat::Json);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(parsed["kvs"][0]["key"], "k1");
        assert_eq!(parsed["kvs"][1]["value"], r"\xff");
        assert_eq!(parsed["more"], true);
        assert_eq!(parsed["applied_index"], 9);

        let reports = vec![
            NodeStatusReport {
                endpoint: "127.0.0.1:7001".to_owned(),
                status: Ok(StatusResponse {
                    node_id: 1,
                    role: Role::Leader as i32,
                    term: 3,
                    leader_id: 1,
                    commit_index: 12,
                    applied_index: 12,
                    last_log_index: 12,
                    snapshot_index: 0,
                    peers: vec![PeerProgress {
                        node_id: 2,
                        match_index: 12,
                        next_index: 13,
                        recently_active: true,
                    }],
                }),
            },
            NodeStatusReport {
                endpoint: "127.0.0.1:7002".to_owned(),
                status: Err("connection refused".to_owned()),
            },
        ];
        let status_tbl = format_status(&reports, OutputFormat::Table);
        assert!(status_tbl.contains("Leader 1 peer replication progress:"));
        assert!(status_tbl.contains("127.0.0.1:7002         ERROR: connection refused"));

        let status_json = format_status(&reports, OutputFormat::Json);
        let st_val: serde_json::Value = serde_json::from_str(&status_json).expect("valid json");
        assert_eq!(st_val["nodes"][0]["role"], "leader");
        assert_eq!(st_val["nodes"][0]["peers"][0]["node_id"], 2);
        assert_eq!(st_val["nodes"][1]["reachable"], false);
    }

    #[test]
    fn health_evaluation_checks_quorum_and_single_leader() {
        let ok_reports = vec![
            NodeStatusReport {
                endpoint: "a".to_owned(),
                status: Ok(StatusResponse {
                    node_id: 1,
                    role: Role::Leader as i32,
                    term: 2,
                    leader_id: 1,
                    ..Default::default()
                }),
            },
            NodeStatusReport {
                endpoint: "b".to_owned(),
                status: Ok(StatusResponse {
                    node_id: 2,
                    role: Role::Follower as i32,
                    term: 2,
                    leader_id: 1,
                    ..Default::default()
                }),
            },
            NodeStatusReport {
                endpoint: "c".to_owned(),
                status: Err("down".to_owned()),
            },
        ];
        let summary = evaluate_health(3, &ok_reports).expect("quorum agrees");
        assert_eq!(summary.term, 2);
        assert_eq!(summary.leader_id, 1);

        let split_reports = vec![ok_reports[0].clone(), ok_reports[2].clone()];
        assert!(evaluate_health(3, &split_reports).is_err());

        let lagging_leader = vec![
            NodeStatusReport {
                endpoint: "a".to_owned(),
                status: Ok(StatusResponse {
                    node_id: 1,
                    role: Role::Leader as i32,
                    term: 3,
                    leader_id: 1,
                    commit_index: 2,
                    applied_index: 2,
                    ..Default::default()
                }),
            },
            NodeStatusReport {
                endpoint: "b".to_owned(),
                status: Ok(StatusResponse {
                    node_id: 2,
                    role: Role::Follower as i32,
                    term: 3,
                    leader_id: 1,
                    commit_index: 5,
                    applied_index: 5,
                    ..Default::default()
                }),
            },
            NodeStatusReport {
                endpoint: "c".to_owned(),
                status: Err("down".to_owned()),
            },
        ];
        assert!(evaluate_health(3, &lagging_leader).is_err());
    }
}
