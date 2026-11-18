#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV_DIR="${ROOT_DIR}/.dev"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cd "${ROOT_DIR}"
cargo build --release -p raftkv-server -p raftkv-cli

mkdir -p "${DEV_DIR}"
PEERS='[{ id = 1, addr = "127.0.0.1:7001" }, { id = 2, addr = "127.0.0.1:7002" }, { id = 3, addr = "127.0.0.1:7003" }]'
for id in 1 2 3; do
  port=$((7000 + id))
  mkdir -p "${DEV_DIR}/data${id}"
  cat > "${DEV_DIR}/node${id}.toml" <<EOF
node_id = ${id}
listen_addr = "127.0.0.1:${port}"
data_dir = "${DEV_DIR}/data${id}"

[cluster]
peers = ${PEERS}
EOF
done

for id in 1 2 3; do
  ./target/release/raftkvd --config "${DEV_DIR}/node${id}.toml" > "${DEV_DIR}/node${id}.log" 2>&1 &
  pid=$!
  PIDS+=("${pid}")
  echo "started node ${id} on 127.0.0.1:$((7000 + id)) (pid=${pid})"
done

echo ""
echo "Cluster running. Try:"
echo "  export RAFTKV_ENDPOINTS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
echo "  ./target/release/raftkvctl status"
echo "  ./target/release/raftkvctl put greeting hello"
echo "  ./target/release/raftkvctl get greeting"
echo "  ./target/release/raftkvctl health"
echo "Press Ctrl-C to stop all nodes."

wait
