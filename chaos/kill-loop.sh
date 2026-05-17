#!/usr/bin/env bash
# Random-kill chaos loop. Each iteration: start simple3, run a write workload,
# kill it at a random offset, restart, verify integrity. Default 100 iterations.
#
# Usage:
#   ./chaos/kill-loop.sh            # 100 iterations against /tmp/chaos-data
#   N=20 ./chaos/kill-loop.sh       # 20 iterations
#   DATA=/var/tmp/chaos ./chaos/kill-loop.sh

set -euo pipefail

N=${N:-100}
DATA=${DATA:-/tmp/simple3-chaos}
SIMPLE3=${SIMPLE3:-./target/release/simple3}
BUCKET=${BUCKET:-chaos}
LOG=${LOG:-/tmp/simple3-chaos.log}

if [[ ! -x "$SIMPLE3" ]]; then
  echo "error: $SIMPLE3 not built. Run: cargo build --release" >&2
  exit 1
fi

rm -rf "$DATA"
mkdir -p "$DATA"

start_server() {
  "$SIMPLE3" --data-dir "$DATA" serve --rate-limit-rps 0 \
    --autovacuum-interval 1 --scrub-interval 0 \
    >> "$LOG" 2>&1 &
  echo $!
}

wait_ready() {
  local pid=$1
  for _ in $(seq 1 30); do
    if curl --fail-with-body -fsS http://127.0.0.1:8080/health > /dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server died before ready" >&2
      return 1
    fi
    sleep 0.1
  done
  return 1
}

write_workload() {
  for i in $(seq 1 200); do
    curl --fail-with-body -sS -X PUT --data-binary "obj-$i-$RANDOM" \
      "http://127.0.0.1:8080/$BUCKET/key-$i" \
      -u "$AK:$SK" > /dev/null 2>&1 || true
  done
}

# Bootstrap: start once to get the root key.
PID=$(start_server)
wait_ready "$PID" || { kill "$PID"; exit 1; }
AK=$(awk '/Access Key ID/ {print $4}' "$LOG" | head -1)
SK=$(awk '/Secret Key/ {print $3}'    "$LOG" | head -1)
# Pre-create the bucket once
curl --fail-with-body -sS -X PUT "http://127.0.0.1:8080/$BUCKET" -u "$AK:$SK" > /dev/null
kill "$PID"
wait "$PID" 2>/dev/null || true

failures=0
for iter in $(seq 1 "$N"); do
  PID=$(start_server)
  if ! wait_ready "$PID"; then
    echo "iter $iter: server failed to come up" >&2
    failures=$((failures + 1))
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
    continue
  fi

  write_workload &
  WORKLOAD_PID=$!

  # Kill simple3 at a random point in (0, 500ms].
  sleep "$(awk 'BEGIN { srand(); printf "%.3f", rand()*0.5 }')"
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  kill "$WORKLOAD_PID" 2>/dev/null || true
  wait "$WORKLOAD_PID" 2>/dev/null || true

  # Recovery + integrity check.
  if ! "$SIMPLE3" --data-dir "$DATA" verify "$BUCKET" > /dev/null 2>&1; then
    echo "iter $iter: verify FAILED" >&2
    failures=$((failures + 1))
  fi

  if (( iter % 10 == 0 )); then
    echo "  ... $iter iterations complete ($failures failures)"
  fi
done

echo
echo "kill loop complete: $((N - failures))/$N iterations passed verify"
exit $failures
