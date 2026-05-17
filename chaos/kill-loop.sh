#!/usr/bin/env bash
# Random-kill chaos loop. Each iteration: start simple3, run a write workload,
# kill it at a random offset, restart, verify integrity. Default 100 iterations.
#
# Usage:
#   ./chaos/kill-loop.sh            # 100 iterations against /tmp/simple3-chaos
#   N=20 ./chaos/kill-loop.sh       # 20 iterations
#   DATA=/var/tmp/chaos ./chaos/kill-loop.sh

set -euo pipefail

N=${N:-100}
DATA=${DATA:-/tmp/simple3-chaos}
SIMPLE3=${SIMPLE3:-./target/release/simple3}
BUCKET=${BUCKET:-chaos}
LOG=${LOG:-/tmp/simple3-chaos.log}
PORT=${PORT:-18080}

if [[ ! -x "$SIMPLE3" ]]; then
  echo "error: $SIMPLE3 not built. Run: cargo build --release" >&2
  exit 1
fi

# Always start from a clean state so the bootstrap root key is regenerated.
case "$DATA" in
  "" | "/" | "$HOME" | "$HOME/")
    echo "refusing to remove unsafe DATA path: '$DATA'" >&2
    exit 1
    ;;
esac
if [[ -d "$DATA" ]]; then
  rm -r "$DATA"
fi
mkdir -p "$DATA"
: > "$LOG"

start_server() {
  "$SIMPLE3" --data-dir "$DATA" serve \
    --host 127.0.0.1 --port "$PORT" --grpc-port 0 \
    --rate-limit-rps 0 \
    --autovacuum-interval 5 --scrub-interval 0 --lifecycle-interval 0 \
    >> "$LOG" 2>&1 &
  echo $!
}

wait_ready() {
  local pid=$1
  for _ in $(seq 1 40); do
    if curl --fail-with-body -sS "http://127.0.0.1:$PORT/health" > /dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server $pid died before ready" >&2
      return 1
    fi
    sleep 0.1
  done
  return 1
}

write_workload() {
  local ak=$1
  local sk=$2
  for i in $(seq 1 200); do
    payload="obj-$i-$RANDOM"
    tmp=$(mktemp)
    printf '%s' "$payload" > "$tmp"
    "$SIMPLE3" --data-dir "$DATA" cp "$tmp" "s3://$BUCKET/key-$i" \
      --endpoint-url "http://127.0.0.1:$PORT" \
      --access-key "$ak" --secret-key "$sk" \
      > /dev/null 2>&1 || true
    rm -f "$tmp"
  done
}

# Bootstrap: start once, capture the root key, create the bucket, stop.
PID=$(start_server)
wait_ready "$PID" || { kill "$PID" 2>/dev/null || true; cat "$LOG"; exit 1; }
AK=$(awk '/Access Key ID/ {print $4}' "$LOG" | head -1)
SK=$(awk '/Secret Key/ {print $3}'    "$LOG" | head -1)
if [[ -z "$AK" || -z "$SK" ]]; then
  echo "failed to parse bootstrap credentials from $LOG" >&2
  kill "$PID" 2>/dev/null || true
  exit 1
fi
"$SIMPLE3" --data-dir "$DATA" mb "s3://$BUCKET" \
  --endpoint-url "http://127.0.0.1:$PORT" \
  --access-key "$AK" --secret-key "$SK" > /dev/null
kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
# Brief wait so the OS releases the bind even when TIME_WAIT is held.
sleep 0.5

failures=0
for iter in $(seq 1 "$N"); do
  PID=$(start_server)
  if ! wait_ready "$PID"; then
    echo "iter $iter: server failed to come up" >&2
    failures=$((failures + 1))
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
    continue
  fi

  write_workload "$AK" "$SK" &
  WORKLOAD_PID=$!

  # Kill simple3 at a random point in (50, 550) ms.
  sleep "$(awk 'BEGIN { srand(); printf "%.3f", 0.05 + rand()*0.5 }')"
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
    echo "  ... $iter/$N iterations complete ($failures failures)"
  fi
done

echo
echo "kill loop complete: $((N - failures))/$N iterations passed verify"
# Exit codes wrap at 256; collapse the failure count to a 0/1 signal for CI.
if (( failures > 0 )); then
  exit 1
fi
exit 0
