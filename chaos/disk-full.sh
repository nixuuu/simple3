#!/usr/bin/env bash
# Disk-full chaos: run simple3 on a tmpfs with a tight size budget, force the
# server to hit ENOSPC, then free space and verify recovery.
#
# Linux-only — relies on tmpfs and `mount`. Run as root or via sudo.

set -euo pipefail

MOUNT=${MOUNT:-/mnt/simple3-chaos}
SIZE=${SIZE:-32M}
SIMPLE3=${SIMPLE3:-./target/release/simple3}
BUCKET=${BUCKET:-chaos}
LOG=${LOG:-/tmp/simple3-disk-full.log}
PORT=${PORT:-18080}
SERVER_PID=""

if [[ "$(uname)" != "Linux" ]]; then
  echo "disk-full chaos is Linux-only (requires tmpfs mount)" >&2
  exit 1
fi

mkdir -p "$MOUNT"
mountpoint -q "$MOUNT" || sudo mount -t tmpfs -o size="$SIZE" tmpfs "$MOUNT"
sudo chown "$USER" "$MOUNT"

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  sudo umount "$MOUNT" 2>/dev/null || true
}
trap cleanup EXIT

"$SIMPLE3" --data-dir "$MOUNT" serve \
  --host 127.0.0.1 --port "$PORT" --grpc-port 0 --rate-limit-rps 0 \
  --autovacuum-interval 0 --scrub-interval 0 --lifecycle-interval 0 \
  > "$LOG" 2>&1 &
SERVER_PID=$!

# Wait for the server to come up before parsing credentials.
for _ in $(seq 1 50); do
  if curl --fail-with-body -sS "http://127.0.0.1:$PORT/health" > /dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server died before ready" >&2
    cat "$LOG" >&2
    exit 1
  fi
  sleep 0.1
done

AK=$(awk '/Access Key ID/ {print $4}' "$LOG" | head -1)
SK=$(awk '/Secret Key/ {print $3}'    "$LOG" | head -1)
if [[ -z "$AK" || -z "$SK" ]]; then
  echo "failed to parse bootstrap credentials from $LOG" >&2
  exit 1
fi

# simple3 requires SigV4 — go through the simple3 client for S3 operations.
"$SIMPLE3" mb "s3://$BUCKET" \
  --endpoint-url "http://127.0.0.1:$PORT" \
  --access-key "$AK" --secret-key "$SK" > /dev/null

echo ">>> filling tmpfs until simple3 returns an error"
PAYLOAD=$(mktemp)
head -c 4M /dev/urandom > "$PAYLOAD"
i=0
ok=1
while (( ok == 1 )); do
  i=$((i + 1))
  if ! "$SIMPLE3" cp "$PAYLOAD" "s3://$BUCKET/k-$i" \
       --endpoint-url "http://127.0.0.1:$PORT" \
       --access-key "$AK" --secret-key "$SK" >/dev/null 2>&1; then
    ok=0
    break
  fi
  if (( i > 200 )); then
    echo "filled 200 objects without error; tmpfs may be larger than expected"
    rm -f "$PAYLOAD"
    exit 1
  fi
done
rm -f "$PAYLOAD"
echo "first ENOSPC-like response at object #$i"

# Server must still be alive — no crash on disk full.
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "FAIL: simple3 died on disk-full" >&2
  exit 1
fi

echo ">>> freeing space and confirming PUT recovers"
df -h "$MOUNT"
# Drop half the previously-written objects.
half=$(( i / 2 ))
for n in $(seq 1 "$half"); do
  "$SIMPLE3" rm "s3://$BUCKET/k-$n" \
    --endpoint-url "http://127.0.0.1:$PORT" \
    --access-key "$AK" --secret-key "$SK" > /dev/null 2>&1 || true
done

# Compact (the admin endpoint legitimately uses Bearer auth).
curl --fail-with-body -sS -X POST "http://127.0.0.1:$PORT/_/compact/$BUCKET" \
  -H "Authorization: Bearer $AK:$SK" > /dev/null

# A small PUT must succeed now.
echo "hello" > /tmp/disk-full-after.txt
"$SIMPLE3" cp /tmp/disk-full-after.txt "s3://$BUCKET/after-free" \
  --endpoint-url "http://127.0.0.1:$PORT" \
  --access-key "$AK" --secret-key "$SK" > /dev/null
rm -f /tmp/disk-full-after.txt
echo "OK: recovered after disk-full"
