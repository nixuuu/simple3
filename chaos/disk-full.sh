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

if [[ "$(uname)" != "Linux" ]]; then
  echo "disk-full chaos is Linux-only (requires tmpfs mount)" >&2
  exit 1
fi

mkdir -p "$MOUNT"
mountpoint -q "$MOUNT" || sudo mount -t tmpfs -o size="$SIZE" tmpfs "$MOUNT"
sudo chown "$USER" "$MOUNT"

cleanup() {
  pkill -f "simple3 .* $MOUNT" 2>/dev/null || true
  sleep 0.5
  sudo umount "$MOUNT" 2>/dev/null || true
}
trap cleanup EXIT

"$SIMPLE3" --data-dir "$MOUNT" serve --rate-limit-rps 0 \
  --autovacuum-interval 0 --scrub-interval 0 \
  > "$LOG" 2>&1 &
SERVER_PID=$!
sleep 0.5
AK=$(awk '/Access Key ID/ {print $4}' "$LOG" | head -1)
SK=$(awk '/Secret Key/ {print $3}'    "$LOG" | head -1)

curl --fail-with-body -sS -X PUT "http://127.0.0.1:8080/$BUCKET" -u "$AK:$SK" > /dev/null

echo ">>> filling tmpfs until simple3 returns 5xx"
PAYLOAD=$(head -c 4M /dev/urandom | base64)
i=0
last=200
while [[ "$last" -lt 500 ]]; do
  i=$((i + 1))
  last=$(curl --fail-with-body -sS -o /dev/null -w '%{http_code}' \
    -X PUT --data-binary "@-" \
    "http://127.0.0.1:8080/$BUCKET/k-$i" -u "$AK:$SK" <<<"$PAYLOAD" || echo 500)
  if (( i > 200 )); then
    echo "filled 200 objects without error; tmpfs may be larger than expected"
    break
  fi
done
echo "first ENOSPC-like response at object #$i (status=$last)"

# Server must still be alive — no crash on disk full.
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "FAIL: simple3 died on disk-full" >&2
  exit 1
fi

echo ">>> freeing space and confirming PUT recovers"
df -h "$MOUNT"
# Drop half the previously-written objects via the admin API.
for k in $(curl --fail-with-body -sS "http://127.0.0.1:8080/$BUCKET" -u "$AK:$SK" \
            | grep -oE '<Key>[^<]+</Key>' \
            | sed -E 's@</?Key>@@g' \
            | head -n "$((i / 2))"); do
  curl --fail-with-body -sS -X DELETE "http://127.0.0.1:8080/$BUCKET/$k" -u "$AK:$SK" > /dev/null
done

# Compact to actually reclaim segments.
curl --fail-with-body -sS -X POST "http://127.0.0.1:8080/_/compact/$BUCKET" \
  -H "Authorization: Bearer $AK:$SK" > /dev/null

# Now a small PUT must succeed.
echo "hello" | curl --fail-with-body -sS -X PUT --data-binary "@-" \
  "http://127.0.0.1:8080/$BUCKET/after-free" -u "$AK:$SK" > /dev/null
echo "OK: recovered after disk-full"
