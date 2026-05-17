#!/usr/bin/env bash
# Stand up identical local instances of simple3 and MinIO for benchmarking.
# Each gets a fresh data directory under /tmp.
#
# Requirements:
#   - simple3 binary built (`cargo build --release`)
#   - minio binary on PATH
#   - mc (MinIO client) on PATH for bucket creation
#
# Outputs an .env file with credentials and ports the run scripts pick up.

set -euo pipefail

ROOT=${ROOT:-/tmp/simple3-bench}
mkdir -p "$ROOT"

SIMPLE3_BIN=${SIMPLE3_BIN:-./target/release/simple3}
SIMPLE3_DATA="$ROOT/simple3-data"
MINIO_DATA="$ROOT/minio-data"
mkdir -p "$SIMPLE3_DATA" "$MINIO_DATA"

# --- simple3 ---
echo ">>> starting simple3 on :8080"
"$SIMPLE3_BIN" --data-dir "$SIMPLE3_DATA" serve --rate-limit-rps 0 > "$ROOT/simple3.log" 2>&1 &
SIMPLE3_PID=$!
sleep 1
SIMPLE3_AK=$(awk '/Access Key ID/ {print $4}' "$ROOT/simple3.log" | head -1)
SIMPLE3_SK=$(awk '/Secret Key/ {print $3}'    "$ROOT/simple3.log" | head -1)

# --- MinIO ---
echo ">>> starting MinIO on :9000"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  minio server "$MINIO_DATA" --address :9000 --console-address :9001 \
  > "$ROOT/minio.log" 2>&1 &
MINIO_PID=$!
sleep 2

# --- bucket on each ---
mc alias set bench-simple3 http://127.0.0.1:8080 "$SIMPLE3_AK" "$SIMPLE3_SK" --api S3v4
mc alias set bench-minio   http://127.0.0.1:9000 minioadmin minioadmin
mc mb -p bench-simple3/warp-bench || true
mc mb -p bench-minio/warp-bench   || true

cat > "$ROOT/env.sh" <<EOF
export SIMPLE3_PID=$SIMPLE3_PID
export MINIO_PID=$MINIO_PID
export SIMPLE3_HOST=127.0.0.1:8080
export MINIO_HOST=127.0.0.1:9000
export SIMPLE3_AK=$SIMPLE3_AK
export SIMPLE3_SK=$SIMPLE3_SK
export MINIO_AK=minioadmin
export MINIO_SK=minioadmin
EOF

echo
echo "PIDs: simple3=$SIMPLE3_PID  minio=$MINIO_PID"
echo "Source $ROOT/env.sh, then run bench/run-warp.sh simple3 / minio."
echo "Tear down with:  kill \$SIMPLE3_PID \$MINIO_PID"
