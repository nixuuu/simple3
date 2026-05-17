#!/usr/bin/env bash
# Compare simple3 and MinIO on a single host using MinIO's warp benchmark.
# Usage:
#   ./bench/run-warp.sh simple3   # run against the local simple3
#   ./bench/run-warp.sh minio     # run against the local MinIO
#
# Both servers must be reachable on the endpoints below; the script will not
# start them. Set $WARP if `warp` is not on PATH.

set -euo pipefail

WARP=${WARP:-warp}
TARGET=${1:?"target required: simple3 or minio"}
BUCKET=${BENCH_BUCKET:-warp-bench}
RESULTS=${RESULTS:-bench/results}
mkdir -p "$RESULTS"

case "$TARGET" in
  simple3)
    HOST=${SIMPLE3_HOST:-127.0.0.1:8080}
    AK=${SIMPLE3_AK:?"SIMPLE3_AK required"}
    SK=${SIMPLE3_SK:?"SIMPLE3_SK required"}
    ;;
  minio)
    HOST=${MINIO_HOST:-127.0.0.1:9000}
    AK=${MINIO_AK:-minioadmin}
    SK=${MINIO_SK:-minioadmin}
    ;;
  *) echo "unknown target: $TARGET" >&2; exit 1 ;;
esac

run_one() {
  local size="$1"
  local threads="$2"
  local duration="${3:-15s}"
  local out="$RESULTS/${TARGET}-mixed-${size}-t${threads}.csv"

  echo ">>> $TARGET / mixed / size=$size / threads=$threads / duration=$duration"
  "$WARP" mixed \
    --host "$HOST" --access-key "$AK" --secret-key "$SK" \
    --bucket "$BUCKET" \
    --obj.size="$size" \
    --concurrent="$threads" \
    --duration="$duration" \
    --benchdata="$out" \
    --no-color
}

# Object-size sweep at concurrency 1
for size in 1KB 64KB 1MB 10MB; do
  run_one "$size" 1
done

# Concurrency sweep at 1 MB
for threads in 8; do
  run_one 1MB "$threads"
done

echo
echo "Results written under $RESULTS/. Summarize with:"
echo "  warp analyze $RESULTS/${TARGET}-mixed-1MB-t8.csv"
