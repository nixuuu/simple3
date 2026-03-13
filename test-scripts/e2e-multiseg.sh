#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
PORT=9877
ENDPOINT="http://localhost:$PORT"
BUCKET="multiseg"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TEST_DATA="$PROJECT_DIR/test-data"
DATA_DIR="$(mktemp -d)"
TMP="$(mktemp -d)"
SERVER_PID=""

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

AWS="aws --endpoint-url $ENDPOINT"

# === Generate test data if missing ===
bash "$SCRIPT_DIR/generate-test-data.sh" "$TEST_DATA"

PASSED=0
FAILED=0

# === Server lifecycle ===
start_server() {
    cargo build --release --manifest-path "$PROJECT_DIR/Cargo.toml" --quiet
    RUST_LOG=warn "$PROJECT_DIR/target/release/simple3" \
        --data-dir "$DATA_DIR" \
        serve --port "$PORT" --autovacuum-interval 0 --max-segment-size-mb 50 &
    SERVER_PID=$!
    # Wait for server to be ready
    for i in $(seq 1 30); do
        if curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/_/stats/nonexistent" 2>/dev/null | grep -q '404'; then
            return 0
        fi
        sleep 0.2
    done
    echo "ERROR: server did not start within 6 seconds"
    exit 1
}

stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

cleanup() {
    stop_server
    rm -rf "$TMP"
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# === Helpers ===
pass() {
    PASSED=$((PASSED + 1))
    printf "\033[32m  PASS\033[0m %s\n" "$1"
}

fail() {
    FAILED=$((FAILED + 1))
    printf "\033[31m  FAIL\033[0m %s\n" "$1"
}

assert_eq() {
    local val1="$1" val2="$2" msg="$3"
    if [ "$val1" = "$val2" ]; then
        pass "$msg"
    else
        fail "$msg (expected '$val2', got '$val1')"
    fi
}

assert_gt() {
    local val1="$1" val2="$2" msg="$3"
    if [ "$val1" -gt "$val2" ] 2>/dev/null; then
        pass "$msg"
    else
        fail "$msg (expected $val1 > $val2)"
    fi
}

assert_ge() {
    local val1="$1" val2="$2" msg="$3"
    if [ "$val1" -ge "$val2" ] 2>/dev/null; then
        pass "$msg"
    else
        fail "$msg (expected $val1 >= $val2)"
    fi
}

assert_file_eq() {
    local file1="$1" file2="$2" msg="$3"
    local h1 h2
    h1=$(md5sum "$file1" | awk '{print $1}')
    h2=$(md5sum "$file2" | awk '{print $1}')
    if [ "$h1" = "$h2" ]; then
        pass "$msg"
    else
        fail "$msg (md5: $h1 vs $h2)"
    fi
}

checksum_manifest() {
    local dir="$1"
    (cd "$dir" && find . -type f -print0 | sort -z | while IFS= read -r -d '' f; do
        md5sum "$f"
    done)
}

assert_manifests_eq() {
    local dir1="$1" dir2="$2" msg="$3"
    local m1 m2
    m1=$(checksum_manifest "$dir1")
    m2=$(checksum_manifest "$dir2")
    if [ "$m1" = "$m2" ]; then
        pass "$msg"
    else
        fail "$msg"
        diff <(echo "$m1") <(echo "$m2") | head -20
    fi
}

admin_stats() {
    curl --fail-with-body -s "$ENDPOINT/_/stats/$1"
}

admin_compact() {
    curl --fail-with-body -s -X POST "$ENDPOINT/_/compact/$1"
}

admin_verify() {
    curl --fail-with-body -s "$ENDPOINT/_/verify/$1"
}

verify_errors() {
    admin_verify "$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['errors']))"
}

stats_dead_bytes() {
    admin_stats "$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_dead_bytes'])"
}

stats_total_size() {
    admin_stats "$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_size'])"
}

stats_segment_count() {
    admin_stats "$1" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['segments']))"
}

# === Setup ===
echo "=== simple3 Multi-Segment E2E Test Suite ==="
echo "segment size: 50 MB"
echo "data dir:     $DATA_DIR"
echo "temp dir:     $TMP"
echo ""

start_server
echo "server started on port $PORT (PID $SERVER_PID)"
echo ""

$AWS s3 mb "s3://$BUCKET" 2>/dev/null || true

# ============================================================
# M1: Upload fill-a + fill-b → single segment fills up
# ============================================================
echo "--- M1: fill segment 1 with fill-a + fill-b (25+25 MB) ---"

$AWS s3 cp "$TEST_DATA/fill-a.bin" "s3://$BUCKET/fill-a.bin" --no-progress
$AWS s3 cp "$TEST_DATA/fill-b.bin" "s3://$BUCKET/fill-b.bin" --no-progress

seg_count=$(stats_segment_count "$BUCKET")
assert_eq "$seg_count" "1" "M1: still 1 segment after 50 MB of data"

dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "M1: dead_bytes == 0"

total=$(stats_total_size "$BUCKET")
assert_gt "$total" "50000000" "M1: total_size > 50 MB"

# ============================================================
# M2: Upload fill-c → forces rotation to segment 2
# ============================================================
echo "--- M2: upload fill-c to trigger segment rotation ---"

$AWS s3 cp "$TEST_DATA/fill-c.bin" "s3://$BUCKET/fill-c.bin" --no-progress

seg_count=$(stats_segment_count "$BUCKET")
assert_eq "$seg_count" "2" "M2: 2 segments after rotation"

# Download all 3 and checksum verify
mkdir -p "$TMP/m2"
$AWS s3 cp "s3://$BUCKET/fill-a.bin" "$TMP/m2/fill-a.bin" --no-progress
$AWS s3 cp "s3://$BUCKET/fill-b.bin" "$TMP/m2/fill-b.bin" --no-progress
$AWS s3 cp "s3://$BUCKET/fill-c.bin" "$TMP/m2/fill-c.bin" --no-progress

assert_file_eq "$TEST_DATA/fill-a.bin" "$TMP/m2/fill-a.bin" "M2: fill-a.bin intact across segments"
assert_file_eq "$TEST_DATA/fill-b.bin" "$TMP/m2/fill-b.bin" "M2: fill-b.bin intact across segments"
assert_file_eq "$TEST_DATA/fill-c.bin" "$TMP/m2/fill-c.bin" "M2: fill-c.bin intact in segment 2"

errors=$(verify_errors "$BUCKET")
assert_eq "$errors" "0" "M2: verify — 0 integrity errors across segments"

# ============================================================
# M3: Upload seg-overflow.bin → file larger than segment (60 MB > 50 MB)
# ============================================================
echo "--- M3: upload 60 MB file (larger than segment) ---"

$AWS s3 cp "$TEST_DATA/seg-overflow.bin" "s3://$BUCKET/seg-overflow.bin" --no-progress

seg_count=$(stats_segment_count "$BUCKET")
assert_ge "$seg_count" "3" "M3: >= 3 segments after 60 MB overflow file"

mkdir -p "$TMP/m3"
$AWS s3 cp "s3://$BUCKET/seg-overflow.bin" "$TMP/m3/seg-overflow.bin" --no-progress
assert_file_eq "$TEST_DATA/seg-overflow.bin" "$TMP/m3/seg-overflow.bin" "M3: seg-overflow.bin intact (file > segment size)"

# Verify earlier files still readable
$AWS s3 cp "s3://$BUCKET/fill-a.bin" "$TMP/m3/fill-a.bin" --no-progress
assert_file_eq "$TEST_DATA/fill-a.bin" "$TMP/m3/fill-a.bin" "M3: fill-a.bin still intact after overflow upload"

# ============================================================
# M4: Delete fill-a + fill-b, compact → segment 1 cleaned
# ============================================================
echo "--- M4: delete fill-a + fill-b, compact, verify no corruption ---"

$AWS s3 rm "s3://$BUCKET/fill-a.bin"
$AWS s3 rm "s3://$BUCKET/fill-b.bin"

dead=$(stats_dead_bytes "$BUCKET")
assert_gt "$dead" "0" "M4: dead_bytes > 0 after deleting 50 MB"

admin_compact "$BUCKET" > /dev/null

dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "M4: dead_bytes == 0 after compact"

# Remaining files must be intact
mkdir -p "$TMP/m4"
$AWS s3 cp "s3://$BUCKET/fill-c.bin" "$TMP/m4/fill-c.bin" --no-progress
$AWS s3 cp "s3://$BUCKET/seg-overflow.bin" "$TMP/m4/seg-overflow.bin" --no-progress

assert_file_eq "$TEST_DATA/fill-c.bin" "$TMP/m4/fill-c.bin" "M4: fill-c.bin intact after compacting other segment"
assert_file_eq "$TEST_DATA/seg-overflow.bin" "$TMP/m4/seg-overflow.bin" "M4: seg-overflow.bin intact after compact"

errors=$(verify_errors "$BUCKET")
assert_eq "$errors" "0" "M4: verify — 0 integrity errors after delete + compact"

# ============================================================
# M5: Overwrite fill-c with seg-overflow content, compact
# ============================================================
echo "--- M5: overwrite fill-c with large content, compact ---"

$AWS s3 cp "$TEST_DATA/seg-overflow.bin" "s3://$BUCKET/fill-c.bin" --no-progress

dead=$(stats_dead_bytes "$BUCKET")
assert_gt "$dead" "0" "M5: dead_bytes > 0 after overwrite"

admin_compact "$BUCKET" > /dev/null

dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "M5: dead_bytes == 0 after compact"

mkdir -p "$TMP/m5"
$AWS s3 cp "s3://$BUCKET/fill-c.bin" "$TMP/m5/fill-c.bin" --no-progress
assert_file_eq "$TEST_DATA/seg-overflow.bin" "$TMP/m5/fill-c.bin" "M5: overwritten fill-c has seg-overflow content"

# seg-overflow.bin still intact
$AWS s3 cp "s3://$BUCKET/seg-overflow.bin" "$TMP/m5/seg-overflow.bin" --no-progress
assert_file_eq "$TEST_DATA/seg-overflow.bin" "$TMP/m5/seg-overflow.bin" "M5: seg-overflow.bin still intact"

# ============================================================
# M6: Upload small test-data/ spanning multiple segments
# ============================================================
echo "--- M6: sync small test files, verify manifest ---"

# Use only the small files (not the 25/60 MB ones) under a prefix
mkdir -p "$TMP/m6-source"
cp "$TEST_DATA/small.txt" "$TMP/m6-source/"
cp "$TEST_DATA/empty.txt" "$TMP/m6-source/"
cp "$TEST_DATA/medium.bin" "$TMP/m6-source/"
cp "$TEST_DATA/large.bin" "$TMP/m6-source/"
cp "$TEST_DATA/unicode-名前.txt" "$TMP/m6-source/"
cp "$TEST_DATA/special chars!.txt" "$TMP/m6-source/"
mkdir -p "$TMP/m6-source/nested/deep"
cp "$TEST_DATA/nested/sibling.txt" "$TMP/m6-source/nested/"
cp "$TEST_DATA/nested/deep/file.txt" "$TMP/m6-source/nested/deep/"

$AWS s3 sync "$TMP/m6-source/" "s3://$BUCKET/small/" --no-progress

mkdir -p "$TMP/m6-download"
$AWS s3 sync "s3://$BUCKET/small/" "$TMP/m6-download/" --no-progress

assert_manifests_eq "$TMP/m6-source" "$TMP/m6-download" "M6: all small files match after sync across multi-segment bucket"

# ============================================================
# M7: Full delete + compact → clean slate, re-upload works
# ============================================================
echo "--- M7: full delete + compact → clean slate ---"

$AWS s3 rm "s3://$BUCKET/" --recursive --quiet

dead=$(stats_dead_bytes "$BUCKET")
assert_gt "$dead" "0" "M7: dead_bytes > 0 after full delete"

admin_compact "$BUCKET" > /dev/null

total=$(stats_total_size "$BUCKET")
dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "M7: dead_bytes == 0 after compact"
assert_eq "$total" "0" "M7: total_size == 0 after full wipe + compact"

# Re-upload and verify storage works after full wipe
$AWS s3 cp "$TEST_DATA/fill-a.bin" "s3://$BUCKET/fill-a.bin" --no-progress
mkdir -p "$TMP/m7"
$AWS s3 cp "s3://$BUCKET/fill-a.bin" "$TMP/m7/fill-a.bin" --no-progress
assert_file_eq "$TEST_DATA/fill-a.bin" "$TMP/m7/fill-a.bin" "M7: fill-a.bin works after full wipe + compact"

errors=$(verify_errors "$BUCKET")
assert_eq "$errors" "0" "M7: verify — 0 integrity errors after full wipe + re-upload"

# ============================================================
# Summary
# ============================================================
echo ""
echo "=== Results: $PASSED passed, $FAILED failed ==="

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
