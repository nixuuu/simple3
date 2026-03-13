#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
ENDPOINT="${S3_ENDPOINT:-http://localhost:8080}"
ADMIN_URL="${ADMIN_URL:-http://localhost:8080}"
BUCKET="e2e-test"
BUCKET2="e2e-test2"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TEST_DATA="$PROJECT_DIR/test-data"
TMP="$(mktemp -d)"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

AWS="aws --endpoint-url $ENDPOINT"

# === Generate test data if missing ===
bash "$SCRIPT_DIR/generate-test-data.sh" "$TEST_DATA"

# === Counters ===
PASSED=0
FAILED=0

# === Helpers ===
cleanup() {
    rm -rf "$TMP"
    $AWS s3 rb "s3://$BUCKET" --force 2>/dev/null || true
    $AWS s3 rb "s3://$BUCKET2" --force 2>/dev/null || true
}
trap cleanup EXIT

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

# Compute sorted MD5 manifest of all files under a directory (relative paths)
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
        echo "  --- expected manifest ---"
        echo "$m1" | head -20
        echo "  --- actual manifest ---"
        echo "$m2" | head -20
    fi
}

object_count() {
    $AWS s3 ls "s3://$1/" --recursive 2>/dev/null | wc -l | tr -d ' '
}

admin_stats() {
    curl --fail-with-body -s "$ADMIN_URL/_/stats/$1"
}

admin_compact() {
    curl --fail-with-body -s -X POST "$ADMIN_URL/_/compact/$1"
}

admin_verify() {
    curl --fail-with-body -s "$ADMIN_URL/_/verify/$1"
}

verify_errors() {
    admin_verify "$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['errors']))"
}

verify_ok_count() {
    admin_verify "$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['verified_ok'])"
}

stats_dead_bytes() {
    admin_stats "$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_dead_bytes'])"
}

stats_total_size() {
    admin_stats "$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['total_size'])"
}

# Count local files for comparison
local_file_count() {
    find "$1" -type f | wc -l | tr -d ' '
}

# === Setup ===
echo "=== simple3 E2E Test Suite ==="
echo "endpoint: $ENDPOINT"
echo "test data: $TEST_DATA"
echo "temp dir:  $TMP"
echo ""

$AWS s3 mb "s3://$BUCKET" 2>/dev/null || true

# ============================================================
# T1: s3 sync — upload + checksum verification
# ============================================================
echo "--- T1: s3 sync upload + checksum verification ---"

$AWS s3 sync "$TEST_DATA/" "s3://$BUCKET/" --no-progress

mkdir -p "$TMP/t1-download"
$AWS s3 sync "s3://$BUCKET/" "$TMP/t1-download/" --no-progress

assert_manifests_eq "$TEST_DATA" "$TMP/t1-download" "T1: all files match after sync round-trip"

local_count=$(local_file_count "$TEST_DATA")
remote_count=$(object_count "$BUCKET")
assert_eq "$remote_count" "$local_count" "T1: object count matches local file count ($local_count)"

dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "T1: dead_bytes == 0 after fresh upload"

errors=$(verify_errors "$BUCKET")
assert_eq "$errors" "0" "T1: verify — 0 integrity errors after fresh upload"

# ============================================================
# T2: s3 sync idempotent — re-sync should not create dead bytes
# ============================================================
echo "--- T2: s3 sync idempotent ---"

$AWS s3 sync "$TEST_DATA/" "s3://$BUCKET/" --no-progress

dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "T2: dead_bytes == 0 after idempotent re-sync"

# ============================================================
# T3: s3 cp — single file upload + download + checksum
# ============================================================
echo "--- T3: s3 cp single file ---"

$AWS s3 cp "$TEST_DATA/large.bin" "s3://$BUCKET/copy-test/large.bin" --no-progress
$AWS s3 cp "s3://$BUCKET/copy-test/large.bin" "$TMP/t3-large.bin" --no-progress

assert_file_eq "$TEST_DATA/large.bin" "$TMP/t3-large.bin" "T3: large.bin checksum matches after cp round-trip"

# ============================================================
# T4: s3 rm — single object delete
# ============================================================
echo "--- T4: s3 rm single object ---"

$AWS s3 rm "s3://$BUCKET/copy-test/large.bin"

dead=$(stats_dead_bytes "$BUCKET")
assert_gt "$dead" "0" "T4: dead_bytes > 0 after deleting large.bin copy"

# Spot-check: original synced small.txt still works
$AWS s3 cp "s3://$BUCKET/small.txt" "$TMP/t4-small.txt" --no-progress
assert_file_eq "$TEST_DATA/small.txt" "$TMP/t4-small.txt" "T4: small.txt still intact after rm"

# ============================================================
# T5: s3 rm --recursive --exclude — selective delete
# ============================================================
echo "--- T5: s3 rm --recursive --exclude ---"

$AWS s3 rm "s3://$BUCKET/" --recursive --exclude "nested/*"

# nested/ objects should still exist
$AWS s3 cp "s3://$BUCKET/nested/sibling.txt" "$TMP/t5-sibling.txt" --no-progress
assert_file_eq "$TEST_DATA/nested/sibling.txt" "$TMP/t5-sibling.txt" "T5: nested/sibling.txt preserved by --exclude"

$AWS s3 cp "s3://$BUCKET/nested/deep/file.txt" "$TMP/t5-deep.txt" --no-progress
assert_file_eq "$TEST_DATA/nested/deep/file.txt" "$TMP/t5-deep.txt" "T5: nested/deep/file.txt preserved by --exclude"

# Non-nested objects should be gone
remaining=$(object_count "$BUCKET")
assert_eq "$remaining" "2" "T5: only 2 nested objects remain"

# ============================================================
# T6: s3 rm --recursive — full delete
# ============================================================
echo "--- T6: s3 rm --recursive (full) ---"

$AWS s3 rm "s3://$BUCKET/" --recursive

remaining=$(object_count "$BUCKET")
assert_eq "$remaining" "0" "T6: 0 objects remaining after full recursive delete"

dead=$(stats_dead_bytes "$BUCKET")
assert_gt "$dead" "0" "T6: dead_bytes > 0 after full delete"

# ============================================================
# T7: compact — verify segment cleanup
# ============================================================
echo "--- T7: compact after full delete ---"

admin_compact "$BUCKET" > /dev/null

total=$(stats_total_size "$BUCKET")
dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "T7: dead_bytes == 0 after compact"
assert_eq "$total" "0" "T7: total_size == 0 after compact (all data deleted)"

# ============================================================
# T8: re-sync after compact — verify clean slate
# ============================================================
echo "--- T8: re-sync after compact ---"

$AWS s3 sync "$TEST_DATA/" "s3://$BUCKET/" --no-progress

mkdir -p "$TMP/t8-resync"
$AWS s3 sync "s3://$BUCKET/" "$TMP/t8-resync/" --no-progress

assert_manifests_eq "$TEST_DATA" "$TMP/t8-resync" "T8: all files match after re-sync on compacted bucket"

errors=$(verify_errors "$BUCKET")
assert_eq "$errors" "0" "T8: verify — 0 integrity errors after re-sync on compacted bucket"

# ============================================================
# T9: s3 sync --exclude — selective copy
# ============================================================
echo "--- T9: s3 sync --exclude '*.bin' ---"

$AWS s3 mb "s3://$BUCKET2" 2>/dev/null || true
$AWS s3 sync "$TEST_DATA/" "s3://$BUCKET2/" --exclude "*.bin" --no-progress

mkdir -p "$TMP/t9-download"
$AWS s3 sync "s3://$BUCKET2/" "$TMP/t9-download/" --no-progress

# Verify .bin files were NOT uploaded
if [ -f "$TMP/t9-download/large.bin" ]; then
    fail "T9: large.bin should not exist with --exclude '*.bin'"
else
    pass "T9: large.bin excluded"
fi
if [ -f "$TMP/t9-download/medium.bin" ]; then
    fail "T9: medium.bin should not exist with --exclude '*.bin'"
else
    pass "T9: medium.bin excluded"
fi

# Verify non-.bin files DO match
assert_file_eq "$TEST_DATA/small.txt" "$TMP/t9-download/small.txt" "T9: small.txt checksum matches"
assert_file_eq "$TEST_DATA/nested/sibling.txt" "$TMP/t9-download/nested/sibling.txt" "T9: nested/sibling.txt checksum matches"

# ============================================================
# T10: overwrite + compact — dead bytes lifecycle
# ============================================================
echo "--- T10: overwrite + compact lifecycle ---"

$AWS s3 cp "$TEST_DATA/small.txt" "s3://$BUCKET/overwrite-test" --no-progress
$AWS s3 cp "$TEST_DATA/large.bin" "s3://$BUCKET/overwrite-test" --no-progress

dead=$(stats_dead_bytes "$BUCKET")
assert_gt "$dead" "0" "T10: dead_bytes > 0 after overwrite"

admin_compact "$BUCKET" > /dev/null

dead=$(stats_dead_bytes "$BUCKET")
assert_eq "$dead" "0" "T10: dead_bytes == 0 after compact"

$AWS s3 cp "s3://$BUCKET/overwrite-test" "$TMP/t10-overwrite.bin" --no-progress
assert_file_eq "$TEST_DATA/large.bin" "$TMP/t10-overwrite.bin" "T10: overwritten key has new content (large.bin)"

errors=$(verify_errors "$BUCKET")
assert_eq "$errors" "0" "T10: verify — 0 integrity errors after overwrite + compact"

# ============================================================
# Summary
# ============================================================
echo ""
echo "=== Results: $PASSED passed, $FAILED failed ==="

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
