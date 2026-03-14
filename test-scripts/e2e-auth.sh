#!/usr/bin/env bash
set -euo pipefail

# === Configuration ===
PORT=9878
GRPC_PORT=50152
ENDPOINT="http://localhost:$PORT"
BUCKET="auth-test"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$(mktemp -d)"
TMP="$(mktemp -d)"
SERVER_PID=""
SERVER_LOG="$TMP/server.log"

SIMPLE3="$PROJECT_DIR/target/release/simple3"

# === Counters ===
PASSED=0
FAILED=0

# === Server lifecycle ===
start_server() {
    RUST_LOG=warn "$SIMPLE3" \
        --data-dir "$DATA_DIR" \
        serve --port "$PORT" --grpc-port "$GRPC_PORT" --autovacuum-interval 0 \
        2>>"$SERVER_LOG" &
    SERVER_PID=$!
    for _ in $(seq 1 30); do
        if curl -s -o /dev/null "$ENDPOINT/" 2>/dev/null; then
            return 0
        fi
        sleep 0.2
    done
    echo "ERROR: server did not start within 6 seconds"
    cat "$SERVER_LOG"
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
    rm -r "$TMP" 2>/dev/null || true
    rm -r "$DATA_DIR" 2>/dev/null || true
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

assert_contains() {
    local haystack="$1" needle="$2" msg="$3"
    if echo "$haystack" | grep -q "$needle"; then
        pass "$msg"
    else
        fail "$msg (expected to contain '$needle')"
    fi
}

assert_http_status() {
    local status="$1" expected="$2" msg="$3"
    if [ "$status" = "$expected" ]; then
        pass "$msg"
    else
        fail "$msg (expected HTTP $expected, got $status)"
    fi
}

# === Build + start server ===
echo "=== simple3 Auth E2E Test Suite ==="
echo "data dir: $DATA_DIR"
echo "temp dir: $TMP"
echo ""

cargo build --release --manifest-path "$PROJECT_DIR/Cargo.toml" --quiet
start_server

# Extract bootstrapped root admin credentials
ROOT_KEY_ID=$(grep "Access Key ID:" "$SERVER_LOG" | awk '{print $NF}')
ROOT_SECRET=$(grep "Secret Key:" "$SERVER_LOG" | awk '{print $NF}')

if [ -z "$ROOT_KEY_ID" ] || [ -z "$ROOT_SECRET" ]; then
    echo "ERROR: could not extract admin credentials from server log:"
    cat "$SERVER_LOG"
    exit 1
fi

echo "root key: $ROOT_KEY_ID"
echo ""

# Admin CLI args (appended after the subcommand)
ADMIN_ARGS="--endpoint-url $ENDPOINT --access-key $ROOT_KEY_ID --secret-key $ROOT_SECRET"

# AWS CLI with admin credentials
export AWS_ACCESS_KEY_ID="$ROOT_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$ROOT_SECRET"
export AWS_DEFAULT_REGION=us-east-1

# ============================================================
# A1: CLI key management (over HTTP)
# ============================================================
echo "--- A1: CLI key management ---"

$SIMPLE3 keys $ADMIN_ARGS list > "$TMP/keys1.txt"
root_line=$(grep "$ROOT_KEY_ID" "$TMP/keys1.txt")
assert_contains "$root_line" "yes" "A1: root key is admin"
assert_contains "$root_line" "root admin" "A1: root key description"

# Create a regular key
$SIMPLE3 keys $ADMIN_ARGS create --description "regular user" > "$TMP/created.txt"
REG_KEY_ID=$(grep "Access Key ID:" "$TMP/created.txt" | awk '{print $4}')
REG_SECRET=$(grep "Secret Key:" "$TMP/created.txt" | awk '{print $3}')

key_count=$($SIMPLE3 keys $ADMIN_ARGS list | tail -n +2 | wc -l | tr -d ' ')
assert_eq "$key_count" "2" "A1: 2 keys after create"

# Show key details
$SIMPLE3 keys $ADMIN_ARGS show "$REG_KEY_ID" > "$TMP/show.txt"
assert_contains "$(cat "$TMP/show.txt")" "regular user" "A1: show displays description"
assert_contains "$(cat "$TMP/show.txt")" "Admin:         no" "A1: regular key is not admin"

# Disable / enable
$SIMPLE3 keys $ADMIN_ARGS disable "$REG_KEY_ID"
$SIMPLE3 keys $ADMIN_ARGS show "$REG_KEY_ID" > "$TMP/show_dis.txt"
assert_contains "$(cat "$TMP/show_dis.txt")" "Enabled:       no" "A1: key disabled"

$SIMPLE3 keys $ADMIN_ARGS enable "$REG_KEY_ID"
$SIMPLE3 keys $ADMIN_ARGS show "$REG_KEY_ID" > "$TMP/show_en.txt"
assert_contains "$(cat "$TMP/show_en.txt")" "Enabled:       yes" "A1: key re-enabled"

# Cannot delete last admin key
if $SIMPLE3 keys $ADMIN_ARGS delete "$ROOT_KEY_ID" 2>/dev/null; then
    fail "A1: should not be able to delete last admin key"
else
    pass "A1: cannot delete last admin key"
fi

# ============================================================
# A2: CLI policy management (over HTTP)
# ============================================================
echo "--- A2: CLI policy management ---"

cat > "$TMP/readonly.json" <<'EOF'
{
    "Version": "2024-01-01",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket", "s3:ListAllMyBuckets"],
            "Resource": "arn:s3:::*"
        }
    ]
}
EOF

$SIMPLE3 policy $ADMIN_ARGS create readonly --document "$TMP/readonly.json"
policy_count=$($SIMPLE3 policy $ADMIN_ARGS list | tail -n +2 | wc -l | tr -d ' ')
assert_eq "$policy_count" "1" "A2: 1 policy after create"

$SIMPLE3 policy $ADMIN_ARGS show readonly > "$TMP/policy_show.txt"
assert_contains "$(cat "$TMP/policy_show.txt")" "s3:GetObject" "A2: policy show contains action"

$SIMPLE3 policy $ADMIN_ARGS attach readonly "$REG_KEY_ID"
$SIMPLE3 keys $ADMIN_ARGS show "$REG_KEY_ID" > "$TMP/show2.txt"
assert_contains "$(cat "$TMP/show2.txt")" "readonly" "A2: policy attached to key"

cat > "$TMP/fullaccess.json" <<'EOF'
{
    "Version": "2024-01-01",
    "Statement": [{ "Effect": "Allow", "Action": "*", "Resource": "arn:s3:::*" }]
}
EOF
$SIMPLE3 policy $ADMIN_ARGS create fullaccess --document "$TMP/fullaccess.json"

# ============================================================
# A3: S3 HTTP auth with admin key
# ============================================================
echo "--- A3: S3 HTTP auth with admin key ---"

aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null
pass "A3: admin key can create bucket"

echo "hello auth test" > "$TMP/testfile.txt"
aws --endpoint-url "$ENDPOINT" s3 cp "$TMP/testfile.txt" "s3://$BUCKET/testfile.txt" --no-progress
pass "A3: admin key can upload object"

aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/" > /dev/null
pass "A3: admin key can list objects"

# ============================================================
# A4: S3 HTTP auth with read-only key
# ============================================================
echo "--- A4: S3 HTTP auth with read-only key ---"

export AWS_ACCESS_KEY_ID="$REG_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$REG_SECRET"

aws --endpoint-url "$ENDPOINT" s3 ls > /dev/null 2>&1
pass "A4: readonly key can list buckets"

aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/" > /dev/null 2>&1
pass "A4: readonly key can list objects"

aws --endpoint-url "$ENDPOINT" s3 cp "s3://$BUCKET/testfile.txt" "$TMP/downloaded.txt" --no-progress 2>/dev/null
assert_eq "$(cat "$TMP/downloaded.txt")" "hello auth test" "A4: readonly key can download object"

if aws --endpoint-url "$ENDPOINT" s3 cp "$TMP/testfile.txt" "s3://$BUCKET/denied.txt" --no-progress 2>/dev/null; then
    fail "A4: readonly key should not be able to upload"
else
    pass "A4: readonly key denied upload"
fi

if aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET/testfile.txt" 2>/dev/null; then
    fail "A4: readonly key should not be able to delete"
else
    pass "A4: readonly key denied delete"
fi

if aws --endpoint-url "$ENDPOINT" s3 mb "s3://denied-bucket" 2>/dev/null; then
    fail "A4: readonly key should not be able to create bucket"
else
    pass "A4: readonly key denied create bucket"
fi

# ============================================================
# A5: Invalid credentials rejected
# ============================================================
echo "--- A5: Invalid credentials ---"

export AWS_ACCESS_KEY_ID="AKNONEXISTENT000000"
export AWS_SECRET_ACCESS_KEY="SKwrongwrongwrongwrongwrongwrongwrongwro"

if aws --endpoint-url "$ENDPOINT" s3 ls 2>/dev/null; then
    fail "A5: invalid credentials should be rejected"
else
    pass "A5: invalid credentials rejected"
fi

export AWS_ACCESS_KEY_ID="$ROOT_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$ROOT_SECRET"

# ============================================================
# A6: Admin HTTP endpoints (curl)
# ============================================================
echo "--- A6: Admin HTTP endpoints ---"

BEARER="Authorization: Bearer ${ROOT_KEY_ID}:${ROOT_SECRET}"

status=$(curl -s -o "$TMP/admin_keys.json" -w '%{http_code}' \
    -H "$BEARER" "$ENDPOINT/_/keys")
assert_http_status "$status" "200" "A6: GET /_/keys returns 200"
key_count=$(python3 -c "import sys,json; print(len(json.load(sys.stdin)['keys']))" < "$TMP/admin_keys.json")
assert_eq "$key_count" "2" "A6: admin API lists 2 keys"

status=$(curl -s -o "$TMP/admin_create.json" -w '%{http_code}' \
    -X POST -H "$BEARER" -H "Content-Type: application/json" \
    -d '{"description":"api-created"}' "$ENDPOINT/_/keys")
assert_http_status "$status" "201" "A6: POST /_/keys returns 201"
API_KEY_ID=$(python3 -c "import sys,json; print(json.load(sys.stdin)['access_key_id'])" < "$TMP/admin_create.json")
assert_contains "$API_KEY_ID" "AK" "A6: created key has AK prefix"

status=$(curl -s -o /dev/null -w '%{http_code}' \
    -X DELETE -H "$BEARER" "$ENDPOINT/_/keys/$API_KEY_ID")
assert_http_status "$status" "200" "A6: DELETE /_/keys/{id} returns 200"

status=$(curl -s -o "$TMP/admin_policies.json" -w '%{http_code}' \
    -H "$BEARER" "$ENDPOINT/_/policies")
assert_http_status "$status" "200" "A6: GET /_/policies returns 200"
pol_count=$(python3 -c "import sys,json; print(len(json.load(sys.stdin)['policies']))" < "$TMP/admin_policies.json")
assert_eq "$pol_count" "2" "A6: admin API lists 2 policies"

status=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/_/keys")
assert_http_status "$status" "401" "A6: /_/keys without auth returns 401"

REG_BEARER="Authorization: Bearer ${REG_KEY_ID}:${REG_SECRET}"
status=$(curl -s -o /dev/null -w '%{http_code}' -H "$REG_BEARER" "$ENDPOINT/_/keys")
assert_http_status "$status" "403" "A6: /_/keys with non-admin key returns 403"

# ============================================================
# A7: gRPC auth
# ============================================================
echo "--- A7: gRPC auth ---"

$SIMPLE3 ls --grpc --endpoint-url "http://localhost:$GRPC_PORT" \
    --access-key "$ROOT_KEY_ID" --secret-key "$ROOT_SECRET" > /dev/null 2>&1
pass "A7: admin key works via gRPC"

$SIMPLE3 ls --grpc --endpoint-url "http://localhost:$GRPC_PORT" \
    --access-key "$REG_KEY_ID" --secret-key "$REG_SECRET" > /dev/null 2>&1
pass "A7: readonly key can list via gRPC"

if $SIMPLE3 ls --grpc --endpoint-url "http://localhost:$GRPC_PORT" \
    --access-key "AKBAD00000000000000" --secret-key "SKBAD0000000000000000000000000000000000" 2>/dev/null; then
    fail "A7: bad credentials should be rejected via gRPC"
else
    pass "A7: bad credentials rejected via gRPC"
fi

# ============================================================
# A8: Policy swap (all via HTTP CLI, no server restart)
# ============================================================
echo "--- A8: Policy swap ---"

$SIMPLE3 policy $ADMIN_ARGS detach readonly "$REG_KEY_ID"
$SIMPLE3 policy $ADMIN_ARGS attach fullaccess "$REG_KEY_ID"

export AWS_ACCESS_KEY_ID="$REG_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$REG_SECRET"

aws --endpoint-url "$ENDPOINT" s3 cp "$TMP/testfile.txt" "s3://$BUCKET/fullaccess.txt" --no-progress 2>/dev/null
pass "A8: key with fullaccess policy can upload"

aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET/fullaccess.txt" 2>/dev/null
pass "A8: key with fullaccess policy can delete"

# ============================================================
# A9: Explicit deny overrides allow
# ============================================================
echo "--- A9: Explicit deny overrides allow ---"

export AWS_ACCESS_KEY_ID="$ROOT_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$ROOT_SECRET"

cat > "$TMP/deny-delete.json" <<'EOF'
{
    "Version": "2024-01-01",
    "Statement": [
        { "Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "arn:s3:::*" }
    ]
}
EOF

$SIMPLE3 policy $ADMIN_ARGS create deny-delete --document "$TMP/deny-delete.json"
$SIMPLE3 policy $ADMIN_ARGS attach deny-delete "$REG_KEY_ID"

# Upload a file with admin key
aws --endpoint-url "$ENDPOINT" s3 cp "$TMP/testfile.txt" "s3://$BUCKET/nodelete.txt" --no-progress 2>/dev/null

export AWS_ACCESS_KEY_ID="$REG_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$REG_SECRET"

aws --endpoint-url "$ENDPOINT" s3 cp "$TMP/testfile.txt" "s3://$BUCKET/canwrite.txt" --no-progress 2>/dev/null
pass "A9: fullaccess + deny-delete can still upload"

if aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET/nodelete.txt" 2>/dev/null; then
    fail "A9: explicit deny should block delete"
else
    pass "A9: explicit deny blocks delete"
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "=== Results: $PASSED passed, $FAILED failed ==="

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
