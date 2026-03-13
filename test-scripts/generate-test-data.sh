#!/usr/bin/env bash
set -euo pipefail

# Generate test data files for E2E tests.
# Skips files that already exist and have the correct size.
# Usage: generate-test-data.sh [test-data-dir]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST_DATA="${1:-$(dirname "$SCRIPT_DIR")/test-data}"

mkdir -p "$TEST_DATA/nested/deep"

# --- Text files ---

create_text() {
    local file="$1"
    shift
    if [ ! -f "$file" ]; then
        printf '%s' "$*" > "$file"
    fi
}

create_text "$TEST_DATA/empty.txt" ""

create_text "$TEST_DATA/small.txt" \
"Hello, this is a small test file for simple3 E2E testing.
It contains a few lines of plain text to verify basic upload and download.
Line three: the quick brown fox jumps over the lazy dog."

create_text "$TEST_DATA/unicode-名前.txt" \
'This file has a Unicode filename (名前 means "name" in Japanese).
日本語テスト: こんにちは世界
Emoji test: cafe with diacritics: cafe
Mixed: abc-абв-αβγ-一二三'

create_text "$TEST_DATA/special chars!.txt" \
"This filename has spaces and special characters.
S3 key encoding must handle this correctly."

create_text "$TEST_DATA/nested/sibling.txt" \
"Sibling file at nested/sibling.txt.
Tests prefix listing alongside deep/ subdirectory."

create_text "$TEST_DATA/nested/deep/file.txt" \
"This file is deeply nested at nested/deep/file.txt.
Used to test prefix/delimiter listing with depth."

# --- Binary files (random) ---

create_random() {
    local file="$1" size="$2"
    if [ -f "$file" ] && [ "$(wc -c < "$file" | tr -d ' ')" = "$size" ]; then
        return
    fi
    echo "  generating $(basename "$file") (${size} bytes)..."
    head -c "$size" /dev/urandom > "$file"
}

create_random "$TEST_DATA/medium.bin"       10240         # 10 KB
create_random "$TEST_DATA/large.bin"        1048576       # 1 MB
create_random "$TEST_DATA/fill-a.bin"       26214400      # 25 MB
create_random "$TEST_DATA/fill-b.bin"       26214400      # 25 MB
create_random "$TEST_DATA/fill-c.bin"       26214400      # 25 MB
create_random "$TEST_DATA/seg-overflow.bin" 62914560      # 60 MB
