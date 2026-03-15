# Code review instructions

You are reviewing a PR in **simple3** — an S3-compatible storage service written in Rust. Segmented append-only data files per bucket, redb for metadata, S3 HTTP + gRPC interfaces, IAM auth.

## How to review

1. Run `gh pr diff $PR_NUMBER` to get the full diff.
2. Read `CODE_STYTLE_RUST.md` at the repo root — it is the primary style/performance checklist for this project. Verify every changed line against it.
3. Read `CLAUDE.md` for architecture context, module layout, and project conventions.
4. For each file in the diff, read the full file with the Read tool to understand surrounding context before commenting.
5. Post findings as inline comments on the exact diff lines using `mcp__github_inline_comment__create_inline_comment`.
6. After all inline comments, post a short PR-level summary via `gh pr comment`.

## What to check

### Rust style (enforced by `CODE_STYTLE_RUST.md`)

- No unnecessary `.clone()`, `.to_owned()`, `.to_string()` — pass `&T` where possible.
- Iterator chains over indexed loops. No `.collect::<Vec<_>>()` just to iterate again.
- `&str` in function args, `String::with_capacity` for hot-path string building, no `format!` in loops.
- Functions under 50 lines, files under 800 lines (1000 for trait impls/serde/sqlx).
- No `unwrap()`/`expect()` in library code (`src/` except `main.rs` and tests). Return `Result`/`Option`.
- `spawn_blocking` for blocking I/O in async context.
- Every `#[allow(clippy::...)]` must have a comment with a real justification.

### Crash safety and data integrity

- Append-only writes to segment files. Metadata updates via redb ACID transactions.
- Delete/put/compaction must update `objects`, `seg_dead`, and `seg_compacting` tables atomically in a single redb write transaction.
- Compaction must set `seg_compacting` flag before starting, remove after atomic rename.
- Temp files (`.tmp_*`, `.mpu_*`) must be cleaned up on error paths (not just happy path).
- `content_md5` must be computed and stored for all upload paths (single-part and multipart).
- After any data write, verify the redb transaction commits before returning success to the caller.

### Locking and concurrency

- `RwLock` on buckets map — writers must not hold the lock across I/O operations.
- `Mutex` on active segment writer — check for deadlock risk if multiple locks are acquired.
- Lock ordering: always acquire buckets map lock before per-bucket locks.
- No `MutexGuard` held across `.await` — use `tokio::sync::Mutex` or drop guard before await.
- Segment `RwLock<File>` for concurrent reads — verify readers don't block writers unnecessarily.

### Error handling

- `anyhow` for application-level errors, `s3_error!` macro for S3 error mapping.
- gRPC uses `tonic::Status` — check that `map_io_err` is used consistently.
- No swallowed errors (silent `let _ = ...` on Results that matter).
- Error messages should include context (bucket name, key, segment id) for debuggability.

### Auth and security

- S3 HTTP auth uses `S3Auth` + `S3Access` traits from s3s. gRPC uses `x-access-key`/`x-secret-key` metadata.
- Admin endpoints require `Authorization: Bearer {ak}:{sk}`.
- Every new RPC or HTTP endpoint must have an auth check — flag any endpoint without one.
- IAM policy evaluation: explicit deny > explicit allow > implicit deny. Verify this order in any policy changes.
- No path traversal — bucket names and object keys must not escape the data directory. Check for `..` in paths.
- No secret material (keys, passwords) logged or included in error responses.

### gRPC specifics

- Downloads stream 256 KB chunks via mpsc channels — never load full object into memory.
- Uploads stream to temp files with MD5 hasher, then call `put_object_streamed`.
- Proto-generated code has justified `#[allow(clippy::...)]` in `grpc.rs` — new suppressions elsewhere need justification.

### Performance

- No allocations in hot loops (object reads, streaming, list operations).
- Large copies use `copy_large` (8 MB buffer) not `std::io::copy`.
- Segment rotation at `DEFAULT_MAX_SEGMENT_SIZE` (4 GB) — check new constants are reasonable.
- `par_iter` only for CPU-bound work on large collections (>1000 items).

### Test coverage — flag if missing

- New storage operations need tests in `tests/storage_test.rs`.
- New auth/policy logic needs unit tests in `src/auth/policy.rs`.
- Edge cases to watch for:
  - Empty bucket operations (list, delete, compact on empty bucket).
  - Concurrent put/delete on same key.
  - Multipart upload: abort after partial parts, complete with missing parts, duplicate part numbers.
  - Compaction during active reads/writes.
  - Objects at segment boundary (exactly at max segment size).
  - Unicode and special characters in bucket names and object keys.
  - Zero-length objects.
  - Very large metadata maps.
  - Auth: expired/disabled keys, detached policies, empty policy documents.

## Output format

- Use inline comments for specific issues (point to exact line, explain why, suggest fix).
- Classify severity: `bug`, `performance`, `style`, `safety`, `test-gap`, `nit`.
- At the end post one summary comment on the PR with a short verdict: approve, request changes, or comment-only. Include counts by severity.
- If the PR looks good, say so briefly. Don't invent issues.
