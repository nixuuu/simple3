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
- `spawn_blocking` for blocking I/O in async context. Specifically: `std::fs::File`, `BufWriter`, `sync_all()`, `fsync` are blocking — wrap in `spawn_blocking`. Flag even pre-existing patterns when they appear in modified code paths.
- Every `#[allow(clippy::...)]` must have a comment with a real justification. Bare suppression without a comment is always a finding. Common offenders: `result_large_err`, `cast_possible_truncation`, `too_many_arguments`, `too_many_lines`.
- No unnecessary stdlib prelude re-imports (e.g., `use std::borrow::ToOwned` — already in prelude).

### Crash safety and data integrity

- Append-only writes to segment files. Metadata updates via redb ACID transactions.
- Delete/put/compaction must update `objects`, `seg_dead`, and `seg_compacting` tables atomically in a single redb write transaction.
- Compaction must set `seg_compacting` flag before starting, remove after atomic rename.
- Temp files (`.tmp_*`, `.mpu_*`) must be cleaned up on error paths (not just happy path).
- `content_md5` must be computed and stored for all upload paths (single-part and multipart).
- `content_crc32c` must be computed during upload streaming (alongside MD5) and passed to `put_object_streamed`. Segment format: `[data][crc32c_le_4bytes]`, `ObjectMeta.length` includes the 4-byte trailer. Use `data_length()` (not `length`) for user-visible sizes and range checks. Never subtract trailer size manually (`meta.length - 4`) — always use `data_length()` which uses `saturating_sub` and guards against underflow on corrupted metadata.
- `read_object()` validates CRC on read for objects with `content_crc32c`. Range reads and gRPC streaming use raw `read_data()` — CRC validation deferred to background scrub. Validate `length >= 4` before accessing CRC trailer to avoid underflow.
- `verify_integrity()` must validate the on-disk 4-byte CRC trailer against metadata, not just recompute CRC from data and compare to `content_crc32c`. A bit flip in the trailer alone must be detected.
- After any data write, verify the redb transaction commits before returning success to the caller.
- All write paths must have consistent rollback on partial failure. If `copy_large` succeeds but CRC write or `sync_all` fails, segment has orphaned data and `w.size` is stale. Both single-part and multipart paths need rollback, not just multipart.

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
- Fail-fast vs best-effort depends on context:
  - **Startup / config**: fail fast on explicit user errors. If user passes `--config /bad/path` and it fails, abort — do not silently fall back to defaults. Only implicit default-path `NotFound` should fall back.
  - **Shutdown**: best-effort. Attempt all operations (sync all buckets), collect all errors, report all failures. One bad bucket must not prevent the rest from being fsynced.
  - Success logs (e.g., "shutdown complete") must not appear when the operation failed. Exit code must reflect actual outcome.

### Auth and security

- S3 HTTP auth uses `S3Auth` + `S3Access` traits from s3s. gRPC uses `x-access-key`/`x-secret-key` metadata.
- Admin endpoints require `Authorization: Bearer {ak}:{sk}`.
- Every new RPC or HTTP endpoint must have an auth check — flag any endpoint without one.
- IAM policy evaluation: explicit deny > explicit allow > implicit deny. Verify this order in any policy changes.
- No path traversal — bucket names and object keys must not escape the data directory. Check for `..` in paths.
- No secret material (keys, passwords) logged or included in error responses.

### gRPC specifics

- Downloads stream 256 KB chunks via mpsc channels — never load full object into memory.
- Uploads stream to temp files with MD5 + CRC32C hashers, then call `put_object_streamed` with `Some(crc)`.
- Proto-generated code has justified `#[allow(clippy::...)]` in `grpc.rs` — new suppressions elsewhere need justification.
- Server spawn functions must not return `Ok(())` while bind is still in progress — verify the server is actually listening before returning success.

### Performance

- No allocations in hot loops (object reads, streaming, list operations).
- Large copies use `copy_large` (8 MB buffer) not `std::io::copy`.
- Segment rotation at `DEFAULT_MAX_SEGMENT_SIZE` (4 GB) — check new constants are reasonable.
- `par_iter` only for CPU-bound work on large collections (>1000 items).

### Shutdown and lifecycle

- Background tasks (`tokio::spawn` for autovacuum, scrub, gRPC server) must be tracked via `JoinHandle` or `JoinSet` and awaited during shutdown. Fire-and-forget spawns are not acceptable for tasks that hold storage locks.
- Graceful shutdown must have a bounded timeout. If connections or RPC streams don't terminate, process must not hang indefinitely.
- Second SIGTERM/SIGINT should force-exit (or at minimum log "send signal again to force exit"). Silent ignore of repeated signals is a UX problem for operators.
- `sync_all()` and other blocking I/O during shutdown must use `spawn_blocking` — same rule as normal async code, no exception for shutdown paths.
- Shutdown must not race with in-progress `spawn_blocking` calls that hold storage locks.

### Config and CLI

- CLI args that can also be set via config file must use `Option<T>`, not `#[arg(default_value_t = ...)]`. Clap's default always populates the value, making it impossible to distinguish "user passed it" from "default" — config file values never take effect.
- Explicit user-provided paths (e.g., `--config /path`) that fail to load must abort with an error. Only implicit default-path `NotFound` should fall back to defaults. Silent fallback on explicit errors hides misconfigurations.

### Serialization and backward compatibility

- `from_bytes()` fallback chains (try V3, then V2, then V1 by deserialization failure) are fragile. bincode 1.x can succeed on truncated data at field boundaries, silently discarding new fields like `content_crc32c`. Guard against silent field loss with explicit format version discriminators or length checks.
- New fields added to serialized structs (`ObjectMeta`, etc.) need a documented migration strategy. Check that old data without the new field deserializes correctly and that the absence is handled (e.g., `None` for optional CRC).

### Documentation accuracy

- When a PR modifies or adds docs, verify every claim against actual code behavior. Conditional behaviors (e.g., CRC checks only run when metadata is present) must not be described as universal.
- Check path examples in docs for nesting and copy-paste errors (e.g., backup creates `backup/data_dir/` but restore command `cp -a backup/ data_dir/` produces `data_dir/data_dir/`).
- Command examples with dynamic values (dates, IDs) must be self-consistent within the same doc section.

### Test coverage — flag if missing

- New storage operations need tests in `tests/storage_test.rs`.
- New auth/policy logic needs unit tests in `src/auth/policy.rs`.
- Tests that inject artifacts after clean shutdown (e.g., creating orphan `.tmp_*` files then calling recovery) don't prove crash recovery. True crash-recovery coverage requires in-flight interruption: kill a child process during write/compaction/multipart and verify recovery.
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
