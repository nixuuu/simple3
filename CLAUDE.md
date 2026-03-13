# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

simple3 is an S3-compatible storage service in Rust. It uses append-only data files per bucket with sled embedded database for metadata indexing. Supports single and multipart uploads, crash-safe compaction, and autovacuum.

## Commands

```bash
cargo build                        # Debug build
cargo build --release              # Release build
cargo test                         # Run all tests
cargo test test_name               # Run single test
cargo lint                         # Clippy with nursery checks (custom alias)
cargo fix-lint                     # Auto-fix clippy issues (custom alias)
cargo bench                        # Run criterion benchmarks
cargo run -- serve                 # Start server (default 0.0.0.0:8080)
cargo run -- compact [bucket]      # Manual compaction
```

## Lint Configuration

Warnings are treated as errors (`-Dwarnings`). Enabled: `clippy::all`, `clippy::pedantic`, `rust-2024-compatibility`. Suppressed: `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`, `missing_panics_doc`. See `.cargo/config.toml` for details.

No `unwrap()` in library code — use `Result`/`Option` propagation. `unwrap()` is allowed only in tests and `main.rs`.

## Architecture

Three modules exported from `lib.rs`:

- **`storage.rs`** — Append-only storage engine. `Storage` manages buckets (lazy-loaded, `RwLock<HashMap>`). `BucketStore` owns a `data.bin` (append-only) + sled DB (objects tree for `ObjectMeta`, meta tree for dead_bytes/compaction flag). Supports multipart uploads via temporary `.mpu_*` files.
- **`s3impl.rs`** — Implements `s3s::S3` trait on `SimpleStorage(Arc<Storage>)`. Maps storage operations to S3 API (CreateBucket, PutObject, GetObject, ListObjectsV2, multipart, etc.). Streams request bodies to temp files, computes MD5 ETags.
- **`types.rs`** — `ObjectMeta` struct (offset, length, content_type, etag, last_modified, user_metadata). Serialized with bincode for sled storage.

### Per-bucket file layout

```
{bucket}/
├── index.db/       # sled database (objects + meta trees)
├── data.bin        # append-only object data
├── .tmp_*          # temp files during put (cleaned on startup)
├── .mpu_*          # multipart part files (cleaned on startup)
└── data.bin.tmp    # temp file during compaction
```

### Key patterns

- **Crash safety**: Append-only writes + sled ACID. Compaction sets `__compacting__` flag before atomic rename; recovery rebuilds if flag found. Orphan temp files cleaned on startup. `truncate_orphans()` trims data.bin to last known object.
- **Dead space tracking**: Overwrite/delete increments `dead_bytes` in meta tree. Autovacuum triggers compaction when `dead_bytes / file_size > threshold`.
- **Locking**: `RwLock` on buckets map and per-bucket data file. sled handles its own concurrency.
- **Error handling**: `anyhow` for application errors, `s3_error!` macro maps `io::Error` to S3 error codes.

## Code Style

Follow `CODE_STYTLE_RUST.md` (Polish). Key points: prefer iterators over indexed loops, minimize allocations on hot paths, keep functions under 50 lines, files under 800 lines. Use `spawn_blocking` for blocking I/O in async context.
