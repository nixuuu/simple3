# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

simple3 is an S3-compatible storage service in Rust. It uses segmented append-only data files per bucket with redb embedded database for metadata indexing. Supports single and multipart uploads, crash-safe compaction, and autovacuum. Existing sled databases are auto-migrated to redb on first startup.

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

- **`storage.rs`** — Segmented append-only storage engine. `Storage` manages buckets (lazy-loaded, `RwLock<HashMap>`). `BucketStore` owns segmented data files + redb DB (three typed tables: `objects` for `ObjectMeta`, `seg_dead` for per-segment dead bytes, `seg_compacting` for compaction flags). Cross-table atomic transactions for delete/put/compaction. Supports multipart uploads via temporary `.mpu_*` files. Auto-migrates from sled (v1/v2) to redb on first open.
- **`s3impl.rs`** — Implements `s3s::S3` trait on `SimpleStorage(Arc<Storage>)`. Maps storage operations to S3 API (CreateBucket, PutObject, GetObject, ListObjectsV2, multipart, etc.). Streams request bodies to temp files, computes MD5 ETags.
- **`types.rs`** — `ObjectMeta` struct (segment_id, offset, length, content_type, etag, last_modified, user_metadata). Serialized with bincode for redb storage.

### Per-bucket file layout

```
{bucket}/
├── index.redb          # redb database (objects + seg_dead + seg_compacting tables)
├── seg_000000.bin      # segmented append-only data files
├── seg_000001.bin
├── .tmp_*              # temp files during put (cleaned on startup)
├── .mpu_*              # multipart part files (cleaned on startup)
└── seg_NNNNNN.bin.tmp  # temp file during compaction
```

### Key patterns

- **Crash safety**: Append-only writes + redb ACID transactions. Compaction sets `seg_compacting` flag before atomic rename; recovery rebuilds if flag found. Orphan temp files cleaned on startup. `truncate_orphans()` trims active segment to last known object.
- **Dead space tracking**: Overwrite/delete atomically increments per-segment dead bytes in `seg_dead` table (same transaction as metadata update). Autovacuum triggers compaction when `dead_bytes / file_size > threshold`.
- **Locking**: `RwLock` on buckets map, `Mutex` on active writer, per-segment `RwLock<File>` for concurrent reads. redb handles its own write serialization.
- **Error handling**: `anyhow` for application errors, `s3_error!` macro maps `io::Error` to S3 error codes.

## Code Style

Follow `CODE_STYTLE_RUST.md`. Key points: prefer iterators over indexed loops, minimize allocations on hot paths, keep functions under 50 lines, files under 800 lines. Use `spawn_blocking` for blocking I/O in async context.
