# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

simple3 is an S3-compatible storage service in Rust. It uses segmented append-only data files per bucket with redb embedded database for metadata indexing. Supports single and multipart uploads, crash-safe compaction, autovacuum, and data integrity verification. Exposes two server interfaces: S3-compatible HTTP and gRPC (tonic). Includes an AWS-CLI-compatible client (`mb`, `rb`, `ls`, `cp`, `sync`) that works over both HTTP and gRPC transports. Existing sled databases are auto-migrated to redb on first startup.

## Commands

```bash
cargo build                        # Debug build
cargo build --release              # Release build
cargo test                         # Run all tests
cargo test test_name               # Run single test
cargo lint                         # Clippy with nursery checks (custom alias)
cargo fix-lint                     # Auto-fix clippy issues (custom alias)
cargo bench                        # Run criterion benchmarks
cargo run -- serve                 # Start server (S3 on :8080, gRPC on :50051)
cargo run -- serve --grpc-port 0   # Start with gRPC disabled
cargo run -- compact [bucket]      # Manual compaction
cargo run -- verify [bucket]       # Verify data integrity
cargo run -- mb s3://bucket        # Create bucket (via HTTP)
cargo run -- rb s3://bucket        # Remove bucket
cargo run -- ls [s3://bucket]      # List buckets/objects
cargo run -- cp src dest           # Copy files to/from S3
cargo run -- sync src dest         # Sync local <-> S3
cargo run -- ls s3://b --grpc      # Any client cmd via gRPC
```

Client commands support `--grpc` flag and AWS env vars (`AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

Build requires `protoc` (protobuf compiler) for gRPC proto compilation: `brew install protobuf`.

## Lint Configuration

Warnings are treated as errors (`-Dwarnings`). Enabled: `clippy::all`, `clippy::pedantic`, `rust-2024-compatibility`. Suppressed: `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`, `missing_panics_doc`. See `.cargo/config.toml` for details.

No `unwrap()` in library code — use `Result`/`Option` propagation. `unwrap()` is allowed only in tests and `main.rs`.

## Architecture

### Binary crate (`main.rs` + `cli/`)

CLI is split into modules under `src/cli/`:
- **`cli/mod.rs`** — `Cli` struct, `Command` enum (Serve, Compact, Verify, Mb, Rb, Ls, Cp, Sync), async `run()` dispatch.
- **`cli/serve.rs`** — Server startup, autovacuum, `AdminService` (HTTP admin endpoints under `/_/`).
- **`cli/compact.rs`** — Compact command (direct storage access).
- **`cli/verify.rs`** — Verify command (direct storage access).
- **`cli/client/`** — Client commands that connect to a running server:
  - `mod.rs` — `ClientArgs` (clap flatten with `--grpc`, `--endpoint-url`, `--access-key`, `--secret-key`), `S3Uri` parser, `Transport` trait, `list_all_objects` helper.
  - `http.rs` — `HttpTransport` using `aws-sdk-s3` (path-style, custom endpoint, SigV4).
  - `grpc.rs` — `GrpcTransport` using generated tonic client stubs.
  - `mb.rs`, `rb.rs`, `ls.rs`, `cp.rs`, `sync_cmd.rs` — Individual command implementations.
  - `progress.rs` — `indicatif` progress bar helpers.

### Library crate (`lib.rs`)

Four modules exported from `lib.rs`:

- **`storage/`** — Segmented append-only storage engine. `Storage` manages buckets (lazy-loaded, `RwLock<HashMap>`). `BucketStore` owns segmented data files + redb DB (three typed tables: `objects` for `ObjectMeta`, `seg_dead` for per-segment dead bytes, `seg_compacting` for compaction flags). Cross-table atomic transactions for delete/put/compaction. Supports multipart uploads via temporary `.mpu_*` files. Auto-migrates from sled (v1/v2) to redb on first open. Sub-modules: `compaction.rs`, `multipart.rs`, `verify.rs`, `migration.rs`.
- **`s3impl.rs`** — Implements `s3s::S3` trait on `SimpleStorage(Arc<Storage>)`. Maps storage operations to S3 API (CreateBucket, PutObject, GetObject, ListObjectsV2, multipart, etc.). Streams request bodies to temp files, computes MD5 ETags.
- **`grpc.rs`** — gRPC server (tonic 0.14). Implements 14 RPCs: object ops (PutObject streaming upload, GetObject streaming download, HeadObject, DeleteObject, DeleteObjects, ListObjects), bulk ops (BulkGet, BulkPut bidirectional streaming), bucket management (Create/Delete/ListBuckets), admin (Stats, Compact, Verify). Proto definition in `proto/simple3.proto`, compiled via `build.rs` using `tonic-prost-build`.
- **`types.rs`** — `ObjectMeta` struct (segment_id, offset, length, content_type, etag, last_modified, user_metadata, content_md5). Serialized with bincode for redb storage. Backward-compatible deserialization via `ObjectMeta::from_bytes()` (handles old layout without content_md5).

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
- **Error handling**: `anyhow` for application errors, `s3_error!` macro maps `io::Error` to S3 error codes. gRPC uses `tonic::Status` with `map_io_err` helper.
- **Data integrity**: `content_md5` field in `ObjectMeta` stores whole-object MD5 for all uploads (single-part and multipart). `verify_integrity()` reads every object from segments and validates against stored hash. CLI: `simple3 verify`, HTTP: `GET /_/verify/{bucket}`, gRPC: `Verify` RPC.
- **gRPC streaming**: Downloads read 256 KB chunks via `read_data()` through mpsc channels (never loads full object into memory). Uploads stream to temp files with MD5 hasher, then call `put_object_streamed`. Generated proto code requires `#![allow(clippy::...)]` suppression for doc_markdown, derive_partial_eq_without_eq, default_trait_access, too_many_lines.

## Code Style

Follow `CODE_STYTLE_RUST.md`. Key points: prefer iterators over indexed loops, minimize allocations on hot paths, keep functions under 50 lines, files under 800 lines. Use `spawn_blocking` for blocking I/O in async context.
