# simple3

S3-compatible storage server in Rust. Single binary, embedded database, no external dependencies.

Uses segmented append-only data files with [redb](https://github.com/cberner/redb) for metadata indexing. Exposes an S3-compatible HTTP API and a gRPC API. Ships with a CLI client that accepts AWS environment variables.

## Features

- S3-compatible HTTP API, works with AWS CLI and any S3 client
- gRPC API with 14 RPCs, including bidirectional streaming for bulk transfers
- Segmented append-only storage with ACID metadata transactions
- Background autovacuum that compacts segments when dead space exceeds a threshold
- MD5 checksums per object, full-bucket integrity verification via CLI, HTTP, or gRPC
- S3-compatible multipart uploads
- Built-in CLI: `mb`, `rb`, `ls`, `cp`, `rm`, `sync` with concurrent transfers
- Client commands work over HTTP or gRPC (`--grpc` flag)
- Range GET via HTTP Range headers
- Auto-migration from sled to redb on first startup

## Quick start

### Build

```bash
# Prerequisites: Rust toolchain, protoc (protobuf compiler)
# macOS: brew install protobuf
# Ubuntu: apt install protobuf-compiler

cargo build --release
```

### Start the server

```bash
# S3 HTTP on :8080, gRPC on :50051
./target/release/simple3 serve

# Custom ports
./target/release/simple3 serve --port 9000 --grpc-port 50051

# Disable gRPC
./target/release/simple3 serve --grpc-port 0

# Custom data directory
./target/release/simple3 --data-dir /mnt/storage serve
```

### Use with AWS CLI

```bash
export AWS_ENDPOINT_URL=http://localhost:8080
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws s3 mb s3://my-bucket
aws s3 cp file.txt s3://my-bucket/
aws s3 ls s3://my-bucket
aws s3 sync ./local-dir s3://my-bucket/backup/
```

### Use with built-in CLI

```bash
simple3 mb s3://my-bucket
simple3 rb s3://my-bucket

simple3 ls
simple3 ls s3://my-bucket
simple3 ls s3://my-bucket/prefix/ --recursive

simple3 cp file.txt s3://my-bucket/
simple3 cp s3://my-bucket/file.txt ./local/
simple3 cp -r ./dir s3://my-bucket/dir/ -j 10

simple3 sync ./local s3://my-bucket/backup/ -j 10
simple3 sync s3://my-bucket/backup/ ./local --delete
simple3 sync s3://src-bucket/ s3://dst-bucket/ --dryrun

simple3 rm s3://my-bucket/file.txt
simple3 rm s3://my-bucket/prefix/ --recursive

# gRPC transport instead of HTTP
simple3 ls s3://my-bucket --grpc
simple3 sync ./local s3://my-bucket/ --grpc -j 10
```

## Server configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `./data` | Storage directory |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8080` | S3 HTTP port |
| `--grpc-port` | `50051` | gRPC port, 0 to disable |
| `--autovacuum-interval` | `300` | Autovacuum interval in seconds, 0 to disable |
| `--autovacuum-threshold` | `0.5` | Compact when dead bytes exceed this fraction of file size |
| `--max-segment-size-mb` | `4096` | Max segment file size in MB |

## Client configuration

The CLI client reads standard AWS environment variables:

| Flag | Env variable | Default |
|------|-------------|---------|
| `--endpoint-url` | `AWS_ENDPOINT_URL` | `http://localhost:8080` |
| `--access-key` | `AWS_ACCESS_KEY_ID` | `test` |
| `--secret-key` | `AWS_SECRET_ACCESS_KEY` | `test` |
| `--region` | `AWS_DEFAULT_REGION` | `us-east-1` |

## Storage layout

```
data/
└── my-bucket/
    ├── index.redb           # redb database (objects, dead bytes, compaction state)
    ├── seg_000000.bin        # append-only data segments
    ├── seg_000001.bin
    └── seg_000002.bin
```

Each bucket is a directory with a redb database and one or more segment files. Objects are appended to the active segment. When a segment exceeds `--max-segment-size-mb`, a new one is created.

Writes are append-only. Overwriting an object appends the new data and marks the old region as dead space. Metadata updates and dead space accounting happen in the same redb transaction.

Compaction rewrites a segment to reclaim dead space. A compaction flag is set before the rewrite begins so that incomplete compactions are recovered on startup. Autovacuum triggers compaction when dead space exceeds the configured threshold.

On startup, orphan temp files are cleaned up and incomplete writes at the end of the active segment are truncated.

## Admin endpoints

```
GET  /_/stats              # storage statistics (JSON)
GET  /_/stats/{bucket}     # per-bucket statistics
POST /_/compact/{bucket}   # trigger compaction
GET  /_/verify/{bucket}    # verify data integrity
```

## Offline tools

```bash
# Compaction (server must be stopped)
simple3 compact              # all buckets
simple3 compact my-bucket    # single bucket

# Integrity verification (server must be stopped)
simple3 verify               # all buckets
simple3 verify my-bucket     # single bucket
```

## Development

```bash
cargo build                  # debug build
cargo build --release        # release build
cargo test                   # run all tests
cargo test test_name         # run specific test
cargo lint                   # clippy with pedantic + nursery
cargo fix-lint               # auto-fix lint issues
cargo bench                  # criterion benchmarks
```

## License

MIT
