# simple3

S3-compatible storage server in Rust. Single binary, embedded database, no external dependencies.

Uses segmented append-only data files with [redb](https://github.com/cberner/redb) for metadata indexing. Exposes an S3-compatible HTTP API and a gRPC API. Ships with a CLI client that accepts AWS environment variables.

## Features

- S3-compatible HTTP API, works with AWS CLI and any S3 client
- gRPC API with 14 RPCs, including bidirectional streaming for bulk transfers
- IAM-style access policies with multi-key authentication and Effect/Action/Resource matching
- Segmented append-only storage with ACID metadata transactions
- Background autovacuum that compacts segments when dead space exceeds a threshold
- MD5 checksums per object, full-bucket integrity verification via CLI, HTTP, or gRPC
- S3-compatible multipart uploads
- Built-in CLI: `mb`, `rb`, `ls`, `cp`, `rm`, `sync`, `keys`, `policy` with concurrent transfers
- Client commands work over HTTP or gRPC (`--grpc` flag)
- Range GET via HTTP Range headers
- TOML config file support

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
# On first start, prints a root admin access key to stderr
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
# Use the access key printed on first server startup
export AWS_ACCESS_KEY_ID=AKxxxxxxxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=SKxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

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

## Authentication

On first startup the server creates a root admin access key and prints it to stderr. Save these credentials, they won't be shown again.

Access keys and IAM-style policies are managed via CLI commands that talk to the running server:

```bash
# key management (requires admin credentials)
simple3 keys list --access-key AKROOT --secret-key SKROOT
simple3 keys create --description "deploy bot" --access-key AKROOT --secret-key SKROOT
simple3 keys disable AKXXXXXXXXXX --access-key AKROOT --secret-key SKROOT

# policy management
simple3 policy create readonly --document policy.json --access-key AKROOT --secret-key SKROOT
simple3 policy attach readonly AKXXXXXXXXXX --access-key AKROOT --secret-key SKROOT
simple3 policy detach readonly AKXXXXXXXXXX --access-key AKROOT --secret-key SKROOT
```

Policy documents follow the AWS IAM format with Effect, Action, and Resource fields:

```json
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
```

Supported actions: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:CreateBucket`, `s3:DeleteBucket`, `s3:ListAllMyBuckets`, `s3:*`, `admin:Stats`, `admin:Compact`, `admin:Verify`, `admin:*`, `*`.

Resources use the format `arn:s3:::{bucket}` or `arn:s3:::{bucket}/{key}` with `*` glob matching.

Admin keys bypass all policy checks. Non-admin keys are denied by default and need explicit Allow policies. Explicit Deny always overrides Allow.

gRPC authentication uses `x-access-key` and `x-secret-key` metadata headers. The built-in CLI sends these automatically when `--access-key` and `--secret-key` are provided.

## Server configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `./data` | Storage directory |
| `--config` | `{data-dir}/simple3.toml` | TOML config file path |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8080` | S3 HTTP port |
| `--grpc-port` | `50051` | gRPC port, 0 to disable |
| `--autovacuum-interval` | `300` | Autovacuum interval in seconds, 0 to disable |
| `--autovacuum-threshold` | `0.5` | Compact when dead bytes exceed this fraction of file size |
| `--max-segment-size-mb` | `4096` | Max segment file size in MB |

Settings can also be specified in a TOML config file at `{data-dir}/simple3.toml`:

```toml
[server]
host = "0.0.0.0"
port = 8080
grpc_port = 50051

[storage]
max_segment_size_mb = 4096
autovacuum_interval = 300
autovacuum_threshold = 0.5
```

CLI flags override config file values.

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
├── _auth.redb               # access keys and IAM policies
├── simple3.toml              # optional config file
└── my-bucket/
    ├── index.redb            # redb database (objects, dead bytes, compaction state)
    ├── seg_000000.bin         # append-only data segments
    ├── seg_000001.bin
    └── seg_000002.bin
```

Each bucket is a directory with a redb database and one or more segment files. Objects are appended to the active segment. When a segment exceeds `--max-segment-size-mb`, a new one is created.

Writes are append-only. Overwriting an object appends the new data and marks the old region as dead space. Metadata updates and dead space accounting happen in the same redb transaction.

Compaction rewrites a segment to reclaim dead space. A compaction flag is set before the rewrite begins so that incomplete compactions are recovered on startup. Autovacuum triggers compaction when dead space exceeds the configured threshold.

On startup, orphan temp files are cleaned up and incomplete writes at the end of the active segment are truncated.

## Admin endpoints

Storage endpoints:

```
GET  /_/stats/{bucket}     # per-bucket statistics
POST /_/compact/{bucket}   # trigger compaction
GET  /_/verify/{bucket}    # verify data integrity
```

Key and policy management (require `Authorization: Bearer {access_key}:{secret_key}` with an admin key):

```
GET    /_/keys                              # list access keys
POST   /_/keys                              # create key
GET    /_/keys/{id}                         # show key details
DELETE /_/keys/{id}                         # delete key
POST   /_/keys/{id}/enable                  # enable key
POST   /_/keys/{id}/disable                 # disable key

GET    /_/policies                           # list policies
POST   /_/policies                           # create policy
GET    /_/policies/{name}                    # show policy document
DELETE /_/policies/{name}                    # delete policy
POST   /_/policies/{name}/attach/{key_id}   # attach policy to key
DELETE /_/policies/{name}/attach/{key_id}   # detach policy from key
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
