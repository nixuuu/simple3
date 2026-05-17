# simple3

S3-compatible storage server written in Rust. Single static binary, embedded
database, no external dependencies. Built around segmented append-only data
files plus [redb](https://github.com/cberner/redb) for metadata. Speaks two
protocols on the same data: an S3-compatible HTTP API (via
[`s3s`](https://crates.io/crates/s3s)) and a gRPC API (tonic).

[![CI](https://github.com/nixuuu/simple3/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nixuuu/simple3/actions/workflows/ci.yml)
[![CodeQL](https://github.com/nixuuu/simple3/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/nixuuu/simple3/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)

## Status

**Beta. Not yet recommended for production.**

What is in good shape:

- Single-node correctness: 142 unit/integration tests, deterministic crash-recovery
  suite, randomised kill loop (100 iterations, zero verify errors on macOS APFS),
  whole-object MD5 + per-block CRC32C with background scrub.
- S3 surface: 19 S3 operations covering buckets, objects, multipart, versioning,
  copy, range GET, presigned URLs.
- Concurrent S3 compatibility suite ([MinIO Mint](https://github.com/minio/mint))
  green in CI.

What is missing:

- No replication. Single-node only. A [replication RFC](docs/rfc-replication.md)
  exists but is not implemented.
- No erasure coding, no quorum writes.
- Multi-region, IAM-style cross-account access, bucket policies (only
  identity-attached policies are supported), ACLs, encryption at rest.
- Long-running production validation. The chaos suite gives strong evidence of
  correctness, not of multi-week stability.

If you need a single-node S3 target for backups, CI artifacts, internal
tooling, or as a development drop-in for AWS S3, the surface should fit. If you
need HA across nodes, this is not it yet.

## What's included

S3 HTTP API: `CreateBucket`, `HeadBucket`, `DeleteBucket`, `ListBuckets`,
`GetBucketLocation`, `PutObject`, `GetObject`, `HeadObject`, `CopyObject`,
`DeleteObject`, `DeleteObjects`, `ListObjectsV2`, `ListObjectVersions`,
`PutBucketVersioning`, `GetBucketVersioning`, `CreateMultipartUpload`,
`UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`. SigV4 auth.
Range GET. Presigned URL generation (GET, PUT).

gRPC API: 22 RPCs mirroring the S3 surface plus `BulkGet` / `BulkPut` for
streaming many objects in one request. Bidirectional streaming for large
payloads (256 KiB chunks via mpsc channels, never loads a full object into
memory).

Storage engine:

- Segmented append-only data files per bucket. Active segment rolls over at
  `max_segment_size_mb` (default 4 GiB).
- Metadata in `index.redb` with three typed tables and cross-table ACID
  transactions.
- Per-segment dead-byte tracking; autovacuum compacts when
  `dead_bytes / file_size > autovacuum_threshold`.
- Crash safety: append-only writes commit metadata only after the byte write
  succeeds. Compaction sets an in-table flag before atomic rename; recovery
  rolls forward or back on startup.
- Whole-object MD5 stored at write time; a 4-byte CRC32C trailer appended to
  every object record in the segment; offline `simple3 verify` reads each
  object and validates both.

Auth and access control:

- IAM-style policies with `Effect` / `Action` / `Resource` matching.
  Explicit Deny overrides Allow.
- Multi-key per server. Root key is bootstrapped on first start.
- Same `AuthStore` used by both HTTP (SigV4) and gRPC (`x-access-key` /
  `x-secret-key` metadata).

Operations:

- `/health` (liveness), `/ready` (returns 503 below `min_disk_free_mb`),
  `/metrics` (Prometheus, optional Basic Auth).
- Structured JSON logging with per-request trace IDs.
- Rate limiting (per-IP, governor).
- Request size limits (`max_object_size_mb`, default 5 GiB).
- Graceful shutdown on `SIGTERM`/`SIGINT`.
- Per-bucket lifecycle rules (auto-expiration by age).
- Admin HTTP endpoints under `/_/` and matching `simple3 keys` /
  `simple3 policy` CLI commands.

Built-in CLI (works against any S3 endpoint, not just simple3):

- `mb`, `rb`, `ls`, `cp`, `rm`, `sync` with concurrent transfers.
- `presign` for time-limited URLs.
- `keys`, `policy` for managing access keys and IAM policies on a running
  server.
- `health` to probe a running server, `compact` / `verify` as offline tools
  against a stopped server's data directory.
- `--grpc` flag to switch transport.

## Quick start

### Prerequisites

- Rust toolchain (`rustup`); tested on 1.95
- `protoc` (protobuf compiler) for gRPC code generation
  - macOS: `brew install protobuf`
  - Debian/Ubuntu: `apt install protobuf-compiler`

### Build

```bash
cargo build --release
```

### Run the server

```bash
./target/release/simple3 serve
# Prints a root access key + secret on first start. Save them; not shown again.
```

Common flags:

```bash
./target/release/simple3 serve --port 9000 --grpc-port 0
./target/release/simple3 --data-dir /var/lib/simple3 serve
./target/release/simple3 --config /etc/simple3/simple3.toml serve
```

### Use with the AWS CLI

```bash
export AWS_ENDPOINT_URL=http://localhost:8080
export AWS_ACCESS_KEY_ID=AKxxxxxxxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=SKxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

aws s3 mb s3://my-bucket
aws s3 cp file.txt s3://my-bucket/
aws s3 sync ./local-dir s3://my-bucket/backup/
```

Tested against `mc` (MinIO client), `rclone` (with `list_version = 2`), and the
official AWS SDKs (Rust, Go, Python boto3, JS).

### Use with the built-in client

```bash
simple3 mb s3://my-bucket
simple3 cp file.txt s3://my-bucket/
simple3 sync ./local s3://my-bucket/backup/ -j 10
simple3 ls s3://my-bucket --recursive
simple3 rm s3://my-bucket/prefix/ --recursive

# Presigned URL (1 h GET)
simple3 presign s3://my-bucket/file.txt --ttl 1h

# Switch transport to gRPC
simple3 sync ./local s3://my-bucket/ --grpc -j 10
```

## Benchmarks

Two sets of numbers below. Engine-level Criterion microbenchmarks isolate the
storage path from the network. The MinIO comparison runs both servers on the
same host with `warp mixed` and reports what `warp` measured.

simple3 is not the fastest S3-compatible server. MinIO leads on small-object
throughput by ~3x at concurrency 1 because of a more aggressive HTTP hot path
and shared bucket layout. The numbers below are reproducible; the scripts are
in [`bench/`](bench/).

### Engine throughput (Criterion, single thread)

Source: `cargo bench` on two hosts, in-process (no network).

- **M1 Pro / APFS**: MacBook Pro, Apple M1 Pro, 32 GB RAM, internal NVMe,
  macOS 26.1.
- **Hetzner / NVMe**: Hetzner bare-metal Linux server with NVMe SSD.

The two hosts make opposite tradeoffs around durability that show up in the
write path: every PUT here is a real `commit()` against redb, which means an
`fsync` per object. On macOS APFS that fsync takes roughly 5 ms, so any PUT
smaller than a few MB hits a ~5 ms floor. On the Linux NVMe host the same
fsync is far cheaper, so small-object PUT throughput is 50-100× higher.

Reads do not commit, so they are similar across both hosts.

#### Writes (one PUT = one redb commit + fsync)

| Operation     | M1 Pro latency | M1 Pro throughput | Hetzner latency | Hetzner throughput |
| ------------- | -------------: | ----------------: | --------------: | -----------------: |
| Write 1 KB    |        4.5 ms  |     0.22 MiB/s    |          55 µs  |       17.6 MiB/s   |
| Write 10 KB   |        4.3 ms  |     2.25 MiB/s    |          61 µs  |        160 MiB/s   |
| Write 1 MB    |        4.9 ms  |      204 MiB/s    |         569 µs  |       1.72 GiB/s   |
| Write 10 MB   |       14.8 ms  |      675 MiB/s    |         5.1 ms  |       1.91 GiB/s   |
| Write 100 MB  |        103 ms  |      968 MiB/s    |          58 ms  |       1.70 GiB/s   |

#### Reads, lists, deletes, compactions (Hetzner)

The exhaustive bench is on Hetzner because the slow Mac fsync makes
Criterion's repeated setup phases impractical (each new measurement
re-prefills the bucket). Only one read case finished on the Mac during
audit: `get/1kb_among_1k` at **1.5 µs / iter**, close to Hetzner's 1.6 µs
for the same shape, confirming reads are not host-bound.

| Operation         | Latency | Throughput |
| ----------------- | ------: | ---------: |
| Read 1 KB         |  1.6 µs |   618 MiB/s |
| Read 10 KB        |  2.7 µs |  3.58 GiB/s |
| Read 1 MB         |  127 µs |  7.66 GiB/s |
| Read 10 MB        |  1.5 ms |  6.62 GiB/s |
| List 1K obj       |  522 µs |          — |
| List 10K obj      |  2.8 ms |          — |
| List 50K obj      |   14 ms |          — |
| Delete (of 1K)    |  2.5 ms |          — |
| Delete (of 10K)   |  2.9 ms |          — |
| Compact 10K × 1KB |   45 ms |          — |
| Compact 1K × 1MB  |  1.37 s |   626 MiB/s |

#### 8-thread concurrent workload (Hetzner)

100 ops/thread for KB sizes, 10 ops/thread for MB sizes.

| Workload          | Latency | Throughput |
| ----------------- | ------: | ---------: |
| 8× GET 1 KB       |  3.6 ms |   220 MiB/s |
| 8× GET 1 MB       |   24 ms |  3.20 GiB/s |
| 8× PUT 1 KB       |  214 ms |  3.65 MiB/s |
| 8× PUT 10 KB      |  233 ms |  34.4 MiB/s |
| Mixed 4R+4W 1 KB  |   65 ms |  12.0 MiB/s |
| Mixed 4R+4W 10 KB |   62 ms |   129 MiB/s |

Multi-thread PUT throughput is well below single-thread × 8 because writes
serialize on a per-bucket redb writer.

### Network throughput vs MinIO (warp mixed, 15 s, loopback)

Source: [`docs/benchmark-minio.md`](docs/benchmark-minio.md). Same host, same
disk, same client, sequential runs. Single-host loopback, not representative
of a real network deployment.

Concurrency 1:

| Object size | Op  | simple3      | MinIO        |
| ----------- | --- | -----------: | -----------: |
| 1 KB        | PUT |    59 obj/s  |   176 obj/s  |
| 1 KB        | GET |   177 obj/s  |   531 obj/s  |
| 64 KB       | PUT |  3.10 MiB/s  | 10.25 MiB/s  |
| 64 KB       | GET |  9.36 MiB/s  | 30.80 MiB/s  |
| 1 MB        | PUT | 36.39 MiB/s  | 57.70 MiB/s  |
| 1 MB        | GET | 111.4 MiB/s  | 172.3 MiB/s  |
| 10 MB       | PUT | 147.6 MiB/s  | 138.2 MiB/s  |
| 10 MB       | GET | 424.8 MiB/s  | 398.2 MiB/s  |

Concurrency 8, 1 MB objects:

| Op  | simple3      | MinIO        |
| --- | -----------: | -----------: |
| PUT |  66.5 MiB/s  | 162.6 MiB/s  |
| GET | 200.8 MiB/s  | 487.4 MiB/s  |

Latency, 1 MB, concurrency 1:

| Op  | Server  |    p50  |    p90  |    p99  |
| --- | ------- | ------: | ------: | ------: |
| PUT | simple3 | 13.6 ms | 15.6 ms | 21.0 ms |
| PUT | MinIO   |  7.9 ms |  9.5 ms | 12.1 ms |
| GET | simple3 |  1.6 ms |  2.9 ms |  4.3 ms |
| GET | MinIO   |  1.9 ms |  3.4 ms |  5.4 ms |

Resident memory after the 15 s mixed run: simple3 61 MiB, MinIO 378 MiB.

Reading the numbers: MinIO leads on small-object throughput because its HTTP
hot path is more aggressively pipelined and its bucket layout shards naturally
across goroutines. simple3 is competitive on 10 MB sequential I/O. The same
per-bucket redb writer that limits the concurrent PUT row above also shows up
here at concurrency 8.

## How it works

Each bucket lives in its own directory:

```
data/
├── _auth.redb              # access keys + IAM policies
├── simple3.toml             # optional config
└── my-bucket/
    ├── index.redb           # objects + dead-byte counters + compaction flag + versions + lifecycle
    ├── seg_000000.bin       # append-only data segments
    ├── seg_000001.bin
    └── .mpu_*               # multipart staging (cleaned on startup)
```

Writes append bytes to the active segment, then commit metadata (offset, length,
CRC32C, MD5, version ID, etc.) into `index.redb` in a single transaction.
Overwrites bump the segment's dead-byte counter in the same transaction. There
is no in-memory metadata index; every read goes through redb. redb itself
maintains a page cache.

Crash safety comes from three invariants:

- Bytes-then-metadata ordering. A reader can't see an object whose metadata
  isn't in redb. The orphan-tail recovery on startup truncates any half-written
  data past the last committed offset.
- Compaction commits a flag in the same transaction that names the tmp segment;
  recovery either finishes the rename or rolls back. The atomic `rename(2)` is
  the commit point.
- Lifecycle deletes record `(key, last_modified)` and re-check at delete time,
  closing the TOCTOU window between scan and prune.

Autovacuum runs in a background tokio task. It reads per-segment dead-byte
counters and triggers compaction on the worst offender when the ratio crosses
`autovacuum_threshold`. While compaction runs, new writes go to a fresh active
segment; reads continue against the old segment until the atomic swap.

The same `Storage` is shared by the HTTP layer (`s3impl.rs`) and the gRPC
layer (`grpc.rs`). Both stream large payloads through 256 KiB chunks rather
than buffering whole objects in memory.

## Authentication and access control

On first startup, `AuthStore` creates a root admin key and prints
`Access Key ID` + `Secret Key` on stderr. Save them; they are not shown again.

Manage keys and policies through the CLI. It talks to the running server via
the admin HTTP API rather than touching `_auth.redb` directly, since redb
locks the file while the server is running.

```bash
export AKROOT=AK... ; export SKROOT=SK...

simple3 keys list                       --access-key $AKROOT --secret-key $SKROOT
simple3 keys create --description "ci"  --access-key $AKROOT --secret-key $SKROOT
simple3 keys disable AKxxxx             --access-key $AKROOT --secret-key $SKROOT

simple3 policy create readonly --document policy.json --access-key $AKROOT --secret-key $SKROOT
simple3 policy attach readonly AKxxxx                 --access-key $AKROOT --secret-key $SKROOT
```

Policy document format (familiar from AWS IAM):

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

Supported actions: `s3:GetObject`, `s3:GetObjectVersion`, `s3:PutObject`,
`s3:DeleteObject`, `s3:DeleteObjectVersion`, `s3:ListBucket`,
`s3:ListBucketVersions`, `s3:PutBucketVersioning`, `s3:GetBucketVersioning`,
`s3:CreateBucket`, `s3:DeleteBucket`, `s3:ListAllMyBuckets`,
`admin:Stats`, `admin:Compact`, `admin:Verify`, plus the globs `s3:*`,
`admin:*`, and `*`. Any S3 operation without an explicit mapping falls
through to `s3:{OperationName}` (for example `s3:CopyObject`,
`s3:HeadBucket`, `s3:GetBucketLocation`), so `s3:*` covers them all.

Resources accept `arn:s3:::{bucket}` or `arn:s3:::{bucket}/{key}` with `*`
glob matching. Admin keys bypass policy checks. Non-admin keys are denied
by default.

gRPC clients pass `x-access-key` and `x-secret-key` as request metadata; the
built-in CLI sets these automatically.

## Configuration

CLI flags take precedence over the TOML config, which takes precedence over
defaults.

All values below are the code defaults; omit any field to keep the default.

```toml
[server]
host = "0.0.0.0"
port = 8080
grpc_port = 50051
shutdown_timeout = 30
log_format = "text"
rate_limit_rps = 0           # 0 disables rate limiting

[storage]
max_segment_size_mb = 4096
autovacuum_interval = 300
autovacuum_threshold = 0.5
scrub_interval = 3600
lifecycle_interval = 3600
min_disk_free_mb = 0         # 0 disables the /ready disk-space gate
max_object_size_mb = 5120
max_list_keys = 1000

[metrics]
username = ""                # empty disables Basic Auth on /metrics
password = ""
```

The `[auth]` table is parsed for forward compatibility but its `enabled`
field has no effect yet; authentication is always on.

The full flag reference is `simple3 serve --help`. Client-side flags
(`--endpoint-url`, `--access-key`, `--secret-key`, `--region`) read AWS
environment variables when not set.

## Operations

| Endpoint                 | Use                                            |
| ------------------------ | ---------------------------------------------- |
| `GET /health`            | liveness                                       |
| `GET /ready`             | 503 if free disk < `min_disk_free_mb`          |
| `GET /metrics`           | Prometheus exposition (optional Basic Auth)    |
| `GET /_/stats/{bucket}`  | segment + dead-byte stats                      |
| `POST /_/compact/{b}`    | manual compaction                              |
| `GET /_/verify/{b}`      | walk every object, validate CRC + MD5          |
| `*/_/lifecycle/{b}`      | GET / PUT / DELETE lifecycle rule              |
| `*/_/keys`               | key CRUD                                       |
| `*/_/policies`           | policy CRUD + attach/detach                    |

Offline tools (server stopped):

```bash
simple3 compact            # all buckets
simple3 compact my-bucket  # single bucket
simple3 verify             # all buckets, exit non-zero on errors
```

The Prometheus exposition covers per-S3-op request duration histograms,
ingress/egress bytes, rate-limit drops, per-bucket segment count and
dead-space ratio, compaction frequency and duration, and active TCP
connections. A starter Grafana dashboard is bundled at
[`docs/grafana-dashboard.json`](docs/grafana-dashboard.json).

## What's not supported

To keep the line between "implemented" and "marketing" sharp:

- **Replication / HA.** Single node only. The RFC in
  [`docs/rfc-replication.md`](docs/rfc-replication.md) describes a possible
  leader-follower design; nothing in the binary implements it.
- **Erasure coding.** No EC, no parity. Use the filesystem (ZFS, btrfs,
  ext4 + mdadm) for disk redundancy.
- **Encryption at rest.** No SSE-S3, no SSE-KMS, no SSE-C. Encrypt the
  underlying volume (LUKS, FileVault, ZFS native).
- **Bucket policies, ACLs, CORS, website hosting, lifecycle transitions.**
  Only identity-attached IAM-style policies and `Expiration` lifecycle rules.
- **Object Lock / WORM / retention.** Not modelled.
- **Cross-account / cross-tenant isolation.** Single tenant.
- **Replication metrics, accelerated transfer, Inventory, Analytics.**

If any of these are blockers for your use case, MinIO single-node or AWS S3
are the obvious alternatives.

## Documentation

| Topic                                         | File                                                         |
| --------------------------------------------- | ------------------------------------------------------------ |
| Production deployment (systemd, monitoring)   | [`docs/production.md`](docs/production.md)                   |
| TLS via reverse proxy (nginx, Traefik, Caddy) | [`docs/tls-proxy.md`](docs/tls-proxy.md)                     |
| Migration from MinIO / AWS S3                 | [`docs/migration.md`](docs/migration.md)                     |
| Backup and restore                            | [`docs/backup.md`](docs/backup.md)                           |
| Benchmark vs MinIO (single node)              | [`docs/benchmark-minio.md`](docs/benchmark-minio.md)         |
| Chaos / fault-injection testing               | [`docs/chaos-testing.md`](docs/chaos-testing.md)             |
| Replication design RFC (not implemented)      | [`docs/rfc-replication.md`](docs/rfc-replication.md)         |

## Development

```bash
cargo build               # debug
cargo build --release     # release
cargo test                # 142 tests, ~10 s on M-series
cargo lint                # clippy with pedantic + nursery (custom alias)
cargo fix-lint            # auto-fix clippy
cargo bench               # criterion microbenchmarks

# Chaos
make chaos                # in-process stress + kill loop (macOS/Linux)
sudo make chaos-disk-full # ENOSPC scenarios (Linux only)
```

The MinIO Mint and ceph s3-tests compatibility suites run in CI
(`.github/workflows/compat-mint.yml`, `compat-ceph.yml`); both are
Docker-based and run against a server you start manually for local
reproduction.

Code style is enforced by clippy: warnings are errors,
`clippy::pedantic` + `clippy::nursery` + `rust-2024-compatibility` are on.
`unwrap()` is banned in library code; the codebase uses `Result`/`Option`
propagation. Files cap at 800 lines, functions at 50.

## License

[MIT](LICENSE.md).
