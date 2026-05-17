# Benchmark vs MinIO (single node)

Same machine, same disk, same client, same parameters — simple3 against a single MinIO instance. The intent is reproducibility, not winning a benchmark. The scripts under [`bench/`](../bench/) bring up both servers locally, run [MinIO's `warp`](https://github.com/minio/warp) against each, and emit raw CSVs you can re-analyze.

## Methodology

- Both servers run on the same host, sequentially. No co-tenancy effects, no cross-host networking.
- Each test starts from an empty `data_dir`.
- `warp mixed` is the canonical workload: a 20% PUT / 80% GET mix over a single bucket, mirroring real S3 traffic.
- 15 s warm-up + run per cell (warp prep + record).
- One run per cell; we record what was measured, not best-of-N.

### Parameters

| Axis | Values |
|---|---|
| Object size | 1 KB, 64 KB, 1 MB, 10 MB |
| Concurrency | 1, 8 (8 only at 1 MB) |
| Duration per cell | 15 s |
| Workload | `warp mixed` (default share) |

### Reproducing

```bash
cargo build --release
./bench/setup.sh
source /tmp/simple3-bench/env.sh

./bench/run-warp.sh simple3
./bench/run-warp.sh minio

# Per-cell breakdown
warp analyze bench/results/simple3-mixed-1MB-t8.csv
warp analyze bench/results/minio-mixed-1MB-t8.csv
```

Tear down with `kill $SIMPLE3_PID $MINIO_PID` (both are exported by `setup.sh`).

## Hardware used for the numbers below

- Apple M-series laptop (macOS Darwin 25), single client process pinned to the host loopback.
- Storage: APFS on the host SSD for both data directories.
- simple3 built with the `release-prod` profile, ran natively. MinIO is `minio/minio:latest` (RELEASE.2024-…), also run natively by `bench/setup.sh` — `docker run` MinIO is fine too, but the bundled script avoids the container layer.
- warp 1.x via the `minio/warp:latest` image.

These numbers are loopback-only and not representative of a real network deployment. Re-run on the production hardware before quoting.

## Results — `warp mixed`, 15 s

### Concurrency 1 (single-threaded)

| Object size | Op | simple3 throughput | MinIO throughput |
|---|---|---:|---:|
| 1 KB  | PUT  | 59 obj/s     | 176 obj/s |
| 1 KB  | GET  | 177 obj/s    | 531 obj/s |
| 1 KB  | STAT | 118 obj/s    | 356 obj/s |
| 64 KB | PUT  | 3.10 MiB/s   | 10.25 MiB/s |
| 64 KB | GET  | 9.36 MiB/s   | 30.80 MiB/s |
| 64 KB | STAT | 100 obj/s    | 336 obj/s |
| 1 MB  | PUT  | 36.39 MiB/s  | 57.70 MiB/s |
| 1 MB  | GET  | 111.44 MiB/s | 172.26 MiB/s |
| 1 MB  | STAT | 77 obj/s     | 121 obj/s |
| 10 MB | PUT  | 147.62 MiB/s | 138.21 MiB/s |
| 10 MB | GET  | 424.75 MiB/s | 398.17 MiB/s |
| 10 MB | STAT | 29 obj/s     | 28 obj/s |

### Concurrency 8

| Object size | Op | simple3 throughput | MinIO throughput |
|---|---|---:|---:|
| 1 MB | PUT  | 66.52 MiB/s  | 162.55 MiB/s |
| 1 MB | GET  | 200.78 MiB/s | 487.35 MiB/s |
| 1 MB | STAT | 139 obj/s    | 341 obj/s |

### Latency percentiles (1 MB, concurrency 1)

| Op  | Server  | p50  | p90  | p99  | Fastest |
|---|---|---:|---:|---:|---:|
| PUT  | simple3 | 13.6 ms | 15.6 ms | 21.0 ms | 10.0 ms |
| PUT  | MinIO   | 7.9 ms  | 9.5 ms  | 12.1 ms | 5.9 ms |
| GET  | simple3 | 1.6 ms  | 2.9 ms  | 4.3 ms  | 0.8 ms |
| GET  | MinIO   | 1.9 ms  | 3.4 ms  | 5.4 ms  | 0.8 ms |
| STAT | simple3 | 0.4 ms  | 0.8 ms  | 1.9 ms  | 0.2 ms |
| STAT | MinIO   | 0.5 ms  | 0.8 ms  | 1.9 ms  | 0.2 ms |

### Peak resident memory (after the 15 s mixed run)

| Server  | RSS |
|---|---:|
| simple3 | 61 MiB |
| MinIO   | 378 MiB |

## Interpreting the results

- **Small objects (1 KB / 64 KB).** Request-rate bound. MinIO is ~3× ahead — its hot path is more aggressively pipelined (HTTP/1.1 keep-alive + smaller metadata transactions). simple3's redb write transaction is the limiting factor at this scale.
- **1 MB.** Disk-bandwidth becomes the floor. MinIO still leads on PUT (~1.6×) and GET (~1.5×).
- **10 MB.** Both servers saturate sequential I/O; simple3's append-only path is slightly faster on GET (5% advantage) and within margin on PUT.
- **Concurrency 8 at 1 MB.** MinIO scales better — its bucket layout shards naturally; simple3 serializes commits on a per-bucket redb writer (~2.4× gap).
- **Memory.** simple3's idle RSS is ~6× smaller. It does not maintain in-memory object indices beyond the redb page cache.

## Known caveats

- All measurements are over the loopback interface on a single host (no Docker, no containers — the scripts run native binaries). There is no network jitter, no NIC saturation, no TLS termination.
- `warp mixed` doesn't exercise multipart. Add `warp put --obj.size=1G --concurrent=4 --duration=60s` to compare the multipart paths if multi-GB objects matter for your workload.
- MinIO's erasure-coded mode is not tested here. Single-disk MinIO is the closest apples-to-apples comparison.
- 15 s is short — quote the duration alongside any numbers you publish; longer runs give stable p99s.
