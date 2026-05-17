# Benchmark vs MinIO (single node)

Same machine, same disk, same client, same parameters — simple3 against a single MinIO instance. The intent is reproducibility, not winning a benchmark. The scripts under [`bench/`](../bench/) bring up both servers locally, run [MinIO's `warp`](https://github.com/minio/warp) against each, and emit raw CSVs you can analyze yourself.

## Methodology

- Both servers run on the same host, sequentially. No co-tenancy effects, no cross-host networking.
- Each test starts from an empty `data_dir`.
- `warp mixed` is the canonical workload: a 20% PUT / 80% GET mix over a single bucket, mirroring real S3 traffic.
- Three repetitions per cell; report median.
- Disk caches are dropped between runs (`sysctl vm.drop_caches=3`) on Linux; on macOS the disk cache is not flushable so results are read-cache-warm.

### Parameters

| Axis | Values |
|---|---|
| Object size | 1 KB, 64 KB, 1 MB, 10 MB |
| Concurrency | 1, 8 |
| Duration per cell | 30 s |
| Workload | `warp mixed` (20% PUT, 80% GET) |

### Metrics

`warp analyze` produces:

- p50 / p99 latency per operation (PUT, GET, STAT)
- Throughput in ops/s and MiB/s
- Peak RSS read from `/proc/<pid>/status` (Linux only)

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

## Hardware tested

This guide ships with the scripts. The reference run that populated `README.md`'s storage-engine numbers used:

- Hetzner EX130-R (Intel Xeon Gold 5412U, 24 cores @ 2.1 GHz, 256 GB DDR5 ECC, 1.92 TB NVMe SSD, Ubuntu 22.04, ext4)

Replace the table below with your own numbers once you've run the scripts. Leave the raw CSVs alongside this doc so others can audit.

## Results template

Drop your `warp analyze` output into the table below and check it in next to this file.

| Workload | Size | Threads | Server | p50 latency | p99 latency | Throughput | RSS |
|---|---|---|---|---|---|---|---|
| mixed | 1 KB | 1 | simple3 | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 1 KB | 1 | minio   | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 64 KB | 1 | simple3 | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 64 KB | 1 | minio   | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 1 MB | 1 | simple3 | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 1 MB | 1 | minio   | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 10 MB | 1 | simple3 | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 10 MB | 1 | minio   | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 1 MB | 8 | simple3 | _fill_ | _fill_ | _fill_ | _fill_ |
| mixed | 1 MB | 8 | minio   | _fill_ | _fill_ | _fill_ | _fill_ |

## Interpreting the results

- **Small objects (1 KB / 64 KB):** dominated by per-request overhead (auth, metadata transaction, fsync). Expect both servers within the same order of magnitude; throughput is more about request rate than disk bandwidth.
- **Large objects (1 MB / 10 MB):** the streaming write path is sequential append in both engines. Bottleneck is the disk for both.
- **Concurrency 8:** highlights metadata-write contention. simple3 serializes object commits on a per-bucket redb writer; MinIO sharded approach can pull ahead at high concurrency on a single large bucket.

## Known caveats

- `warp mixed` doesn't exercise multipart. Add `warp put --obj.size=1G --concurrent=4 --duration=60s` to compare the multipart paths.
- MinIO's erasure-coded mode is not tested here. Single-disk MinIO is the closest apples-to-apples comparison.
- The 30 s duration is a smoke-test default. Longer runs (5+ min) are required for stable p99s; quote the duration alongside any numbers you publish.
