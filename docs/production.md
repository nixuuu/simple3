# Production deployment

A single-server reference for running simple3 on Linux. The guide assumes systemd, a Prometheus-compatible monitoring stack, and ZFS/LVM-snapshot-capable storage for backups.

## Filesystem layout

```text
/var/lib/simple3/           # data_dir, owned by simple3:simple3, mode 0750
  _auth.redb
  bucket-a/
  ...
/etc/simple3/simple3.toml   # config (mode 0640, root:simple3)
/var/log/simple3/           # only used if logs are redirected; systemd's journal is fine too
```

Mount the `data_dir` on its own filesystem (ZFS dataset, LVM volume, dedicated block device). Reasons:

- Snapshots and backup tooling operate on a single mount.
- The autovacuum and lifecycle sweeps stress disk I/O; isolating them keeps the rest of the host responsive.
- A full data filesystem won't take the OS down.

Create the user and directories:

```bash
useradd --system --home-dir /var/lib/simple3 --shell /usr/sbin/nologin simple3
install -d -o simple3 -g simple3 -m 0750 /var/lib/simple3
install -d -o root    -g simple3 -m 0750 /etc/simple3
```

## Reference TOML config

`/etc/simple3/simple3.toml`:

```toml
[server]
host = "127.0.0.1"            # rely on the reverse proxy for TLS and IP exposure
port = 8080
grpc_port = 50051
shutdown_timeout = 60
log_format = "json"           # ships through journald
rate_limit_rps = 200          # per-IP, applied after the proxy

[storage]
max_segment_size_mb = 4096
autovacuum_interval = 300     # 5 min
autovacuum_threshold = 0.5
scrub_interval = 21600        # 6 h
lifecycle_interval = 3600     # 1 h
min_disk_free_mb = 5120       # /ready returns 503 below this
max_object_size_mb = 5120     # 5 GiB cap on single PUT and multipart total
max_list_keys = 1000

[metrics]
# Empty fields disable basic auth. Set both to enable.
username = "metrics"
password = ""

[auth]
enabled = true
```

CLI flags override TOML, and TOML overrides defaults.

## systemd unit

`/etc/systemd/system/simple3.service`:

```ini
[Unit]
Description=simple3 S3-compatible storage
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=simple3
Group=simple3
ExecStart=/usr/local/bin/simple3 \
    --data-dir /var/lib/simple3 \
    --config /etc/simple3/simple3.toml \
    serve
Restart=on-failure
RestartSec=5
TimeoutStopSec=90              # > shutdown_timeout in the config
KillSignal=SIGTERM
LimitNOFILE=65536

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/simple3
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
MemoryDenyWriteExecute=true
SystemCallArchitectures=native

# Resource ceilings — adjust to the host
MemoryHigh=8G
MemoryMax=12G
TasksMax=4096

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now simple3
journalctl -u simple3 -f
```

The first startup prints the bootstrap root key on stderr. Capture it from `journalctl -u simple3 --since "1 minute ago"` — it is not shown again.

## Reverse proxy

Run TLS termination in nginx, Traefik, or Caddy. The full configs live in [`docs/tls-proxy.md`](tls-proxy.md). Make sure the proxy forwards `X-Forwarded-For`; the rate limiter relies on the source IP.

## Monitoring

simple3 exposes Prometheus metrics on the same port as the S3 API at `/metrics` (basic auth-gated when `metrics.username`/`metrics.password` are set in the config).

### Prometheus scrape

```yaml
scrape_configs:
  - job_name: simple3
    metrics_path: /metrics
    basic_auth:
      username: metrics
      password: <secret>
    static_configs:
      - targets: ["simple3.internal:8080"]
```

### Key metrics

| Metric | Type | Why it matters |
|---|---|---|
| `simple3_requests_total{op,status}` | counter | Request rate and error budget |
| `simple3_request_duration_seconds_bucket` | histogram | p50/p99 per S3/gRPC op |
| `simple3_bytes_received_total` / `simple3_bytes_sent_total` | counter | Ingress/egress throughput |
| `simple3_rate_limited_total{protocol}` | counter | 429 frequency; raise `rate_limit_rps` if non-zero under normal load |
| `simple3_buckets_total` | gauge | Bucket count |
| `simple3_total_size_bytes` / `simple3_total_dead_bytes` | gauge | Capacity and reclaimable space |
| `simple3_compaction_running` | gauge | Long autovacuum stalls indicate either thrashing or a misconfigured threshold |
| `simple3_connections_active` | gauge | TCP fan-in |

### Alert rules (sample)

```yaml
groups:
  - name: simple3
    rules:
      - alert: Simple3Down
        expr: up{job="simple3"} == 0
        for: 2m
        annotations:
          summary: "simple3 process not reachable"

      - alert: Simple3DeadSpaceHigh
        expr: simple3_total_dead_bytes / simple3_total_size_bytes > 0.4
        for: 30m
        annotations:
          summary: "Dead-space ratio > 40 % — autovacuum may be lagging"

      - alert: Simple3RateLimitSpike
        expr: rate(simple3_rate_limited_total[5m]) > 1
        for: 10m
        annotations:
          summary: "Sustained 429s; check rate_limit_rps or upstream load"

      - alert: Simple3HighLatency
        expr: histogram_quantile(0.99, rate(simple3_request_duration_seconds_bucket{op=~"PutObject|GetObject"}[5m])) > 2
        for: 15m
        annotations:
          summary: "p99 latency for PUT/GET > 2 s"
```

### Grafana

A starter dashboard is bundled at [`docs/grafana-dashboard.json`](grafana-dashboard.json) — import via *Dashboards → New → Import* and point it at your Prometheus datasource. It includes the panels the alert rules above depend on (request rate, p50/p99 latency, ingress/egress, active connections, dead-space ratio per bucket, compaction frequency, rate-limit drops, segments per bucket).

## Backups

simple3's data is consistent at any filesystem snapshot, so the recommended flow is snapshot + replicate. The procedure is documented in [`docs/backup.md`](backup.md); the production aspect is the schedule.

### Daily snapshot via cron

```bash
cat <<'EOF' >/etc/cron.daily/simple3-snapshot
#!/bin/sh
set -eu
DATE=$(date +%Y%m%d)
zfs snapshot tank/simple3@$DATE
zfs send -i tank/simple3@yesterday tank/simple3@$DATE | \
  ssh backup-host zfs receive tank/simple3
zfs destroy tank/simple3@yesterday || true
zfs rename tank/simple3@$DATE tank/simple3@yesterday
EOF
chmod 0755 /etc/cron.daily/simple3-snapshot
```

For ext4/XFS hosts without filesystem-level snapshots, run a cold copy during off-hours instead:

```bash
systemctl stop simple3
rsync -aH --delete /var/lib/simple3/ backup-host:/var/lib/simple3-backup/
systemctl start simple3
```

## Resource sizing

Order-of-magnitude guidance derived from the benchmark methodology in
[`docs/benchmark-minio.md`](benchmark-minio.md). Treat them as a starting
point and validate against your workload before committing capacity.

### Disk

- **Useful capacity ≈ raw capacity × 0.85.** simple3 reclaims overwritten data lazily through autovacuum, so steady-state dead space settles around 5–15 %.
- **redb overhead** scales with object count: roughly `300 bytes × (live_objects + versions)`. 10 M objects ⇒ ~3 GiB of `index.redb`.
- **Multipart staging** uses `.mpu_*` files under each bucket; size them for the largest concurrent multipart upload (default cap is 5 GiB × concurrent_uploads).

### Memory

- Steady-state RSS scales with `(open_connections × ~512 KiB) + redb_cache (~200 MiB) + per-bucket overhead (~5 MiB)`.
- Plan for 2–4 GiB on small servers; 8 GiB+ once concurrency exceeds a few hundred connections.

### CPU

- PUT/GET hot paths are streaming + MD5 + CRC32C. Per-core throughput approaches the disk bandwidth for sequential I/O.
- Multipart assembly and compaction are CPU-bound during the rewrite phase; expect short spikes on busy buckets.

## Validated locally

The pieces in this guide were exercised in isolation on a development machine
(macOS, Darwin 25, OrbStack-managed Docker):

- The TOML config is parsed by `simple3 serve` and merges with CLI flags as
  shown — covered by unit tests in `src/cli/config.rs`.
- The systemd unit syntax was validated with `systemd-analyze verify`. It has
  not been run under a live systemd instance; do so before relying on the
  hardening directives in production.
- Prometheus scrape works against `/metrics` — confirmed via `curl` and the
  `tests/limits_test.rs` integration tests that drive the same code path.
- TLS proxy termination via nginx was verified end-to-end —
  see [`docs/tls-proxy.md`](tls-proxy.md).
- The benchmark numbers in [`docs/benchmark-minio.md`](benchmark-minio.md) come
  from a single-host loopback run, not the resource-sizing reference hardware.

For embedded or edge deployments (single-binary, no proxy, no monitoring), the
defaults from `simple3 serve` are enough — most of this guide is about
operational fit, not features. Before quoting the sizing numbers below for a
specific deployment, re-measure on the target hardware.
