# Chaos and fault-injection tests

The deterministic crash-recovery suite in `tests/crash_recovery_test.rs` covers known failure shapes — orphaned segment tail, mid-compaction interruption, etc. This document covers the stochastic surface: random kills, disk exhaustion, concurrent write storms, and partial writes.

## Layout

| Path | What it covers | Runner |
|---|---|---|
| `tests/chaos_test.rs` | 8-writer + compaction stress; orphan-tail recovery on segment | `cargo test --test chaos_test` |
| `chaos/kill-loop.sh` | Random SIGKILL during writes, restart, verify (100 iter default) | shell script |
| `chaos/disk-full.sh` | tmpfs-backed ENOSPC scenario | shell script, Linux-only |

The Rust tests run as part of `cargo test` and exercise in-process scenarios. The shell scripts launch the real binary and exercise out-of-process failure modes.

## Running

```bash
# In-process stress (always runs)
cargo test --test chaos_test -- --nocapture

# Out-of-process kill loop (100 iterations, several minutes)
cargo build --release
./chaos/kill-loop.sh

# Disk-full path (Linux, root for tmpfs)
sudo ./chaos/disk-full.sh
```

A Makefile target wraps the lot:

```make
chaos:
	cargo test --test chaos_test -- --nocapture
	./chaos/kill-loop.sh
	./chaos/disk-full.sh
```

## Scenarios

### Random kill loop

`chaos/kill-loop.sh` runs `simple3 serve`, blasts ~200 PUTs at random keys, then sends `SIGKILL` at a random offset within the first 500 ms. After each kill it runs `simple3 verify <bucket>` and counts failures. The loop preserves the `data_dir` across iterations, so the state grows over time — a single corruption persists and eventually surfaces.

Default thresholds:

- 100 iterations
- 200 PUTs per iteration
- Kill at `Uniform(0, 500 ms)`
- One bucket, 200-key keyspace

Adjust with `N=`, `DATA=`, `BUCKET=` env vars.

### Disk-full on tmpfs

`chaos/disk-full.sh` mounts a 32 MiB tmpfs at `/mnt/simple3-chaos`, runs simple3 against it, and PUTs 4 MiB objects until the server returns 5xx. It then:

1. Verifies the server is still alive (no crash on ENOSPC).
2. Deletes half the objects + triggers a compaction.
3. Confirms a fresh PUT succeeds after space is reclaimed.

Linux-only because of `tmpfs` and `mount`.

### Concurrent writer + compaction stress

`chaos_eight_writers_plus_compaction_keep_data_consistent` (Rust test) spawns 8 writer threads and 1 compaction thread against the same bucket for 2 s. The writers rewrite the same 20 keys, maximising dead-byte churn. The compactor sweeps every 50 ms. At the end, `verify_integrity` must report zero errors.

This exercises:

- Per-bucket writer mutex contention.
- redb transaction conflicts between the put path and `compact_segment`.
- Atomic segment swap under read pressure (none here — verify reads after).

### Partial write recovery

`chaos_partial_write_to_segment_tail_recovers_via_truncation` simulates a torn write: it appends 12 KiB of junk to the latest segment after the server is shut down, then re-opens storage. The recovery path must truncate the orphan tail. The test asserts:

- Segment shrinks on reopen.
- All committed objects still list.
- `verify_integrity` is clean.

This is the deterministic counterpart of an `LD_PRELOAD`-based partial write injector. A real torn-write test on a journaling filesystem is unlikely to differ from this scenario because the metadata commit (`commit_put`) happens after the segment write — any byte past the last committed offset is, by definition, orphan tail.

## What's intentionally not here

- **LD_PRELOAD-based fault injection.** It is portable in theory and brittle in practice (libc versions, dynamic linker quirks, Apple's SIP). The orphan-tail test gives equivalent coverage with one line of `fs::write`.
- **Process-level fuzzing.** simple3 has no parser surface that fuzzing would meaningfully exercise; the request body goes straight to disk after framing by `s3s`/`hyper`.
- **Network chaos (toxiproxy/blockade).** Out of scope here — the failure modes those tools surface live in the proxy/network layer, not in simple3 itself.

## Findings

The current test suite has caught (and the fixes are in `main`):

- Orphan-tail recovery left the active segment writer pointed past the last valid offset, causing subsequent writes to skip a region. Fixed by `truncate_orphans` at startup.
- A compaction interrupted between `seg_NNNNNN.bin.tmp` rename and metadata commit would re-run on restart but skip the `seg_compacting` clear. Fixed by recovering from the per-segment flag.
- Concurrent put + delete on the same key with versioning enabled used to race on the version_id assignment. Fixed by reading versioning state inside the write transaction.

Add new findings here as the kill loop and disk-full scripts surface them, alongside the fixing commit hash.
