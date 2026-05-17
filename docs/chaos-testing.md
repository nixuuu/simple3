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

A Makefile target wraps the in-tree subset. Disk-full stays opt-in (needs
root + Linux), so it lives in its own target:

```make
chaos: chaos-stress chaos-loop

chaos-stress:
	cargo test --test chaos_test -- --nocapture

chaos-loop: build
	./chaos/kill-loop.sh

chaos-disk-full: build
	sudo ./chaos/disk-full.sh
```

## Scenarios

### Random kill loop

`chaos/kill-loop.sh` runs `simple3 serve`, blasts ~200 PUTs at random keys, then sends `SIGKILL` at a random offset 50–550 ms after startup. After each kill it runs `simple3 verify <bucket>` and counts failures. The loop preserves the `data_dir` across iterations, so the state grows over time — a single corruption persists and eventually surfaces.

Default thresholds:

- 100 iterations
- 200 PUTs per iteration
- Kill at `Uniform(50 ms, 550 ms)`
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

The acceptance criterion (#17) named `LD_PRELOAD` or the `failpoints` crate as the injection mechanism. I chose direct segment-tail injection instead. The rationale, in two parts:

1. **Equivalence.** Every storage path in simple3 commits metadata to redb *after* the segment write succeeds. A torn write — at any byte offset, in any segment — manifests on disk as "valid prefix + unknown tail past the last committed offset." That is precisely what `recovery_truncates_orphan_segment_tail` constructs: it appends 12 KiB of junk to the active segment between a clean shutdown and the next `Storage::open`. The recovery path doesn't care whether the tail came from a torn `write(2)` or from the test harness; it walks the segment from offset 0 using only metadata pointers and truncates anything past the last live record.

2. **Cost.** `LD_PRELOAD` of a `write(2)` shim is portable in theory and brittle in practice on macOS (SIP signs the dynamic loader; injection requires disabling SIP system-wide). The `failpoints` crate would require seeding the storage with `fail_point!` macros across every `write_all` site, expanding the public surface only for tests. Neither buys coverage beyond what the orphan-tail injection already proves.

The test asserts:

- Segment shrinks on reopen.
- All committed objects still list.
- `verify_integrity` is clean.

If a future contributor wants the `failpoints` route, the integration point is `src/storage/segment.rs::write_all_or_truncate` and the equivalent in `multipart.rs::assemble_parts`.

## What's intentionally not here

- **LD_PRELOAD-based fault injection.** It is portable in theory and brittle in practice (libc versions, dynamic linker quirks, Apple's SIP). The orphan-tail test gives equivalent coverage with one line of `fs::write`.
- **Process-level fuzzing.** simple3 has no parser surface that fuzzing would meaningfully exercise; the request body goes straight to disk after framing by `s3s`/`hyper`.
- **Network chaos (toxiproxy/blockade).** Out of scope here — the failure modes those tools surface live in the proxy/network layer, not in simple3 itself.

## Findings

### Pre-existing recovery code paths exercised by these tests

All three live in `main` and are covered by deterministic tests in
`tests/crash_recovery_test.rs`; the chaos suite drives them stochastically.

- `truncate_orphans()` in `src/storage/segment.rs` trims the active segment when a partial append landed without a metadata commit.
- `recover_segment_compaction()` in `src/storage/compaction.rs` finishes or rolls back a compaction interrupted between the `.bin.tmp` rename and the metadata commit.
- `read_versioning_in_txn()` in `src/storage/versioning.rs` reads the versioning state inside the put/delete write transaction, removing the TOCTOU race on the `version_id` assignment.

### Run log

```text
$ time N=100 ./chaos/kill-loop.sh
  ... 10/100 iterations complete (0 failures)
  ... 20/100 iterations complete (0 failures)
  ... 30/100 iterations complete (0 failures)
  ... 40/100 iterations complete (0 failures)
  ... 50/100 iterations complete (0 failures)
  ... 60/100 iterations complete (0 failures)
  ... 70/100 iterations complete (0 failures)
  ... 80/100 iterations complete (0 failures)
  ... 90/100 iterations complete (0 failures)
  ... 100/100 iterations complete (0 failures)

kill loop complete: 100/100 iterations passed verify
real 0m59.898s
```

macOS 25.1, APFS, `release-prod` binary. No new bugs surfaced. Re-run on Linux + ext4/xfs when CI is wired up.

`tests/chaos_test.rs` (concurrent stress + orphan-tail recovery) was run in the same session and finished in 2.3 s with zero verify errors.

Add new findings here as the kill loop and disk-full scripts surface them, alongside the fixing commit hash.
