# RFC: Async leader-follower replication

Status: draft

## Goal

Add read replicas for availability. A single leader accepts writes per bucket; one or more followers receive updates asynchronously and serve read-only traffic.

Non-goals for v1: strong consistency, automatic leader election, multi-leader writes.

## Current architecture

Each bucket owns:
- Append-only segment files (`seg_NNNNNN.bin`), data + 4-byte CRC32C trailer per object
- A redb database (`index.redb`) with three tables: `OBJECTS` (key to bincode `ObjectMeta`), `SEG_DEAD` (per-segment dead bytes), `SEG_COMPACTING` (compaction-in-progress flag)

A write is two-phase: append data to segment, then commit metadata in a redb transaction. A `Mutex<ActiveWriter>` serializes all appends within a bucket. Crash recovery truncates orphaned segment tails and replays interrupted compactions.

Relevant files: `src/storage/mod.rs`, `src/storage/compaction.rs`, `src/types.rs`.

## Candidate approaches

### (A) Segment shipping

Leader streams segment data (append tails) plus an operation log to followers. Each follower maintains its own segments and redb, replaying operations locally.

Replication unit (logical model):

```rust
enum ReplicationOp {
    Put { key: String, length: u64, meta: ObjectMeta },
    Delete { key: String },
}
```

The `REPL_LOG` stores only the metadata (key, length, `ObjectMeta`), not the object data itself. On the wire, the gRPC stream sends the `ReplicationOp` header followed by the object data in 256 KB chunks (reusing the existing chunked streaming path from gRPC downloads). This avoids loading full objects into memory on either side.

Leader writes each op to a per-bucket replication log (redb table `REPL_LOG`, keyed by `op_seq: u64`) in the same transaction as the put/delete. Follower replays: appends data to its own active segment, commits metadata to its own redb.

Pros:
- Followers are self-contained after catch-up; no callbacks to leader for reads
- No stale-reference risk (follower owns its data)
- Replication log doubles as an audit trail

Cons:
- Network bandwidth proportional to write volume (full object data shipped)
- Follower segment layout diverges from leader (different IDs, offsets)

### (B) Metadata sync with on-demand data fetch

Leader replicates only metadata operations. Follower fetches segment data lazily on first read or via background pull.

Pros:
- Lower replication bandwidth for write-heavy, read-light workloads
- Metadata-only stream is small

Cons:
- Read latency spike on cold objects (data not yet fetched)
- Stale reference: leader may compact a segment before follower fetches it, leaving the follower with a dangling pointer
- Requires a data-fetch protocol with retry/fallback logic

### Recommendation

Approach (A). The stale-reference problem in (B) adds complexity that outweighs the bandwidth savings, especially given that simple3 targets small-to-medium deployments where write volumes are moderate.

## Replication protocol

Transport: gRPC bidirectional streaming over the existing tonic server. gRPC provides lower latency than HTTP polling and fits the existing server infrastructure.

### Sequence numbers

Each bucket gets a monotonically increasing `op_seq: u64`. The leader assigns the next sequence number inside the same redb write transaction that commits the put or delete. No gaps in the sequence.

### Log storage

New redb table per bucket:

| Table | Key | Value |
|-------|-----|-------|
| `REPL_LOG` | `u64` (op_seq) | bincode `ReplicationOp` |

Written in the same transaction as the OBJECTS table update. This guarantees that every committed mutation has a corresponding log entry.

### Streaming

1. Follower opens `StreamReplicationLog(bucket, from_seq)` gRPC stream.
2. Leader sends all ops from `from_seq + 1` to current head (catch-up phase).
3. After catch-up, leader holds the stream open and pushes new ops as they commit (tail phase).
4. Follower sends periodic acks with its applied `op_seq` so the leader can track lag.

For puts, the leader sends the `ReplicationOp` header (metadata only) followed by object data in 256 KB chunks over the same gRPC stream. The follower assembles chunks into its local segment before committing metadata. gRPC stream-level flow control handles backpressure when a follower cannot keep up; no application-level backpressure is needed for v1.

### Follower apply

On receiving a `Put`:
1. Receive chunked data, compute CRC32C on the fly. Reject the op if the computed CRC does not match the value in `ObjectMeta.content_crc32c`. This catches corruption introduced by the network layer.
2. Append verified data + CRC32C trailer to local active segment.
3. Build `ObjectMeta` with local segment ID and offset.
4. Commit to local OBJECTS table and update local SEG_DEAD if overwriting.

On receiving a `Delete`:
1. Remove key from local OBJECTS table, update local SEG_DEAD.

Apply is idempotent: if the follower already has `op_seq >= received`, skip.

## Consistency model

**Eventual consistency.** A follower reflects the leader's state as of some past `op_seq`.

Guarantees:
- Monotonic reads per follower: `op_seq` only advances, so a follower never shows an older version of an object after showing a newer one.
- Bounded staleness (configurable): the follower tracks its lag in `op_seq` count and wall-clock time. If lag exceeds a threshold, the follower can report itself unhealthy to a load balancer.

Not guaranteed:
- Read-your-writes across leader and follower. A client that writes to the leader and immediately reads from a follower may see stale data.

## Failover and promotion

v1 uses manual promotion only.

### Promotion steps

1. Stop the leader (or confirm it is unreachable).
2. Among available followers, pick the one with the highest applied `op_seq`.
3. On the chosen follower, run `simple3 promote --epoch <new_epoch>`.
4. Redirect client traffic (DNS, load balancer config, or endpoint-url change).
5. Remaining followers reconnect to the new leader and continue streaming.

### Split-brain prevention

A leadership epoch (monotonic `u64`) is stored in `_repl.redb` at the data_dir root. `_repl.redb` holds global replication state: leadership epoch and registered followers with their durable `applied_op_seq` checkpoints. Per-bucket replication state (`REPL_LOG` table, current `op_seq`) lives in each bucket's `index.redb`.

The leader includes its epoch in every replication message. Followers reject messages from a leader with a stale epoch. The `promote` command increments the epoch, so if the old leader comes back, its messages are rejected.

No quorum or consensus protocol in v1. The operator is responsible for ensuring only one node is promoted.

## Compaction interaction

Compaction is a local optimization. Each node compacts independently.

- Leader compacts based on its own SEG_DEAD thresholds. Compaction rewrites segment offsets, but new puts after compaction use the new offsets naturally. The replication log contains the data itself, not segment references, so offset changes are invisible to followers.
- Follower compacts based on its own SEG_DEAD counters. Its segment layout is independent of the leader's.

### Log trimming

The leader can trim `REPL_LOG` entries only when both conditions hold:
1. `op_seq < min(applied_op_seq)` across all registered followers (using durable checkpoints persisted in `_repl.redb`, not just connected followers).
2. Entry age exceeds a configurable retention window (time- or sequence-based floor).

This prevents unnecessary full resyncs after brief follower disconnections. A periodic background task handles trimming. To deregister a permanently removed follower, the operator runs `simple3 follower remove <id>`.

If a follower falls behind beyond the oldest retained log entry, it cannot catch up incrementally. It must perform a full resync: a cold copy of the leader's data_dir (as described in `docs/backup.md`), then reconnect from the new head `op_seq`.

## Required interface changes

| Component | Change |
|-----------|--------|
| `BucketStore` | New `REPL_LOG` redb table; write `ReplicationOp` in same txn as put/delete |
| `BucketStore` | `read_repl_log(from_seq) -> Vec<ReplicationOp>` |
| `BucketStore` | `current_op_seq() -> u64` |
| `Storage` | `ReplicationManager` struct: owns follower connections, tracks per-follower `applied_op_seq`, runs log trimming |
| `types.rs` | `ReplicationOp` enum with bincode serialization |
| `proto/` | New RPCs: `StreamReplicationLog`, `AckReplication`, `GetReplicationStatus` |
| `grpc.rs` | Implement replication RPCs |
| `cli/serve.rs` | Config flags: `--role` (`leader` or `follower`), `--leader-addr`, `--replication-port` |
| `cli/` | New subcommand: `simple3 promote --epoch N` |
| `_repl.redb` | New file at data_dir root: stores leadership epoch and registered followers with durable checkpoints |

## Open questions

1. **Per-bucket vs global sequence numbers.** Per-bucket is simpler and matches the existing per-bucket isolation. Cross-bucket consistency (e.g., "list all buckets at a consistent point") would require a global sequence or vector clock. Per-bucket is sufficient for v1.

2. **Inline data vs separate data channel.** Shipping data inline in the gRPC stream is simple but a single large object blocks replication of subsequent small objects. A separate data channel with backpressure could help, at the cost of protocol complexity.

3. **Replication log in redb vs separate file.** Storing in redb keeps it transactional (log entry committed atomically with the mutation). A separate append-only file would avoid growing the redb database. Recommend redb for correctness, with periodic trimming to bound size.

4. **Auth replication.** `_auth.redb` is global, not per-bucket. Options: (a) replicate auth changes on a separate stream, (b) treat auth as leader-only (followers proxy auth checks to leader), (c) require manual auth sync. Recommend (b) for v1 simplicity. Trade-off: proxying auth adds a network round-trip per authenticated follower read, partially reducing the read-replica latency benefit.

5. **Follower reads during catch-up.** Options: (a) serve reads with a staleness warning header (`X-Replication-Lag`), (b) reject reads until caught up within threshold. Recommend (a) for availability.

6. **Multipart uploads.** Only the completed object (after `complete_multipart_upload`) generates a replication op. In-progress `.mpu_*` parts are leader-local transient state and not replicated.

7. **Segment ID divergence.** Leader and follower will have different segment IDs and offsets. This is expected and correct since each node manages its own segment files independently.

8. **Replication channel security.** The existing gRPC auth uses `x-access-key`/`x-secret-key` metadata. Options for replication streams: (a) reuse the same mechanism, (b) dedicated replication token, (c) mTLS between nodes. For v1, a dedicated long-lived replication token stored in `_repl.redb` is the simplest option that avoids coupling replication auth to user-facing IAM.
