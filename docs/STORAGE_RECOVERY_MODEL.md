# AurumMQ Storage Recovery Model (PR8)

This document describes what `aurum-storage` persists, what stays ephemeral, and how a single-node broker rebuilds queue **ready** state after restart.

## What is durable

| Artifact | Contents |
|----------|----------|
| Payload log | Message bytes (batched) |
| Queue index | `queue_seq` → `PayloadRef` mapping per queue |
| Ack ledger | `AckRange`, `AckMask`, `NackRequeue`, `DeadLetter` events |

Publish path (broker):

```text
append payload batch
  → append queue index entries
  → apply in-memory publish
```

Ack path:

```text
apply in-memory ack (aurum-core)
  → append ack ledger entries
```

If storage append fails after in-memory success, the broker reports an internal error and marks storage unhealthy.

## What is ephemeral

Not restored on restart:

- Consumer sessions and delivery tags
- Prefetch / credit state
- Inflight delivery windows
- Sparse retry masks in memory
- Routing tables and exchange bindings (PR8 scope)

## Recovery algorithm

For each queue, `RecoveryBuilder`:

1. Scan all queue index entries → `BTreeMap<queue_seq, PayloadRef>`.
2. Scan all ack ledger entries → settled sequence numbers.
3. **Ready set** = indexed sequences not present in settled set.
4. `next_seq` = max indexed sequence + 1 (or `0` if empty).

```text
RecoveredQueueImage {
  queue_id,
  next_seq,
  ready: Vec<RecoveredMessage>,  // sorted by queue_seq
}
```

### Inflight / unacked after restart

PR8 treats all durable messages that are not explicitly acked or dead-lettered as **ready**:

```text
inflight  → not restored
unacked   → become ready again (redeliver)
acked     → excluded from ready
dead-lettered → excluded from ready
```

This matches broker behavior after all consumer connections are lost: unacked messages are redelivered.

`NackRequeue` entries are recorded for future use; PR8 recovery does not need them because unacked messages are already re-queued by the rule above.

### Partial writes

| Condition | Action |
|-----------|--------|
| Truncated final record | Ignore partial tail; recover valid prefix |
| Corrupt record in the middle | Return `RecoveryError::CorruptRecord` |
| Index entry without payload | Not expected in PR8 happy path |

Repeated recovery of the same segment files must produce identical `RecoveredQueueImage` values.

## Broker integration

`InMemoryShardExecutor::with_durable_storage` optionally wraps `AppendOnlyShardStorage`.

After restart:

```text
open storage at data_dir
declare queues (or recover discovers queue dirs)
recover_queue_from_storage(queue_id)
  → rebuilds in-memory ready count via publish_contiguous(ready.len())
```

Payload bytes are not loaded into the hot queue in PR8; the broker uses placeholder payloads for the contiguous ready path. Full payload hydration is a later PR.

## Confirm semantics vs durability

| Durability | When publish is considered durable |
|------------|-------------------------------------|
| `Buffered` | Record appended to OS buffers |
| `FsyncOnFlush` | After explicit `flush()` / fsync boundary |

Consumer confirms and publisher confirms remain protocol-level concerns; storage durability is independent of delivery tags.

## Failure modes

- **Storage unhealthy** — further durable writes rejected until re-open / recovery.
- **Ack without prior index** — should not occur in correct broker operation.
- **Duplicate recovery** — idempotent: ready count derived from index minus ack ledger.

## Future work (out of PR8)

- Checkpoints / snapshots to avoid full segment scans
- Segment rotation and compaction
- Cluster-wide epoch and replication
- Restoring inflight state from optional delivery journals
