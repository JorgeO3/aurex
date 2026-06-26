# AurumMQ Storage Format v0

This document describes the on-disk append-only format implemented by `aurum-storage` in PR8.

All multi-byte integers are **little-endian**. Records are scanned sequentially within a segment file.

## Directory layout

```text
{data_dir}/
  payload/
    payload-0000000000000001.seg
  queue-{queue_id}/
    index-0000000000000001.seg
    ack-0000000000000001.seg
```

- **Payload log** — shared across queues; stores raw message bytes.
- **Queue index log** — maps `queue_seq` → `PayloadRef` for one queue.
- **Ack ledger** — durable settlement events for one queue.

PR8 uses a single segment per stream (`SegmentId(1)`). Segment rotation is reserved for later PRs.

## Record envelope

Every durable batch is wrapped in a fixed 40-byte header followed by a body.

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `magic` — `0x4155_524D` (`"AURM"`) |
| 4 | 2 | `version` — `0` |
| 6 | 2 | `kind` — see [Record kinds](#record-kinds) |
| 8 | 2 | `flags` — see [Flags](#flags) |
| 10 | 2 | `header_len` — `40` |
| 12 | 4 | `body_len` |
| 16 | 4 | `record_crc32c` |
| 20 | 8 | `stream_id` |
| 28 | 8 | `base_seq` |
| 36 | 4 | `count` — items in batch |

`record_crc32c` covers the header (with this field zeroed) plus the full body using CRC32C (Castagnoli).

Writers set `flags = HAS_CRC32C | BATCHED` on encode.

### Record kinds

| Value | Name | Body |
|-------|------|------|
| 1 | `PayloadBatch` | payload batch |
| 2 | `QueueIndexBatch` | queue index entries |
| 3 | `AckLedgerBatch` | ack ledger entries |
| 4 | `Checkpoint` | reserved |
| 5 | `Manifest` | reserved |

### Flags

```text
RecordFlags (u16):
  COMPRESSED   = 1 << 0   (reserved in v0)
  HAS_CRC32C   = 1 << 1
  BATCHED      = 1 << 2

QueueIndexFlags / AckLedgerFlags: none defined in v0
```

### Stream IDs

`stream_id` disambiguates logical streams inside one physical segment:

```text
1                          payload log
queue_id | (1 << 32)       queue index for queue_id
queue_id | (2 << 32)       ack ledger for queue_id
```

## Payload batch body

```text
u32  message_count
u32  payload_len[message_count]   // per-message byte lengths
u8   payload_bytes[sum(payload_len)]
```

A `PayloadRef` points into one batch record:

```rust
pub struct PayloadRef {
    pub segment_id: SegmentId,
    pub offset: LogOffset,   // record start in segment
    pub index: u32,          // message index inside batch
    pub len: u32,            // payload byte length
    pub checksum: u32,       // CRC32C of payload bytes
}
```

## Queue index batch body

```text
u32  entry_count
repeat entry_count times:
  u32  queue_id
  u64  queue_seq
  u64  payload_segment_id
  u64  payload_offset
  u32  payload_index
  u32  payload_len
  u32  payload_checksum
  u16  flags
```

## Ack ledger batch body

```text
u32  entry_count
repeat entry_count times:
  u8   event_kind
  u32  queue_id
  ... event-specific fields ...
```

| `event_kind` | Name | Fields after `queue_id` |
|--------------|------|-------------------------|
| 1 | `AckRange` | `u64 start`, `u32 len` |
| 2 | `AckMask` | `u64 block_base`, `u8 word_index`, `u64 mask` |
| 3 | `NackRequeue` | `u64 start`, `u32 len` |
| 4 | `DeadLetter` | `u64 start`, `u32 len` |

## Segment scan rules

1. Read records from offset `0` until EOF or error.
2. **Truncated tail** — fewer than 40 bytes remain, or body is incomplete: stop; valid prefix ends at last complete record.
3. **CRC mismatch** on a complete record: treat as corruption (PR8 does not skip middle corruption).
4. Unknown `kind` or unsupported `version`: reject decode.

## Compatibility

- `version == 0` is the only supported format in PR8.
- New record kinds and flags may be added in later versions; readers must reject unknown versions.
- `Checkpoint` and `Manifest` kinds are reserved and not written in PR8.

## Durability modes

| Mode | Behavior |
|------|----------|
| `Buffered` | `write()` to OS page cache; visible after process restart once buffers flush |
| `FsyncOnFlush` | `fsync` on explicit `flush()` / per-batch sync where configured |

PR8 broker integration uses `Buffered` by default.
