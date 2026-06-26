# PR8 — Initial Append-Only Storage Engine

## Status

**Target PR:** PR8  
**Area:** `crates/hot-path/aurum-storage` + minimal integration with `aurum-broker`  
**Depends on:**

- PR3: Rabbit-like delivery semantics in `aurum-core`
- PR4: Internal Command Protocol
- PR5: Single-node in-memory broker executor
- PR6: Minimal compiled routing
- PR7: Minimal native protocol adapter

## One-line goal

PR8 introduces the first durable, append-only storage layer for AurumMQ:

> Persist published messages, queue index entries, and settlement events as append-only logs, then recover deterministic single-node queue state after restart.

This PR must not attempt full Kafka/RabbitMQ-grade storage yet. It should create the **storage foundation**: file format, segment writer/reader, payload references, ack ledger, recovery rules, tests, and an integration point for the in-memory broker.

---

## Why PR8 now

Up to PR7, AurumMQ can conceptually do this:

```text
native protocol bytes
  -> native adapter
  -> CommandBatch
  -> compiled routing
  -> in-memory broker executor
  -> aurum-core queue + consumer sessions
  -> native output frames
```

But the system is still memory-only. If the process crashes, published messages and acks are lost.

PR8 adds the next layer:

```text
CommandBatch
  -> broker executor
  -> aurum-core state transition
  -> append-only storage side effects
  -> durable recovery image
```

The storage layer must be designed now because later PRs depend on stable durable identities:

```text
PayloadRef
QueueIndexEntry
AckLedgerEntry
SegmentId
LogOffset
StorageEpoch
RecoveryCheckpoint
```

If those are wrong, AMQP, native confirms, routing, clustering, replication, and recovery will all have to change later.

---

## Design philosophy

### The storage layer is not the queue engine

`aurum-core` owns in-memory delivery semantics:

```text
ready ranges
sparse ready masks
inflight masks
acked masks
retry masks
consumer sessions
delivery tags
```

`aurum-storage` owns durable facts:

```text
payload bytes were appended
queue seq maps to payload ref
ack/nack/dead-letter event happened
checkpoint/snapshot was created
segment is valid up to offset X
```

The storage layer must not know AMQP, native protocol frames, TCP connections, or consumer channels.

### Append-only first

PR8 must avoid random in-place updates. Durable state is expressed as logs:

```text
payload log:
  stores payload bytes and payload batch metadata

queue index log:
  maps queue sequence numbers to payload refs

ack ledger:
  stores settlement events: AckRange, AckMask, Nack, DeadLetter, etc.
```

Recovery builds current state by replaying:

```text
latest snapshot/checkpoint, if any
+ queue index entries after checkpoint
+ ack ledger entries after checkpoint
```

PR8 can start without full snapshots, but the file format and APIs must leave a place for them.

### Batch-first

No per-message fsync. No per-message file header if avoidable. No per-message heap allocation in the write path.

The basic unit is:

```text
PublishBatch
AckBatch
NackBatch
QueueIndexBatch
PayloadBatch
```

### Format stability over cleverness

AurumMQ can optimize later with `io_uring`, direct I/O, compression dictionaries, zero-copy, and segment indexes. PR8 must first make the durable format simple, testable, deterministic, and forward-compatible.

### Correctness before throughput

PR8 must survive:

```text
truncated last record
corrupt checksum
partial flush
restart after publish before ack
restart after ack
restart after nack/requeue
unknown future record kind
segment rotation boundary
```

---

## What PR8 is

PR8 is:

```text
- Append-only segment files.
- Stable record headers.
- Payload log writer/reader.
- Queue index log writer/reader.
- Ack ledger writer/reader.
- Recovery scanner.
- Initial StorageEngine API.
- Durable mode integration into InMemoryShardExecutor.
- Tests for recovery after simulated crashes.
- Storage benchmark/experiment.
```

## What PR8 is not

PR8 is not:

```text
- Full storage compaction.
- Full snapshot engine.
- Replication.
- Raft/VSR/Paxos.
- Cluster recovery.
- io_uring production backend.
- Direct I/O.
- Multi-disk placement.
- NUMA-local storage scheduling.
- Encryption-at-rest.
- Full compression strategy.
- Kafka-like long-retention stream storage.
- AMQP persistence compatibility polish.
```

Those come later.

---

## Target directory structure

Recommended structure inside `crates/hot-path/aurum-storage`:

```text
crates/hot-path/aurum-storage/
  src/
    lib.rs

    error.rs
    ids.rs
    flags.rs
    config.rs

    record/
      mod.rs
      header.rs
      kind.rs
      flags.rs
      codec.rs
      checksum.rs

    segment/
      mod.rs
      file.rs
      writer.rs
      reader.rs
      scanner.rs
      rotation.rs

    io/
      mod.rs
      std_file.rs
      backend.rs
      write_buf.rs

    payload/
      mod.rs
      log.rs
      batch.rs
      ref.rs

    queue_index/
      mod.rs
      entry.rs
      log.rs

    ack_ledger/
      mod.rs
      entry.rs
      log.rs

    recovery/
      mod.rs
      scanner.rs
      image.rs
      policy.rs

    snapshot/
      mod.rs
      manifest.rs
      checkpoint.rs

    tests_support/
      mod.rs
      tempdir.rs
      crash.rs
```

If this feels too large for one PR, implement the modules as small files with intentionally minimal code. The structure should be created early so future work has a place.

---

## Crate dependency rules

`aurum-storage` should depend on:

```text
aurum-types
aurum-internal-protocol, only if needed for payload/command structs
smallvec / arrayvec, if already accepted in workspace
bytes, only in adapters/buffers where useful
crc32c, if already accepted
tracing, cold path only
```

`aurum-storage` should not depend on:

```text
aurum-core
aurum-protocol-native
aurum-protocol-amqp
aurum-broker
aurum-routing
aurum-runtime
```

Reason:

```text
storage should persist durable facts, not in-memory queue internals.
```

If storage needs an `AckRange`, `QueueId`, `PayloadRef`, or `QueueSeq`, that type should live in `aurum-types` or be mirrored as a neutral storage type.

---

## Static dispatch, dynamic dispatch, enums, and generics

### Hot/warm path rule

File append and record encoding can be hot under throughput workloads. Avoid `dyn Trait` inside the inner append loop.

Preferred pattern:

```rust
pub struct SegmentWriter<B, C>
where
    B: IoBackend,
    C: Checksum,
{
    backend: B,
    checksum: C,
}
```

or:

```rust
pub enum IoBackendKind {
    StdFile(StdFileBackend),
    // IoUring(IoUringBackend), later
    // DirectIo(DirectIoBackend), later
}
```

Using an enum is often better than `Box<dyn IoBackend>` for runtime-selectable but still matchable storage backends. The match happens once per batch, not once per message.

### Cold path rule

Dynamic dispatch is acceptable for:

```text
CLI utilities
admin tools
recovery inspectors
test harnesses
operator integration
storage plugin registry
```

Example:

```rust
pub trait StorageFactory {
    fn open(&self, config: &StorageConfig) -> Result<Box<dyn StorageEngine>, StorageError>;
}
```

This is fine because it is not the write-loop path.

### Enums for durable state

Use enums for durable record kinds and recovery decisions:

```rust
#[repr(u16)]
pub enum RecordKind {
    PayloadBatch = 1,
    QueueIndexBatch = 2,
    AckLedgerBatch = 3,
    Checkpoint = 4,
    Manifest = 5,
}
```

```rust
pub enum RecoveryAction {
    ApplyRecord,
    TruncateSegmentAt(LogOffset),
    StopAtCorruption,
    IgnoreFutureRecord,
}
```

### Bitflags for compact durable flags

Use bitflags for record and segment flags:

```rust
bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct RecordFlags: u16 {
        const COMPRESSED = 1 << 0;
        const HAS_CRC32C = 1 << 1;
        const BATCHED = 1 << 2;
        const RESERVED_FUTURE = 1 << 15;
    }
}
```

Potential flags:

```text
RecordFlags
SegmentFlags
PayloadFlags
QueueIndexFlags
AckLedgerFlags
RecoveryFlags
FlushFlags
```

---

## Initial storage model

PR8 should implement three durable logs.

### 1. Payload log

Stores actual payload bytes.

```text
payload-0000000000000001.seg
payload-0000000000000002.seg
...
```

A payload batch record contains:

```text
base_payload_id
message_count
payload byte lengths
payload bytes
optional per-message metadata
crc32c
```

It returns payload refs:

```rust
pub struct PayloadRef {
    pub segment_id: SegmentId,
    pub offset: LogOffset,
    pub len: u32,
    pub checksum: u32,
}
```

For PR8, `PayloadRef` can point to the batch record plus per-message offset inside the batch. Keep it simple but explicit.

### 2. Queue index log

Maps queue sequence numbers to payload refs.

```text
queue-{queue_id}-index-0000000000000001.seg
```

A queue index entry should minimally contain:

```rust
pub struct QueueIndexEntry {
    pub queue_id: QueueId,
    pub queue_seq: QueueSeq,
    pub payload_ref: PayloadRef,
    pub flags: QueueIndexFlags,
}
```

This is how recovery knows which queue owns which durable messages.

### 3. Ack ledger

Stores settlement events.

```text
queue-{queue_id}-ack-0000000000000001.seg
```

Initial events:

```rust
pub enum AckLedgerEntry {
    AckRange {
        queue_id: QueueId,
        start: QueueSeq,
        len: u32,
    },
    AckMask {
        queue_id: QueueId,
        block_base: QueueSeq,
        word_index: u8,
        mask: u64,
    },
    NackRequeue {
        queue_id: QueueId,
        start: QueueSeq,
        len: u32,
    },
    DeadLetterPlaceholder {
        queue_id: QueueId,
        start: QueueSeq,
        len: u32,
        reason: DeadLetterReason,
    },
}
```

For PR8, delivery state is ephemeral. On recovery:

```text
inflight messages are not restored as inflight.
unacked durable messages become ready again.
acked/dead-lettered messages are not ready.
```

This matches the expected broker behavior after consumer connection loss: unacked messages are redelivered.

---

## Initial record format

Use a fixed-size record header. Keep it aligned and easy to scan.

Suggested header:

```rust
#[repr(C)]
pub struct RecordHeader {
    pub magic: u32,
    pub version: u16,
    pub kind: u16,
    pub flags: u16,
    pub header_len: u16,
    pub body_len: u32,
    pub record_crc32c: u32,
    pub stream_id: u64,
    pub base_seq: u64,
    pub count: u32,
    pub reserved: u32,
}
```

Size: 40 bytes. If we want cache-line alignment, pad to 64 bytes later.

Recommended constants:

```rust
pub const AURUM_RECORD_MAGIC: u32 = 0x4155_524D; // "AURM"
pub const RECORD_VERSION_V0: u16 = 0;
```

### Header fields

```text
magic:
  identifies AurumMQ record

version:
  durable format version

kind:
  PayloadBatch, QueueIndexBatch, AckLedgerBatch, etc.

flags:
  compression/checksum/batch/future bits

header_len:
  allows future extension

body_len:
  number of bytes after header

record_crc32c:
  checksum over header-with-zeroed-crc + body, or body only
  Decide explicitly and test.

stream_id:
  logical log identifier: payload log id, queue id, etc.

base_seq:
  base queue seq or payload seq

count:
  number of logical entries in this record
```

### Endianness

Use little-endian explicitly for all numeric fields.

Do not rely on native-endian layout when writing files.

Even if `RecordHeader` is `repr(C)`, encoding/decoding should use explicit little-endian methods.

---

## Crash safety and recovery rules

### Append rules

A segment is valid up to the last complete, checksum-valid record.

If recovery sees:

```text
valid record
valid record
partial record
```

It truncates at the start of the partial record.

If recovery sees:

```text
valid record
checksum mismatch
```

Default PR8 behavior:

```text
stop and return RecoveryError::CorruptRecord
```

Optional policy:

```text
truncate corrupt tail only if corruption is at final record and configured as repair mode
```

Do not silently skip corrupt records in the middle.

### Durability modes

Initial modes:

```rust
pub enum DurabilityMode {
    MemoryOnly,
    Buffered,
    FsyncOnFlush,
    FsyncEveryBatch,
}
```

PR8 should implement:

```text
Buffered
FsyncOnFlush
```

`FsyncEveryBatch` may be implemented if simple, but it is not required for the first slice.

### Publish confirm semantics in PR8

For integration with `InMemoryBroker`:

```text
MemoryOnly:
  confirm after in-memory queue state transition

Buffered:
  confirm after record is appended to OS buffer

FsyncOnFlush:
  confirm after explicit flush/fsync boundary
```

Do not pretend buffered writes are fully durable. Expose this clearly in `PublishConfirm` metadata or executor config.

---

## Recovery semantics

Initial recovery image:

```rust
pub struct RecoveredQueueImage {
    pub queue_id: QueueId,
    pub next_seq: QueueSeq,
    pub published: Vec<RecoveredQueueEntry>,
    pub acked_ranges: Vec<AckRange>,
    pub acked_masks: Vec<AckMask>,
    pub dead_lettered: Vec<DeadLetterRange>,
}
```

But avoid exposing huge vectors as the final design. This is initial scaffolding.

The recovery builder should be able to produce:

```text
ready messages = queue index entries - acked/dead-lettered
inflight = empty
retry = empty or reconstructed only if persisted explicitly
next_seq = max(queue_seq) + 1
```

For PR8, it is acceptable if recovery reconstructs ready state by replaying all segment records. Later PRs add checkpoints/snapshots to avoid scanning everything.

### Determinism requirement

Given the same segment files, recovery must produce the same `RecoveredQueueImage` every time.

No wall-clock time. No non-deterministic ordering.

---

## Integration with `aurum-broker`

PR8 should not replace the in-memory executor. It should add a storage-backed mode.

Recommended design:

```rust
pub enum BrokerStorageMode<S> {
    MemoryOnly,
    Durable(S),
}
```

or:

```rust
pub struct InMemoryShardExecutor<S = NoopStorage> {
    storage: S,
    // existing fields...
}
```

Where:

```rust
pub trait ShardStorage {
    fn append_publish_batch(&mut self, batch: StoragePublishBatch) -> Result<PayloadRefBatch, StorageError>;
    fn append_ack_batch(&mut self, batch: StorageAckBatch) -> Result<(), StorageError>;
    fn flush(&mut self) -> Result<(), StorageError>;
}
```

For hot path static dispatch:

```rust
InMemoryShardExecutor<NoopStorage>
InMemoryShardExecutor<AppendOnlyStorage<StdFileBackend>>
```

For tests/CLI/cold mode, `Box<dyn ShardStorage>` is acceptable, but not inside the high-throughput executor path.

### Important dependency direction

`aurum-broker` may depend on `aurum-storage`.

`aurum-storage` must not depend on `aurum-broker`.

---

## Slice plan

## Slice 0 — Audit current types and dependency direction

Goal: avoid duplicate IDs and dependency cycles.

Tasks:

```text
1. Inspect QueueId, ShardId, RouteId, QueueSeq, DeliveryTag, PayloadHandle.
2. Decide which storage-facing types live in aurum-types.
3. Ensure aurum-storage depends only on aurum-types and optional internal protocol types.
4. Add compile-time tests or comments for dependency direction.
```

Expected output:

```text
- Short section in PR description: type ownership map.
- No duplicate QueueSeq/PayloadRef definitions across crates.
```

---

## Slice 1 — Record format and codec

Goal: implement durable record encoding/decoding without file I/O.

Files:

```text
record/header.rs
record/kind.rs
record/flags.rs
record/codec.rs
record/checksum.rs
```

Types:

```rust
RecordHeader
RecordKind
RecordFlags
RecordDecodeError
RecordEncodeError
```

Tests:

```text
header roundtrip
invalid magic
unsupported version
unknown kind
truncated header
truncated body
crc mismatch
future flag handling
```

Acceptance:

```text
cargo test -p aurum-storage record
```

---

## Slice 2 — Segment writer/reader with std file backend

Goal: append and scan records from one segment file.

Files:

```text
io/std_file.rs
segment/file.rs
segment/writer.rs
segment/reader.rs
segment/scanner.rs
```

Types:

```rust
SegmentId
LogOffset
SegmentWriter<B>
SegmentReader<B>
SegmentScanResult
```

Initial backend:

```rust
StdFileBackend
```

Operations:

```rust
append_record(kind, stream_id, base_seq, count, body)
flush()
sync_data()
scan_records()
truncate_to(offset)
```

Tests:

```text
append one record
append many records
reopen and scan
truncated tail is detected
valid prefix is recoverable
segment offset increases monotonically
```

Acceptance:

```text
single segment append/read works without payload/queue semantics.
```

---

## Slice 3 — Payload log

Goal: persist payload batches and return payload refs.

Files:

```text
payload/ref.rs
payload/batch.rs
payload/log.rs
```

Types:

```rust
PayloadRef
PayloadBatch
PayloadRefBatch
PayloadLog
```

API:

```rust
impl PayloadLog {
    pub fn append_batch(&mut self, batch: PayloadBatch<'_>) -> Result<PayloadRefBatch, StorageError>;
    pub fn read_payload(&mut self, payload_ref: PayloadRef) -> Result<Vec<u8>, StorageError>;
}
```

PR8 can use `Vec<u8>` for read path. The write path should prefer borrowed slices.

Tests:

```text
append/read one payload
append/read batch
payload refs point to correct bytes
large payload
zero-length payload if allowed or rejected explicitly
crc detects corruption
```

---

## Slice 4 — Queue index log

Goal: persist mapping from queue seq to payload ref.

Files:

```text
queue_index/entry.rs
queue_index/log.rs
```

Types:

```rust
QueueIndexEntry
QueueIndexBatch
QueueIndexLog
QueueIndexFlags
```

API:

```rust
append_queue_index_batch(queue_id, entries)
scan_queue_index(queue_id)
```

Tests:

```text
append index entries
recover index entries
queue ids do not mix
seq order preserved
segment rotation preserves scan order
```

---

## Slice 5 — Ack ledger

Goal: persist settlement events.

Files:

```text
ack_ledger/entry.rs
ack_ledger/log.rs
```

Types:

```rust
AckLedgerEntry
AckLedgerBatch
AckLedgerLog
AckLedgerFlags
```

Initial events:

```text
AckRange
AckMask
NackRequeue
DeadLetterPlaceholder
```

Tests:

```text
append ack range
append ack mask
append nack requeue
append dead-letter placeholder
scan in order
unknown future event handling
```

---

## Slice 6 — Recovery builder

Goal: rebuild deterministic single-node queue image from logs.

Files:

```text
recovery/image.rs
recovery/scanner.rs
recovery/policy.rs
```

Types:

```rust
RecoveredQueueImage
RecoveredMessage
RecoveryPolicy
RecoveryReport
```

Algorithm:

```text
1. Scan payload logs enough to validate payload records.
2. Scan queue index logs and collect queue_seq -> payload_ref.
3. Scan ack ledger and mark settled/dead-lettered seqs.
4. Build ready set = indexed seqs - settled/dead-lettered.
5. Set inflight = empty.
6. Set next_seq = max indexed seq + 1.
7. Emit RecoveryReport.
```

Tests:

```text
recover published messages
recover after ack range
recover after ack mask
recover after nack requeue
recover after dead-letter placeholder
recover after truncated tail
recovery deterministic across repeated runs
```

---

## Slice 7 — Integrate storage with in-memory broker executor

Goal: make PR5 executor optionally durable.

Tasks:

```text
1. Add NoopStorage.
2. Add AppendOnlyShardStorage.
3. Wire publish path:
   payload append -> queue index append -> queue publish state transition -> confirm policy.
4. Wire ack path:
   core ack -> ack ledger append.
5. Wire nack/dead-letter placeholder path.
6. Add executor recovery constructor.
```

Important ordering decision:

For PR8, use this order for durable publish:

```text
append payload batch
append queue index batch
apply in-memory publish
emit confirm according to durability mode
```

For ack:

```text
apply in-memory ack
append ack ledger
emit settlement result according to durability mode
```

Alternative ack order is possible. Document the decision and its crash implications.

Crash implication:

```text
If ack is applied in memory but ack ledger append fails, executor must return error and/or mark queue storage-failed.
```

Do not silently continue after storage append failure.

Recommended state:

```rust
pub enum ShardStorageHealth {
    Healthy,
    Failed(StorageErrorKind),
}
```

Once storage is failed, durable mode should reject further writes until recovery/restart.

---

## Slice 8 — Storage experiment / benchmark

Create:

```text
experiments/h6-storage-engine/
```

Workloads:

```text
append_payload_batches
append_queue_index_batches
append_ack_ranges
publish_ack_recover
crash_tail_recover
```

Metrics:

```text
ns/message buffered
MB/s payload append
records/sec ack ledger
recovery messages/sec
segment scan MB/s
fsync latency if enabled
```

Initial commands:

```bash
cargo run --release -p h6-storage-engine -- \
  --messages=1048576 \
  --payload-bytes=256 \
  --batch=128 \
  --mode=buffered
```

and:

```bash
cargo run --release -p h6-storage-engine -- \
  --messages=1048576 \
  --payload-bytes=256 \
  --batch=128 \
  --mode=fsync-on-flush
```

Do not optimize before correctness tests pass.

---

## Slice 9 — Documentation

Add:

```text
docs/STORAGE_FORMAT_V0.md
docs/STORAGE_RECOVERY_MODEL.md
```

`STORAGE_FORMAT_V0.md` should document:

```text
record header
record kinds
flags
endianness
segment naming
payload ref format
queue index format
ack ledger format
compatibility rules
```

`STORAGE_RECOVERY_MODEL.md` should document:

```text
what is durable
what is ephemeral
what happens to inflight messages after restart
what happens to partially written records
what confirm modes mean
```

---

## Testing matrix

### Unit tests

```text
record codec
segment append/read
payload log
queue index log
ack ledger
recovery image
```

### Integration tests

```text
publish -> restart -> deliver
publish -> ack -> restart -> not deliver acked
publish -> nack requeue -> restart -> deliver
publish -> dead-letter placeholder -> restart -> not ready
partial last record -> restart -> truncate tail
corrupt middle record -> recovery error
```

### Differential tests

Use a simple model:

```text
ModelDurableQueue
  Vec<ModelMessage>
  Vec<ModelAckEvent>
```

Compare against recovered storage image.

### Fuzzing later

Not required for PR8, but leave hooks for:

```text
random record bytes
random segment truncations
random operation sequences
```

---

## Performance guidelines

### Avoid per-message allocations

Payload write path may accept borrowed slices:

```rust
pub struct PayloadBatch<'a> {
    pub payloads: &'a [PayloadSlice<'a>],
}
```

Use `SmallVec` / `ArrayVec` for common small metadata batches.

### Vectored writes

If simple, implement batch body construction with contiguous buffer first.

Later use `write_vectored` for:

```text
header
length table
payload slices
```

Do not overcomplicate PR8. Correct record format first.

### Segment rotation

Implement simple max segment size:

```rust
pub struct SegmentConfig {
    pub max_segment_bytes: u64,
}
```

Default for tests can be small. Production default later can be larger.

### Checksums

Use CRC32C over record body or over header+body. Decide once and document.

Recommendation:

```text
CRC over header with crc field zeroed + body.
```

This catches header corruption too.

---

## Error model

Use enums, not stringly errors.

```rust
pub enum StorageError {
    Io(StorageIoError),
    InvalidRecord(InvalidRecordError),
    CorruptRecord(CorruptRecordError),
    UnsupportedVersion { version: u16 },
    UnknownRecordKind { kind: u16 },
    SegmentFull,
    StorageFailed,
    RecoveryFailed(RecoveryError),
}
```

Use smaller enums inside modules:

```rust
pub enum RecordDecodeError { ... }
pub enum SegmentScanError { ... }
pub enum RecoveryError { ... }
```

Do not panic on corrupt storage in normal APIs.

Panic is acceptable only for internal invariant violations in tests/debug.

---

## Open design decisions to close during PR8

### 1. Queue index before or after in-memory state?

Recommended:

```text
append storage first, then apply in-memory state for durable publish.
```

Reason: avoids exposing unpersisted messages as durable.

### 2. Ack ledger before or after in-memory ack?

Recommended for PR8:

```text
apply core ack, append ledger, fail shard on append failure.
```

Alternative:

```text
append ledger, then apply core ack.
```

This may be cleaner for durability, but if core ack fails after storage append, recovery sees an ack that runtime rejected.

Decision should be based on core error semantics. Document it.

### 3. Persist nacks?

For PR8:

```text
Nack requeue does not need to persist if recovery treats all unacked as ready.
```

But if redelivery count matters across restart, we need a ledger event.

Recommended:

```text
Persist NackRequeue only if it changes durable metadata such as delivery_count.
Otherwise defer exact redelivery count persistence.
```

### 4. Persist delivery attempts?

Not in PR8 unless already easy.

Initial rule:

```text
consumer delivery state is ephemeral.
After restart, unacked messages are redelivered.
```

### 5. Payload dedup/fanout

PR8 should allow payload refs to be reused by multiple queue index entries, but does not need full fanout optimization yet.

---

## Acceptance criteria

PR8 is closed when:

```text
1. aurum-storage has record codec, segment writer/reader, payload log, queue index log, ack ledger, and recovery builder.
2. Record format is documented in STORAGE_FORMAT_V0.md.
3. Recovery model is documented in STORAGE_RECOVERY_MODEL.md.
4. Append-only storage can persist publish batches.
5. AckRange/AckMask are persisted and recovered.
6. Recovery rebuilds ready messages deterministically.
7. Truncated tail record is handled safely.
8. Corrupt middle record returns a recovery error.
9. InMemoryShardExecutor can run with NoopStorage and AppendOnlyStorage.
10. aurum-core still does not depend on aurum-storage.
11. aurum-storage does not depend on aurum-core, protocols, or broker.
12. cargo test --workspace passes.
13. h6-storage-engine experiment runs and reports basic throughput/recovery metrics.
```

---

## Recommended implementation order

Do not implement integration first.

Recommended order:

```text
1. Record format.
2. Segment append/read/scan.
3. Payload log.
4. Queue index log.
5. Ack ledger.
6. Recovery builder.
7. Broker integration.
8. Storage experiment.
9. Documentation polish.
```

This keeps the storage engine testable before it touches broker execution.

---

## Final architectural note

PR8 should preserve the same architecture principle used since PR1:

```text
core does queue semantics.
storage persists durable facts.
protocols translate bytes to commands.
routing translates routes to queue targets.
broker composes the planes.
```

The storage engine must be append-only, batch-first, deterministic under recovery, and independent from protocol details. This is the storage foundation that later replication, clustering, snapshots, compaction, and cloud/on-prem deployment will rely on.
