# AurumMQ — PR3 Plan: Rabbit-like Delivery Semantics in `aurum-core`

This document defines the complete implementation plan for PR3 of `crates/hot-path/aurum-core`.

PR1/PR2 established the first production-shaped queue engine direction:

```text
HybridRangeBlockQueue
├── sequential ready path -> DeliveryRange
├── sparse/retry path     -> DeliveryMask
├── inflight/acked/retry  -> block-level bitsets
├── active block lists    -> sparse/retry/dirty blocks
└── model-based testing   -> correctness wall
```

PR3 adds the missing Rabbit-like delivery semantics **inside the core**, but still **without** implementing AMQP parsing, storage, networking, runtime, clustering, or persistence.

The purpose of PR3 is to bridge this gap:

```text
Optimized queue engine:
  QueueSeq / DeliveryRange / DeliveryMask / AckRange / AckMask

Rabbit-like broker semantics:
  ConsumerSession / prefetch / delivery_tag / ack / nack / reject / requeue / redelivery
```

The result should be a clean internal delivery layer that an AMQP adapter can use later without contaminating `aurum-core` with AMQP types.

---

## 0. Executive summary

PR3 implements:

```text
ConsumerSession
ConsumerCredit / prefetch
DeliveryTag generation
DeliveryWindow
Ack one
Ack multiple
Nack one
Nack multiple
Reject
Requeue
Redelivery flag
Delivery attempt accounting
Consumer cancel / disconnect handling
Dead-letter decision placeholder
Model-based tests for Rabbit-like semantics
Benchmarks for ack/nack/session overhead
```

PR3 must preserve the core architectural rule:

```text
Protocol adapters may receive per-message commands.
The queue engine must receive ranges/masks/batches.
```

So PR3 introduces a layer like this:

```text
Protocol adapter later
  receives delivery_tag / multiple / requeue
        ↓
ConsumerSession
  delivery_tag -> DeliveredSegment -> AckRange/AckMask/NackRange/NackMask
        ↓
HybridRangeBlockQueue
  applies ranges/masks
```

PR3 is not about AMQP wire compatibility yet. It is about implementing the internal semantics required to support AMQP correctly later.

---

## 1. Current state assumed by this plan

Current `aurum-core` has a queue engine in:

```text
crates/hot-path/aurum-core/src/queue.rs
```

It currently exposes or internally has:

```text
HybridRangeBlockQueue
MsgBlock
DeliveryWork
DeliveryRange
DeliveryMask
deliver(max_messages, out)
ack_work(work)
nack_work_to_retry(work)
retry_all_now()
ack_id(seq)
```

The queue engine already uses:

```text
sequential_head / sequential_tail
block-level inflight bitsets
block-level acked bitsets
block-level retry bitsets
block-level sparse_ready bitsets
active sparse/retry block lists
```

PR3 must not undo that work.

---

## 2. Scope

### 2.1 Included in PR3

PR3 includes:

```text
- Internal consumer/session model.
- Delivery tag generation.
- Delivery tag to queue-work mapping.
- Prefetch/credit accounting.
- Ack individual.
- Ack multiple.
- Nack individual.
- Nack multiple.
- Reject as a special case of nack one.
- Requeue true/false behavior.
- Redelivery flag propagation.
- Delivery attempt count plumbing.
- Consumer cancellation handling.
- Dead-letter placeholder, not full DLX routing.
- Model queue/session tests.
- Deterministic and randomized differential tests.
- Benchmarks for session overhead.
```

### 2.2 Explicitly out of scope

PR3 must not implement:

```text
- AMQP frame parsing.
- AMQP connection/channel state machine.
- Native protocol frame parsing.
- Persistent ack ledger.
- Segment log storage.
- DLX exchange routing.
- Delayed retry timing wheel.
- io_uring.
- nio/glommio/monoio runtime.
- TCP transport.
- Clustering.
- Replication.
- NUMA placement.
- Kubernetes/operator.
```

PR3 may define types that future layers will use, but it must not implement those layers.

---

## 3. Non-negotiable design rules

### 3.1 No protocol types in `aurum-core`

Do not import or mention:

```text
AMQPFrame
BasicAck
BasicNack
BasicReject
ChannelFrame
NativeFrame
KafkaRecordBatch
HTTP request/response types
```

Instead, define protocol-neutral types:

```rust
AckRequest
NackRequest
RejectRequest
CreditUpdate
ConsumerSession
DeliveryTag
```

The later AMQP adapter maps:

```text
basic.ack(tag, multiple)
        ↓
AckRequest { tag, mode }
```

### 3.2 Hot path uses concrete types, generics, or enums, not `dyn Trait`

Allowed in hot path:

```text
- concrete structs
- enums
- const generics
- generic type parameters when monomorphization is useful
- bitflags/newtype flags
```

Not allowed in hot path:

```text
- Box<dyn Trait>
- Arc<dyn Trait>
- callback trait objects
- dynamic backend calls per delivery/ack/nack
```

If several hot-path strategies are needed, prefer one of these:

```rust
// Option A: enum dispatch, if chosen at runtime but finite.
pub enum DeliveryWindowBackend {
    Segment(SegmentDeliveryWindow),
    FixedRing(FixedRingDeliveryWindow),
}

// Option B: static dispatch, if chosen at compile time.
pub struct ConsumerSession<W: DeliveryWindowOps> {
    window: W,
}
```

Use dynamic dispatch only in cold/control paths, for example:

```text
- observability sinks
- debug hooks
- test or simulation adapters
- future plugin callbacks
- admin inspection providers
```

### 3.3 Enums model state transitions

Use enums for domain states and transition results:

```rust
pub enum DeliveryDisposition {
    Delivered,
    Redelivered,
}

pub enum NackDisposition {
    Requeue,
    Drop,
    DeadLetter,
}

pub enum AckMode {
    One,
    Multiple,
}

pub enum NackMode {
    One,
    Multiple,
}
```

Avoid boolean soup in public/internal APIs:

```rust
// Bad in core API
fn nack(tag: u64, multiple: bool, requeue: bool);

// Better
fn nack(request: NackRequest);
```

### 3.4 Bitflags model compact flags

Use bitflags for compact internal flags where combinations are valid:

```rust
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DeliveryFlags: u16 {
        const REDELIVERED = 1 << 0;
        const PERSISTENT  = 1 << 1; // future storage integration
    }
}
```

Suggested flag groups:

```text
DeliveryFlags:
  REDELIVERED
  MAY_REQUEUE
  RESERVED_*

ConsumerFlags:
  CANCELLED
  BLOCKED
  AUTO_ACK_ALLOWED_LATER

WindowSegmentFlags:
  REDELIVERED
  HAS_HOLES
  DEAD_LETTER_CANDIDATE
```

Do not use bitflags where a closed set of mutually exclusive states is better represented by an enum.

### 3.5 Queue engine remains range/mask-first

The session layer may receive `DeliveryTag`, but it must convert to:

```text
AckRange
AckMask
NackRange
NackMask
```

before touching the queue engine.

The queue engine must not become:

```text
HashMap<DeliveryTag, Message>
LinkedList<Message>
VecDeque<MessageNode>
```

---

## 4. Target module structure

Refactor or extend `aurum-core` toward this structure:

```text
crates/hot-path/aurum-core/src/
  lib.rs

  queue/
    mod.rs
    constants.rs
    block.rs
    lists.rs
    state.rs
    work.rs
    error.rs
    hybrid.rs
    publish.rs
    delivery.rs
    ack.rs
    nack.rs
    retry.rs
    invariants.rs
    model.rs

  consumer/
    mod.rs
    id.rs
    flags.rs
    credit.rs
    tag.rs
    window.rs
    segment.rs
    session.rs
    ack.rs
    nack.rs
    cancel.rs
    model.rs
    invariants.rs

  tests/
    // optional only if using in-module test organization
```

If the project is still early, it is acceptable to keep this as fewer files initially, but the public module boundary should be clear:

```rust
pub mod queue;
pub mod consumer;
```

Export only stable surface types from `lib.rs`:

```rust
pub use queue::{HybridRangeBlockQueue, DeliveryBatch, QueueCoreError};
pub use consumer::{ConsumerSession, ConsumerCredit, DeliveryTag, AckRequest, NackRequest};
```

---

## 5. Data model

### 5.1 Core IDs

Prefer small transparent newtypes over raw integers:

```rust
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerId(pub u64);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelId(pub u32);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryTag(pub u64);
```

Rabbit-like rule:

```text
DeliveryTag is monotonic per channel/session.
DeliveryTag is not equal to QueueSeq.
```

This distinction is important because:

```text
- one queue seq may be redelivered with a new delivery tag;
- delivery tag scope is session/channel-local;
- ack multiple is defined over delivery tags, not queue seqs;
- future AMQP compatibility depends on tag semantics.
```

### 5.2 Consumer credit

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerCredit {
    prefetch: u32,
    in_flight: u32,
}
```

Invariants:

```text
0 <= in_flight <= prefetch
available = prefetch - in_flight
prefetch == 0 means either unlimited or disabled depending on selected policy
```

Decision for PR3:

```text
Use explicit policy for prefetch=0.
Do not hard-code AMQP behavior invisibly.
```

Suggested enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchMode {
    Limited(u32),
    Unlimited,
}
```

For PR3 tests, use `Limited(n)` only. Add `Unlimited` after deterministic tests are stable.

### 5.3 Delivery references

A delivery from the queue engine is compact:

```rust
pub enum DeliveredRef {
    Range(DeliveryRange),
    Mask(DeliveryMask),
}
```

A `ConsumerSession` assigns delivery tags to delivered refs.

The session must preserve this mapping:

```text
DeliveryTag -> QueueSeq(s) / DeliveryMask bits
```

without forcing the queue engine into message-by-message processing.

### 5.4 Delivery segments

Use segments in the delivery window:

```rust
pub enum DeliveredSegment {
    Range(RangeSegment),
    Mask(MaskSegment),
}
```

Range segment:

```rust
pub struct RangeSegment {
    pub first_tag: DeliveryTag,
    pub start_seq: Seq,
    pub len: u32,
    pub flags: SegmentFlags,
}
```

Mask segment:

```rust
pub struct MaskSegment {
    pub first_tag: DeliveryTag,
    pub block: BlockIndex,
    pub word: WordIndex,
    pub original_mask: u64,
    pub remaining_rank_mask: u64,
    pub count: u8,
    pub flags: SegmentFlags,
}
```

Why `remaining_rank_mask` instead of mutating only `original_mask`?

A `DeliveryMask` may be non-contiguous:

```text
original message-bit mask: bits {1, 5, 7}
delivery tags assigned:   tag 10 -> bit 1, tag 11 -> bit 5, tag 12 -> bit 7
```

If tag 11 is acked first and removed, rank-based mapping must not shift tag 12. Therefore the segment must preserve original delivery-rank positions. `remaining_rank_mask` tracks which original delivery positions remain unacked.

For full-segment ack/nack, convert the whole `original_mask` or remaining positions into an `AckMask`/`NackMask`.

For partial prefix ack, take the lowest remaining ranks up to the requested tag.

For individual ack, clear exactly one rank position.

### 5.5 Segment flags

Use bitflags:

```rust
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SegmentFlags: u16 {
        const REDELIVERED = 1 << 0;
        const HAS_HOLES   = 1 << 1;
    }
}
```

`HAS_HOLES` is set when an individual ack/nack removes an item from the middle of a segment.

---

## 6. DeliveryWindow design

### 6.1 Responsibility

`DeliveryWindow` owns the mapping from session-local delivery tags to delivered queue work.

It must support:

```text
insert delivered ranges/masks
ack one tag
ack multiple tags <= tag
nack one tag
nack multiple tags <= tag
cancel all unacked
count unacked
release credit count
```

### 6.2 Data structure choice

For PR3, use a segment deque:

```rust
pub struct DeliveryWindow {
    segments: VecDeque<DeliveredSegment>,
    unacked_count: u32,
}
```

This is acceptable because:

```text
- prefetch bounds the number of unacked messages;
- segments are batch-shaped, not one node per message;
- the common path appends at tail and removes/acks from prefix;
- ack multiple is prefix-oriented;
- individual out-of-order ack can split or mark holes.
```

Later optimization alternatives:

```text
FixedRingDeliveryWindow:
  fixed-capacity ring for low allocation.

SegmentVecDeliveryWindow:
  Vec + head index if pop_front cost matters.

TaggedIndexWindow:
  optional side index for large prefetch and random ack.
```

### 6.3 Dispatch strategy for window backends

Do not use `dyn DeliveryWindow` in hot path.

Use static dispatch when experimenting:

```rust
pub struct ConsumerSession<W = SegmentDeliveryWindow> {
    window: W,
    // ...
}
```

or enum dispatch if runtime selection is needed:

```rust
pub enum DeliveryWindowStorage {
    Segment(SegmentDeliveryWindow),
    FixedRing(FixedRingDeliveryWindow),
}
```

For PR3, implement only:

```rust
SegmentDeliveryWindow
```

but leave naming and APIs compatible with future replacement.

### 6.4 Avoid per-message HashMap

Do not use:

```rust
HashMap<DeliveryTag, QueueSeq>
```

in the hot path by default.

If a future benchmark proves large-prefetch random ack is expensive, add a side index behind an enum backend:

```rust
pub enum DeliveryWindowStorage {
    Segment(SegmentDeliveryWindow),
    Indexed(IndexedDeliveryWindow),
}
```

But do not start there.

---

## 7. ConsumerSession design

### 7.1 Struct shape

```rust
pub struct ConsumerSession<W = SegmentDeliveryWindow> {
    id: ConsumerId,
    channel_id: ChannelId,
    next_tag: u64,
    credit: ConsumerCredit,
    window: W,
    flags: ConsumerFlags,
}
```

### 7.2 Consumer flags

```rust
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ConsumerFlags: u16 {
        const CANCELLED = 1 << 0;
        const BLOCKED   = 1 << 1;
    }
}
```

### 7.3 Responsibilities

`ConsumerSession` should:

```text
- enforce credit before delivery;
- assign delivery tags;
- insert delivered refs into DeliveryWindow;
- answer AckRequest/NackRequest;
- convert delivery-tag operations into queue-engine batches;
- release credit after ack/nack/reject/cancel;
- surface delivery metadata such as redelivered flag;
- avoid protocol-specific semantics in its API names.
```

### 7.4 What it must not do

`ConsumerSession` must not:

```text
- parse AMQP methods;
- know about exchange/queue binding;
- write ack ledger records;
- perform network I/O;
- call async runtime;
- allocate per message in normal delivery;
- use dynamic dispatch in hot methods.
```

---

## 8. Request/result types

### 8.1 Ack request

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckRequest {
    pub tag: DeliveryTag,
    pub mode: AckMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    One,
    Multiple,
}
```

### 8.2 Nack request

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackRequest {
    pub tag: DeliveryTag,
    pub mode: NackMode,
    pub disposition: NackDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackMode {
    One,
    Multiple,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackDisposition {
    Requeue,
    Drop,
    DeadLetter,
}
```

For PR3:

```text
Requeue -> queue.nack_*_to_retry or sparse_ready depending policy
Drop    -> mark dead/drop placeholder
DeadLetter -> mark dead-letter placeholder, no DLX routing yet
```

### 8.3 Reject request

Reject is equivalent to nack one:

```rust
pub struct RejectRequest {
    pub tag: DeliveryTag,
    pub disposition: NackDisposition,
}
```

Validation:

```text
RejectRequest must not have Multiple.
```

### 8.4 Apply result

Use explicit results:

```rust
pub struct AckApplyResult {
    pub acked: u32,
    pub released_credit: u32,
}

pub struct NackApplyResult {
    pub nacked: u32,
    pub requeued: u32,
    pub dropped: u32,
    pub dead_lettered: u32,
    pub released_credit: u32,
}
```

### 8.5 Errors

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryError {
    InvalidDeliveryTag,
    DeliveryTagAlreadySettled,
    ConsumerCancelled,
    InsufficientCredit,
    QueueInvariantViolation,
}
```

Do not panic for invalid delivery tags in release code. Return errors. Debug assertions are allowed for internal invariants.

---

## 9. Ack semantics

### 9.1 Ack one

```text
ack(tag, One)
```

Behavior:

```text
1. Find the segment containing tag.
2. Remove exactly that tag from DeliveryWindow.
3. Convert the mapped message to AckRange or AckMask.
4. Apply to HybridRangeBlockQueue.
5. Release one credit.
6. Return AckApplyResult { acked: 1, released_credit: 1 }.
```

For a `RangeSegment`, ack one may:

```text
- remove whole segment if len == 1;
- trim front if tag == first_tag;
- trim back if tag == last_tag;
- split into two segments if tag is in the middle;
- or convert to a small hole-tracking form if that proves faster later.
```

For PR3, splitting is acceptable because random individual ack is not the primary hot path.

For a `MaskSegment`, ack one:

```text
- compute delivery-rank offset = tag - first_tag;
- check that rank is still unacked in remaining_rank_mask;
- map rank to message bit in original_mask;
- clear rank from remaining_rank_mask;
- emit AckMask with a single message bit;
- remove segment if remaining_rank_mask == 0.
```

### 9.2 Ack multiple

```text
ack(tag, Multiple)
```

Behavior:

```text
1. Ack all unacked delivery tags <= tag in that ConsumerSession.
2. Emit compact AckRange/AckMask batches.
3. Remove fully acknowledged prefix segments.
4. Partially update the segment containing tag if needed.
5. Release credit equal to acked count.
```

This is the most important Rabbit-like fast path.

Optimization rule:

```text
Full RangeSegment -> one AckRange.
Full MaskSegment  -> one AckMask.
Partial range     -> one AckRange + segment trim.
Partial mask      -> one AckMask built from prefix ranks + segment update.
```

Never loop over all messages if a range/mask can represent the operation.

### 9.3 Ack batches

`ConsumerSession` should be able to build an internal batch:

```rust
pub struct AckBatch {
    pub ranges: Vec<AckRange>,
    pub masks: Vec<AckMask>,
}
```

Initially this can use `Vec`.

Later replace with:

```text
SmallVec<[AckRange; 8]>
SmallVec<[AckMask; 16]>
```

or `arrayvec` if no heap allocation is desired in the hot path.

### 9.4 Invalid/double ack

Policy decision for PR3:

```text
Invalid tag -> DeliveryError::InvalidDeliveryTag.
Double ack  -> DeliveryError::InvalidDeliveryTag or DeliveryTagAlreadySettled.
```

Do not silently ignore invalid tags in core tests. Protocol adapters can map errors to protocol-specific behavior later.

---

## 10. Nack/reject semantics

### 10.1 Nack one

```text
nack(tag, One, Requeue)
```

Behavior:

```text
1. Remove tag from DeliveryWindow.
2. Convert to NackRange/NackMask.
3. Apply to queue engine.
4. Mark message as redelivery candidate.
5. Release credit.
```

For PR3, `Requeue` may map to:

```text
inflight -> retry -> retry_all_now -> sparse_ready
```

or directly:

```text
inflight -> sparse_ready
```

depending on the current queue engine API.

Recommended PR3 decision:

```text
Use retry path for requeue.
Add a simple `requeue_now` helper only if needed for cleaner semantics.
```

### 10.2 Nack multiple

```text
nack(tag, Multiple, disposition)
```

Same prefix behavior as ack multiple, but applies nack disposition.

Must compact into:

```text
NackRange / NackMask
```

where possible.

### 10.3 Reject

```text
reject(tag, disposition)
```

Equivalent to:

```text
nack(tag, One, disposition)
```

but exists as a separate core request type because protocols expose it separately.

### 10.4 Requeue false

For PR3:

```text
NackDisposition::Drop       -> mark dead/dropped placeholder.
NackDisposition::DeadLetter -> mark dead-letter placeholder.
```

Full DLX routing is out of scope, but the core should distinguish:

```text
dropped
vs
dead-letter candidate
```

because later storage/routing/AMQP will need different events.

### 10.5 Redelivery

When a message is requeued and delivered again:

```text
- DeliveryFlags::REDELIVERED must be set.
- delivery_attempt_count should increment.
```

If the current `MsgBlock` does not yet store delivery counts, PR3 may introduce a minimal side structure:

```rust
pub struct DeliveryAttempts {
    // initially Vec<u16> or block-local arrays
}
```

But avoid adding a heavy per-message object.

Recommended PR3 compromise:

```text
Add redelivery tracking before full delivery_count.
Use a block-level redelivered bitset if count is not needed yet.
```

Future:

```text
block.delivery_count: [u16; MSGS_PER_BLOCK]
```

only if retry/DLQ policy needs exact counts in the hot path.

---

## 11. Consumer cancel/disconnect semantics

When a consumer/session is cancelled or disconnected:

```text
all unacked deliveries must be settled by policy
```

Initial policy:

```text
requeue all unacked
mark as redelivery candidates
release all credit
clear DeliveryWindow
mark session cancelled
```

API:

```rust
pub enum CancelDisposition {
    RequeueUnacked,
    DropUnacked,
    DeadLetterUnacked,
}

pub fn cancel(&mut self, disposition: CancelDisposition, queue: &mut HybridRangeBlockQueue)
    -> CancelResult;
```

For PR3, only `RequeueUnacked` is required.

Tests must prove:

```text
- all unacked are no longer inflight;
- credit returns to zero in_flight;
- redelivery occurs on next delivery;
- ack after cancel fails;
- second cancel is idempotent or returns a clear error by policy.
```

---

## 12. Delivery path

### 12.1 Deliver with credit

`ConsumerSession` should not ask the queue for more than available credit.

```rust
pub fn deliver_from_queue(
    &mut self,
    queue: &mut HybridRangeBlockQueue,
    max_messages: u32,
    out: &mut SessionDeliveryBatch,
) -> Result<u32, DeliveryError>;
```

Algorithm:

```text
1. If cancelled, return ConsumerCancelled.
2. available = min(max_messages, credit.available()).
3. If available == 0, return 0.
4. queue.deliver(available, delivery_work).
5. Assign tags to each delivered range/mask.
6. Insert segments into DeliveryWindow.
7. Reserve credit equal to delivered count.
8. Return a protocol-neutral delivery batch with tag information.
```

### 12.2 Session delivery batch

```rust
pub struct SessionDeliveryBatch {
    pub segments: Vec<TaggedDeliverySegment>,
}

pub enum TaggedDeliverySegment {
    Range {
        first_tag: DeliveryTag,
        range: DeliveryRange,
        flags: DeliveryFlags,
    },
    Mask {
        first_tag: DeliveryTag,
        mask: DeliveryMask,
        count: u8,
        flags: DeliveryFlags,
    },
}
```

This is not a protocol frame. It is the internal representation from which AMQP/native adapters can serialize deliveries later.

### 12.3 Tag assignment for masks

For a `DeliveryMask` with count `n`:

```text
first_tag = next_tag
assigned tags = first_tag..first_tag+n-1
```

The order of bits in a mask is:

```text
low bit to high bit
```

This must be documented and tested.

---

## 13. Backend strategy: static vs dynamic dispatch

### 13.1 Hot backends

Potential future hot backends:

```text
SegmentDeliveryWindow
FixedRingDeliveryWindow
IndexedDeliveryWindow
```

Do not use `dyn Trait` per operation.

Acceptable strategies:

#### Static generic backend

```rust
pub struct ConsumerSession<W: DeliveryWindowOps = SegmentDeliveryWindow> {
    window: W,
}
```

Pros:

```text
- monomorphization;
- inlining;
- no vtable;
- best for benchmarks.
```

Cons:

```text
- more generic code;
- potential compile-time/code-size growth.
```

#### Enum backend

```rust
pub enum ConsumerWindow {
    Segment(SegmentDeliveryWindow),
    FixedRing(FixedRingDeliveryWindow),
}
```

Pros:

```text
- finite strategy set;
- no vtable;
- runtime selectable;
- easy to debug.
```

Cons:

```text
- match dispatch per method;
- less inlining than pure generics.
```

PR3 decision:

```text
Implement `ConsumerSession<W = SegmentDeliveryWindow>` using generics.
Do not introduce enum backend until a second backend exists.
```

### 13.2 Cold backends

Dynamic dispatch is allowed for:

```text
- event sinks;
- instrumentation;
- model observers;
- debug tracers;
- future plugin hooks;
- policy evaluators not called per message.
```

Example:

```rust
pub trait DeliveryObserver {
    fn on_ack(&self, result: &AckApplyResult);
}
```

But this must not be called per message in the main hot loop unless behind sampling/debug features.

---

## 14. Bitflags dependency policy

The project currently may not have `bitflags` in workspace dependencies.

PR3 should add it if we implement flags with the crate:

```toml
[workspace.dependencies]
bitflags = "2"
```

Then in `aurum-core/Cargo.toml`:

```toml
bitflags.workspace = true
```

Use bitflags for compact internal metadata only.

Do not overuse bitflags where enums are clearer.

Recommended split:

```text
Enums:
  AckMode
  NackMode
  NackDisposition
  CancelDisposition
  DeliveryWindowOpResult
  ConsumerState

Bitflags:
  DeliveryFlags
  SegmentFlags
  ConsumerFlags
```

---

## 15. Model-based testing plan

PR3 must extend the PR2 model to include consumer/session behavior.

### 15.1 ModelConsumerSession

```rust
pub struct ModelConsumerSession {
    next_tag: u64,
    prefetch: u32,
    in_flight: u32,
    unacked: VecDeque<ModelDelivery>,
}
```

A model delivery can be simple:

```rust
pub struct ModelDelivery {
    pub tag: DeliveryTag,
    pub seq: Seq,
    pub redelivered: bool,
}
```

The model can be message-by-message because it is not optimized.

### 15.2 Differential comparisons

Compare:

```text
ModelQueue + ModelConsumerSession
vs
HybridRangeBlockQueue + ConsumerSession
```

After each operation compare:

```text
queue counts
inflight count
acked count
retry count
ready count
unacked tag set
credit state
redelivery flags for next delivery
invalid tag behavior
```

### 15.3 Deterministic test cases

Required tests:

```text
1. prefetch_limits_delivery
2. ack_one_releases_credit
3. ack_multiple_prefix
4. ack_multiple_partial_range
5. ack_multiple_partial_mask
6. ack_one_middle_of_range_splits_segment
7. nack_one_requeue_redelivers
8. nack_multiple_requeue
9. reject_requeue_false_marks_dead_placeholder
10. cancel_requeues_all_unacked
11. invalid_delivery_tag_errors
12. double_ack_errors
13. redelivery_assigns_new_delivery_tag
14. delivery_tag_monotonic_per_session
15. delivery_tag_scope_is_session_local
```

### 15.4 Randomized operations

Define random ops:

```rust
pub enum SessionOp {
    Publish { count: u16 },
    Deliver { max: u16 },
    AckRandomOne,
    AckMultipleRandomTag,
    NackRandomOneRequeue,
    NackMultipleRequeue,
    RetryAllNow,
    CancelAndRecreateConsumer,
}
```

Run thousands of deterministic-seed sequences.

Use a simple xorshift RNG first to avoid dependencies.

Later add `proptest`.

---

## 16. Benchmarks for PR3

Add or extend experiment:

```text
experiments/h1-queue-engine
```

or create:

```text
experiments/h2-consumer-session
```

Recommended new experiment:

```text
experiments/h2-consumer-session
```

Benchmarks:

```text
1. deliver_ack_one:
   deliver batch, ack individual one by one.

2. deliver_ack_multiple:
   deliver batch, ack multiple at last tag.

3. deliver_nack_requeue:
   deliver batch, nack all requeue, retry_all_now, deliver again.

4. random_ack_with_prefetch_window:
   prefetch 128/1024, random ack inside current window.

5. cancel_requeue:
   deliver up to prefetch, cancel, redeliver.
```

Metrics:

```text
ns/message
cycles/message via perf
instructions/message
branch-misses/message
LLC-load-misses/message
allocations if allocator instrumentation is available
```

Important benchmark rule:

```text
Do not compare only queue engine anymore.
Measure queue engine + ConsumerSession overhead.
```

---

## 17. Event surface for future storage

PR3 does not implement storage, but it should avoid making storage impossible.

The session/queue operations should be able to produce logical events later:

```rust
pub enum CoreEvent {
    DeliveredRange { range: DeliveryRange },
    DeliveredMask { mask: DeliveryMask },
    AckedRange { range: AckRange },
    AckedMask { mask: AckMask },
    NackedRange { range: NackRange, disposition: NackDisposition },
    NackedMask { mask: NackMask, disposition: NackDisposition },
    RequeuedRange { /* later */ },
    RequeuedMask { /* later */ },
    DeadLetterCandidate { /* later */ },
}
```

PR3 may not need to expose `CoreEvent` yet. But design APIs so this can be added without rewriting the session layer.

---

## 18. Error policy and protocol mapping

Core errors are not protocol errors.

Example:

```text
Core:
  DeliveryError::InvalidDeliveryTag

AMQP adapter later:
  maps to channel exception / protocol-specific behavior
```

So PR3 should define clear core errors and not guess AMQP behavior yet.

Recommended errors:

```rust
pub enum ConsumerError {
    InvalidDeliveryTag,
    DeliveryTagAlreadySettled,
    ConsumerCancelled,
    InsufficientCredit,
    EmptyDelivery,
    InternalInvariantViolation,
}
```

Use `Result<T, ConsumerError>` for request handling.

Do not panic for user/protocol-originated invalid inputs.

---

## 19. Implementation slices

### Slice 1 — Types and module skeleton

Files:

```text
src/consumer/mod.rs
src/consumer/id.rs
src/consumer/flags.rs
src/consumer/credit.rs
src/consumer/tag.rs
src/consumer/segment.rs
src/consumer/window.rs
src/consumer/session.rs
src/consumer/error.rs
```

Implement:

```text
ConsumerId
ChannelId
DeliveryTag
AckMode
NackMode
NackDisposition
CancelDisposition
DeliveryFlags
SegmentFlags
ConsumerFlags
ConsumerError
```

Acceptance:

```bash
cargo check -p aurum-core
```

### Slice 2 — ConsumerCredit

Implement:

```rust
ConsumerCredit::limited(prefetch)
ConsumerCredit::available()
ConsumerCredit::reserve(n)
ConsumerCredit::release(n)
ConsumerCredit::in_flight()
ConsumerCredit::prefetch()
```

Tests:

```text
reserve cannot exceed available
release cannot underflow
prefetch zero policy is explicit
```

### Slice 3 — DeliveredSegment and DeliveryWindow

Implement:

```text
RangeSegment
MaskSegment
DeliveredSegment
SegmentDeliveryWindow
```

Core methods:

```rust
push_range(first_tag, DeliveryRange, flags)
push_mask(first_tag, DeliveryMask, flags)
ack_one(tag) -> AckBatch
ack_multiple(tag) -> AckBatch
nack_one(tag, disposition) -> NackBatch
nack_multiple(tag, disposition) -> NackBatch
drain_all(disposition) -> NackBatch
unacked_count()
```

Tests:

```text
range full ack
range partial prefix ack
range middle ack split
mask full ack
mask partial prefix ack
mask individual ack preserves tag-to-bit mapping
invalid tag errors
```

### Slice 4 — Ack/Nack batch types

If not already in `aurum-types`, add protocol-neutral core batch types:

```rust
AckRange
AckMask
AckBatch
NackRange
NackMask
NackBatch
```

Avoid naming them AMQP-specific.

Initial implementation can mirror `DeliveryRange` and `DeliveryMask`.

### Slice 5 — ConsumerSession delivery path

Implement:

```rust
deliver_from_queue(queue, max, out)
```

Behavior:

```text
uses credit
calls queue.deliver
assigns tags
inserts segments
returns tagged delivery batch
```

Tests:

```text
prefetch limits delivery
credit reserved after delivery
tags monotonic
mask delivery assigns consecutive tags in low-bit order
```

### Slice 6 — ConsumerSession ack path

Implement:

```rust
ack(AckRequest, queue) -> AckApplyResult
```

Behavior:

```text
DeliveryWindow -> AckBatch -> HybridRangeBlockQueue
release credit
```

Tests:

```text
ack one range
ack multiple range
ack one mask
ack multiple mask
invalid tag
double ack
```

### Slice 7 — ConsumerSession nack/reject path

Implement:

```rust
nack(NackRequest, queue) -> NackApplyResult
reject(RejectRequest, queue) -> NackApplyResult
```

For PR3:

```text
Requeue -> retry path
Drop/DeadLetter -> placeholder/dead state if available, or tracked result if not yet represented in queue.
```

If `HybridRangeBlockQueue` cannot yet represent dead/drop, add a minimal bitset/state or return a `DeadLetterCandidateBatch` for future handling.

Do not silently ack dropped messages; represent the disposition.

### Slice 8 — Cancel/disconnect

Implement:

```rust
cancel(CancelDisposition, queue) -> CancelResult
```

Required first mode:

```text
RequeueUnacked
```

Tests:

```text
cancel requeues all unacked
credit released
redelivery occurs
ack after cancel errors
```

### Slice 9 — ModelConsumerSession and differential tests

Add model tests comparing optimized and model session behavior.

Acceptance:

```bash
cargo test -p aurum-core
```

### Slice 10 — Benchmarks

Add:

```text
experiments/h2-consumer-session
```

or extend H1 if keeping fewer experiments.

Required workloads:

```text
deliver_ack_one
deliver_ack_multiple
deliver_nack_requeue
random_ack_prefetch_128
random_ack_prefetch_1024
cancel_requeue
```

---

## 20. Core invariants

Add invariant checks under debug/test builds.

### 20.1 ConsumerSession invariants

```text
next_tag is always > all tags ever issued by this session.
unacked_count == credit.in_flight.
credit.in_flight <= prefetch.
window tags are strictly increasing by segment.
segments do not overlap in tag range.
no segment has zero live deliveries.
cancelled consumers cannot deliver new messages.
```

### 20.2 Queue/session consistency invariants

After delivery:

```text
delivered messages are inflight in queue.
window contains exactly delivered count.
credit.in_flight increased by delivered count.
```

After ack:

```text
acked messages are no longer inflight.
acked messages are not redeliverable.
window no longer contains acked tags.
credit released equals acked count.
```

After nack requeue:

```text
nacked messages are no longer inflight.
nacked messages are retry/sparse-ready candidates.
window no longer contains nacked tags.
credit released equals nacked count.
redelivery flag will be set on next delivery.
```

After cancel:

```text
window is empty.
credit.in_flight == 0.
all previous unacked messages were requeued/dropped/dead-lettered by selected policy.
```

---

## 21. Performance constraints

PR3 is a semantics PR, but it must not destroy H1.

Constraints:

```text
- No per-message heap allocation in normal range delivery.
- No HashMap in default DeliveryWindow.
- No dyn dispatch in delivery/ack/nack hot path.
- Ack multiple over full range should emit one AckRange.
- Ack multiple over full mask should emit one AckMask.
- Nack multiple should mirror ack multiple compactness.
- Individual ack may be slower, but must remain bounded by prefetch/window size.
```

Measurement targets are not hard gates yet, but PR3 should record benchmark numbers.

---

## 22. Interaction with future AMQP adapter

PR3 APIs should make AMQP adapter implementation straightforward later:

```text
AMQP basic.consume / basic.qos
  -> ConsumerSession with prefetch

AMQP basic.deliver
  <- SessionDeliveryBatch

AMQP basic.ack(tag, multiple)
  -> AckRequest { tag, mode }

AMQP basic.nack(tag, multiple, requeue)
  -> NackRequest { tag, mode, disposition }

AMQP basic.reject(tag, requeue)
  -> RejectRequest

AMQP consumer cancel / connection close
  -> cancel(RequeueUnacked)
```

But PR3 must keep names neutral and avoid importing AMQP crates.

---

## 23. Interaction with future native protocol

The native protocol should be able to use stronger batch forms:

```text
ACK_RANGE
ACK_MASK
ACK_BATCH
NACK_BATCH
```

PR3 should expose batch-friendly APIs so native protocol does not have to degrade to delivery-tag-only behavior.

Possible future path:

```text
native client receives DeliveryRange/DeliveryMask metadata
native client can ack by compact batch
```

For now, `ConsumerSession` remains the authority for tag mapping.

---

## 24. Interaction with future storage

PR3 should not persist anything, but it should prepare clean event boundaries:

```text
ack applied
nack applied
requeue requested
dead-letter candidate
consumer cancel requeue
```

Later `aurum-storage` will persist:

```text
AckRange / AckMask
NackRange / NackMask
DeadLetterCandidate
Redelivery metadata
```

Avoid APIs that hide these transitions completely.

---

## 25. Open design questions

These should be answered during implementation, not left ambiguous.

### 25.1 Does requeue go to retry or sparse_ready directly?

Recommended PR3 answer:

```text
Use retry path first because it models Rabbit-like redelivery and future backoff.
Expose a `requeue_now` helper only if benchmark/clarity requires it.
```

### 25.2 Do we store delivery_count now?

Recommended PR3 answer:

```text
Store redelivered flag now.
Add delivery_count only when DLQ max-deliveries policy is implemented.
```

### 25.3 Should `prefetch=0` mean unlimited?

Recommended PR3 answer:

```text
Do not encode this as a magic integer in core.
Use PrefetchMode::Unlimited explicitly.
AMQP adapter can map AMQP qos semantics later.
```

### 25.4 Should invalid ack panic, ignore, or error?

Recommended PR3 answer:

```text
Return ConsumerError.
Protocol adapter decides protocol-level consequence later.
```

### 25.5 Should DeliveryWindow use VecDeque?

Recommended PR3 answer:

```text
Yes for first implementation.
Benchmark later against fixed ring.
Do not introduce HashMap until there is evidence it is needed.
```

---

## 26. Acceptance criteria

PR3 is complete when:

```text
1. `aurum-core` has a `consumer` module.
2. ConsumerSession can deliver from HybridRangeBlockQueue respecting credit.
3. Delivery tags are monotonic per session.
4. Delivery tags map to DeliveryRange/DeliveryMask correctly.
5. Ack one works.
6. Ack multiple works.
7. Nack one requeue works.
8. Nack multiple requeue works.
9. Reject works as nack one.
10. Consumer cancel requeues all unacked.
11. Redelivery flag is represented and tested.
12. Invalid/double tags return errors.
13. ModelConsumerSession differential tests pass.
14. Randomized session operation tests pass.
15. H1 benchmark still uses the real queue engine.
16. H2/session benchmark exists or H1 has session workloads.
17. No AMQP/native protocol types appear in aurum-core.
18. No `dyn Trait` appears in hot consumer/session methods.
19. `cargo test -p aurum-core` passes.
20. `cargo check --workspace --all-targets` passes.
```

---

## 27. Suggested implementation order

Execute in this exact order:

```text
1. Add consumer module skeleton and neutral request/result types.
2. Add bitflags dependency if using bitflags crate.
3. Implement ConsumerCredit and tests.
4. Implement DeliveredSegment + SegmentDeliveryWindow.
5. Implement AckBatch/NackBatch internal types if missing.
6. Implement ack one/multiple at DeliveryWindow level only.
7. Implement nack one/multiple at DeliveryWindow level only.
8. Implement ConsumerSession delivery path.
9. Connect ConsumerSession ack path to HybridRangeBlockQueue.
10. Connect ConsumerSession nack/requeue path to HybridRangeBlockQueue.
11. Implement cancel/requeue-all.
12. Add deterministic tests.
13. Add ModelConsumerSession.
14. Add randomized differential tests.
15. Add H2 benchmark.
16. Run perf/benchmark sanity check.
17. Update docs/README with PR3 status.
```

Do not connect AMQP before step 16.

---

## 28. Code review checklist

Reviewers/AI agents must check:

```text
- Does aurum-core depend on protocol crates? It must not.
- Are hot methods generic/concrete/enum-based rather than dyn-dispatched?
- Are ack multiple and nack multiple compact?
- Does DeliveryWindow preserve tag-to-bit mapping for mask segments with holes?
- Are invalid tags handled as errors?
- Does cancel release credit and clear the window?
- Does redelivery use a new delivery tag?
- Does the model test cover partial range and partial mask cases?
- Are bitflags used only for combinable flags?
- Are enums used for mutually exclusive states/modes?
- Does any method allocate per message in the common range path?
- Does cargo test -p aurum-core pass?
```

---

## 29. Final design intent

After PR3, AurumMQ should have this internal shape:

```text
HybridRangeBlockQueue
  owns message state as ranges/masks/bitsets

ConsumerSession
  owns delivery tags, prefetch, unacked window

DeliveryWindow
  maps tags to DeliveredSegment

Ack/Nack logic
  converts tag operations into AckRange/AckMask/NackRange/NackMask

Protocol adapters later
  become thin translation layers
```

This is the foundation for RabbitMQ compatibility without sacrificing the performance architecture.

