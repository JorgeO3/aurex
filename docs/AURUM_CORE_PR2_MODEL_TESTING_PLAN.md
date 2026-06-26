# AurumMQ — PR2 Plan: Model-Based Testing and Queue Semantics Hardening

This document is the implementation plan for the next AurumMQ milestone after the first `aurum-core` queue-engine skeleton.

The goal is not more performance work yet. The goal is to prove that the `HybridRangeBlockQueue` semantics are correct before we continue building storage, routing, protocols, runtime, replication, or AMQP compatibility.

## 0. Why this PR matters

AurumMQ's core data plane is intentionally unusual:

```text
sequential ready range
+ block-level sparse bitsets
+ inflight bitsets
+ acked bitsets
+ retry bitsets
+ active block lists
+ range/mask-first APIs
```

This design is promising for performance, but it is also easy to get subtly wrong. Bugs in this layer would contaminate everything else:

```text
AMQP adapter
native protocol
storage recovery
ack ledger
redelivery
DLQ
replication
cluster failover
```

So the immediate objective is:

> Build a slow, obvious reference model and continuously compare the optimized queue engine against it.

The optimized implementation may be hard to reason about internally. The model must be easy to reason about.

## 1. Scope of PR2

### 1.1 Included

PR2 includes:

```text
- ModelQueue reference implementation.
- Observable queue state snapshots.
- Deterministic differential tests.
- Randomized operation sequences.
- Invariant checks after each public operation.
- Basic proptest scaffolding if dependency is added.
- Better semantic tests for publish/deliver/ack/nack/retry.
- H1 experiment wired to the real aurum-core queue engine.
```

### 1.2 Not included

PR2 must not implement:

```text
- AMQP basic.ack/basic.nack adapter semantics.
- Native protocol frame parsing.
- Persistent storage.
- Ack ledger.
- Timing wheel.
- io_uring.
- NIO/glommio/monoio runtime.
- Cluster replication.
- NUMA placement.
- Optimized SIMD/SWAR kernels beyond the current core helpers.
```

Those come later. PR2 is the correctness wall.

## 2. Desired final state after PR2

After this PR, we should be able to say:

```text
For a wide range of deterministic and randomized operation sequences,
HybridRangeBlockQueue and ModelQueue produce equivalent logical state.
```

The acceptance command should be:

```bash
cargo test -p aurum-core
cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=deliver_ack --variant=both
cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=nack_retry_ack --variant=both
```

The final PR should also make it difficult for future contributors or AI agents to accidentally break semantics while optimizing.

## 3. Core semantic model

### 3.1 Message state model

The model queue should use a very simple state enum:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelMessageState {
    Ready,
    Inflight,
    Acked,
    Retry,
    Dead,
}
```

Optional later states:

```text
ScheduledRetry
Expired
DeadLettered
```

But for PR2, keep it small:

```text
Ready
Inflight
Acked
Retry
Dead
```

### 3.2 ModelQueue structure

The model should be deliberately boring:

```rust
pub struct ModelQueue {
    states: Vec<ModelMessageState>,
    ready_order: std::collections::VecDeque<u64>,
    retry_order: std::collections::VecDeque<u64>,
    next_seq: u64,
}
```

This is not the production layout. It is the oracle.

It should favor clarity over speed.

### 3.3 Model operations

The model must support the same logical operations as the optimized queue:

```rust
impl ModelQueue {
    pub fn new() -> Self;
    pub fn with_messages(total: u64) -> Self;
    pub fn publish_contiguous(&mut self, count: u32) -> ModelPublishRange;

    pub fn deliver(&mut self, max_messages: u32) -> Vec<u64>;

    pub fn ack_id(&mut self, seq: u64) -> ModelAckResult;
    pub fn ack_range(&mut self, start: u64, len: u32) -> ModelAckResult;
    pub fn ack_ids<I>(&mut self, seqs: I) -> ModelAckResult
    where
        I: IntoIterator<Item = u64>;

    pub fn nack_id_to_retry(&mut self, seq: u64) -> ModelNackResult;
    pub fn nack_range_to_retry(&mut self, start: u64, len: u32) -> ModelNackResult;
    pub fn nack_ids_to_retry<I>(&mut self, seqs: I) -> ModelNackResult
    where
        I: IntoIterator<Item = u64>;

    pub fn retry_all_now(&mut self) -> u32;

    pub fn counts(&self) -> ModelCounts;
    pub fn state_of(&self, seq: u64) -> Option<ModelMessageState>;
}
```

The model can be implemented in test-only code, but it is better to place it in `src/queue/model.rs` behind a feature or `#[cfg(any(test, feature = "model"))]` so experiments and fuzzers can reuse it.

## 4. Optimized queue state snapshot

To compare the optimized core with the model, we need a logical snapshot API.

### 4.1 Add `MessageState`

In `aurum-core/src/queue/state.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    Ready,
    Inflight,
    Acked,
    Retry,
    Dead,
    Unknown,
}
```

For PR2, `Dead` may not be produced yet. Keep it for future compatibility if desired.

### 4.2 Add `QueueCounts`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCounts {
    pub published: u64,
    pub ready: u64,
    pub inflight: u64,
    pub acked: u64,
    pub retry: u64,
    pub dead: u64,
}
```

### 4.3 Add debug state API

```rust
impl HybridRangeBlockQueue {
    pub fn debug_state_of(&self, seq: Seq) -> Option<MessageState>;
    pub fn debug_counts(&self) -> QueueCounts;
    pub fn debug_collect_states(&self, limit: usize) -> Vec<(Seq, MessageState)>;
}
```

These are not hot-path APIs. They are for tests, fuzzing, diagnostics and future admin introspection.

Use feature gates if necessary:

```rust
#[cfg(any(test, feature = "debug-state"))]
```

But in early research it is acceptable to keep them public until the engine stabilizes.

## 5. Invariants

The invariant checker is the second safety net after the model.

### 5.1 Add `InvariantViolation`

In `aurum-core/src/queue/invariants.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    SequentialRangeInvalid {
        head: u64,
        tail: u64,
        next_seq: u64,
    },
    BitStateConflict {
        block: u32,
        word: u8,
        mask: u64,
        left: &'static str,
        right: &'static str,
    },
    SparseWordMaskMismatch {
        block: u32,
        expected: u8,
        actual: u8,
    },
    RetryWordMaskMismatch {
        block: u32,
        expected: u8,
        actual: u8,
    },
    SparseListMismatch {
        block: u32,
        should_be_linked: bool,
        is_linked: bool,
    },
    RetryListMismatch {
        block: u32,
        should_be_linked: bool,
        is_linked: bool,
    },
    CountMismatch {
        expected_total: u64,
        actual_total: u64,
    },
}
```

Adjust exact fields to current implementation.

### 5.2 Required invariants

Every public operation must leave the queue in a valid state.

#### Range invariants

```text
0 <= sequential_head <= sequential_tail <= next_seq
```

If there is a fixed capacity:

```text
next_seq <= capacity
```

If dynamic publish is implemented:

```text
blocks.len() is enough to cover next_seq
```

#### State exclusivity invariants

For every block word:

```text
inflight & acked == 0
inflight & retry == 0
inflight & sparse_ready == 0
acked & retry == 0
acked & sparse_ready == 0
retry & sparse_ready == 0
```

A message can be in sequential ready range without a bit set. That is intentional.

#### Word mask invariants

For every block:

```text
sparse_word_mask == nonzero_words(sparse_ready)
retry_word_mask == nonzero_words(retry)
```

#### Active list invariants

For every block:

```text
block is in sparse list iff sparse_word_mask != 0
block is in retry list iff retry_word_mask != 0
```

If the list implementation does not allow cheap membership checks yet, add a debug-only membership map or list traversal inside `validate_invariants()`.

#### Count invariants

Counts should satisfy:

```text
ready + inflight + acked + retry + dead == published
```

Where:

```text
ready = sequential_ready_len + sparse_ready_count
```

## 6. Operation semantics to lock down

### 6.1 `publish_contiguous(count)`

Expected semantics:

```text
- Allocates/reserves sequence numbers [start, start + count).
- Adds them to sequential ready range.
- Does not set sparse_ready bits.
- Does not set inflight bits.
- Does not set retry bits.
- Does not set acked bits.
```

Edge cases:

```text
count = 0 => no-op or empty range, decide explicitly.
count crosses block boundary.
count crosses several blocks.
```

Recommended behavior:

```text
count = 0 returns len=0 and changes nothing.
```

### 6.2 `deliver(max_messages, out)`

Expected semantics:

```text
- If max_messages = 0, deliver nothing.
- Prefer sequential ready range for normal path unless policy says sparse/retry priority first.
- Move delivered messages to inflight.
- Return compact DeliveryRange and/or DeliveryMask batches.
- Do not deliver acked messages.
- Do not deliver retry messages until retry is moved to ready.
- Do not deliver the same message twice while inflight.
```

For PR2, choose delivery priority explicitly:

```text
Option A: sequential ready first, then sparse ready.
Option B: sparse ready first, then sequential ready.
```

Recommendation for now:

```text
sequential ready first
```

Reason: H1's main fast path is sequential publish/deliver/ack. Retry priority/fairness can be a policy later.

### 6.3 `ack_range(start, len)`

Expected semantics:

```text
- For each seq in range:
  - if Inflight -> Acked
  - if Acked -> idempotent no-op for PR2
  - if Ready/Retry/Unknown -> either error or ignored based on policy
```

For PR2, choose one policy.

Recommendation:

```text
Checked API returns result.
Unchecked/internal benchmark API can be idempotent.
```

Concrete design:

```rust
pub enum AckMode {
    Strict,
    Idempotent,
}
```

But avoid adding too much API surface too early. Simpler:

```rust
pub fn try_ack_range(...) -> Result<AckOutcome, QueueError>;
pub fn ack_range_lossy_or_idempotent(...) -> AckOutcome;
```

For PR2, at minimum document the current behavior.

### 6.4 `ack_mask(mask)`

Expected semantics:

```text
- Mask must target a valid block and word.
- Mask bits are expected to be inflight.
- Inflight bits are cleared.
- Acked bits are set.
- Active lists are updated if needed.
```

### 6.5 `ack_id(seq)`

Expected semantics:

```text
- Fallback path.
- Must produce same final state as ack_range(seq, 1).
- Not the hot path.
```

Add a test:

```text
ack_id(seq) == ack_range(seq, 1)
```

### 6.6 `nack_mask_to_retry(mask)`

Expected semantics:

```text
- Mask bits must be inflight.
- Inflight bits are cleared.
- Retry bits are set.
- Retry word mask is updated.
- Retry active block list is updated.
- Message is not ready until retry is moved to ready.
```

### 6.7 `retry_all_now()`

Expected semantics:

```text
- Move all retry bits to sparse_ready bits.
- Clear retry bits.
- Update retry/sparse word masks.
- Move blocks between active lists.
- Return number of moved messages.
```

This is a test/helper API. Later it will be replaced by a timing wheel-driven due-retry API.

## 7. Differential testing design

### 7.1 Test harness structure

Create:

```text
crates/hot-path/aurum-core/tests/queue_model_diff.rs
crates/hot-path/aurum-core/tests/queue_semantics.rs
crates/hot-path/aurum-core/tests/queue_invariants.rs
```

Alternatively, keep tests under `src/queue/tests.rs` while the API is still private. Integration tests are better once APIs are public.

### 7.2 Operation enum

```rust
#[derive(Debug, Clone)]
enum TestOp {
    Publish { count: u32 },
    Deliver { max: u32 },
    AckLastDelivery,
    NackLastDeliveryToRetry,
    AckSomeInflight,
    NackSomeInflightToRetry,
    RetryAllNow,
    AckIdMaybe { seq: u64 },
}
```

Keep a test harness state:

```rust
struct DiffHarness {
    model: ModelQueue,
    core: HybridRangeBlockQueue,
    last_model_delivery: Vec<u64>,
    last_core_delivery: DeliveryWork,
}
```

### 7.3 Key challenge: comparing deliveries

The optimized queue may return:

```text
DeliveryRange
DeliveryMask
```

The model returns:

```text
Vec<Seq>
```

Add a test-only expansion function:

```rust
fn expand_delivery_work(work: &DeliveryWork) -> Vec<u64>;
```

Then compare as ordered vectors if delivery order is specified.

For PR2, specify ordering:

```text
Sequential ranges should preserve sequence order.
Sparse masks should be expanded in increasing seq order.
Combined delivery order should be deterministic.
```

If future scheduler policies allow different orders, compare sets instead. For now, compare exact order; it catches more bugs.

### 7.4 Comparing state

After every op:

```rust
core.validate_invariants().unwrap();
assert_eq!(core.debug_counts(), model.counts().into());
```

Then sample states:

```text
- first 512 messages
- last 512 messages
- around block boundaries 63/64/65, 255/256/257
- random sample from published range
```

Full state comparison is acceptable for small tests. For large randomized tests, sample.

### 7.5 Deterministic random generator

Use a tiny local xorshift generator to avoid pulling dependencies immediately:

```rust
struct XorShift64(u64);
```

Required behavior:

```text
- deterministic seed
- reproducible failure output
- print seed and op index on panic
```

Failure output should include:

```text
seed
operation index
operation
model counts
core counts
last delivery
```

### 7.6 Deterministic test cases

Implement these before randomized tests:

```text
publish_deliver_ack_one_block
publish_deliver_ack_cross_word
publish_deliver_ack_cross_block
nack_retry_ack_one_block
nack_retry_ack_cross_block
ack_range_equivalent_to_ack_ids
ack_mask_equivalent_to_ack_ids
retry_all_now_empty_is_noop
retry_all_now_partial_blocks
mixed_ack_nack_same_delivery
```

### 7.7 Randomized tests

Start with fixed seeds:

```rust
const SEEDS: &[u64] = &[
    0xDEAD_BEEF_CAFE_BABE,
    0x1234_5678_9ABC_DEF0,
    0xA11C_E5EE_D15E_A5E5,
    0xF00D_F00D_F00D_F00D,
];
```

Workload sizes:

```text
small_random: 1_000 ops, max publish 32, max deliver 64
medium_random: 10_000 ops, max publish 128, max deliver 256
boundary_random: focuses around 64/256 boundaries
```

Do not start with huge tests. Debuggability matters.

## 8. Property testing with `proptest`

This is optional in the first PR2 slice but should be planned.

### 8.1 Add dependency

In root `Cargo.toml` workspace dependencies:

```toml
proptest = "1"
```

In `aurum-core/Cargo.toml`:

```toml
[dev-dependencies]
proptest = { workspace = true }
```

### 8.2 Operation strategy

```rust
prop::collection::vec(queue_op_strategy(), 1..500)
```

With bounded values:

```text
Publish count: 0..256
Deliver max: 0..512
Ack/Nack choices based on harness inflight state
```

Important: pure generated operations may produce many no-ops. It is fine initially, but targeted deterministic tests are more valuable early.

### 8.3 Shrinking

The value of `proptest` is shrinking. Ensure operations are simple enums with small fields so failing sequences shrink well.

### 8.4 Failure reproducibility

When `proptest` fails, copy the generated minimal sequence into a deterministic regression test.

## 9. Ack/Nack batch design preparation

Even if PR2 does not fully implement explicit `AckBatch`, the tests should prepare for it.

### 9.1 Core work types

Target shape:

```rust
pub struct DeliveryBatch {
    pub ranges: SmallVec<[DeliveryRange; 4]>,
    pub masks: SmallVec<[DeliveryMask; 8]>,
}

pub struct AckBatch {
    pub ranges: SmallVec<[AckRange; 4]>,
    pub masks: SmallVec<[AckMask; 8]>,
}

pub struct NackBatch {
    pub ranges: SmallVec<[NackRange; 4]>,
    pub masks: SmallVec<[NackMask; 8]>,
    pub reason: NackReason,
}
```

If `smallvec` is not added yet, use `Vec` with reuse.

### 9.2 Wrappers from delivery

For the current implementation:

```rust
fn ack_delivery_work(work: &DeliveryWork) -> AckBatch;
fn nack_delivery_work(work: &DeliveryWork, reason: NackReason) -> NackBatch;
```

This mirrors what protocol adapters will eventually do.

## 10. H1 experiment integration

The H1 experiment must benchmark the real `aurum-core` implementation.

### 10.1 Remove duplicated engine

In `experiments/h1-queue-engine`, remove or archive duplicate implementations of:

```text
HybridRangeBlockQueue
MsgBlock
BlockList
DeliveryWork
```

Import from core:

```rust
use aurum_core::queue::{HybridRangeBlockQueue, DeliveryWork};
```

Keep baselines local:

```text
per_message_vecdeque
```

### 10.2 Required workloads after PR2

Keep current:

```text
deliver_ack
random_ack
nack_retry_ack
```

Add:

```text
ack_multiple
windowed_random_ack
mixed_interleaved
```

#### `ack_multiple`

Simulates AMQP `basic.ack(multiple=true)`:

```text
publish N
deliver batches
ack all delivered up to delivery tag X using ranges
```

Expected: hybrid should be extremely strong.

#### `windowed_random_ack`

More realistic than global random ack:

```text
prefetch window = 128/1024/4096
acks arrive randomly only within active window
```

Expected: hybrid should do better than global random ack.

#### `mixed_interleaved`

Avoid phase-separated unrealistic workloads:

```text
loop:
  publish some
  deliver some
  ack some
  nack some
  retry due sometimes
```

Expected: catches invariants and models real broker pressure.

## 11. Documentation updates required

PR2 should update:

```text
docs/AURUM_CORE_IMPLEMENTATION_PLAN.md
docs/PROJECT_VISION_AND_AI_CONTEXT.md
experiments/h1-queue-engine/README.md
```

Add a short section:

```text
Queue correctness policy:
  Optimized queue engine changes must pass ModelQueue differential tests.
```

## 12. Implementation slices

Do not implement everything in one huge commit. Suggested slices:

### Slice 1 — Observable state and invariants

Files:

```text
src/queue/state.rs
src/queue/invariants.rs
src/queue/hybrid.rs
```

Tasks:

```text
- Add MessageState.
- Add QueueCounts.
- Add debug_state_of.
- Add debug_counts.
- Add validate_invariants.
- Add deterministic invariant tests.
```

DoD:

```bash
cargo test -p aurum-core queue_invariants
```

### Slice 2 — ModelQueue

Files:

```text
src/queue/model.rs
```

Tasks:

```text
- Add ModelQueue.
- Add ModelMessageState.
- Add ModelCounts.
- Add publish/deliver/ack/nack/retry operations.
- Add simple unit tests for ModelQueue itself.
```

DoD:

```bash
cargo test -p aurum-core model_queue
```

### Slice 3 — Deterministic differential tests

Files:

```text
tests/queue_model_diff.rs
```

Tasks:

```text
- Add DiffHarness.
- Add expansion of DeliveryWork to Vec<Seq>.
- Add fixed operation sequences.
- Compare deliveries, counts, states.
```

DoD:

```bash
cargo test -p aurum-core queue_model_diff
```

### Slice 4 — Randomized differential tests

Files:

```text
tests/queue_random_diff.rs
```

Tasks:

```text
- Add XorShift64.
- Add TestOp generator.
- Add failure context dump.
- Add fixed seeds.
```

DoD:

```bash
cargo test -p aurum-core queue_random_diff
```

### Slice 5 — H1 experiment uses aurum-core

Files:

```text
experiments/h1-queue-engine/src/main.rs
experiments/h1-queue-engine/README.md
```

Tasks:

```text
- Remove duplicate optimized queue implementation.
- Use aurum-core HybridRangeBlockQueue.
- Keep baseline local.
- Add ack_multiple/windowed_random_ack/mixed_interleaved if time allows.
```

DoD:

```bash
cargo run --release -p h1-queue-engine -- --messages=1048576 --batch=128 --workload=deliver_ack --variant=both
```

### Slice 6 — Optional `proptest`

Files:

```text
tests/queue_proptest.rs
```

Tasks:

```text
- Add workspace dependency.
- Add generated operation sequence strategy.
- Add regression capture instructions.
```

DoD:

```bash
cargo test -p aurum-core queue_proptest
```

## 13. Queue error policy for PR2

This must be explicit.

### 13.1 Current benchmark-friendly behavior

The current queue may treat some invalid operations as idempotent/no-op.

This is acceptable only if documented.

### 13.2 Production direction

Long-term, we need two APIs:

```rust
pub fn try_ack_batch(&mut self, batch: &AckBatch) -> Result<AckOutcome, QueueError>;

pub(crate) unsafe fn ack_batch_unchecked(&mut self, batch: &AckBatch) -> AckOutcome;
```

The checked API is used by protocol adapters. The unchecked/internal API can be used when the caller has already validated masks/ranges.

### 13.3 PR2 recommendation

For PR2:

```text
- Keep existing fast behavior if needed.
- Add checked helpers where easy.
- Tests should verify documented behavior, not implicit behavior.
```

## 14. Regression tests to add immediately when bugs appear

Every differential-test bug must produce a named regression test:

```rust
#[test]
fn regression_retry_block_unlinked_too_early() { ... }
```

The regression should include:

```text
- The failing seed.
- The minimal operation sequence.
- The expected final counts/state.
```

Do not rely only on randomized tests to preserve bug fixes.

## 15. Review checklist

Before merging PR2, review:

```text
[ ] aurum-core still does not depend on AMQP/native/storage/runtime.
[ ] ModelQueue is obviously correct and not optimized.
[ ] HybridRangeBlockQueue has invariant checks.
[ ] Public operations leave valid invariants.
[ ] Differential tests compare counts and state.
[ ] Random tests print reproducible seed/context.
[ ] H1 experiment uses aurum-core implementation directly.
[ ] No per-message heap allocation was introduced in hot-path structures.
[ ] No dyn Trait was introduced in hot-path queue code.
[ ] No async/runtime dependency was introduced in aurum-core.
```

## 16. Commands

Use the uploaded toolchain in the sandbox:

```bash
PATH=/mnt/data/rust/bin:$PATH cargo fmt --all
PATH=/mnt/data/rust/bin:$PATH cargo check -p aurum-core --all-targets
PATH=/mnt/data/rust/bin:$PATH cargo test -p aurum-core
PATH=/mnt/data/rust/bin:$PATH cargo test --workspace
```

Benchmark smoke test:

```bash
PATH=/mnt/data/rust/bin:$PATH cargo run --release -p h1-queue-engine -- \
  --messages=1048576 \
  --batch=128 \
  --workload=deliver_ack \
  --variant=both
```

## 17. What comes after PR2

Once PR2 is green:

```text
PR3: explicit AckBatch/NackBatch and Rabbit-like ack groundwork.
PR4: H1.2 benchmark workloads and perf/asm review.
PR5: internal command protocol integration.
PR6: compiled routing minimal direct exchange.
PR7: in-memory broker loop with native protocol mock.
```

Do not start AMQP before:

```text
- ack multiple is represented cleanly,
- delivery tags have a local mapping,
- core has checked ack/nack outcomes,
- model-based tests are stable.
```

## 18. Final PR2 acceptance criteria

PR2 is complete when:

```text
1. ModelQueue exists and is intentionally simple.
2. HybridRangeBlockQueue exposes debug state/counts.
3. validate_invariants catches structural bugs.
4. Deterministic model-diff tests pass.
5. Randomized model-diff tests pass with fixed seeds.
6. H1 benchmark uses aurum-core, not a duplicated queue engine.
7. The workspace compiles and tests pass.
8. The README/docs explain that future optimizations must preserve model equivalence.
```

If this PR is done well, future optimizations become much safer: we can rewrite the hot path, change masks, alter block size, introduce active-word lists, or add timing-wheel retry while continuously proving equivalence against the model.
