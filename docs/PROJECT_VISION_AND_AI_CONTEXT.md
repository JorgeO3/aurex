# AurumMQ — Project Vision and AI Context

This document is the high-level context file for humans and AI agents working on AurumMQ. It explains what the project is trying to become, what it is not, the architectural philosophy, the current workspace model, and the rules that should guide implementation decisions.

AurumMQ is currently a research-first Rust workspace. The goal is to validate the foundational performance and correctness hypotheses before the project grows into a full broker.

---

## 1. One-sentence vision

**AurumMQ is a RabbitMQ-like smart broker with a modern high-performance data plane: RabbitMQ ergonomics and compatibility at the edge, but internally built around thread-per-core shards, compiled routing, range/mask-first queue engines, append-only logs, NUMA locality, and broker-managed delivery semantics.**

The project should feel familiar to RabbitMQ users:

```text
exchange
queue
binding
publish
consume
ack
nack
retry
DLQ
TTL
prefetch
publisher confirms
```

But internally it should behave more like a purpose-built performance engine:

```text
route_id
CommandBatch
DeliveryRange
DeliveryMask
AckRange
AckMask
queue shard ownership
thread-per-core runtime
append-only payload log
queue index log
ack ledger
compiled route tables
NUMA-aware placement
```

---

## 2. Why this project exists

RabbitMQ has excellent ergonomics and a powerful smart-broker model. Producers publish to exchanges, the broker routes intelligently, consumers stay simple, and features like acknowledgements, dead-letter queues, retries, TTL, priorities, publisher confirms and prefetch are practical and productive.

Kafka-like systems, Redpanda, Iggy and similar log-first systems are usually stronger for throughput and replay/streaming workloads, but they often push more complexity to the consumer: partitions, offsets, consumer groups, ordering constraints and commit strategy.

AurumMQ exists to explore this space:

```text
RabbitMQ-like API and semantics
+ Kafka/Iggy/Redpanda-like internal data plane
+ Rust implementation
+ CPU-cache-friendly algorithms
+ low latency
+ high throughput
+ simple deployment path from single VPS to distributed cluster
```

The product thesis is:

> There is room for a broker that is easier to consume than Kafka, faster and more hardware-aware than a traditional RabbitMQ-style queue engine, and still operationally simple enough to run on one VPS, bare metal, Kubernetes, or cloud infrastructure.

---

## 3. Strategic positioning

AurumMQ should **not** become a generic clone of any existing system.

### 3.1 Not just RabbitMQ in Rust

A direct reimplementation of RabbitMQ would inherit too many historical assumptions:

```text
per-message object-heavy internals
legacy compatibility everywhere
queue-centric mutable state
less explicit hardware locality
less DOD-first modeling
```

AurumMQ should preserve the useful external model but redesign the internals.

### 3.2 Not just Kafka/Iggy with another protocol

AurumMQ should not expose a streaming-first mental model as the main user experience.

The user-facing model should remain:

```text
publish message
broker routes
consumer receives
consumer ack/nack
broker handles retry/DLQ/redelivery
```

Offsets, partitions, shard ownership and logs are internal implementation details unless the user explicitly chooses stream mode.

### 3.3 The intended niche

AurumMQ aims to be:

```text
faster than traditional RabbitMQ-style queues for durable work queues and fanout
simpler than Kafka for broker-managed work queues
compatible enough with RabbitMQ/AMQP for migration paths
more hardware-aware than typical broker designs
suitable for single-node and horizontally scalable deployments
```

---

## 4. Core design principles

These principles should guide every implementation decision.

### 4.1 Hot path is not object-oriented

The core should not model messages as rich objects in the hot path.

Avoid this in hot code:

```rust
struct Message {
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    state: MessageState,
}
```

Prefer this:

```text
blocks
ranges
masks
arrays
bitsets
offsets
logs
IDs
```

The hot path should be Data-Oriented Design first.

### 4.2 Range/mask-first, not message-first

The core should process:

```text
DeliveryRange
DeliveryMask
AckRange
AckMask
NackMask
RetryMask
```

Message IDs are allowed at protocol boundaries, but the core should coalesce them into ranges and masks whenever possible.

Wrong internal shape:

```rust
fn pop_ready() -> MessageId
fn ack(message_id: MessageId)
```

Preferred internal shape:

```rust
fn deliver(max: usize, out: &mut DeliveryBatch) -> usize
fn ack_range(start: Seq, len: u32)
fn ack_mask(mask: AckMask)
fn nack_mask(mask: NackMask)
```

### 4.3 Single writer ownership beats unnecessary lock-free complexity

Inside a shard, queue state should be owned by one core/thread.

Use ordinary non-atomic state inside the owner:

```text
ready range
inflight bitsets
acked bitsets
retry bitsets
consumer credit tables
scheduler state
```

Use lock-free structures at ownership boundaries:

```text
SPSC rings
MPSC lanes
remote free queues
RCU/ArcSwap snapshots
cross-shard mailboxes
```

Do not make the queue engine multi-writer unless there is a very strong reason.

### 4.4 Static dispatch in hot paths

Hot-path code should use concrete types, const generics and monomorphization where useful.

Avoid `dyn Trait` in:

```text
queue engine
routing hot path
ack engine
scheduler
storage append path
kernel loops
```

Dynamic dispatch is acceptable in cold paths:

```text
plugins
management API
auth providers
protocol gateway configuration
operator code
admin tooling
```

### 4.5 Protocols live at the edge

`aurum-core` must never depend on AMQP, Kafka, HTTP, WebSocket or native wire frame types.

Protocol adapters translate external frames into an internal command protocol:

```text
AMQP frame / Native frame / Future Kafka gateway
        ↓
Protocol adapter
        ↓
Internal CommandBatch
        ↓
Compiled routing
        ↓
Owner shard
        ↓
Queue engine
```

This keeps compatibility concerns from contaminating the data plane.

### 4.6 Compiled routing, not dynamic rule evaluation per message

Bindings and exchange declarations are user-friendly and dynamic, but the hot path should not evaluate them as dynamic objects per message.

Control plane:

```text
exchanges + bindings + policies
        ↓
route compiler
        ↓
immutable RouteTable version
```

Data plane:

```text
route_id -> QueueSet -> shard groups
```

The routing layer should behave like a compiled query plan or compiled regex: the flexible representation is transformed into compact, cache-friendly lookup tables.

### 4.7 Append-only storage model

Durable state should be organized around append-only logs:

```text
payload log       -> message bytes
queue index log   -> queue-visible references
ack ledger        -> ack/nack/retry/DLQ transitions
snapshots         -> recovery acceleration
```

Avoid random writes per message in the storage hot path.

### 4.8 NUMA-first architecture

NUMA locality is a design constraint, not a later optimization.

A node should be modeled as:

```text
NUMA cell
  cores
  shards
  memory pools
  network queues
  storage queues
```

Cross-NUMA communication must be explicit, batched and measured.

### 4.9 Cloud-agnostic and on-prem friendly

AurumMQ should run in:

```text
single VPS
multiple VPSs by static IP/DNS seeds
bare metal/on-prem
Docker/Podman
K3s
Kubernetes/EKS/AKS/GKE/OpenShift
```

Kubernetes should be an operational interface, not the internal brain. The broker owns shard placement, replication, failover and routing epochs.

---

## 5. User-facing product vision

### 5.1 Compatibility path

A RabbitMQ user should eventually be able to point existing applications at AurumMQ with minimal changes for common AMQP 0-9-1 workflows:

```text
basic.publish
basic.consume
basic.ack
basic.nack
queue.declare
exchange.declare
queue.bind
prefetch
publisher confirms
DLQ
TTL
```

AMQP compatibility is a first-class goal, but not the first implementation phase.

### 5.2 Native high-performance path

New clients should use the native protocol.

Native protocol goals:

```text
batch-first
route_id-first
AckRange/AckMask support
DeliveryRange/DeliveryMask support
flow control built in
compression blocks
shard/leader redirects
minimal repeated strings
low overhead frame format
```

A typical native flow:

```text
RESOLVE_ROUTE(exchange, routing_key) -> route_id
PUBLISH_BATCH(route_id, messages)
DELIVERY_BATCH(ranges/masks)
ACK_BATCH(ranges/masks)
```

### 5.3 Queue modes

AurumMQ should eventually support multiple queue modes instead of pretending one abstraction handles all tradeoffs:

```text
fifo    -> stronger ordering, lower maximum parallelism
work    -> high-throughput work queue, no global ordering promise
keyed   -> ordering per key, scalable across keys
stream  -> replay/log mode, Kafka/Iggy-like use cases
```

The first implementation focus is `work`-style queue behavior.

---

## 6. Main architecture model

```text
Clients
  ├── AMQP clients
  ├── Native clients
  └── Future gateways: Kafka/MQTT/STOMP/etc.
        ↓
Protocol adapters / gateways
        ↓
Internal Command Protocol
        ↓
Compiled Routing Layer
        ↓
Thread-per-core / NUMA-aware Shard Runtime
        ↓
Queue Engine: ranges + block bitsets
        ↓
Append-only Storage: payload log + queue index + ack ledger
        ↑
Control Plane: metadata, policies, route compiler
        ↑
Cluster Plane: shard map, placement, replication, epochs
        ↑
Ops Plane: CLI, observability, Kubernetes operator
```

---

## 7. Workspace organization

The workspace is intentionally split into small crates to improve compile times, dependency caching and architectural boundaries.

Current intended organization:

```text
crates/
  foundation/
    aurum-types
    aurum-kernels
    aurum-intrusive
    aurum-concurrency

  edge/
    aurum-internal-protocol
    aurum-plugin-api
    aurum-gateway-api
    aurum-protocol-native
    aurum-protocol-amqp
    aurum-transport

  hot-path/
    aurum-core
    aurum-routing
    aurum-storage
    aurum-runtime

  control-plane/
    aurum-control-plane

  cluster/
    aurum-cluster
    aurum-replication

  ops/
    aurum-observability
    aurum-operator

  apps/
    aurum-broker
    aurum-cli

experiments/
  h1-queue-engine
```

---

## 8. Crate responsibilities

### 8.1 `aurum-types`

Shared IDs and primitive domain types:

```text
Seq
QueueId
RouteId
ShardId
BlockIndex
WordIndex
DeliveryTag
QueueSetId
```

This crate should stay small and stable.

### 8.2 `aurum-kernels`

Low-level operations:

```text
bitset operations
range masks
SWAR scans
future SIMD kernels
ack masks
routing scans
```

Scalar/SWAR should be the first implementation. Explicit SIMD should only be introduced after measurement.

### 8.3 `aurum-intrusive`

Index-intrusive data structures:

```text
Link
BlockList
Generational arena
active block lists
```

Prefer index-based intrusive lists over raw-pointer intrusive structures in the hot core, unless a measured case requires otherwise.

### 8.4 `aurum-concurrency`

Concurrency primitives for ownership boundaries:

```text
SPSC ring
MPSC lanes
RCU/ArcSwap-like snapshots
remote free queues
atomic backoff
loom models
```

Do not use MPMC global queues as the default hot path.

### 8.5 `aurum-core`

The queue engine.

Responsibilities:

```text
publish ranges
deliver ranges/masks
ack ranges/masks
nack/retry masks
maintain ready/inflight/acked/retry state
maintain active block lists
provide model-testable semantics
```

Non-responsibilities:

```text
AMQP parsing
native protocol parsing
network I/O
disk I/O
replication
cluster membership
routing compilation
```

### 8.6 `aurum-routing`

Compiled routing:

```text
route_id -> QueueSet
exchange/binding compilation
fanout/direct/topic/header routing tables
QueueSet grouped by shard
route table versions/epochs
```

Initial focus: direct/fanout route ID lookup. Topic/header can come later.

### 8.7 `aurum-internal-protocol`

Internal command representation between adapters/routing/runtime/core:

```text
PublishBatch
AckBatch
NackBatch
CreditUpdate
ResolveRoute
DeliveryBatch
```

This is not the external native wire protocol. It is the broker's internal command model.

### 8.8 `aurum-protocol-native`

External high-performance protocol adapter.

Responsibilities:

```text
frame parsing
route resolution messages
publish batch frames
ack batch frames
flow control frames
translation to/from internal commands
```

### 8.9 `aurum-protocol-amqp`

AMQP 0-9-1 compatibility adapter.

Responsibilities:

```text
AMQP connection/channel semantics
basic.publish/basic.consume/basic.ack/basic.nack
queue/exchange declarations
prefetch/publisher confirms
translation to internal command batches
ack coalescing into AckRange/AckMask where possible
```

This crate must not leak AMQP types into `aurum-core`.

### 8.10 `aurum-storage`

Storage abstraction and append-only persistence:

```text
payload log
queue index log
ack ledger
snapshots
recovery
compaction
checksums
compression
```

### 8.11 `aurum-runtime`

Thread-per-core runtime scaffolding:

```text
shard ownership
worker/core mapping
NUMA cell mapping
mailboxes
scheduling budgets
runtime backend experiments
```

Candidate runtime backends can include manual loops, `nio`, Glommio, Monoio or custom event loops, but `aurum-core` must remain runtime-independent.

### 8.12 `aurum-control-plane`

Metadata and configuration:

```text
vhosts
users/permissions later
exchanges
bindings
queues
policies
route compiler inputs
metadata snapshots
```

### 8.13 `aurum-cluster`

Cluster topology and placement:

```text
NodeDescriptor
FailureDomain
ShardMap
ShardPlacement
node capacity
NUMA/topology metadata
routing epochs
```

### 8.14 `aurum-replication`

Replication groups and quorum profiles:

```text
RF=1
RF=3
witness profiles
learners
quorum-durable confirms
future flexible quorum experiments
```

### 8.15 `aurum-observability`

Metrics and tracing support:

```text
per-shard counters
histograms
p99/p999 latency
queue depth
ack latency
storage latency
replication lag
```

### 8.16 `aurum-operator`

Future Kubernetes operator scaffolding.

### 8.17 `aurum-broker`

Composition crate wiring all planes together.

### 8.18 `aurum-cli`

Command-line interface:

```text
aurum init
aurum join
aurum cluster status
aurum node drain
aurum bench
```

---

## 9. Current research hypotheses

These hypotheses define the project. They must be validated early.

| ID | Hypothesis | Status |
|---|---|---|
| H1 | Hybrid range + block-bitset queue engine can beat per-message queues | In progress; promising after mask/range-first rewrite |
| H2 | Ack bitmaps + AckRange/AckMask can express Rabbit-like ack/nack/redelivery semantics | Next after H1 |
| H3 | Native protocol with `route_id` reduces routing cost for hot routes | Planned |
| H4 | Append-only queue index + ack ledger can recover deterministically without full scans | Planned |
| H5 | Thread-per-core scales better than work stealing for hot queue state | Planned |
| H6 | NUMA-local placement has measurable impact and must be modeled | Planned |
| H7 | AMQP adapter can translate to CommandBatch without contaminating core | Planned |
| H8 | Shard map + route table epochs solve rebalance/failover cleanly | Planned |

---

## 10. H1 current design decision

The initial `block-bitset intrusive` attempt did not win because it used bitsets but still processed one message at a time.

Correct direction:

```text
range-first for sequential ready state
block-bitset/mask-first for sparse/retry/irregular state
active block lists for sparse blocks only
ack/nack/retry via ranges and masks
```

The queue engine should evolve toward:

```text
HybridRangeBlockQueue
├── sequential ready range
├── sparse ready block bitsets
├── inflight bitsets
├── acked bitsets
├── retry bitsets
├── active block lists
├── dirty block tracking
├── DeliveryRange / DeliveryMask
├── AckRange / AckMask
└── NackMask / RetryMask
```

---

## 11. Implementation order

Do not start with the whole broker.

### Phase 1 — `aurum-core`

Goal: make H1 real.

```text
HybridRangeBlockQueue
model-based tests
invariants
benchmarks using aurum-core directly
perf/asm review
```

### Phase 2 — Ack semantics

Goal: prove Rabbit-like delivery correctness.

```text
ack individual fallback
ack multiple
nack
reject
redelivery
prefetch-window behavior
no duplicate unless redelivery expected
no message loss
```

### Phase 3 — Internal command protocol

Goal: define the stable boundary between adapters and core.

```text
PublishBatch
AckBatch
NackBatch
CreditUpdate
DeliveryBatch
```

### Phase 4 — Compiled routing minimal path

Goal: prove `route_id -> QueueSet -> shard groups`.

```text
direct exchange
fanout exchange
route cache
QueueSet grouped by shard
```

### Phase 5 — In-memory shard runtime

Goal: prove thread-per-core ownership without storage/network complexity.

```text
shards
mailboxes
CommandBatch ingestion
DeliveryBatch output
synthetic producer/consumer
```

### Phase 6 — Append-only storage prototype

Goal: persist queue state and recover deterministically.

```text
payload log
queue index log
ack ledger
snapshot
crash/restart tests
```

### Phase 7 — Native protocol MVP

Goal: end-to-end single-node broker with native clients.

### Phase 8 — AMQP compatibility subset

Goal: RabbitMQ-like migration path for common operations.

### Phase 9 — Clustering and replication

Goal: RF=3 shard replication, shard map epochs, failover.

### Phase 10 — Ops/Deployment

Goal: single VPS, static cluster, Docker/Podman, Kubernetes operator.

---

## 12. Non-goals for the current stage

Do not implement these until the core is validated:

```text
full AMQP
Kafka gateway
Kubernetes operator
Raft/VSR production implementation
io_uring storage backend
NUMA scheduler
TLS
user management
full management UI
plugin system
dynamic loading
```

These are important later, but premature now.

---

## 13. Dependency policy

Use crates aggressively for non-differentiating work:

```text
benchmarks
property testing
fuzzing
CLI
config
observability
checksums
compression
HTTP admin
cold-path maps
```

Be conservative in the hot path:

```text
queue engine
ack engine
scheduler
routing fast path
shard mailbox
storage append path
```

A crate is acceptable in hot code only if:

```text
1. it preserves layout/control
2. it does not allocate unexpectedly
3. it does not hide dynamic dispatch
4. it wins or ties in benchmark
5. its invariants are understandable
```

---

## 14. Runtime policy

Investigate runtimes like `nio`, Glommio, Monoio and manual loops, but do not couple the architecture to any one of them early.

`aurum-runtime` should expose a backend boundary:

```text
spawn shard on worker/core
send batch to shard mailbox
run shard loop
collect metrics
```

`aurum-core` must remain runtime-free.

---

## 15. Lock-free policy

Use lock-free algorithms where they belong:

```text
SPSC rings
MPSC lanes
RCU snapshots
remote-free queues
metrics snapshots
cross-shard mailboxes
```

Do not force lock-free inside the queue engine if single-writer ownership is faster and simpler.

Memory-ordering discipline:

```text
Relaxed -> counters/approximate metrics only
Release -> publish initialized data
Acquire -> consume published data
AcqRel -> CAS/RMW operations
SeqCst -> requires explicit justification and benchmark
```

Every custom lock-free primitive should have:

```text
loom model
stress test
SAFETY.md or safety comments
benchmarks vs baseline
```

---

## 16. Distributed-system vision

AurumMQ should support arbitrary physical topology sizes:

```text
1 node
2 nodes
2 nodes + witness
3 nodes
4 nodes
6+ nodes
```

But quorum groups should be designed per shard:

```text
RF=1 for dev/small
RF=3 default HA
RF=5 for critical queues
2 data + witness profiles
learners for rebalancing
future flexible quorums only after formal validation
```

Do not require the whole cluster to have an odd number of machines. Instead:

```text
physical cluster can be any size
per-shard voting sets are chosen by placement engine
```

---

## 17. Deployment vision

AurumMQ should have three official deployment modes:

### 17.1 Standalone

```text
one VPS
one binary
systemd/Docker
RF=1
local durable confirms
```

### 17.2 Static cluster

```text
multiple VPS/on-prem nodes
static IPs or DNS seeds
join tokens
mTLS/WireGuard/private network recommended
internal shard placement/rebalance
```

### 17.3 Kubernetes

```text
Kubernetes operator
StatefulSets/PVCs/Services/CRDs
cloud-agnostic via CSI/StorageClass
works on EKS/AKS/GKE/OpenShift/K3s/on-prem
```

Kubernetes launches and observes nodes. AurumMQ owns the data placement and replication semantics.

---

## 18. AI agent guidance

If you are an AI agent working on this project, follow these rules.

### 18.1 Always preserve architecture boundaries

Do not make `aurum-core` depend on protocol, storage, runtime, cluster or operator crates.

### 18.2 Prefer small, benchmarkable changes

This project is research-first. Add small implementations with tests and benchmarks before broad feature work.

### 18.3 Do not introduce per-message hot-path objects

If a change creates per-message allocations, linked nodes, hash map operations or protocol objects in the hot path, it is probably wrong.

### 18.4 Keep adapters at the edge

AMQP/native/Kafka/etc. should translate to internal commands. They should not leak wire protocol types into the queue engine.

### 18.5 Use model-based tests for semantics

For queue behavior, maintain a simple reference model and compare the optimized engine against it.

### 18.6 Performance claims require measurement

Do not claim an optimization is faster unless there is a benchmark or a clear reasoning note explaining why it is expected and what must be measured.

### 18.7 Correctness comes before performance

A faster queue that loses, duplicates or redelivers incorrectly is invalid.

### 18.8 Keep documents updated

When architectural direction changes, update:

```text
docs/PROJECT_VISION_AND_AI_CONTEXT.md
docs/ARCHITECTURE_MODEL.md
docs/AURUM_CORE_IMPLEMENTATION_PLAN.md
README.md
```

---

## 19. Current immediate next task

The next concrete work item is:

```text
Implement the real aurum-core queue module according to docs/AURUM_CORE_IMPLEMENTATION_PLAN.md.
```

Specifically:

```text
1. Refactor aurum-core/src/queue.rs into queue/ modules.
2. Implement HybridRangeBlockQueue as the canonical queue engine.
3. Add invariant checks.
4. Add ModelQueue reference implementation.
5. Add differential tests.
6. Make experiments/h1-queue-engine use aurum-core directly.
7. Re-run H1 benchmarks.
```

---

## 20. The final shape we are aiming for

The long-term product should be able to do this:

```bash
aurum init --single-node
```

Then later:

```bash
aurum join --seed 10.8.0.1:9700 --token xxx
aurum cluster status
aurum rebalance apply
```

And application developers should be able to choose:

```text
AMQP/RabbitMQ-compatible path:
  easy migration

Native path:
  maximum throughput/low latency
```

Internally, the system should remain:

```text
range/mask-first
thread-per-core
NUMA-aware
append-only
compiled-routing
shard-owned
broker-managed
```

That is the essence of AurumMQ.
