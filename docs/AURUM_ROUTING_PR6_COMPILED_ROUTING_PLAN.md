# PR6 — Minimal Compiled Routing Layer

**Project:** AurumMQ  
**Phase:** After PR5 — Single-node in-memory broker executor  
**Scope:** `aurum-routing` + integration with `aurum-internal-protocol` and `aurum-broker`  
**Status:** Implementation plan  

---

## 0. Executive summary

PR6 introduces the first real routing layer for AurumMQ.

Until PR5, the in-memory broker can execute internal commands directly against known queues. That is useful for proving that `CommandBatch -> InMemoryShardExecutor -> aurum-core` works. But a Rabbit-like broker is not just a queue executor. Producers publish to exchanges and routing keys; the broker resolves those declarations into one or more queues.

PR6 adds the minimal version of that capability:

```text
exchange + routing_key
        ↓
compiled route table
        ↓
route_id / QueueSet
        ↓
shard-grouped queue targets
        ↓
InMemoryShardExecutor publish
```

The important architectural point is that routing must be split into two paths:

```text
Cold/control path:
  exchange declarations, queue bindings, route compiler, route table rebuilds

Hot/data path:
  route_id -> QueueSet -> grouped shard targets
```

The hot path must not traverse dynamic binding objects, allocate strings, evaluate protocol-specific types, or use `dyn Trait` per publish.

---

## 1. Why PR6 comes now

PR5 validated the internal command protocol against the queue engine and consumer sessions.

The next pressure point is routing. If we start the native protocol or AMQP adapter before routing, protocol adapters will be forced to publish directly to `QueueId`, which is not how the broker should work.

PR6 provides the missing abstraction:

```text
Protocol adapter:
  parses protocol-specific publish request
  asks routing layer to resolve route or uses cached route_id
  emits internal PublishBatch with RouteTarget

Routing layer:
  owns exchange/binding semantics
  returns QueueSet grouped by ShardId

Broker executor:
  takes QueueSet groups and appends to queue engines
```

This preserves the dependency boundary:

```text
aurum-core:
  knows ranges/masks/queues/consumers
  does not know exchanges or routing strings

in-memory broker:
  executes commands and applies routed targets

routing:
  compiles declarative rules to route tables

protocol adapters:
  translate AMQP/native requests to internal commands
```

---

## 2. Goals

PR6 must deliver:

1. A real `aurum-routing` crate with explicit route table structures.
2. Minimal exchange model: `direct` as P0, `fanout` as low-risk P1 inside the same PR if time allows.
3. `RouteId` resolution and validation.
4. Route table epochs/versions to detect stale route IDs.
5. Queue sets grouped by shard for future horizontal scale.
6. Integration with PR5's in-memory broker executor.
7. Tests for route compilation, route resolution, route table swaps, stale route IDs, unroutable publishes, and fanout/direct routing.
8. A small routing benchmark/experiment comparing route ID vs name/key lookup.

---

## 3. Non-goals

Do not implement in PR6:

```text
AMQP parser
native TCP protocol
topic exchange full matcher
headers exchange predicate VM
persistent route metadata
cluster-wide route table consensus
runtime thread-per-core mailboxes
storage durability
Kafka gateway
operator/Kubernetes integration
```

Topic and headers exchange should be designed for, but not completed in PR6.

---

## 4. Architectural principle

The central principle:

> Compile routing declarations into immutable, cache-friendly route tables. The data plane uses route IDs or array/table lookups, not protocol strings and mutable binding objects.

This is analogous to:

```text
regex source -> compiled regex
SQL query -> query plan
firewall rules -> packet classification table
Rabbit-like bindings -> route table
```

PR6 does not implement a JIT. It compiles routing into data structures, not machine code.

---

## 5. Target architecture

```text
Protocol adapters / tests / future clients
        │
        ▼
ResolveRoute(exchange, routing_key)
        │
        ▼
aurum-routing::RouteTable
        │
        ├── cold/warm path:
        │     exchange_id + routing_key -> RouteId
        │
        └── hot path:
              route_id -> QueueSetRef
                    │
                    ▼
              ShardGroupedQueueSet
                    │
                    ▼
              InMemoryBroker / ShardExecutor
                    │
                    ▼
              HybridRangeBlockQueue per QueueId
```

For PR6 single-node mode, every queue can be assigned to `ShardId::LOCAL` or `ShardId(0)`, but the data structure must already be grouped by `ShardId` so the design is not rewritten in PR7/PR8/cluster work.

---

## 6. Crate responsibilities

### 6.1 `aurum-types`

Owns stable small IDs and common value types:

```rust
QueueId
ExchangeId
RouteId
RouteTableVersion
ShardId
RoutingKeyHash
```

These types must be protocol-neutral.

### 6.2 `aurum-routing`

Owns:

```text
Exchange declarations
Binding declarations
RoutingConfig
RouteCompiler
RouteTable
RouteTableBuilder
QueueSetStorage
Direct exchange compiler
Fanout exchange compiler if included
Route resolution
RouteTable validation
```

### 6.3 `aurum-internal-protocol`

Should define or adapt:

```text
ResolveRouteCommand
RouteResolvedEvent
PublishRouteRef
RouteStale error
Unroutable error
```

But it should not depend on `aurum-routing` internals.

### 6.4 `aurum-broker`

Owns integration:

```text
InMemoryBroker holds Arc<RouteTable> or ArcSwap<RouteTable>
PublishBatch with RouteId gets resolved to QueueSet
PublishBatch with QueueId remains allowed only for tests/internal direct mode
Output confirms/errors generated per publish batch
```

---

## 7. Dependency rules

Allowed:

```text
aurum-routing -> aurum-types
aurum-internal-protocol -> aurum-types
aurum-broker -> aurum-routing + aurum-internal-protocol + aurum-core
aurum-protocol-native -> aurum-internal-protocol
aurum-protocol-amqp -> aurum-internal-protocol
```

Forbidden:

```text
aurum-core -> aurum-routing
aurum-core -> aurum-internal-protocol
aurum-routing -> aurum-core
aurum-routing -> AMQP/native protocol crates
aurum-internal-protocol -> aurum-core
```

Reason:

```text
core must stay protocol-free and routing-free
routing must not know queue-engine internals
internal protocol must be the neutral boundary
```

---

## 8. Data model

### 8.1 IDs

Add or verify these in `aurum-types`:

```rust
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExchangeId(pub u32);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueId(pub u32);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShardId(pub u32);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RouteTableVersion(pub u64);
```

`RouteId` should carry enough information to detect stale or invalid route references.

Recommended shape:

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RouteId {
    pub index: u32,
    pub generation: u32,
}
```

Alternative packed form:

```rust
#[repr(transparent)]
pub struct RouteId(u64);
```

Recommended for PR6: explicit struct for clarity. We can pack later.

### 8.2 Route table version

`RouteTableVersion` increments whenever the route table is rebuilt.

Every resolved route should contain:

```rust
pub struct ResolvedRoute {
    pub route_id: RouteId,
    pub version: RouteTableVersion,
    pub flags: RouteFlags,
}
```

The publish path validates:

```text
if publish.route_version != current_route_table.version:
    return RouteStale or attempt compatibility lookup depending policy
```

PR6 should implement strict stale detection. Grace windows can come later.

---

## 9. Exchange model

### 9.1 Exchange kinds

Use enums, not trait objects, for the route table model.

```rust
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeKind {
    Direct = 0,
    Fanout = 1,
    Topic = 2,
    Headers = 3,
}
```

PR6 implements:

```text
Direct: required
Fanout: optional but recommended if small
Topic: placeholder only
Headers: placeholder only
```

### 9.2 Exchange flags

Use bitflags for declaration semantics and future protocol mapping:

```rust
bitflags::bitflags! {
    pub struct ExchangeFlags: u16 {
        const DURABLE = 1 << 0;
        const AUTO_DELETE = 1 << 1;
        const INTERNAL = 1 << 2;
        const SYSTEM = 1 << 3;
    }
}
```

Flags are mostly cold/control path in PR6, but define them now to prevent ad-hoc booleans.

### 9.3 Exchange declaration

```rust
pub struct ExchangeDecl {
    pub id: ExchangeId,
    pub name: ExchangeName,
    pub kind: ExchangeKind,
    pub flags: ExchangeFlags,
}
```

For PR6, names can be `String`/`Arc<str>` in cold path. The hot path must use `ExchangeId` or `RouteId`.

---

## 10. Binding model

### 10.1 Binding declaration

```rust
pub struct BindingDecl {
    pub exchange_id: ExchangeId,
    pub queue_id: QueueId,
    pub routing_key: RoutingKey,
    pub flags: BindingFlags,
    pub target_shard: ShardId,
}
```

In PR6, `target_shard` can be supplied by the in-memory broker or defaulted to `ShardId(0)`. Later, the placement engine will assign it.

### 10.2 Binding flags

```rust
bitflags::bitflags! {
    pub struct BindingFlags: u16 {
        const ACTIVE = 1 << 0;
        const SYSTEM = 1 << 1;
    }
}
```

Keep it small.

### 10.3 Duplicate bindings

The compiler must deduplicate repeated bindings:

```text
same exchange_id + routing_key + queue_id = one target
```

This avoids duplicate deliveries from repeated declarations.

---

## 11. Routing keys and hashes

### 11.1 Deterministic hash

Do not use randomized `HashMap` hash values as route hashes. Route hashes must be deterministic across processes and future clusters.

For PR6, implement a simple deterministic hash in `aurum-routing` or `aurum-kernels`:

```text
FNV-1a 64-bit or another simple stable hash
```

This is not for security. It is a stable routing fingerprint.

```rust
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RoutingKeyHash(pub u64);
```

The compiler can still use `HashMap` internally, but the public/compiled key must use `RoutingKeyHash`.

### 11.2 Route key identity

For direct exchange route resolution, use:

```rust
pub struct DirectRouteKey {
    pub exchange_id: ExchangeId,
    pub routing_hash: RoutingKeyHash,
    pub routing_len: u16,
}
```

Hash + length reduces accidental equality on obvious collisions. For PR6, collision resolution can fallback to storing the original routing key in cold storage or comparing interned string IDs.

Recommended PR6 correctness behavior:

```text
cold resolve path stores exact routing key string in direct map key
compiled RouteEntry stores stable hash/len for metadata
hot route_id path does not compare strings
```

---

## 12. QueueSet representation

This is one of the most important design decisions.

The route table should not return `Vec<QueueId>` as the fundamental hot result. It should return a representation that can scale to fanout and shards.

### 12.1 Minimal adaptive enum

```rust
pub enum QueueSetRef<'a> {
    Empty,
    One(QueueTarget),
    Small(&'a SmallQueueSet),
    ShardGrouped(&'a ShardGroupedQueueSet),
}
```

For owned storage:

```rust
pub enum QueueSetEntry {
    Empty,
    One(QueueTarget),
    Small(SmallQueueSet),
    ShardGrouped(ShardGroupedQueueSet),
}
```

### 12.2 Queue target

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueTarget {
    pub shard_id: ShardId,
    pub queue_id: QueueId,
}
```

### 12.3 Small queue set

Use `SmallVec` or fixed array. For hot data, fixed is easier to control.

```rust
pub struct SmallQueueSet {
    pub len: u8,
    pub targets: [QueueTarget; 4],
}
```

If the project already uses `smallvec`, a `SmallVec<[QueueTarget; 4]>` is acceptable for PR6. But the compiled table should avoid heap allocations for the common case.

### 12.4 Shard grouped queue set

Future horizontal scaling needs grouping by shard:

```rust
pub struct ShardQueueGroup {
    pub shard_id: ShardId,
    pub first_target: u32,
    pub target_count: u16,
}

pub struct ShardGroupedQueueSet {
    pub first_group: u32,
    pub group_count: u16,
}
```

For PR6, a simpler owned representation is fine:

```rust
pub struct ShardGroupedQueueSet {
    pub groups: SmallVec<[ShardQueueGroupOwned; 4]>,
}

pub struct ShardQueueGroupOwned {
    pub shard_id: ShardId,
    pub queues: SmallVec<[QueueId; 4]>,
}
```

But the design must make it clear that this may move to compact arena storage later.

---

## 13. Route table structure

Recommended PR6 structure:

```rust
pub struct RouteTable {
    pub version: RouteTableVersion,
    exchanges: Vec<CompiledExchange>,
    route_entries: Vec<RouteEntry>,
    queue_sets: QueueSetStorage,
    direct_index: DirectResolveIndex,
}
```

### 13.1 RouteEntry

```rust
pub struct RouteEntry {
    pub generation: u32,
    pub exchange_id: ExchangeId,
    pub routing_hash: RoutingKeyHash,
    pub routing_len: u16,
    pub queue_set_id: QueueSetId,
    pub flags: RouteFlags,
}
```

### 13.2 RouteFlags

```rust
bitflags::bitflags! {
    pub struct RouteFlags: u16 {
        const EMPTY = 0;
        const FANOUT = 1 << 0;
        const DIRECT = 1 << 1;
        const HAS_MULTIPLE_TARGETS = 1 << 2;
        const UNROUTABLE = 1 << 3;
    }
}
```

### 13.3 QueueSetId

```rust
#[repr(transparent)]
pub struct QueueSetId(pub u32);
```

### 13.4 CompiledExchange

```rust
pub struct CompiledExchange {
    pub id: ExchangeId,
    pub kind: ExchangeKind,
    pub flags: ExchangeFlags,
    pub route_root: u32,
}
```

For direct exchange, `route_root` can point into direct map metadata. For fanout, it can point to a queue set.

---

## 14. Route compiler

### 14.1 Input

```rust
pub struct RoutingConfig {
    pub version: RouteTableVersion,
    pub exchanges: Vec<ExchangeDecl>,
    pub bindings: Vec<BindingDecl>,
}
```

### 14.2 Compiler

```rust
pub struct RouteCompiler;

impl RouteCompiler {
    pub fn compile(config: &RoutingConfig) -> Result<RouteTable, RouteCompileError>;
}
```

### 14.3 Compiler responsibilities

```text
validate unique exchange ids
validate exchange kind supported
validate queues are targetable
validate binding exchange exists
deduplicate duplicate bindings
group targets by shard
choose QueueSet representation
build resolve indexes
build route_id entries
return immutable RouteTable
```

### 14.4 Cold vs hot data

The compiler may use:

```text
HashMap
Vec
String
sorting
allocation
```

The compiled data plane table should prefer:

```text
Vec/Box<[T]>
small fixed arrays
integer IDs
route IDs
queue set IDs
```

---

## 15. Route resolution API

### 15.1 Cold/warm path resolve

```rust
impl RouteTable {
    pub fn resolve_direct(
        &self,
        exchange_id: ExchangeId,
        routing_key: &[u8],
    ) -> Result<ResolvedRoute, RouteResolveError>;
}
```

For PR6, direct exchange resolution can be warm path. It can use a map internally.

### 15.2 Hot path lookup

```rust
impl RouteTable {
    #[inline(always)]
    pub fn get_by_route_id(
        &self,
        route_id: RouteId,
        expected_version: RouteTableVersion,
    ) -> Result<QueueSetRef<'_>, RouteLookupError>;
}
```

This should be:

```text
bounds check
version/generation check
array lookup
queue set lookup
```

No string comparison.
No dynamic dispatch.
No allocation.

### 15.3 Optional direct queue target

For tests and internal management, keep a direct `QueueId` publish target:

```rust
pub enum PublishTarget {
    Queue(QueueId),
    Route { route_id: RouteId, version: RouteTableVersion },
}
```

This should exist in `aurum-internal-protocol`, not `aurum-core`.

---

## 16. Integration with internal protocol

PR4 likely introduced command batches. PR6 should adapt publish routing without breaking the boundary.

### 16.1 Publish target

Recommended protocol type:

```rust
pub enum PublishTarget {
    Queue(QueueId),
    Route(RoutePublishTarget),
}

pub struct RoutePublishTarget {
    pub route_id: RouteId,
    pub route_version: RouteTableVersion,
}
```

Do not embed routing key strings in hot `PublishBatch`.

### 16.2 Resolve route command

Add:

```rust
pub struct ResolveRouteCommand {
    pub request_id: CorrelationId,
    pub exchange_id: ExchangeId,
    pub routing_key: SmallVec<[u8; 64]> or Bytes-like cold payload,
}
```

Output:

```rust
pub struct RouteResolvedEvent {
    pub request_id: CorrelationId,
    pub route_id: RouteId,
    pub route_version: RouteTableVersion,
    pub flags: RouteFlags,
}
```

For PR6, if the internal protocol does not yet have request/response event channels, this can live in `aurum-broker` tests and be formalized later. But the type should be designed now.

### 16.3 Error types

```rust
pub enum RouteCommandError {
    ExchangeNotFound,
    UnsupportedExchangeKind,
    Unroutable,
    RouteIdStale,
    RouteIdInvalid,
    RouteGenerationMismatch,
}
```

These should map into `CommandErrorBatch`.

---

## 17. Integration with PR5 in-memory broker

### 17.1 Broker owns route table

```rust
pub struct InMemoryBroker {
    route_table: Arc<RouteTable>,
    shards: Vec<InMemoryShardExecutor>,
}
```

If `arc-swap` is already accepted as a workspace dependency:

```rust
pub struct InMemoryBroker {
    route_table: ArcSwap<RouteTable>,
    shards: Vec<InMemoryShardExecutor>,
}
```

For PR6, `Arc<RouteTable>` is enough unless route table swap is tested in-process.

### 17.2 Publishing by route

Flow:

```text
PublishBatch target = Route(route_id, version)
        ↓
InMemoryBroker::execute
        ↓
route_table.get_by_route_id(route_id, version)
        ↓
QueueSet groups
        ↓
for each ShardGroup:
    send/apply queue publish to matching shard executor
        ↓
confirm when all targets succeed
```

### 17.3 Publishing by queue remains supported

For tests/internal direct path:

```text
PublishTarget::Queue(queue_id)
```

This lets PR5 tests keep working while PR6 adds routing.

### 17.4 Fanout and confirms

If a route returns multiple queues:

```text
Publish confirm succeeds only if all target queue appends succeed.
```

Partial failure policy for PR6:

```text
No partial routing success allowed.
If any target fails, return CommandError.
Since PR6 is in-memory and local, this should only happen for invalid queue state.
```

In distributed/storage phases, this becomes more nuanced.

---

## 18. Static dispatch, dynamic dispatch, enums, generics

### 18.1 Hot path

Use enums and arrays:

```rust
match exchange.kind { ... }
route_id -> route_entries[index]
queue_set_id -> queue_sets[index]
```

Do not use:

```rust
Box<dyn ExchangeRouter>
Box<dyn QueueSet>
HashMap lookup per route_id publish
String compare per route_id publish
```

### 18.2 Cold path

Dynamic dispatch is allowed only if it simplifies compile-time route compiler extensions, e.g. future plugin route compilers.

Example allowed in cold path:

```rust
trait ExchangeCompiler {
    fn compile(&self, decls: &[BindingDecl]) -> Result<CompiledExchange, Error>;
}
```

But PR6 can avoid dynamic dispatch completely.

### 18.3 Generics

Use generics only where they materially help static dispatch or future benchmarking.

Possible:

```rust
RouteCompiler<H: RouteHasher>
```

But avoid over-generic APIs in PR6. Too many generics make the route layer harder to evolve.

Recommended:

```text
fixed deterministic hash function in PR6
generic hasher later only if benchmark/architecture requires it
```

### 18.4 Enums and bitflags

Use enums for closed semantic states:

```text
ExchangeKind
RouteLookupResult
QueueSetKind
RouteCompileError
RouteResolveError
```

Use bitflags for orthogonal flags:

```text
ExchangeFlags
BindingFlags
RouteFlags
QueueSetFlags
```

Do not use bitflags for mutually exclusive states.

---

## 19. Slice plan

## Slice 0 — Audit current PR4/PR5 protocol types

Before coding routing, inspect the actual current types:

```text
aurum-types
aurum-internal-protocol
aurum-broker in-memory executor
aurum-routing stub
```

Decide:

```text
where QueueId currently lives
where RouteId currently lives
how PublishBatch currently targets queues
how CommandErrorBatch is represented
whether SmallVec/ArrayVec is already dependency
```

Do not start by adding duplicate IDs.

Acceptance:

```text
No duplicate QueueId/ShardId/RouteId definitions.
A short note in PR description explains where each ID lives.
```

---

## Slice 1 — Add routing identity types

Implement or consolidate:

```text
ExchangeId
RouteId
RouteTableVersion
RoutingKeyHash
QueueSetId
```

Add helpers:

```rust
RouteId::new(index, generation)
RouteId::index()
RouteId::generation()
RouteTableVersion::initial()
RouteTableVersion::next()
```

Acceptance:

```text
cargo test -p aurum-types
No protocol-specific types introduced.
```

---

## Slice 2 — Implement routing declarations

In `aurum-routing`:

```text
exchange.rs
binding.rs
config.rs
flags.rs
error.rs
```

Types:

```text
ExchangeDecl
ExchangeKind
ExchangeFlags
BindingDecl
BindingFlags
RoutingConfig
RouteCompileError
```

Acceptance:

```text
Can construct RoutingConfig with direct exchange and bindings.
Validation catches missing exchange and duplicate exchange id.
```

---

## Slice 3 — Implement QueueSet representation

In `aurum-routing/src/queue_set.rs`:

```text
QueueTarget
QueueSetId
QueueSetEntry
QueueSetRef
SmallQueueSet
ShardGroupedQueueSet
QueueSetStorage
```

Required behavior:

```text
0 targets -> Empty
1 target -> One
2..=4 same/small targets -> Small
larger or multi-shard -> ShardGrouped
```

For PR6, this threshold can be simple.

Acceptance:

```text
queue set builder deduplicates targets
queue set builder groups by ShardId
unit tests for Empty/One/Small/ShardGrouped
```

---

## Slice 4 — Implement direct exchange compiler

Input:

```text
ExchangeDecl(kind=Direct)
Bindings for that exchange
```

Output:

```text
DirectResolveIndex
RouteEntry list
QueueSetStorage entries
```

Important:

```text
same routing_key may target multiple queues
same queue must not appear twice
route_id created for every direct routing key with at least one target
unroutable key returns Unroutable
```

Acceptance tests:

```text
direct one binding resolves to One target
direct same key two queues resolves to multi-target set
duplicate binding deduped
different keys resolve different route ids
unknown key returns Unroutable
```

---

## Slice 5 — Implement RouteTable and route_id lookup

Add:

```text
RouteTable
RouteEntry
ResolvedRoute
RouteLookupError
RouteResolveError
```

APIs:

```rust
RouteTable::version()
RouteTable::resolve(exchange_id, routing_key)
RouteTable::get_by_route_id(route_id, version)
```

Acceptance:

```text
route_id lookup does not allocate
route_id generation mismatch returns error
route table version mismatch returns stale error
out-of-bounds route id returns invalid error
```

---

## Slice 6 — Fanout exchange optional but recommended

Fanout is simple and useful for validating multi-target queue sets.

Behavior:

```text
routing_key ignored
all bound queues receive publish
route_id may represent exchange-level route
```

Tests:

```text
fanout no bindings -> Empty/Unroutable depending policy
fanout three queues -> multi-target QueueSet
fanout ignores routing_key
```

Policy question:

```text
Empty fanout route:
  return Unroutable in PR6
```

This can be changed later for AMQP mandatory/non-mandatory semantics.

---

## Slice 7 — Integrate with internal protocol

Update `aurum-internal-protocol`:

```text
PublishTarget::Route(RoutePublishTarget)
ResolveRouteCommand
RouteResolvedEvent
RouteCommandError mapping
```

If `PublishBatch` already has a target field, extend it carefully.

Do not break direct queue target tests.

Acceptance:

```text
PublishBatch can target QueueId or RouteId
ResolveRouteCommand can be represented without AMQP/native strings leaking into core
CommandErrorBatch can carry routing errors
```

---

## Slice 8 — Integrate with in-memory broker

Add route table to broker composition.

Behavior:

```text
execute ResolveRoute -> RouteResolvedEvent
execute PublishBatch(Route) -> route table lookup -> queue set -> shard executors
execute PublishBatch(Queue) -> old direct path
```

For PR6, single-node routing can assume all queue targets map to `ShardId(0)`.

Acceptance tests:

```text
route resolve then publish by route delivers to consumer
publish stale route returns error
publish unroutable returns error
fanout/direct multi-queue publish reaches all queues
old publish-by-queue tests still pass
```

---

## Slice 9 — Add h4/h5 routing experiment

Create:

```text
experiments/h4-routing
```

or extend current command-protocol experiment.

Workloads:

```text
route_id_publish_hot
resolve_direct_hot_key
resolve_direct_many_keys
fanout_small
route_table_swap_stale_id
```

Metrics:

```text
ns/publish route_id
ns/resolve exchange+routing_key
allocations if possible
relative ratio route_id vs string resolve
```

Expected outcome:

```text
route_id path should be much cheaper than string resolve
fanout small should be cheap enough for in-memory executor
```

---

## Slice 10 — Documentation

Add docs:

```text
docs/AURUM_ROUTING_PR6_COMPILED_ROUTING_PLAN.md
crates/aurum-routing/README.md
```

Document:

```text
compiled routing concept
route_id lifecycle
route table versioning
why route table is immutable
why queue sets are shard-grouped
how this supports native protocol and AMQP adapter
what is intentionally not implemented yet
```

---

## 20. Correctness tests

Minimum test set:

```rust
#[test]
fn direct_exchange_resolves_single_queue() {}

#[test]
fn direct_exchange_resolves_multiple_queues_for_same_key() {}

#[test]
fn direct_exchange_deduplicates_duplicate_bindings() {}

#[test]
fn direct_exchange_unknown_key_is_unroutable() {}

#[test]
fn route_id_lookup_returns_same_queue_set_as_resolve() {}

#[test]
fn stale_route_version_is_rejected() {}

#[test]
fn route_generation_mismatch_is_rejected() {}

#[test]
fn route_table_rebuild_keeps_old_table_immutable() {}

#[test]
fn queue_set_groups_targets_by_shard() {}

#[test]
fn in_memory_broker_publish_by_route_delivers() {}

#[test]
fn in_memory_broker_publish_stale_route_errors() {}
```

---

## 21. Property tests / model tests

Optional but valuable:

```text
generate random direct bindings
generate random routing keys
compile route table
compare resolve result against simple model map
```

Model:

```text
HashMap<(ExchangeId, RoutingKey), BTreeSet<QueueTarget>>
```

Compare:

```text
RouteTable.resolve == model lookup
QueueSet targets exactly equal model targets
```

This is cheap and should catch compiler bugs early.

---

## 22. Performance requirements

PR6 performance requirements are modest but important:

```text
route_id lookup:
  array lookup + generation/version check
  no allocation
  no string comparison

resolve_direct:
  can use map/string lookup
  should be benchmarked separately

queue set iteration:
  grouped by shard
  no duplicate queue targets
```

Do not prematurely optimize topic/header routing in PR6.

---

## 23. Scaling considerations

PR6 must not assume single queue target.

It must support:

```text
one publish -> many queues
one publish -> many shards
route table versioning
route id staleness
future route table swap
```

Even if PR5 broker has one shard today, the PR6 data model must be ready for:

```text
ShardId(0)
ShardId(1)
ShardId(N)
```

The dispatch layer can still handle only local shard in implementation, but queue sets should carry shard IDs.

---

## 24. Failure semantics

For PR6:

```text
Exchange not found -> error
Routing key not bound -> Unroutable error
Route version stale -> RouteStale error
Route id invalid -> RouteInvalid error
Route generation mismatch -> RouteInvalid/Stale error
Target queue not found in executor -> QueueNotFound error
Partial target failure -> CommandError for publish batch
```

AMQP-specific behavior like `mandatory=false` silently dropping unroutable messages is not implemented here. It belongs in the AMQP adapter mapping layer later.

The internal protocol should preserve enough information so adapters can decide:

```text
AMQP mandatory=false -> maybe treat Unroutable as dropped confirm
AMQP mandatory=true -> return basic.return later
Native -> explicit error by default
```

---

## 25. Route table updates

PR6 should implement immutable table rebuilds but not full live hot swap unless simple.

Design:

```text
RoutingConfig v1 -> RouteTable v1
RoutingConfig v2 -> RouteTable v2
Broker swaps Arc<RouteTable>
Old route IDs using v1 become stale under strict policy
```

Strict stale policy is easiest:

```text
publish with old version -> RouteStale
client/adapter resolves again
```

Later we can support grace periods:

```text
keep old route table versions alive for N seconds
or route id compatibility if target unchanged
```

Not needed in PR6.

---

## 26. Suggested file layout

```text
crates/aurum-routing/src/
  lib.rs
  id.rs              # if IDs not in aurum-types
  flags.rs
  exchange.rs
  binding.rs
  key.rs
  hash.rs
  config.rs
  queue_set.rs
  table.rs
  compiler.rs
  direct.rs
  fanout.rs          # optional
  error.rs
  tests.rs

crates/aurum-routing/tests/
  direct_routing.rs
  route_table.rs
  queue_set.rs
  model_diff.rs      # optional
```

Internal protocol changes:

```text
crates/aurum-internal-protocol/src/
  route.rs
  publish.rs
  error.rs
```

Broker integration:

```text
crates/apps/aurum-broker/src/in_memory/
  routing.rs
  broker.rs
  shard.rs
```

Experiment:

```text
experiments/h4-routing/
  Cargo.toml
  src/main.rs
```

---

## 27. Implementation order

Recommended coding order:

```text
1. ID audit and route ID types
2. Routing declarations
3. QueueSet builder/storage
4. Direct route compiler
5. RouteTable resolve + route_id lookup
6. Unit tests for aurum-routing
7. Internal protocol target update
8. InMemoryBroker route integration
9. Integration tests
10. Routing benchmark experiment
11. Documentation
```

Do not start with broker integration before route table unit tests pass.

---

## 28. Acceptance criteria

PR6 is complete when:

```text
1. aurum-routing has real declarations, compiler, RouteTable, QueueSet.
2. Direct exchange works.
3. Fanout exchange works or is explicitly deferred with placeholders/tests skipped.
4. RouteId lookup works and detects stale version/generation mismatch.
5. QueueSet groups targets by ShardId.
6. PublishBatch can target RouteId.
7. InMemoryBroker can publish by route.
8. Old direct QueueId publish path still works.
9. Route table rebuild does not mutate old table.
10. Unroutable and stale routes produce CommandErrorBatch entries.
11. cargo test --workspace passes.
12. Routing experiment shows route_id hot path cheaper than resolving strings.
13. aurum-core remains free of routing/protocol dependencies.
```

---

## 29. Risks

### Risk 1 — Overengineering route table too early

Mitigation:

```text
Implement direct + simple QueueSet first.
Keep topic/header placeholders.
Avoid MPHF/FST/SIMD routing in PR6.
```

### Risk 2 — Internal protocol becomes routing-specific

Mitigation:

```text
Expose only RouteId/RouteVersion/PublishTarget.
Keep compiler/table internals in aurum-routing.
```

### Risk 3 — Fanout duplicates payload handling complexity

Mitigation:

```text
In PR6, in-memory payload handles can be cloned cheaply.
Real payload dedup belongs to storage PR.
Confirm only after all queue appends succeed.
```

### Risk 4 — Route IDs invalidated too aggressively

Mitigation:

```text
Strict stale policy is acceptable in PR6.
Graceful compatibility can be added later.
```

### Risk 5 — Too much HashMap in hot path

Mitigation:

```text
Only resolve path may use map.
Publish by RouteId must be array/table lookup.
Add tests or comments enforcing this.
```

---

## 30. Future PRs enabled by PR6

After PR6:

```text
PR7 — Native protocol minimum
  ResolveRoute
  PublishBatch(route_id)
  ConsumeStart
  AckBatch
  NackBatch

PR8 — Storage append-only initial
  route confirms tied to durable append

PR9 — AMQP adapter initial
  exchange.declare
  queue.bind
  basic.publish -> resolve/direct route

Later — Topic exchange compiler
  tokenization
  DFA/trie
  route_id hot cache

Later — Header exchange predicate bytecode

Later — Shard map integration
  QueueSet groups dispatch to remote shard mailboxes
```

---

## 31. Final design rule

The main rule of PR6:

> Declarative routing is cold. Route IDs and queue sets are hot.

That means:

```text
Cold path:
  names, strings, binding declarations, HashMap, compiler

Hot path:
  RouteId, RouteTableVersion, QueueSetId, QueueTarget, ShardId
```

If a publish already has a valid `RouteId`, the broker should not need to know the original exchange name or routing key.

