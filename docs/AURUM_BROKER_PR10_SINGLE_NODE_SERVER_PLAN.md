# PR10 — Single-Node Networked Broker Server

Status: **closed (PR10)**  
Target crate(s): `aurum-broker`, `aurum-transport`, `aurum-cli`, protocol crates  
Depends on: PR4, PR5, PR6, PR7, PR8, PR9  
Primary goal: turn the existing in-memory/durable internal executor + routing + native/AMQP adapters into a runnable **single-node broker process**.

---

## 1. Why PR10 exists

By the end of PR9, the project should have these pieces:

```text
aurum-core
  queue engine + consumer session + Rabbit-like ack/nack/requeue semantics

aurum-internal-protocol
  CommandBatch / output batches / neutral protocol boundary

aurum-broker
  in-memory shard executor

aurum-routing
  minimal compiled routing / direct exchange / route_id

aurum-protocol-native
  transport-neutral native codec + adapter

aurum-storage
  initial append-only storage engine

aurum-protocol-amqp
  initial AMQP 0-9-1 adapter
```

But those pieces still do not form a real server. PR10 creates the first runnable broker:

```text
Native TCP client / AMQP TCP client
        ↓
aurum-transport listener
        ↓
protocol session adapter
        ↓
CommandBatch
        ↓
SingleNodeBroker / InMemoryShardExecutor / optional AppendOnlyStorage
        ↓
BrokerOutputBatch
        ↓
protocol outbound frames
        ↓
client
```

The purpose is not maximum performance yet. The purpose is to validate that all previous layers compose cleanly in a real process without contaminating `aurum-core`.

---

## 2. Non-goals

PR10 must **not** implement:

```text
multi-node clustering
replication
Raft / VSR / flexible quorum
thread-per-core production runtime
NUMA placement
io_uring production backend
TLS/mTLS production security
full RabbitMQ compatibility
Kafka gateway
Kubernetes operator behavior
advanced observability dashboard
```

PR10 may include placeholders and configuration fields for those future features, but it should not implement them.

Important: PR10 should also **not** redesign the core queue engine. `aurum-core` should remain protocol-free.

---

## 3. PR10 architectural objective

PR10 introduces a first server composition:

```text
apps/aurum-broker
  SingleNodeBroker
  SingleNodeBrokerConfig
  BrokerService
  ProtocolSessionRunner
  InProcessCommandDispatcher

edge/aurum-transport
  TcpListenerBackend
  Connection
  ConnectionId
  ReadBuffer / WriteBuffer
  ListenerConfig

apps/aurum-cli
  aurum broker start --config aurum.toml
  aurum broker dev
```

The resulting broker should be able to run locally:

```bash
aurum broker dev --native 127.0.0.1:7777 --amqp 127.0.0.1:5672
```

or:

```bash
aurum broker start --config examples/single-node.toml
```

---

## 4. High-level runtime diagram

```text
┌──────────────────────────────────────────────────────────────────┐
│                          aurum-broker                            │
│                                                                  │
│  ┌────────────────┐      ┌────────────────────────────┐          │
│  │ Native listener│─────▶│ NativeProtocolSession       │          │
│  └────────────────┘      └─────────────┬──────────────┘          │
│                                         │ CommandBatch             │
│  ┌────────────────┐      ┌─────────────▼──────────────┐          │
│  │ AMQP listener  │─────▶│ AMQPProtocolSession         │          │
│  └────────────────┘      └─────────────┬──────────────┘          │
│                                         │                         │
│                           ┌─────────────▼──────────────┐          │
│                           │ SingleNodeBroker            │          │
│                           │ routing + executor + storage │          │
│                           └─────────────┬──────────────┘          │
│                                         │ BrokerOutputBatch        │
│               ┌─────────────────────────┴───────────────────┐     │
│               │ protocol outbound adapters                    │     │
│               └───────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. Key design rule: composition must be replaceable

PR10 is a **single-node** server, but it must not block future PRs.

Future PRs should be able to replace:

```text
SingleNodeBroker
  with ShardedBroker / ThreadPerCoreBroker

InMemoryShardExecutor
  with ShardRuntimeExecutor

StdTcp transport
  with mio / nio / glommio / monoio / io_uring transport backend

NoopStorage
  with AppendOnlyStorage / DirectIoStorage
```

Therefore PR10 should define clear seams:

```text
Protocol sessions produce CommandBatch.
Broker service executes CommandBatch.
Protocol sessions encode BrokerOutputBatch.
Storage backend is selected during broker construction.
Transport backend is selected during listener construction.
```

---

## 6. Static dispatch vs dynamic dispatch policy

The user preference is correct: use static dispatch in hot paths, dynamic dispatch in cold/non-hot paths.

### 6.1 Static dispatch / enums in hot or warm paths

Use concrete types, generics, or enums for:

```text
Shard executor
Storage backend selected for executor
Protocol session state machine
Broker output processing
Queue/consumer operations
Route table lookup
```

Preferred pattern:

```rust
pub enum StorageBackend {
    Noop(NoopStorage),
    AppendOnly(AppendOnlyStorage),
}

impl StorageBackend {
    #[inline]
    pub fn append_publish_batch(&mut self, batch: &PublishBatch) -> StorageResult {
        match self {
            Self::Noop(s) => s.append_publish_batch(batch),
            Self::AppendOnly(s) => s.append_publish_batch(batch),
        }
    }
}
```

Why enum instead of `Box<dyn Storage>`?

```text
The executor may call storage once per publish batch or ack batch.
This is warm/hot enough that we want inlining opportunities and predictable dispatch.
```

For protocol sessions:

```rust
pub enum ProtocolSession {
    Native(NativeServerSession),
    Amqp(AmqpServerSession),
}
```

or separate typed listener runners:

```rust
run_listener::<NativeProtocolSession>(...)
run_listener::<AmqpProtocolSession>(...)
```

Both are acceptable. Avoid `dyn ProtocolSession` per frame in the first version.

### 6.2 Dynamic dispatch in cold paths

Dynamic dispatch is acceptable for:

```text
CLI subcommands
configuration loaders
admin plugins
future external gateway registry
observability exporters
operator integration
setup-time backend factories
```

Example:

```rust
pub trait ConfigSource {
    fn load(&self) -> Result<BrokerConfig, ConfigError>;
}
```

This is cold and does not matter.

---

## 7. Enums and bitflags policy

Use enums to model explicit protocol and broker states.

Examples:

```rust
#[repr(u8)]
pub enum ServerState {
    Starting = 0,
    Running = 1,
    Draining = 2,
    Stopping = 3,
    Stopped = 4,
}

#[repr(u8)]
pub enum ListenerKind {
    Native = 0,
    Amqp = 1,
    AdminHttp = 2,
}

#[repr(u8)]
pub enum BrokerMode {
    DevInMemory = 0,
    SingleNodePersistent = 1,
}
```

Use `bitflags` for compact state/capability masks:

```rust
bitflags::bitflags! {
    pub struct ListenerFlags: u16 {
        const ENABLED = 1 << 0;
        const TCP_NODELAY = 1 << 1;
        const LOW_LATENCY = 1 << 2;
        const ALLOW_PLAINTEXT = 1 << 3;
    }
}

bitflags::bitflags! {
    pub struct BrokerFeatureFlags: u32 {
        const NATIVE_PROTOCOL = 1 << 0;
        const AMQP_PROTOCOL = 1 << 1;
        const APPEND_ONLY_STORAGE = 1 << 2;
        const ROUTE_ID_FAST_PATH = 1 << 3;
    }
}
```

Do not use strings for internal state.

---

## 8. Proposed crate changes

### 8.1 `aurum-broker`

Add:

```text
crates/apps/aurum-broker/src/
  lib.rs
  config.rs
  service.rs
  single_node/
    mod.rs
    broker.rs
    executor.rs
    dispatcher.rs
    output.rs
    lifecycle.rs
    error.rs
  sessions/
    mod.rs
    native.rs
    amqp.rs
    protocol.rs
```

Responsibilities:

```text
construct single-node broker
own route table and executor
own storage backend selection
receive CommandBatch
return BrokerOutputBatch
manage protocol sessions at a high level
provide lifecycle methods: start, stop, drain
```

### 8.2 `aurum-transport`

Add or expand:

```text
crates/edge/aurum-transport/src/
  lib.rs
  tcp/
    mod.rs
    listener.rs
    connection.rs
    blocking.rs
  buffer.rs
  config.rs
  error.rs
  connection_id.rs
```

PR10 transport can be simple:

```text
std::net::TcpListener
std::net::TcpStream
one OS thread per connection or one thread per listener for dev mode
blocking I/O is acceptable in PR10
```

Why blocking is acceptable in PR10:

```text
PR10 validates composition and real protocol bytes.
PR11/PR12 can replace transport with thread-per-core runtime.
```

Do not over-engineer transport yet.

### 8.3 `aurum-cli`

Add commands:

```text
aurum broker dev
aurum broker start --config <file>
aurum broker check-config --config <file>
```

Minimal CLI implementation may use hand-written parsing or a crate if already accepted by the project policy. If adding a CLI crate, prefer `clap` in the cold path.

### 8.4 `aurum-observability`

Add minimal counters only:

```text
accepted_connections
active_connections
frames_in
frames_out
commands_in
commands_failed
publish_confirmed
deliveries_sent
acks_applied
nacks_applied
bytes_in
bytes_out
```

No Prometheus HTTP endpoint required yet, but types should exist.

---

## 9. Broker service API

Introduce a central service API in `aurum-broker`:

```rust
pub struct SingleNodeBroker<S = StorageBackend> {
    state: ServerState,
    routes: RouteTable,
    executor: InMemoryShardExecutor<S>,
    metrics: BrokerMetrics,
}

impl<S> SingleNodeBroker<S>
where
    S: StorageLike,
{
    pub fn execute(&mut self, batch: CommandBatch) -> BrokerOutputBatch;
    pub fn route_table_version(&self) -> RouteTableVersion;
    pub fn health(&self) -> BrokerHealth;
}
```

If `StorageLike` becomes too awkward because of enum dispatch, use:

```rust
pub struct SingleNodeBroker {
    state: ServerState,
    routes: RouteTable,
    executor: InMemoryShardExecutor<StorageBackend>,
    metrics: BrokerMetrics,
}
```

Recommended for PR10: **enum backend** over trait object.

---

## 10. Protocol session design

Protocol sessions should have the same conceptual shape.

```rust
pub trait ProtocolSessionLike {
    fn on_bytes(&mut self, input: &[u8], out: &mut Vec<CommandBatch>) -> SessionResult;
    fn on_broker_output(&mut self, output: BrokerOutputBatch, out: &mut Vec<u8>) -> SessionResult;
    fn is_closed(&self) -> bool;
}
```

But do not use this trait as `dyn` per frame in the default path. Use it as design documentation or test helper. The concrete session structs should be used directly.

```rust
pub struct NativeServerSession {
    codec: NativeCodec,
    inbound: NativeInboundAdapter,
    outbound: NativeOutboundAdapter,
    write_buffer: BytesMut,
}

pub struct AmqpServerSession {
    codec: AmqpFrameCodec,
    inbound: AmqpInboundAdapter,
    outbound: AmqpOutboundAdapter,
    channel_state: AmqpConnectionState,
    write_buffer: BytesMut,
}
```

---

## 11. Connection loop for PR10

For development, a simple blocking loop is fine:

```rust
fn run_native_connection(mut stream: TcpStream, broker: Arc<Mutex<SingleNodeBroker>>) {
    let mut session = NativeServerSession::new();
    let mut read_buf = [0u8; 64 * 1024];

    loop {
        let n = stream.read(&mut read_buf)?;
        if n == 0 { break; }

        let command_batches = session.decode_inbound(&read_buf[..n])?;

        for command_batch in command_batches {
            let output = broker.lock().unwrap().execute(command_batch);
            let bytes = session.encode_output(output)?;
            stream.write_all(&bytes)?;
        }
    }
}
```

This is not the final architecture. It is a PR10 harness. The final architecture will replace:

```text
Arc<Mutex<SingleNodeBroker>>
```

with:

```text
shard-owned broker state + SPSC/MPSC mailboxes
```

But PR10 should keep the hot core clean and validate end-to-end semantics first.

If the mutex becomes too ugly, use a single broker thread and command channel:

```text
connection thread -> CommandEnvelope -> broker thread -> ResponseEnvelope -> connection thread
```

Recommended PR10 approach:

```text
Option A for simplicity:
  Arc<Mutex<SingleNodeBroker>>

Option B if time allows:
  broker thread + std::sync::mpsc
```

Since PR11 will introduce real runtime/backpressure, PR10 can choose the simplest correct option.

---

## 12. Storage integration policy

PR10 should support two modes:

```text
DevInMemory:
  NoopStorage
  fast local testing

SingleNodePersistent:
  AppendOnlyStorage
  uses PR8 storage
```

Configuration:

```toml
[broker]
mode = "single-node-persistent"

[storage]
backend = "append-only"
data_dir = "./data"
durability = "flush-on-batch"

[listeners.native]
enabled = true
bind = "127.0.0.1:7777"

[listeners.amqp]
enabled = true
bind = "127.0.0.1:5672"
```

Do not implement sophisticated fsync policy in PR10 if PR8 does not expose it yet. Wire the option but keep behavior simple.

---

## 13. Routing integration policy

PR10 should expose a bootstrap route config.

For dev mode:

```toml
[[exchanges]]
name = "amq.direct"
kind = "direct"

[[queues]]
name = "test.queue"

[[bindings]]
exchange = "amq.direct"
queue = "test.queue"
routing_key = "test"
```

At startup:

```text
config -> RoutingConfig -> RouteCompiler -> RouteTable
```

Do not implement runtime binding changes through AMQP management yet unless PR9 already has minimal declarations wired. If declarations exist, route-table rebuild can be supported by calling the route compiler and atomically replacing the route table in the broker.

For PR10, acceptable modes:

```text
static startup routing config
+ optional runtime declare/bind if PR9 already supports it cleanly
```

---

## 14. Output routing back to connections

Single-node in-memory execution has an important issue: deliveries are asynchronous by nature.

A consumer connection sends:

```text
CONSUME_START / basic.consume
```

The broker may later produce:

```text
DeliveryEventBatch
```

PR10 needs a way to send deliveries to the correct connection.

Minimal model:

```rust
pub struct ConnectionRegistry {
    by_consumer: HashMap<ConsumerId, ConnectionId>,
}
```

When broker output contains deliveries:

```text
DeliveryEventBatch { consumer_id, ... }
```

PR10 sends frames to the corresponding session/connection.

Simplest implementation:

```text
single connection loop calls broker.execute()
broker.execute() returns deliveries only for that connection
```

Better PR10 implementation:

```text
broker owns output queues per ConnectionId
connection loop periodically drains its own output queue
```

Recommended PR10:

```text
For native harness:
  synchronous request/response path is enough.

For AMQP consume tests:
  add ConnectionRegistry + per-connection output queues.
```

This is important because AMQP deliveries are pushed, not merely responses.

---

## 15. Backpressure and buffer limits

Even in PR10, add simple defensive limits:

```text
max_frame_size
max_connection_read_buffer
max_connection_write_buffer
max_inflight_command_batches
max_payload_bytes_per_batch
max_connections
```

Use bitflags/enums:

```rust
#[repr(u8)]
pub enum BackpressureAction {
    Accept = 0,
    PauseRead = 1,
    CloseConnection = 2,
}
```

Do not implement advanced credit-based network backpressure yet. But do not allow unbounded `Vec<u8>` growth.

---

## 16. Error mapping

PR10 should centralize error mapping:

```text
BrokerError -> Native error frame
BrokerError -> AMQP channel.close / connection.close
BrokerError -> CLI/log diagnostic
```

Create:

```text
crates/apps/aurum-broker/src/error.rs
crates/apps/aurum-broker/src/error_map.rs
```

or adapter-specific mapping:

```text
aurum-protocol-native/src/error_map.rs
aurum-protocol-amqp/src/error_map.rs
```

Rules:

```text
core errors remain neutral
internal protocol errors remain neutral
adapters choose protocol-specific close/error frames
```

---

## 17. Slice plan

### Slice 0 — Audit PR7/PR9 integration points

Goal: make sure native and AMQP adapters expose transport-neutral functions that can be used by a server loop.

Checklist:

```text
Native codec can decode partial frames.
Native adapter can emit CommandBatch.
Native outbound can encode BrokerOutputBatch.
AMQP codec can decode partial frames.
AMQP adapter can emit CommandBatch/control commands.
AMQP outbound can encode BrokerOutputBatch to frames.
```

Acceptance:

```text
No protocol adapter calls aurum-core directly.
```

---

### Slice 1 — Broker configuration

Add:

```text
aurum-broker/src/config.rs
examples/single-node.toml
```

Types:

```rust
pub struct SingleNodeBrokerConfig {
    pub mode: BrokerMode,
    pub storage: StorageConfig,
    pub listeners: ListenerConfigSet,
    pub routing: RoutingBootstrapConfig,
    pub limits: BrokerLimits,
}
```

Acceptance:

```text
config defaults exist
config can be constructed in tests
CLI can print/check config
```

---

### Slice 2 — SingleNodeBroker service

Add:

```text
aurum-broker/src/single_node/broker.rs
aurum-broker/src/single_node/service.rs
```

Implement:

```rust
pub fn new(config: SingleNodeBrokerConfig) -> Result<Self, BrokerInitError>;
pub fn execute(&mut self, batch: CommandBatch) -> BrokerOutputBatch;
pub fn drain_connection_outputs(&mut self, connection_id: ConnectionId) -> BrokerOutputBatch;
```

Acceptance:

```text
unit test: publish by route -> confirm
unit test: consume + publish -> delivery output
unit test: ack -> settlement output
```

---

### Slice 3 — Storage backend selection

Add enum:

```rust
pub enum StorageBackend {
    Noop(NoopStorage),
    AppendOnly(AppendOnlyStorage),
}
```

Integrate with executor.

Acceptance:

```text
Noop mode works.
AppendOnly mode opens data directory and appends publish/ack facts.
If AppendOnly fails, broker init fails with clear error.
```

---

### Slice 4 — Native TCP listener

Add:

```text
aurum-transport/src/tcp/blocking.rs
aurum-broker/src/sessions/native.rs
```

Implement a development listener:

```rust
pub fn run_native_listener(addr: SocketAddr, broker: SharedBroker) -> Result<(), TransportError>;
```

Acceptance:

```text
integration test can connect with TcpStream
send HELLO
send RESOLVE_ROUTE
send PUBLISH_BATCH
receive confirm
```

If full integration tests are too hard in CI, create a local harness binary.

---

### Slice 5 — AMQP TCP listener

Add:

```text
aurum-broker/src/sessions/amqp.rs
```

Implement minimal AMQP server loop using PR9 codec/adapter.

Acceptance:

```text
AMQP protocol header accepted.
connection.start/start-ok/tune/tune-ok/open/open-ok transcript works.
basic.publish to static route works.
basic.consume receives delivery.
basic.ack settles delivery.
```

This does not need to pass arbitrary RabbitMQ clients yet, but it should pass our transcript tests.

---

### Slice 6 — CLI start/dev commands

Add to `aurum-cli`:

```text
aurum broker dev
aurum broker start --config examples/single-node.toml
aurum broker check-config --config examples/single-node.toml
```

Acceptance:

```text
cargo run -p aurum-cli -- broker check-config --config examples/single-node.toml
cargo run -p aurum-cli -- broker dev
```

---

### Slice 7 — Metrics and lifecycle

Add minimal lifecycle:

```text
starting -> running -> draining -> stopped
```

Add counters:

```text
connections accepted
commands processed
frames decoded
frames encoded
publish confirms
errors
```

Acceptance:

```text
broker health() returns Running after startup
metrics snapshot can be read without panics
```

---

### Slice 8 — End-to-end experiments

Create:

```text
experiments/h7-single-node-server
```

Workloads:

```text
native_publish_confirm
native_publish_consume_ack
native_nack_requeue_ack
amqp_transcript_publish
amqp_transcript_consume_ack
persistent_restart_smoke if storage recovery exists
```

Acceptance:

```bash
cargo run --release -p h7-single-node-server -- --protocol=native --messages=100000
cargo run --release -p h7-single-node-server -- --protocol=amqp-transcript --messages=10000
```

---

## 18. Testing plan

### Unit tests

```text
config defaults
broker init NoopStorage
broker init AppendOnlyStorage temporary dir
execute publish command
execute consume command
execute ack/nack command
error mapping
```

### Integration tests

```text
native codec over TcpStream
AMQP transcript over TcpStream
broker output to correct connection
multiple consumers on one queue
consumer disconnect/cancel requeues inflight
```

### Regression tests

```text
large payload rejected if over max_frame_size
unknown route returns command error
stale route epoch returns stale error
malformed frame closes connection/session
write buffer limit triggers backpressure action
```

---

## 19. Performance expectations

PR10 is not the final performance architecture. Still, avoid obvious anti-patterns:

```text
No per-message dynamic dispatch.
No protocol types inside aurum-core.
No unbounded buffers.
No per-message heap allocation in broker executor if avoidable.
No string routing in hot native publish path.
No storage fsync per message unless explicitly configured.
```

It is acceptable that the development TCP server uses blocking I/O and mutexes. Those are not the final hot data plane.

PR10 perf target:

```text
Native in-process path should remain close to PR5.
Native TCP path should be functional and reasonably fast.
AMQP path should be correct enough for transcript tests, not optimized.
```

---

## 20. Documentation deliverables

Add:

```text
docs/AURUM_BROKER_PR10_SINGLE_NODE_SERVER_PLAN.md
docs/SINGLE_NODE_SERVER_MODEL.md
docs/CONFIGURATION_V0.md
examples/single-node.toml
```

`SINGLE_NODE_SERVER_MODEL.md` should explain:

```text
this is not the final thread-per-core runtime
why protocol adapters remain outside core
how broker output is routed back to connections
how storage mode is selected
what will be replaced in PR11/PR12
```

---

## 21. Acceptance criteria

PR10 is complete when:

```text
1. `aurum broker dev` starts a single-node broker.
2. Native protocol can connect over TCP.
3. Native client/harness can resolve route, publish batch, consume, ack, nack.
4. AMQP transcript harness can connect, publish, consume, ack.
5. Broker can run with NoopStorage.
6. Broker can run with AppendOnlyStorage if PR8 provides it.
7. Command errors are mapped to native/AMQP errors.
8. No protocol crate is imported by aurum-core.
9. No storage file-format type is imported by aurum-core.
10. Route table is constructed at startup from config.
11. Connection output routing works for pushed deliveries.
12. cargo check --workspace --all-targets passes.
13. cargo test --workspace passes.
14. h7-single-node-server experiment runs.
```

---

## 22. Main risks

### Risk 1 — Mutex-based server hides future ownership issues

PR10 may use `Arc<Mutex<SingleNodeBroker>>` for simplicity. This must be documented as temporary. Do not let this leak into the hot-path design.

Mitigation:

```text
Keep broker execution API batch-based.
Keep shard/executor ownership isolated.
Plan PR11 thread-per-core replacement.
```

### Risk 2 — Protocol sessions become too coupled to broker internals

Mitigation:

```text
Protocol sessions only see CommandBatch and BrokerOutputBatch.
No direct queue/core access.
```

### Risk 3 — AMQP pushed deliveries are awkward

Mitigation:

```text
Implement ConnectionRegistry and per-connection output queues early.
Do not treat every delivery as a direct response to the last frame.
```

### Risk 4 — Storage mode contaminates executor

Mitigation:

```text
Use StorageBackend enum or generic executor parameter.
Do not expose storage record format to core.
```

---

## 23. What comes after PR10

Recommended next PRs:

```text
PR11 — Runtime backend research / thread-per-core skeleton
  evaluate nio/glommio/monoio/manual
  shard mailboxes
  pinned shard loops

PR12 — Improved storage recovery + broker restart
  deterministic recovery of payload/index/ack ledger

PR13 — AMQP compatibility hardening
  RabbitMQ client smoke tests
  more method semantics

PR14 — Native client SDK
  Rust client first

PR15 — Multi-shard single-node routing
  QueueSet grouped by shard
  shard ownership inside one process
```

PR10 is the bridge between “library components” and “actual broker process”.
