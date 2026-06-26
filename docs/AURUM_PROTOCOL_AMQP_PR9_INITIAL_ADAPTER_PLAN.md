# PR9 — Initial AMQP 0-9-1 Compatibility Adapter

## Status

**Target PR:** PR9  
**Area:** `crates/aurum-protocol-amqp` + minimal integration with `aurum-broker`, `aurum-routing`, and `aurum-internal-protocol`  
**Depends on:**

- PR3: Rabbit-like delivery semantics in `aurum-core`
- PR4: Internal Command Protocol
- PR5: Single-node in-memory broker executor
- PR6: Minimal compiled routing
- PR7: Minimal native protocol adapter
- PR8: Initial append-only storage engine

## One-line goal

PR9 introduces the first real AMQP 0-9-1 compatibility boundary:

> Decode a minimal RabbitMQ-compatible AMQP session, translate AMQP methods/properties/body frames into AurumMQ internal command batches, and encode broker outputs back into AMQP method/content/body frames without leaking AMQP types into `aurum-core`.

This PR is not full RabbitMQ compatibility. It is the first compatibility slice that proves existing AMQP clients can eventually sit on top of AurumMQ's internal command protocol.

---

## Why PR9 now

Up to PR8, AurumMQ has a native-first internal architecture:

```text
native protocol bytes
  -> NativeCodec
  -> CommandBatch
  -> compiled routing
  -> in-memory/durable broker executor
  -> aurum-core queue + consumer sessions
  -> output event batches
```

PR9 adds the first RabbitMQ-facing adapter path:

```text
AMQP 0-9-1 frames
  -> AmqpCodec
  -> AmqpConnectionState / AmqpChannelState
  -> AMQP method translator
  -> CommandBatch / control command / route resolve
  -> broker executor
  -> AMQP outbound frames
```

This is the point where we validate the most important product promise:

```text
Rabbit-like ergonomics outside.
AurumMQ range/mask/log data plane inside.
```

---

## Core design principle

`aurum-protocol-amqp` is an **edge adapter**, not broker logic.

It may know:

```text
AMQP frames
AMQP classes/methods
connection/channel state
content headers
basic properties
delivery tags as exposed to AMQP
consumer tags
protocol errors
```

It must not own:

```text
queue state
ack bitmap state
routing table internals
storage file formats
cluster placement
thread-per-core scheduling
```

`aurum-core` must remain free of AMQP names and frame types.

The dependency rule is:

```text
aurum-protocol-amqp
  depends on aurum-types
  depends on aurum-internal-protocol
  may depend on aurum-plugin-api / aurum-gateway-api
  must not depend on aurum-core
  must not depend on aurum-storage
  must not depend on aurum-runtime
```

`aurum-broker` composes the adapter with routing, broker execution, storage, and command output handling.

---

## Scope of PR9

PR9 should implement enough AMQP 0-9-1 to support this flow in a transport-neutral harness:

```text
protocol header
connection.start / start-ok
connection.tune / tune-ok
connection.open / open-ok
channel.open / open-ok
exchange.declare / declare-ok
queue.declare / declare-ok
queue.bind / bind-ok
basic.qos / qos-ok
basic.consume / consume-ok
basic.publish + content header + body frames
basic.deliver + content header + body frames
basic.ack
basic.nack
basic.reject
basic.cancel / cancel-ok
channel.close / close-ok
connection.close / close-ok
```

The minimum working scenario:

```text
AMQP client declares exchange/queue/binding
AMQP client publishes persistent/non-persistent message
AMQP consumer receives Basic.Deliver + content
AMQP consumer acks
AMQP consumer nacks with requeue=true
AMQP consumer cancels
```

PR9 must be transport-neutral. It decodes/encodes AMQP bytes, but it does not need to open TCP sockets yet. TCP belongs to a later transport/runtime PR.

---

## Non-goals of PR9

Do not implement yet:

```text
TLS listener
real TCP accept loop
full RabbitMQ management API
full SASL/auth system
full AMQP transactions
tx.select / tx.commit / tx.rollback
confirm.select full publisher confirm semantics
basic.get synchronous polling
exchange.delete / queue.delete / queue.purge semantics
alternate exchanges
mandatory/immediate full behavior
headers exchange full routing
priority queues
AMQP federation/shovel
automatic connection recovery client logic
full RabbitMQ policy compatibility
AMQP 1.0
```

It is acceptable to parse unsupported methods and return deterministic channel/connection close errors.

---

## High-level architecture

```text
crates/aurum-protocol-amqp
├── wire
│   ├── frame.rs          # AMQP frame header/body/end marker
│   ├── codec.rs          # incremental frame decode / encode
│   ├── constants.rs      # class IDs, method IDs, frame kinds, reply codes
│   ├── field_table.rs    # AMQP field table values
│   ├── shortstr.rs       # shortstr validation/codec
│   ├── longstr.rs        # longstr codec
│   └── properties.rs     # BasicProperties + property flags
│
├── method
│   ├── mod.rs            # AmqpMethod enum
│   ├── connection.rs
│   ├── channel.rs
│   ├── exchange.rs
│   ├── queue.rs
│   ├── basic.rs
│   └── confirm.rs        # placeholder/minimal confirm.select if needed
│
├── session
│   ├── connection.rs     # AmqpConnectionState
│   ├── channel.rs        # AmqpChannelState
│   ├── content.rs        # pending content assembly
│   ├── consumers.rs      # consumer_tag <-> ConsumerId mapping
│   ├── route_cache.rs    # per-channel AMQP publish route cache
│   └── error.rs
│
├── translate
│   ├── inbound.rs        # AMQP -> internal/control commands
│   ├── outbound.rs       # broker outputs -> AMQP frames
│   ├── control.rs        # declare/bind/open/close semantics
│   └── errors.rs         # internal errors -> channel.close/connection.close
│
├── harness
│   └── in_memory.rs      # transport-neutral AMQP transcript runner
│
└── lib.rs
```

---

## Module responsibilities

### `wire::codec`

Responsible for raw AMQP frames:

```text
Frame header:
  frame_type: u8
  channel: u16
  size: u32
  payload: [u8; size]
  frame_end: 0xCE
```

It must support incremental decode:

```rust
pub enum DecodeStatus<T> {
    Complete(T),
    NeedMore,
}
```

It must enforce:

```text
max frame size
valid frame end marker
valid frame type
no panic on truncated input
no allocation bombs
```

The codec is allowed to use `bytes::BytesMut` and `bytes::Buf/BufMut` in PR9. It must keep payload copies explicit.

### `wire::properties`

AMQP publish content arrives as:

```text
basic.publish method frame
content header frame
one or more content body frames
```

`BasicProperties` should be parsed into a cold metadata struct:

```rust
pub struct BasicProperties {
    pub content_type: Option<ShortStr>,
    pub content_encoding: Option<ShortStr>,
    pub headers: FieldTable,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<ShortStr>,
    pub reply_to: Option<ShortStr>,
    pub expiration: Option<ShortStr>,
    pub message_id: Option<ShortStr>,
    pub timestamp: Option<u64>,
    pub message_type: Option<ShortStr>,
    pub user_id: Option<ShortStr>,
    pub app_id: Option<ShortStr>,
}
```

In PR9 we only need to preserve and roundtrip the common fields used by `basic.publish`/`basic.deliver`. Headers can start as a conservative field-table implementation with a limited set of value types.

### `method::*`

Expose typed AMQP methods:

```rust
pub enum AmqpMethod {
    Connection(ConnectionMethod),
    Channel(ChannelMethod),
    Exchange(ExchangeMethod),
    Queue(QueueMethod),
    Basic(BasicMethod),
    Confirm(ConfirmMethod),
}
```

Use enums for method families, not trait objects. This is edge/cold-ish code, but enums keep decoding explicit, testable, and easy to map to reply codes.

### `session::*`

Track AMQP connection/channel protocol state:

```rust
pub enum ConnectionPhase {
    AwaitProtocolHeader,
    AwaitStartOk,
    AwaitTuneOk,
    AwaitOpen,
    Open,
    Closing,
    Closed,
}

pub enum ChannelPhase {
    Closed,
    Opening,
    Open,
    Closing,
}
```

Track per-channel state:

```rust
pub struct AmqpChannelState {
    pub channel_id: AmqpChannelId,
    pub phase: ChannelPhase,
    pub prefetch_count: u16,
    pub consumers: ConsumerTagMap,
    pub pending_content: Option<PendingContent>,
    pub route_cache: AmqpRouteCache,
}
```

### `translate::inbound`

Convert AMQP methods into internal operations.

Examples:

```text
queue.declare
  -> control command: declare queue

exchange.declare
  -> control command: declare exchange

queue.bind
  -> control command: bind queue to exchange

basic.qos
  -> channel/session prefetch update

basic.consume
  -> internal consumer start / credit command

basic.publish + header + body
  -> route resolve + PublishBatch

basic.ack
  -> AckCommandBatch

basic.nack / basic.reject
  -> NackCommandBatch
```

This module must not directly mutate `aurum-core`. It produces commands or calls a narrow broker-facing adapter trait supplied by `aurum-broker`.

### `translate::outbound`

Convert broker outputs into AMQP frames:

```text
DeliveryEventBatch
  -> basic.deliver method frame
  -> content header frame
  -> content body frame(s)

PublishConfirmBatch
  -> basic.ack if confirm mode is enabled later

CommandErrorBatch
  -> channel.close or connection.close depending on scope

ConsumerStarted
  -> basic.consume-ok

ConsumerCanceled
  -> basic.cancel-ok / basic.cancel
```

---

## Important semantic gap: AMQP delivery metadata

AMQP `basic.deliver` requires fields that native protocol may not require:

```text
consumer_tag
delivery_tag
redelivered
exchange
routing_key
```

Therefore PR9 must ensure broker delivery events carry enough cold metadata to encode AMQP deliveries.

Options:

### Option A — store AMQP metadata in delivery events

```rust
pub struct DeliveryMetadata {
    pub exchange_name: Option<ShortStringId>,
    pub routing_key: Option<RoutingKeyId>,
    pub protocol_meta_ref: Option<MessageMetadataRef>,
}
```

Pros:

```text
clear
AMQP outbound can encode accurately
works for initial MVP
```

Cons:

```text
may duplicate strings in early implementation
```

### Option B — adapter-local publish metadata table

The AMQP adapter stores metadata by payload/message ref and recovers it on delivery.

Pros:

```text
keeps internal events smaller
```

Cons:

```text
breaks if delivery happens from another adapter/process later
bad for storage/recovery
```

### Decision for PR9

Use **Option A** with explicit cold metadata references. It is correct and future-friendly. Later storage can persist metadata once per payload batch, not per delivery.

---

## Route resolution strategy

AMQP publishes include string exchange/routing key on every message:

```text
basic.publish(exchange="orders", routing_key="created.eu")
```

AurumMQ hot path wants:

```text
RouteId + RouteTableVersion
```

PR9 should implement a per-channel route cache:

```rust
pub struct AmqpRouteCacheKey {
    pub exchange_hash: u64,
    pub routing_key_hash: u64,
    pub route_table_version: RouteTableVersion,
}

pub struct AmqpRouteCacheEntry {
    pub route_id: RouteId,
    pub queue_set_id: QueueSetId,
}
```

Flow:

```text
basic.publish received
  -> check channel route cache
  -> cache hit: emit PublishBatch::Route(route_id)
  -> cache miss: request route resolve from broker/routing layer
  -> update cache
  -> emit PublishBatch::Route(route_id)
```

In PR9, the harness can resolve synchronously through an in-memory broker facade. Later, real runtime can make route resolution asynchronous.

If route table version changes:

```text
broker returns RouteStale
adapter invalidates channel route cache
adapter resolves again
```

---

## Command boundary design

PR9 will likely expose a small broker-facing trait so the AMQP adapter can be tested without TCP:

```rust
pub trait AmqpBrokerPort {
    fn handle_control(&mut self, command: AmqpControlCommand) -> AmqpControlResult;
    fn handle_command_batch(&mut self, batch: CommandBatch) -> BrokerOutputBatch;
    fn resolve_route(&mut self, request: AmqpRouteResolveRequest) -> AmqpRouteResolveResult;
}
```

This trait is **not** used inside the hot queue engine. It is an edge/harness integration boundary. Dynamic dispatch is acceptable here if used by test harnesses or plugin loading:

```text
OK:
  Box<dyn AmqpBrokerPort> in test harness or external gateway

Prefer static dispatch:
  AmqpSession<B: AmqpBrokerPort> in embedded broker
```

Design rule:

```text
Adapter/harness boundary can use dynamic dispatch.
Hot command execution must remain static/batch-oriented.
```

---

## Static dispatch, dynamic dispatch, generics, enums, bitflags

### Static dispatch

Use generics for embedded adapters:

```rust
pub struct AmqpSession<B> {
    broker: B,
    connection: AmqpConnectionState,
}
```

or:

```rust
impl<B: AmqpBrokerPort> AmqpSession<B> {
    pub fn receive_bytes(&mut self, input: &[u8], output: &mut Vec<AmqpFrame>) -> Result<(), AmqpError>
}
```

This lets the compiler inline harness/broker boundary where possible.

### Dynamic dispatch

Allowed for:

```text
external gateway mode
plugin registry
test harness polymorphism
cold admin/control integration
```

Avoid in:

```text
per-frame decode hot loop if unnecessary
per-message publish path
ack/nack command application
batch execution
```

### Enums

Use enums for protocol states and typed methods:

```rust
pub enum AmqpFramePayload {
    Method(AmqpMethod),
    Header(ContentHeader),
    Body(Bytes),
    Heartbeat,
}

pub enum BasicMethod {
    Qos(BasicQos),
    Consume(BasicConsume),
    Publish(BasicPublish),
    Ack(BasicAck),
    Nack(BasicNack),
    Reject(BasicReject),
    Cancel(BasicCancel),
}
```

Enums are preferred here because AMQP method decoding is a finite protocol state machine.

### Bitflags

Use bitflags for AMQP flags and properties:

```rust
bitflags::bitflags! {
    pub struct BasicPublishFlags: u8 {
        const MANDATORY = 1 << 0;
        const IMMEDIATE = 1 << 1;
    }
}

bitflags::bitflags! {
    pub struct BasicAckFlags: u8 {
        const MULTIPLE = 1 << 0;
    }
}
```

For content properties, property flags are part of AMQP wire encoding and should be represented as a compact flag type.

---

## Slice plan

## Slice 0 — Audit current protocol/broker outputs

Before coding, audit PR4-PR8 types:

```text
CommandBatch
PublishBatch
AckCommandBatch
NackCommandBatch
DeliveryEventBatch
BrokerOutputBatch
PublishConfirmBatch
CommandErrorBatch
RouteResolvedEvent
PayloadHandle / PayloadRef
Message metadata type, if any
```

Questions to answer:

```text
Can PublishBatch carry exchange/routing metadata?
Can DeliveryEventBatch carry AMQP delivery metadata?
Can CommandErrorBatch distinguish channel vs connection scope?
Can AckCommandBatch target a specific consumer/channel session?
Can NackCommandBatch represent requeue=true/false?
```

Expected output:

```text
small compatibility adjustments to aurum-internal-protocol if needed
no AMQP types in aurum-core
```

---

## Slice 1 — AMQP wire frame codec

Implement:

```text
wire/constants.rs
wire/frame.rs
wire/codec.rs
wire/error.rs
```

Types:

```rust
pub enum FrameKind {
    Method,
    Header,
    Body,
    Heartbeat,
}

pub struct FrameHeader {
    pub kind: FrameKind,
    pub channel: u16,
    pub size: u32,
}

pub struct RawFrame {
    pub header: FrameHeader,
    pub payload: Bytes,
}
```

Tests:

```text
protocol header parse
frame roundtrip
truncated frame -> NeedMore
bad frame end -> ProtocolError
unknown frame type -> ProtocolError
oversized frame -> ProtocolError
heartbeat frame decode/encode
```

DoD:

```text
cargo test -p aurum-protocol-amqp wire::
```

---

## Slice 2 — AMQP primitive codec

Implement:

```text
shortstr
longstr
field table
booleans packed in method flags
u8/u16/u32/u64 AMQP big-endian parsing
```

Types:

```rust
pub struct ShortStr(SmallVec<[u8; 32]>);
pub struct LongStr(Bytes);
pub enum FieldValue { ... }
pub struct FieldTable { ... }
```

Supported `FieldValue` in PR9:

```text
Bool
ShortShortInt
ShortShortUInt
ShortInt
ShortUInt
LongInt
LongUInt
LongLongInt
LongLongUInt
Float
Double
Decimal
ShortString
LongString
Timestamp
FieldArray
FieldTable
Void
```

If implementing full field tables is too large for PR9, support a strict subset and reject unsupported field value tags with deterministic protocol errors. However, queue/exchange arguments often use field tables, so avoid making this too narrow.

Tests:

```text
shortstr max length
shortstr invalid too long
field table empty
field table with x-dead-letter-exchange-like argument
field table roundtrip
longstr fragmented input
```

---

## Slice 3 — Method decoder/encoder subset

Implement typed methods for:

```text
Connection.Start
Connection.StartOk
Connection.Tune
Connection.TuneOk
Connection.Open
Connection.OpenOk
Connection.Close
Connection.CloseOk

Channel.Open
Channel.OpenOk
Channel.Close
Channel.CloseOk

Exchange.Declare
Exchange.DeclareOk

Queue.Declare
Queue.DeclareOk
Queue.Bind
Queue.BindOk

Basic.Qos
Basic.QosOk
Basic.Consume
Basic.ConsumeOk
Basic.Cancel
Basic.CancelOk
Basic.Publish
Basic.Deliver
Basic.Ack
Basic.Nack
Basic.Reject

Confirm.Select
Confirm.SelectOk        # optional placeholder if cheap
```

Do not use a trait object per method. Use enums.

Tests:

```text
method class/method IDs map correctly
known method roundtrip
unknown class -> protocol error
unknown method -> protocol error
basic.publish flags parse mandatory/immediate
basic.ack multiple flag parse
basic.nack multiple/requeue flags parse
```

---

## Slice 4 — Connection and channel state machine

Implement:

```text
AmqpConnectionState
AmqpChannelState
AmqpSession
```

Connection handshake:

```text
client protocol header
server connection.start
client connection.start-ok
server connection.tune
client connection.tune-ok
client connection.open
server connection.open-ok
```

Channel flow:

```text
channel.open -> channel.open-ok
channel.close -> channel.close-ok
```

Protocol errors:

```text
method before connection open
method on closed channel
content body without pending publish
header without pending publish
body size exceeds content header body_size
channel id 0 used for channel method where invalid
unknown channel
```

Tests:

```text
valid handshake transcript
method before open returns connection.close
channel method on unopened channel returns channel.close
channel.close is idempotent enough for harness
heartbeat accepted in open state
```

---

## Slice 5 — Content assembly for `basic.publish`

AMQP publish is multi-frame:

```text
basic.publish method
content header with body_size + properties
body frame(s)
```

Implement:

```rust
pub struct PendingPublishContent {
    pub channel: AmqpChannelId,
    pub publish: BasicPublish,
    pub properties: Option<BasicProperties>,
    pub expected_body_size: u64,
    pub body: BytesMut,
}
```

Rules:

```text
body frames accumulate until expected_body_size
if body too large -> channel.close
if another publish starts before previous content complete -> channel.close
zero-length body is valid after header
body may span multiple frames
```

When complete:

```text
PendingPublishContent
  -> AmqpPublishIntent
  -> route cache / route resolve
  -> PublishBatch
```

Tests:

```text
single body frame publish
multi body frame publish
zero-byte publish
body before header -> error
header without publish -> error
body too large -> error
publish with delivery_mode=2 maps to persistent flag
```

---

## Slice 6 — Control commands: declare/bind/qos/consume/cancel

AMQP declarations must update the routing/control plane through `aurum-broker`, not `aurum-core`.

Implement broker-facing control intents:

```rust
pub enum AmqpControlCommand {
    DeclareExchange(DeclareExchange),
    DeclareQueue(DeclareQueue),
    BindQueue(BindQueue),
    StartConsumer(StartConsumer),
    CancelConsumer(CancelConsumer),
    SetQos(SetQos),
}
```

Mapping:

```text
exchange.declare
  -> routing/control declare exchange
  -> exchange.declare-ok

queue.declare
  -> broker/control declare queue
  -> queue.declare-ok

queue.bind
  -> routing/control bind queue
  -> queue.bind-ok

basic.qos
  -> update channel/session prefetch
  -> basic.qos-ok

basic.consume
  -> create ConsumerSession through broker executor
  -> basic.consume-ok

basic.cancel
  -> cancel ConsumerSession
  -> basic.cancel-ok
```

Tests:

```text
declare direct exchange then queue then bind
basic.qos sets prefetch
basic.consume creates consumer tag mapping
basic.cancel removes consumer mapping
passive declare unsupported behavior deterministic
```

---

## Slice 7 — Publish mapping

Map complete publish content to internal publish command.

AMQP source:

```text
exchange: ShortStr
routing_key: ShortStr
mandatory: bool
immediate: bool
properties: BasicProperties
payload: Bytes
```

Internal target:

```text
route_id + route_table_version
payload handle/ref
message metadata ref
publish flags
```

Initial mapping:

```text
delivery_mode=2 -> persistent intent
priority -> message priority if queue supports it later
expiration -> TTL metadata placeholder
headers -> cold metadata
mandatory -> return error if unroutable later
immediate -> unsupported deterministic error or ignored by explicit policy
```

PR9 decision:

```text
mandatory=false, unroutable -> drop + optional confirm/success policy
mandatory=true, unroutable -> basic.return later or channel error placeholder
immediate=true -> not supported; return channel.close or soft error according to config
```

Prefer to implement a deterministic `UnsupportedImmediateFlag` error now rather than silently lying.

Tests:

```text
publish to declared direct route
publish route cache hit on second publish
publish route stale invalidates cache
unroutable publish deterministic behavior
persistent flag mapping
headers/properties preserved in delivery metadata
```

---

## Slice 8 — Delivery mapping

Map broker delivery events to AMQP outbound frames.

Broker output:

```text
DeliveryEventBatch
  consumer_id
  delivery_tag
  redelivered
  payload ref/bytes
  metadata
```

AMQP output:

```text
basic.deliver method frame
content header frame
content body frame(s)
```

Need mapping:

```text
ConsumerId -> consumer_tag
DeliveryTag -> AMQP delivery_tag
redelivery flag -> basic.deliver.redelivered
exchange/routing metadata -> basic.deliver.exchange/routing_key
BasicProperties -> content header properties
Payload bytes -> body frames respecting frame_max
```

Tests:

```text
broker delivery encodes basic.deliver
large body splits by frame_max
redelivered flag set after nack requeue
consumer_tag is correct
properties roundtrip from publish to deliver
```

---

## Slice 9 — Ack/nack/reject mapping

AMQP inbound:

```text
basic.ack(delivery_tag, multiple)
basic.nack(delivery_tag, multiple, requeue)
basic.reject(delivery_tag, requeue)
```

Internal:

```text
AckCommandBatch
NackCommandBatch
```

Mapping rules:

```text
basic.ack multiple=false -> AckOne/Tag command
basic.ack multiple=true  -> AckMultiple up to tag
basic.nack requeue=true  -> Nack/Requeue
basic.nack requeue=false -> Nack/DeadLetter placeholder
basic.reject             -> same as nack multiple=false
```

Errors:

```text
invalid delivery tag -> channel.close
unknown consumer/channel -> channel.close
ack after channel closed -> ignore or connection close according to phase
```

Tests:

```text
ack one removes delivery from unacked
ack multiple coalesces tags
nack requeue redelivers with redelivered=true
nack no requeue dead-letter/drop placeholder
reject requeue works
invalid delivery tag produces channel.close
```

---

## Slice 10 — In-memory AMQP transcript harness

Create an experiment:

```text
experiments/h7-amqp-adapter
```

or:

```text
experiments/h6-amqp-adapter
```

depending on existing numbering.

It should run transcript-level tests:

```text
1. handshake
2. channel.open
3. exchange.declare
4. queue.declare
5. queue.bind
6. basic.qos(prefetch=128)
7. basic.consume
8. basic.publish payload
9. receive basic.deliver
10. basic.ack
```

Workloads:

```text
handshake_only
publish_deliver_ack_1
publish_deliver_ack_many
publish_nack_requeue_ack
fragmented_body_publish
multi_channel_publish_consume
```

Metrics:

```text
frames/sec
messages/sec
bytes/sec
allocations/message if available
route cache hit rate
encoded frames/message
```

Do not optimize too early; the purpose is correctness and boundary validation.

---

## Slice 11 — Documentation

Add:

```text
docs/AMQP_COMPATIBILITY_V0.md
docs/AMQP_ADAPTER_ARCHITECTURE.md
```

`AMQP_COMPATIBILITY_V0.md` should include a support matrix:

```text
Connection.Start/Open        supported
Channel.Open                 supported
Exchange.Declare direct      supported
Exchange.Declare fanout      optional/minimal
Queue.Declare                supported
Queue.Bind                   supported
Basic.Publish                supported
Basic.Consume                supported
Basic.Deliver                supported
Basic.Ack                    supported
Basic.Nack                   supported
Basic.Reject                 supported
Basic.Qos                    supported
Confirm.Select               placeholder/optional
Transactions                 not supported
Basic.Get                    not supported
Headers exchange             not supported in PR9
```

`AMQP_ADAPTER_ARCHITECTURE.md` should explain:

```text
AMQP adapter is edge only
AMQP frame types do not enter aurum-core
route cache behavior
content assembly
error mapping
control command boundary
```

---

## Correctness invariants

PR9 must enforce these invariants:

```text
1. A content header is only valid after basic.publish.
2. Body frames are only valid after content header.
3. Completed content body size equals header body_size.
4. AMQP delivery tags remain scoped to channel/session.
5. AMQP acks are translated to internal ack batches, not core direct calls.
6. AMQP nacks preserve requeue semantics.
7. Redelivery flag is set after requeue.
8. `aurum-core` does not depend on AMQP types.
9. `aurum-protocol-amqp` does not depend on `aurum-core`.
10. Unsupported methods produce deterministic protocol errors.
11. Route cache invalidation handles route table version changes.
12. Frame decoding never panics on malformed input.
```

---

## Performance guidelines

PR9 is not a final performance PR, but avoid known bad choices.

### Avoid

```text
String allocation per frame where ShortStr/Bytes can be used
HashMap lookup per publish after route cache hit
Vec allocation per small property set if SmallVec works
copying payload multiple times
trait-object dispatch per frame in embedded mode
AMQP types leaking into hot command executor
```

### Acceptable in PR9

```text
some allocations in field table/property parsing
HashMap in channel/consumer registries
Vec for outbound frame accumulation in harness
synchronous route resolution in harness
```

### Static dispatch policy

Use concrete/generic types for embedded adapter:

```rust
AmqpSession<B: AmqpBrokerPort>
```

Dynamic dispatch acceptable only for:

```text
sidecar/external gateway mode
test harnesses
plugin registry
```

### Batch policy

Even though AMQP frames are message-oriented, the adapter should group where possible:

```text
multiple completed publishes -> PublishBatch
multiple acks -> AckCommandBatch
multiple outbound deliveries -> DeliveryEventBatch -> frames
```

---

## Error mapping policy

Define error scope explicitly:

```rust
pub enum AmqpErrorScope {
    Connection,
    Channel(AmqpChannelId),
}
```

Examples:

```text
malformed frame header       -> connection.close
unknown channel method state -> channel.close
invalid delivery tag         -> channel.close
unsupported immediate flag   -> channel.close or basic.return policy
unsupported class/method     -> connection.close or channel.close depending class
```

PR9 should not panic for protocol errors. It should produce close frames and mark state as closing/closed.

---

## Security and resource limits

PR9 must define limits even in the harness:

```text
max_frame_size
max_channels
max_pending_content_bytes
max_field_table_bytes
max_shortstr_len = 255
max_body_size for harness
heartbeat timeout placeholder
```

Connection authentication can be minimal:

```text
PLAIN accepted by test auth provider
username/password stored as cold connection metadata
real auth provider later
```

TLS is explicitly out of scope; transport layer handles TLS later.

---

## Suggested API surface

```rust
pub struct AmqpSession<B> {
    broker: B,
    connection: AmqpConnectionState,
    channels: ChannelTable,
    codec: AmqpCodec,
}

impl<B: AmqpBrokerPort> AmqpSession<B> {
    pub fn receive_bytes(&mut self, input: &[u8], out: &mut AmqpOutbound) -> Result<(), AmqpSessionError>;
    pub fn receive_frame(&mut self, frame: RawFrame, out: &mut AmqpOutbound) -> Result<(), AmqpSessionError>;
    pub fn drain_broker_outputs(&mut self, out: &mut AmqpOutbound) -> Result<(), AmqpSessionError>;
}
```

Outbound collection:

```rust
pub struct AmqpOutbound {
    pub frames: Vec<RawFrame>,
}
```

Later, this can write directly to a transport sink without collecting a `Vec`.

---

## Test plan

### Unit tests

```text
wire frame codec
primitive codec
field table codec
properties codec
method codec
state machine transitions
content assembly
route cache
error mapping
```

### Integration tests

```text
handshake transcript
publish/consume/ack transcript
publish fragmented body transcript
nack requeue transcript
invalid frame transcript
channel close transcript
multi-channel transcript
```

### Differential/model tests

Where useful, compare:

```text
AMQP ack/nack behavior
vs
ConsumerSession model behavior
```

### Fuzz targets later

PR9 can create TODO scaffolding for:

```text
fuzz_amqp_frame_decode
fuzz_field_table_decode
fuzz_method_decode
fuzz_content_assembly
```

Fuzzing can be a follow-up PR.

---

## Acceptance criteria

PR9 is complete when:

```text
1. `aurum-protocol-amqp` has a real transport-neutral AMQP frame codec.
2. The minimal method subset decodes and encodes as typed enums.
3. Connection/channel handshake works in a transcript harness.
4. exchange.declare, queue.declare, queue.bind work against the in-memory broker/control facade.
5. basic.publish assembles content frames and produces internal PublishBatch.
6. basic.consume creates a consumer and maps consumer_tag to internal ConsumerId.
7. broker deliveries encode to basic.deliver + content header + body frames.
8. basic.ack maps to AckCommandBatch.
9. basic.nack/basic.reject map to NackCommandBatch with requeue semantics.
10. Invalid protocol states produce connection.close/channel.close, not panics.
11. AMQP types do not enter `aurum-core`.
12. `cargo test --workspace` passes.
13. A new AMQP compatibility document exists.
14. A new AMQP transcript experiment exists.
```

---

## Implementation order summary

```text
Slice 0: audit PR4-PR8 internal types for AMQP metadata gaps
Slice 1: wire frame codec
Slice 2: AMQP primitive codec
Slice 3: method decoder/encoder subset
Slice 4: connection/channel state machine
Slice 5: basic.publish content assembly
Slice 6: declare/bind/qos/consume/cancel control mapping
Slice 7: publish mapping with route cache
Slice 8: delivery mapping
Slice 9: ack/nack/reject mapping
Slice 10: transcript harness experiment
Slice 11: AMQP docs/support matrix
```

---

## Main risk

The biggest PR9 risk is allowing AMQP compatibility to distort the core.

Do not solve AMQP by doing this:

```text
AMQP frame -> aurum-core direct mutation
```

Always preserve:

```text
AMQP frame
  -> adapter state
  -> internal command batch / control command
  -> broker executor
  -> output event batch
  -> AMQP outbound frames
```

If the internal command protocol is missing a concept needed by AMQP, extend the internal protocol in neutral broker terms. Do not import AMQP types into the core.

---

## After PR9

The next natural PRs are:

```text
PR10 — Transport runtime MVP
  TCP listener for native + AMQP using aurum-transport
  no thread-per-core yet, or minimal single-thread runtime

PR11 — Direct exchange/routing hardening
  better route cache invalidation
  mandatory publish behavior
  unroutable returns

PR12 — Publisher confirms
  native and AMQP confirm.select semantics
  durable confirm integration with PR8 storage

PR13 — H7/H8 performance pass
  measure AMQP overhead vs native route_id path
```
