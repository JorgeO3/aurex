# PR7 — Minimal Native Protocol Adapter

**Project:** AurumMQ  
**Phase:** After PR6 — Minimal Compiled Routing Layer  
**Scope:** `aurum-protocol-native` + integration with `aurum-internal-protocol`, `aurum-routing`, and the in-memory broker executor  
**Status:** Implementation plan  

---

## 0. Executive summary

PR7 introduces the first real external-facing protocol for AurumMQ: the **native binary protocol**.

This is **not** AMQP. It is our ergonomic, high-performance protocol designed around the internal model we have been building:

```text
route_id-first publishing
batch-first commands
range/mask-first ack/nack
credit-based delivery
correlation_id-based responses
zero/low-copy payload layout
```

PR7 should **not** implement the final TCP server, TLS, authentication, storage, cluster redirects, or thread-per-core runtime. Instead, it should implement the protocol **codec + adapter** in a transport-neutral way, and prove that it can drive the PR5/PR6 in-memory broker through the PR4 internal command protocol.

The target flow:

```text
Native frame bytes
        ↓
aurum-protocol-native codec
        ↓
NativeCommand / NativeRequest
        ↓
NativeAdapter
        ↓
aurum-internal-protocol::CommandBatch
        ↓
InMemoryBroker / InMemoryShardExecutor
        ↓
internal output batches
        ↓
NativeAdapter
        ↓
Native response/event frames
```

The key architectural goal:

> PR7 must prove that native clients can publish, consume, ack, nack and resolve routes without leaking protocol-specific types into `aurum-core`.

---

## 1. Why PR7 comes now

PR4 gave us the internal command boundary.  
PR5 proved a single-node in-memory executor can apply those commands to `aurum-core`.  
PR6 introduced compiled routing and route IDs.

Now we need the first protocol adapter that exercises the architecture from the outside.

Do **not** start with AMQP. AMQP compatibility is valuable, but it forces us to inherit historical semantics and frame shapes. The native protocol lets us validate the ideal path first:

```text
resolve route once
publish by route_id many times
ack/nack by batch
consume by credit
receive delivery batches
```

This validates the product's high-performance API before we implement a legacy compatibility adapter.

---

## 2. PR7 goals

PR7 must deliver:

1. A real `aurum-protocol-native` crate.
2. A binary frame header with versioning, op code, flags, correlation ID, stream/session ID, and body length.
3. Encode/decode roundtrip for protocol frames.
4. A transport-neutral codec over byte buffers.
5. Native operations for:
   - `HELLO`
   - `RESOLVE_ROUTE`
   - `PUBLISH_BATCH`
   - `CONSUME_START`
   - `CREDIT_UPDATE`
   - `ACK_BATCH`
   - `NACK_BATCH`
   - `CANCEL_CONSUMER`
   - `HEARTBEAT`
6. Native responses/events for:
   - `HELLO_OK`
   - `ROUTE_RESOLVED`
   - `PUBLISH_CONFIRM_BATCH`
   - `DELIVERY_BATCH`
   - `SETTLEMENT_RESULT_BATCH`
   - `CONSUMER_OK`
   - `ERROR`
   - `HEARTBEAT_ACK`
7. Translation from native requests to `aurum-internal-protocol::CommandBatch`.
8. Translation from internal broker outputs back to native frames.
9. In-memory integration harness: native bytes -> broker -> native bytes.
10. A small experiment/benchmark comparing:
    - route resolution + publish
    - route_id-only publish
    - ack ranges/masks
11. Strong tests for malformed frames, stale route versions, unknown op codes, truncated frames, oversized frames, and roundtrip encoding.

---

## 3. Non-goals

Do **not** implement in PR7:

```text
AMQP adapter
Kafka gateway
TCP listener
QUIC listener
TLS
authentication/authorization
persistent storage
io_uring
thread-per-core runtime
cluster redirects
compression negotiation beyond flags placeholders
payload checksums beyond optional frame-level validation placeholder
schema registry
flow-control across network sockets
```

PR7 is a protocol and adapter PR, not a networking PR.

---

## 4. Architectural principle

The native protocol must be:

```text
external at the edge
batch-first in shape
route_id-first for hot publishing
credit-based for delivery
range/mask-first for settlement
transport-neutral in PR7
```

The native protocol must **not** become the broker core.

Forbidden dependencies:

```text
aurum-core -> aurum-protocol-native
aurum-routing -> aurum-protocol-native
aurum-internal-protocol -> aurum-protocol-native
```

Allowed dependencies:

```text
aurum-protocol-native -> aurum-types
aurum-protocol-native -> aurum-internal-protocol
aurum-protocol-native -> bytes / smallvec / bitflags

aurum-broker -> aurum-protocol-native only in tests/harnesses or optional adapter integration
```

The core stays protocol-free.

---

## 5. Target architecture

```text
Native client / test harness
        │
        ▼
NativeCodec
  bytes -> NativeFrame -> NativeRequest
        │
        ▼
NativeAdapter
  NativeRequest -> CommandBatch
        │
        ▼
InMemoryBroker
  CommandBatch -> BrokerOutputBatch
        │
        ▼
NativeAdapter
  BrokerOutputBatch -> NativeResponse / NativeEvent
        │
        ▼
NativeCodec
  NativeFrame -> bytes
```

PR7 should keep this path in-process and deterministic.

The later PR for transport will wrap this same codec/adapter with TCP/QUIC/listeners.

---

## 6. Crate responsibilities

### 6.1 `aurum-types`

Owns protocol-neutral identifiers and small value types:

```rust
QueueId
ExchangeId
RouteId
RouteTableVersion
ShardId
ConsumerId
DeliveryTag
CorrelationId
StreamId
```

If `CorrelationId` or `StreamId` do not exist yet, PR7 should add them here or in `aurum-internal-protocol` if they are not globally useful.

### 6.2 `aurum-internal-protocol`

Owns the neutral command/event boundary:

```rust
CommandBatch
PublishBatch
ResolveRouteCommand
ConsumeStartCommand
CreditUpdateCommand
AckCommandBatch
NackCommandBatch
CancelConsumerCommand

BrokerOutputBatch
DeliveryEventBatch
PublishConfirmBatch
SettlementResultBatch
CommandErrorBatch
```

PR7 may need minor additions, but should not change this crate into a native-protocol-specific layer.

### 6.3 `aurum-protocol-native`

Owns:

```text
wire constants
frame header
op codes
flags
native request/response enums
binary codec
adapter from native to internal commands
adapter from internal broker outputs to native events
protocol errors
roundtrip tests
```

### 6.4 `aurum-broker`

Owns integration harness:

```text
NativeInMemoryHarness
Native request bytes -> broker -> native response bytes
```

This must be test/harness-level integration, not a transport server.

### 6.5 `aurum-transport`

No major implementation in PR7. Only add TODOs or interfaces if strictly needed.

---

## 7. Native protocol design

### 7.1 Design goals

The protocol should be optimized for:

```text
low parsing overhead
batch operations
route ID reuse
minimal repeated strings
correlation IDs for pipelining
transport independence
future leader redirects
future compression/checksum flags
```

The protocol should be ergonomic for client SDKs:

```text
client.resolve_route("orders", "created") -> RouteRef
client.publish_batch(route_ref, messages)
client.consume(queue, prefetch)
client.ack_range(first_tag, len)
client.nack_batch(...)
```

### 7.2 Endianness

Use **little-endian** for all numeric fields.

Reason:

```text
AurumMQ is performance-oriented and Rust/native clients will dominate the fast path.
Little-endian is natural on common server CPUs.
The protocol explicitly defines byte order, so cross-platform clients can encode/decode correctly.
```

Do not use host-endian implicitly.

### 7.3 Header size

Use a fixed-size 32-byte header for PR7.

Proposed layout:

```rust
#[repr(C)]
pub struct NativeFrameHeader {
    pub magic: u16,          // b"AQ" or fixed u16 constant
    pub version: u8,         // protocol major/minor encoded simply in PR7
    pub header_len: u8,      // 32 for PR7

    pub op: u16,             // NativeOp
    pub flags: u16,          // FrameFlags

    pub stream_id: u32,      // logical stream/channel/session lane
    pub correlation_id: u64, // request/response correlation

    pub body_len: u32,       // bytes after the fixed header
    pub reserved: u32,       // future: checksum/header crc/extension offset
}
```

32 bytes is intentional:

```text
small enough for cache friendliness
large enough for correlation/stream/body/versioning
fixed offsets for simple parsing
future-compatible via reserved/header_len
```

### 7.4 Frame flags

Use `bitflags`:

```rust
bitflags! {
    pub struct FrameFlags: u16 {
        const NONE        = 0;
        const RESPONSE    = 1 << 0;
        const EVENT       = 1 << 1;
        const ERROR       = 1 << 2;
        const COMPRESSED  = 1 << 3; // placeholder, not implemented in PR7
        const HAS_EXT     = 1 << 4; // placeholder for future extensions
        const MORE        = 1 << 5; // multi-frame batch continuation, future
    }
}
```

In PR7, reject `COMPRESSED` and `HAS_EXT` unless explicitly supported.

---

## 8. Native operations

Define a stable `NativeOp` enum.

```rust
#[repr(u16)]
pub enum NativeOp {
    Hello = 1,
    HelloOk = 2,

    ResolveRoute = 10,
    RouteResolved = 11,

    PublishBatch = 20,
    PublishConfirmBatch = 21,

    ConsumeStart = 30,
    ConsumerOk = 31,
    CreditUpdate = 32,
    DeliveryBatch = 33,
    CancelConsumer = 34,
    ConsumerCancelled = 35,

    AckBatch = 40,
    NackBatch = 41,
    SettlementResultBatch = 42,

    Heartbeat = 50,
    HeartbeatAck = 51,

    Error = 255,
}
```

Do not use strings for operation dispatch.

Implement:

```rust
impl TryFrom<u16> for NativeOp
```

Unknown op => `NativeProtocolError::UnknownOp`.

---

## 9. Body encodings

Use explicit binary bodies, not `serde/bincode/postcard` in the hot protocol path.

Reason:

```text
serde is excellent for config/control/admin.
For the native hot protocol, we want stable offsets, predictable layout, and manual control.
```

Serde can be used later for admin or debugging, not for publish hot path.

### 9.1 String encoding

For cold/warm requests like route resolution:

```text
u16 len
bytes[len]
```

Limit lengths in PR7:

```text
exchange name <= 255 or 1024 bytes
routing key <= 4096 bytes
```

Reject invalid/truncated strings.

### 9.2 Batch arrays

For hot bodies, prefer:

```text
body header
fixed descriptors[count]
payload bytes region
```

Not:

```text
record header + payload + record header + payload + ...
```

The descriptor table allows fast validation and future zero-copy.

---

## 10. Key request/response bodies

### 10.1 `HELLO`

Purpose:

```text
version negotiation
client capabilities
server capabilities later
debuggability
```

Body PR7:

```text
u16 client_major
u16 client_minor
u64 client_capabilities
u16 client_name_len
bytes client_name
```

Response `HELLO_OK`:

```text
u16 server_major
u16 server_minor
u64 server_capabilities
u64 connection_id
```

Capabilities bitflags:

```rust
bitflags! {
    pub struct NativeCapabilities: u64 {
        const ROUTE_ID        = 1 << 0;
        const PUBLISH_BATCH   = 1 << 1;
        const ACK_RANGE       = 1 << 2;
        const ACK_MASK        = 1 << 3;
        const NACK_RANGE      = 1 << 4;
        const NACK_MASK       = 1 << 5;
        const DELIVERY_BATCH  = 1 << 6;
        const COMPRESSION     = 1 << 7; // not active in PR7
    }
}
```

### 10.2 `RESOLVE_ROUTE`

Body:

```text
u64 route_table_version_hint
u32 exchange_id_hint       // 0 if unknown
u16 exchange_len
u16 routing_key_len
bytes exchange
bytes routing_key
```

Response `ROUTE_RESOLVED`:

```text
u64 route_table_version
u32 route_id
u32 queue_set_id_or_hint
u16 target_group_count
... optional shard target summary in PR7 if already available
```

For PR7, keep it minimal:

```text
route_table_version
route_id
```

The broker can publish by `route_id` later.

### 10.3 `PUBLISH_BATCH`

Body header:

```text
u64 route_table_version
u32 route_id
u32 batch_flags
u32 count
u32 descriptor_table_len
```

Descriptor per message:

```text
u32 payload_offset
u32 payload_len
u16 message_flags
u16 reserved
```

Payload region:

```text
bytes payloads concatenated
```

Constraints:

```text
count > 0
count <= MAX_PUBLISH_BATCH_MESSAGES
payload_len <= MAX_PAYLOAD_SIZE
all descriptors must fit inside body
payload regions must not overlap out of bounds
```

For PR7, payload can be copied into test/in-memory handles. Later storage/runtime can use references.

`message_flags`:

```rust
bitflags! {
    pub struct NativeMessageFlags: u16 {
        const PERSISTENT   = 1 << 0;
        const MANDATORY    = 1 << 1;
        const COMPRESSED   = 1 << 2; // future
    }
}
```

Response `PUBLISH_CONFIRM_BATCH`:

```text
u64 correlation_id
u32 accepted_count
u32 failed_count
optional per-message error table later
```

For PR7, batch-level success/failure is enough unless current internal protocol already supports per-message confirms.

### 10.4 `CONSUME_START`

Body:

```text
u32 queue_id
u32 consumer_id_hint      // 0 means assign
u32 prefetch
u16 consumer_flags
```

Response `CONSUMER_OK`:

```text
u32 queue_id
u64 consumer_id
u32 effective_prefetch
```

`consumer_flags`:

```rust
bitflags! {
    pub struct NativeConsumerFlags: u16 {
        const MANUAL_ACK = 1 << 0;
        const EXCLUSIVE  = 1 << 1; // future
    }
}
```

### 10.5 `CREDIT_UPDATE`

Body:

```text
u64 consumer_id
u32 credit_delta
u16 flags
```

Flags:

```rust
bitflags! {
    pub struct CreditFlags: u16 {
        const ABSOLUTE = 1 << 0; // credit_delta means set instead of add
    }
}
```

### 10.6 `DELIVERY_BATCH`

Response/event body:

```text
u64 consumer_id
u32 count
u32 descriptor_table_len
```

Descriptor:

```text
u64 delivery_tag
u32 payload_offset
u32 payload_len
u16 delivery_flags
u16 reserved
```

Delivery flags:

```rust
bitflags! {
    pub struct NativeDeliveryFlags: u16 {
        const REDELIVERED = 1 << 0;
        const RANGE_START = 1 << 1; // optional future metadata
    }
}
```

PR7 can encode deliveries as individual descriptors even if internally they came from ranges/masks.

Reason:

```text
The protocol must be ergonomic and explicit for clients.
The core still remains range/mask-first internally.
Delivery serialization is allowed to expand tags/payloads at the adapter edge.
```

Later optimization can add range delivery descriptors.

### 10.7 `ACK_BATCH`

Body:

```text
u64 consumer_id
u32 op_count
u16 flags
```

Operations should support at least:

```text
AckOne(tag)
AckRange(first_tag, len)
AckMultiple(tag)
```

Encoding:

```text
u8 ack_op_kind
u8 reserved
u16 reserved
u64 tag
u32 len_or_zero
u32 reserved
```

`AckOpKind`:

```rust
#[repr(u8)]
pub enum NativeAckOpKind {
    One = 1,
    Range = 2,
    MultipleUpTo = 3,
}
```

Adapter maps this to internal `AckCommandBatch`.

### 10.8 `NACK_BATCH`

Similar to ACK, but includes disposition:

```rust
#[repr(u8)]
pub enum NativeNackDisposition {
    Requeue = 1,
    Drop = 2,
    DeadLetter = 3,
}
```

Operations:

```text
NackOne(tag, disposition)
NackRange(first_tag, len, disposition)
NackMultipleUpTo(tag, disposition)
```

Adapter maps this to internal `NackCommandBatch`.

### 10.9 `CANCEL_CONSUMER`

Body:

```text
u64 consumer_id
u8 cancel_disposition
```

Disposition:

```text
RequeueUnacked
DropUnacked
DeadLetterUnacked
```

### 10.10 `ERROR`

Body:

```text
u16 error_code
u16 message_len
u64 correlation_id_of_failed_request
bytes message
```

Stable error codes:

```rust
#[repr(u16)]
pub enum NativeErrorCode {
    MalformedFrame = 1,
    UnsupportedVersion = 2,
    UnknownOp = 3,
    InvalidFlags = 4,
    BodyTooLarge = 5,
    RouteNotFound = 100,
    RouteStale = 101,
    QueueNotFound = 102,
    ConsumerNotFound = 103,
    InvalidDeliveryTag = 104,
    Internal = 500,
}
```

---

## 11. Important representation decisions

### 11.1 Use enums for stable protocol variants

Use `#[repr(u16)]` or `#[repr(u8)]` enums for wire-level discriminants:

```rust
NativeOp
NativeAckOpKind
NativeNackOpKind
NativeNackDisposition
NativeErrorCode
```

But do not expose raw enums directly across untrusted bytes. Decode via `TryFrom` and error on unknown values.

### 11.2 Use bitflags for composable state

Use `bitflags` for:

```text
FrameFlags
NativeCapabilities
NativeMessageFlags
NativeDeliveryFlags
NativeConsumerFlags
CreditFlags
```

Reject unsupported flag combinations in PR7.

### 11.3 Static dispatch vs dynamic dispatch

Hot path:

```text
NativeCodec concrete type
NativeAdapter concrete type
no dyn Trait per frame
no trait object per op
match on NativeOp is acceptable
```

Cold/future path:

```text
Plugin/gateway registry may use dyn Trait
Transport registry may use dyn Trait
Admin/debug sinks may use dyn Trait
```

PR7 codec should be concrete and easily inlineable.

### 11.4 Generic buffer strategy

Do not over-genericize PR7.

Recommended:

```rust
pub struct NativeCodec {
    max_frame_len: usize,
}

impl NativeCodec {
    pub fn decode(&mut self, src: &mut BytesMut) -> Result<Option<NativeFrame>, NativeDecodeError>;
    pub fn encode(&mut self, frame: &NativeFrame, dst: &mut BytesMut) -> Result<(), NativeEncodeError>;
}
```

Use `bytes::BytesMut` in the adapter/codec. This is edge-plane code. It does not contaminate `aurum-core`.

Later we can add a lower-level zero-copy codec for runtime/transport.

### 11.5 Payload ownership

In PR7:

```text
wire payload -> Bytes/BytesMut slice -> adapter copies or references into internal PayloadHandle according to existing PR4 types
```

Do not design the final zero-copy storage path in PR7.

But the frame body layout should be compatible with later zero-copy:

```text
descriptor table + contiguous payload region
```

---

## 12. Proposed module layout

```text
crates/edge/aurum-protocol-native/src/
  lib.rs
  wire/
    mod.rs
    constants.rs
    header.rs
    op.rs
    flags.rs
    error_code.rs
  codec/
    mod.rs
    decode.rs
    encode.rs
    cursor.rs
  message/
    mod.rs
    hello.rs
    route.rs
    publish.rs
    consume.rs
    delivery.rs
    settlement.rs
    error.rs
  adapter/
    mod.rs
    inbound.rs
    outbound.rs
    session.rs
  test_support/
    mod.rs
```

Keep wire-level and adapter-level types separate.

Example distinction:

```rust
NativePublishBatchFrame
  wire/body representation

PublishBatch
  internal command representation from aurum-internal-protocol
```

Do not name both `PublishBatch` in the same namespace.

---

## 13. Adapter design

### 13.1 Inbound adapter

```rust
pub struct NativeInboundAdapter {
    session: NativeSessionState,
}

impl NativeInboundAdapter {
    pub fn translate_frame(&mut self, frame: NativeFrame) -> Result<CommandBatch, NativeAdapterError>;
}
```

Responsibilities:

```text
validate protocol state
map route_id publishes to internal publish commands
map resolve route to internal route command
map ack/nack ops to internal settlement commands
map consume/credit/cancel to internal consumer commands
```

Do not execute broker logic here.

### 13.2 Outbound adapter

```rust
pub struct NativeOutboundAdapter;

impl NativeOutboundAdapter {
    pub fn translate_outputs(&mut self, outputs: BrokerOutputBatch, out: &mut SmallVec<[NativeFrame; 8]>);
}
```

Responsibilities:

```text
internal delivery events -> DELIVERY_BATCH frames
internal confirms -> PUBLISH_CONFIRM_BATCH frames
internal settlement results -> SETTLEMENT_RESULT_BATCH frames
internal errors -> ERROR frames
```

### 13.3 Session state

Native protocol needs minimal session state:

```rust
pub struct NativeSessionState {
    pub protocol_version: ProtocolVersion,
    pub connection_id: u64,
    pub last_seen_route_table_version: RouteTableVersion,
    pub capabilities: NativeCapabilities,
}
```

Do not put queue state here. Queue/consumer state belongs to broker/core.

---

## 14. Integration with PR6 routing

PR7 must support two publishing modes:

### 14.1 Resolve route then publish by route ID

```text
RESOLVE_ROUTE(exchange="orders", routing_key="created")
        ↓
ROUTE_RESOLVED(route_table_version=12, route_id=44)
        ↓
PUBLISH_BATCH(route_table_version=12, route_id=44, count=N)
```

This is the desired hot path.

### 14.2 Direct queue publish only for tests/internal mode

PR5 may still support direct QueueId publish. PR7 should not expose that as the primary native API unless explicitly marked as an internal/test op.

Recommended:

```text
Native public protocol: route_id publish
Internal test helpers: queue_id publish
```

---

## 15. Integration with PR5 broker executor

PR7 should include a test harness, not a real network server:

```rust
pub struct NativeInMemoryHarness {
    codec: NativeCodec,
    inbound: NativeInboundAdapter,
    outbound: NativeOutboundAdapter,
    broker: InMemoryBroker,
}
```

API:

```rust
impl NativeInMemoryHarness {
    pub fn send_bytes(&mut self, bytes: &[u8]) -> Vec<u8>;
    pub fn send_frame(&mut self, frame: NativeFrame) -> Vec<NativeFrame>;
}
```

This lets us write end-to-end tests:

```text
encode RESOLVE_ROUTE
  -> decode
  -> route compiler/broker
  -> encode ROUTE_RESOLVED
```

and:

```text
HELLO
RESOLVE_ROUTE
PUBLISH_BATCH
CONSUME_START
CREDIT_UPDATE
DELIVERY_BATCH
ACK_BATCH
PUBLISH_CONFIRM_BATCH
```

---

## 16. Slices / implementation plan

### Slice 0 — Audit PR4/PR5/PR6 types

Before adding new types, inspect existing:

```text
CommandBatch
PublishBatch
ResolveRouteCommand
RouteId
RouteTableVersion
ConsumerId
DeliveryTag
AckRequest/NackRequest
BrokerOutputBatch
```

Do not duplicate IDs.

Acceptance:

```text
A short comment or doc section in PR7 noting reused types and any added types.
```

---

### Slice 1 — Wire constants, header, op codes, flags

Implement:

```text
wire/constants.rs
wire/header.rs
wire/op.rs
wire/flags.rs
wire/error_code.rs
```

Tests:

```text
header encodes to exactly 32 bytes
header decode rejects wrong magic
header decode rejects unsupported version
NativeOp::try_from rejects unknown op
FrameFlags rejects unsupported combinations where applicable
```

Acceptance:

```bash
cargo test -p aurum-protocol-native wire
```

---

### Slice 2 — Codec skeleton

Implement:

```rust
NativeCodec::decode
NativeCodec::encode
NativeFrame { header, body }
NativeDecodeError
NativeEncodeError
```

Requirements:

```text
support partial frames: decode returns Ok(None)
reject frames larger than max_frame_len
reject header_len < 32
reject body_len exceeding available bytes
keep unread bytes in BytesMut
```

Tests:

```text
encode/decode empty HEARTBEAT frame
partial header
partial body
two frames in one buffer
oversized body
unknown op
```

---

### Slice 3 — Message body encoders/decoders

Implement body types:

```text
Hello
HelloOk
ResolveRoute
RouteResolved
PublishBatchFrame
ConsumeStart
CreditUpdate
AckBatchFrame
NackBatchFrame
CancelConsumer
DeliveryBatchFrame
PublishConfirmBatchFrame
SettlementResultBatchFrame
ErrorFrame
```

Do not over-engineer. PR7 may use simple manual cursor helpers:

```rust
read_u16_le
read_u32_le
read_u64_le
read_bytes
write_u16_le
...
```

Tests:

```text
roundtrip each body type
invalid string length
invalid publish descriptor table
payload offset out of bounds
invalid ack op kind
invalid nack disposition
```

---

### Slice 4 — Inbound adapter to internal commands

Implement:

```text
adapter/inbound.rs
```

Mappings:

```text
HELLO -> no broker command; returns session response directly or adapter output
RESOLVE_ROUTE -> CommandBatch::ResolveRoute
PUBLISH_BATCH -> CommandBatch::Publish
CONSUME_START -> CommandBatch::Consumer/CreateConsumer
CREDIT_UPDATE -> CommandBatch::Consumer/CreditUpdate
ACK_BATCH -> CommandBatch::Ack
NACK_BATCH -> CommandBatch::Nack
CANCEL_CONSUMER -> CommandBatch::Consumer/Cancel
HEARTBEAT -> adapter-level HeartbeatAck
```

Design decision:

```text
Some ops are adapter-local: HELLO, HEARTBEAT.
Some ops become broker commands: publish, route, consume, ack, nack.
```

Return type:

```rust
pub enum NativeInboundResult {
    BrokerCommand(CommandBatch),
    ImmediateResponse(NativeFrame),
}
```

This avoids sending HELLO into the broker.

Tests:

```text
publish frame -> PublishBatch internal command
ack range frame -> AckCommandBatch
nack requeue frame -> NackCommandBatch
resolve route frame -> ResolveRouteCommand
hello -> immediate HelloOk
```

---

### Slice 5 — Outbound adapter from broker outputs

Implement:

```text
adapter/outbound.rs
```

Mappings:

```text
RouteResolvedEvent -> ROUTE_RESOLVED
PublishConfirmBatch -> PUBLISH_CONFIRM_BATCH
DeliveryEventBatch -> DELIVERY_BATCH
SettlementResultBatch -> SETTLEMENT_RESULT_BATCH
CommandErrorBatch -> ERROR
ConsumerCreated/ConsumerOk -> CONSUMER_OK
```

If PR5/PR6 output types are not yet expressive enough, PR7 may add minimal neutral output variants to `aurum-internal-protocol`.

Tests:

```text
publish confirm output -> native confirm frame
route resolved output -> native route frame
delivery output -> native delivery frame
invalid delivery output -> error frame
```

---

### Slice 6 — Native in-memory harness

Implement in `aurum-broker` or test-support:

```text
NativeInMemoryHarness
```

Flow:

```text
bytes -> codec -> inbound adapter -> broker -> outbound adapter -> codec -> bytes
```

Tests:

```text
hello roundtrip
resolve route end-to-end
publish by route_id end-to-end
consume + credit + delivery end-to-end
ack delivery end-to-end
nack requeue redelivery end-to-end
cancel consumer end-to-end
```

---

### Slice 7 — Experiment `h5-native-protocol`

Add:

```text
experiments/h5-native-protocol
```

Workloads:

```text
resolve_route_only
publish_route_id_batch
publish_resolve_each_time
publish_consume_ack
publish_consume_nack_requeue_ack
```

Metrics:

```text
ns/message
frames/sec
bytes encoded/sec
allocations if measurable
route_id publish vs resolve-each-publish ratio
```

Acceptance:

```bash
cargo run --release -p h5-native-protocol -- --messages=1048576 --batch=128 --workload=publish_route_id_batch
```

---

### Slice 8 — Documentation

Add:

```text
docs/NATIVE_PROTOCOL_V0.md
```

Include:

```text
frame header
endianness
op codes
flags
body layouts
error codes
example flows
non-goals
compatibility/versioning notes
```

Update README docs section.

---

## 17. Testing strategy

### Unit tests

```text
header encode/decode
frame encode/decode
body encode/decode
error mapping
adapter mapping
```

### Negative tests

```text
wrong magic
unsupported version
unknown op
truncated frame
body too large
invalid string len
invalid descriptor table
invalid route_id version
invalid ack op
invalid nack disposition
unsupported flags
```

### Integration tests

```text
native hello
native resolve route
native publish confirm
native consume delivery
native ack
native nack redelivery
native cancel consumer
```

### Differential tests

Where practical:

```text
native ACK_BATCH -> internal AckCommandBatch -> ConsumerSession behavior
native NACK_BATCH -> internal NackCommandBatch -> ConsumerSession behavior
```

Do not duplicate all PR3 model tests here. PR7 tests the adapter boundary.

---

## 18. Performance strategy

PR7 is not final performance tuning, but should avoid obvious mistakes:

```text
No serde for hot frames.
No JSON.
No per-message dynamic dispatch.
No string route lookup in publish hot path.
No Vec allocations for every small ack batch if SmallVec/ArrayVec is available.
No protocol type leakage into core.
```

Allowed in PR7:

```text
BytesMut allocation in codec/tests
Vec in cold route resolution body
copying payloads in the in-memory harness
match on NativeOp
```

Future optimization hooks:

```text
descriptor table + payload region layout
route_id hot path
ack ranges/masks
correlation IDs for pipelining
fixed header offsets
```

---

## 19. Ergonomics goals

A future Rust client should feel like:

```rust
let mut client = AurumClient::connect(...).await?;
let route = client.resolve_route("orders", "created").await?;
client.publish_batch(route, messages).await?;

let consumer = client.consume("orders.created", Prefetch::new(128)).await?;
while let Some(batch) = consumer.next_batch().await? {
    process(&batch);
    consumer.ack_batch(batch.ack_range()).await?;
}
```

PR7 does not implement this SDK, but the protocol must make this API natural.

---

## 20. Error semantics

PR7 should treat malformed input as protocol errors:

```text
Malformed frame -> ERROR + close connection later
Unknown op -> ERROR
Unsupported version -> ERROR
Oversized frame -> ERROR
Invalid flags -> ERROR
```

Transport close is not implemented in PR7, but errors should carry enough info for a future transport layer to decide:

```rust
pub enum ErrorSeverity {
    Recoverable,
    CloseConnection,
    InternalBug,
}
```

For broker errors:

```text
route not found -> ERROR(RouteNotFound)
route stale -> ERROR(RouteStale)
queue not found -> ERROR(QueueNotFound)
consumer not found -> ERROR(ConsumerNotFound)
invalid delivery tag -> ERROR(InvalidDeliveryTag)
```

---

## 21. Versioning strategy

PR7 defines protocol version `0` or `1` explicitly.

Recommended:

```rust
pub const NATIVE_PROTOCOL_MAJOR: u16 = 0;
pub const NATIVE_PROTOCOL_MINOR: u16 = 1;
```

Even if header has only `u8 version`, body `HELLO` should negotiate major/minor.

Compatibility rule:

```text
Same major required.
Minor can be lower/equal if features are negotiated by capability bits.
```

---

## 22. Security posture

PR7 does not implement auth/TLS, but it must still be safe against malformed bytes.

Hard rules:

```text
All lengths checked.
No panic on malformed input.
No unchecked slicing from untrusted body.
No integer overflow in offset + len.
Max frame len enforced.
Unsupported flags rejected.
Unknown enum values rejected.
```

This is a protocol parser. Treat all input as hostile.

---

## 23. Acceptance criteria

PR7 is complete when:

```text
1. aurum-protocol-native has a real frame header, op codes, flags, errors, and codec.
2. Native frames encode/decode roundtrip.
3. Native requests translate to aurum-internal-protocol commands.
4. Broker outputs translate to native response/event frames.
5. Native in-memory harness can execute hello -> resolve_route -> publish -> consume -> ack.
6. Native in-memory harness can execute nack requeue -> redelivery -> ack.
7. Route ID publish is the primary publish path.
8. Malformed frame tests pass.
9. aurum-core has zero dependency on aurum-protocol-native.
10. aurum-routing has zero dependency on aurum-protocol-native.
11. cargo test --workspace passes.
12. h5-native-protocol experiment runs in release mode.
13. docs/NATIVE_PROTOCOL_V0.md exists.
```

---

## 24. Risks and mitigations

### Risk 1 — Protocol gets too complex too early

Mitigation:

```text
No compression.
No auth.
No TCP.
No cluster redirects.
No multi-frame continuation.
Only minimal operations.
```

### Risk 2 — Native protocol leaks into core

Mitigation:

```text
Keep all native types in aurum-protocol-native.
Translate at adapter boundary.
Core only sees queue/consumer/range/mask types.
```

### Risk 3 — Payload ownership design blocks zero-copy later

Mitigation:

```text
Use descriptor table + payload region from day one.
Do not bake Vec<Vec<u8>> into wire format.
Allow PR7 adapter to copy, but keep wire layout zero-copy-friendly.
```

### Risk 4 — Route resolution duplicates routing logic

Mitigation:

```text
Native adapter does not resolve routing itself.
It emits ResolveRouteCommand or calls broker route service.
Route compiler/table remains in aurum-routing.
```

### Risk 5 — Ack/Nack protocol does not match PR3 semantics

Mitigation:

```text
ACK_BATCH and NACK_BATCH directly model one/range/multiple.
Adapter maps to ConsumerSession commands.
Add integration tests for each semantic case.
```

---

## 25. What comes after PR7

Recommended next PRs:

```text
PR8 — Append-only storage initial version
  payload log
  queue index log
  ack ledger
  crash/recovery model

PR9 — Minimal TCP transport for native protocol
  listener
  connection loop
  codec integration
  in-memory broker over TCP

PR10 — AMQP adapter initial compatibility
  basic.publish
  basic.consume
  basic.ack
  basic.nack
  qos/prefetch
```

Alternative if we want networking before storage:

```text
PR8 — Native TCP transport
PR9 — Storage
```

But the storage PR should not be delayed too long, because publish confirms and recovery semantics depend on it.

---

## 26. Implementation checklist

```text
[ ] Audit current IDs and command/event types.
[ ] Add wire constants/header/op/flags/error_code modules.
[ ] Implement NativeFrame and NativeCodec encode/decode.
[ ] Implement body encoders/decoders for P0 ops.
[ ] Implement NativeInboundAdapter.
[ ] Implement NativeOutboundAdapter.
[ ] Add malformed frame tests.
[ ] Add body roundtrip tests.
[ ] Add adapter mapping tests.
[ ] Add NativeInMemoryHarness.
[ ] Add end-to-end native harness tests.
[ ] Add h5-native-protocol experiment.
[ ] Add docs/NATIVE_PROTOCOL_V0.md.
[ ] Update README.
[ ] cargo test --workspace.
```
