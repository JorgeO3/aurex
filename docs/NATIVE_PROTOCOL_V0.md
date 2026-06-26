# Native Protocol v0

Transport-neutral binary protocol for AurumMQ clients. PR7 implements the codec and adapter; TCP/TLS comes in a later PR.

## Endianness

All multi-byte integers are **little-endian**.

## Frame header (32 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 2 | `magic` — `0x5141` (`"AQ"`) |
| 2 | 1 | `version` — wire revision (`1` in PR7) |
| 3 | 1 | `header_len` — `32` |
| 4 | 2 | `op` — `NativeOp` |
| 6 | 2 | `flags` — `FrameFlags` |
| 8 | 4 | `stream_id` |
| 12 | 8 | `correlation_id` |
| 20 | 4 | `body_len` |
| 24 | 4 | `reserved` |
| 28 | 4 | `reserved2` |

## Operations (`NativeOp`)

| Code | Name | Direction |
|------|------|-----------|
| 1 | HELLO | client → server |
| 2 | HELLO_OK | server → client |
| 10 | RESOLVE_ROUTE | client → server |
| 11 | ROUTE_RESOLVED | server → client |
| 20 | PUBLISH_BATCH | client → server |
| 21 | PUBLISH_CONFIRM_BATCH | server → client |
| 30 | CONSUME_START | client → server |
| 31 | CONSUMER_OK | server → client |
| 32 | CREDIT_UPDATE | client → server |
| 33 | DELIVERY_BATCH | server → client (event) |
| 34 | CANCEL_CONSUMER | client → server |
| 40 | ACK_BATCH | client → server |
| 41 | NACK_BATCH | client → server |
| 42 | SETTLEMENT_RESULT_BATCH | server → client |
| 50 | HEARTBEAT | either |
| 51 | HEARTBEAT_ACK | either |
| 255 | ERROR | server → client |

## Frame flags

- `RESPONSE` (0x01) — reply to a request
- `EVENT` (0x02) — unsolicited server event
- `ERROR` (0x04) — error disposition
- `COMPRESSED`, `HAS_EXT`, `MORE` — reserved; rejected in PR7

## Hot path flow

```text
RESOLVE_ROUTE(exchange, routing_key)
  → ROUTE_RESOLVED(route_table_version, route_id_packed)
PUBLISH_BATCH(route_table_version, route_id_packed, descriptors + payloads)
  → PUBLISH_CONFIRM_BATCH
CONSUME_START(queue_id, prefetch)
  → CONSUMER_OK
CREDIT_UPDATE(consumer_id, delta)
  → DELIVERY_BATCH
ACK_BATCH / NACK_BATCH
  → SETTLEMENT_RESULT_BATCH
```

`route_id_packed` is a `u64`: low 32 bits = route index, high 32 bits = generation.

## Body layouts (summary)

### HELLO

```text
u16 client_major
u16 client_minor
u64 client_capabilities
u16 client_name_len
bytes client_name
```

### RESOLVE_ROUTE

```text
u64 route_table_version_hint
u32 exchange_id_hint   // 0 if resolving by name
u16 exchange_len
u16 routing_key_len
bytes exchange
bytes routing_key
```

### PUBLISH_BATCH

```text
u64 route_table_version
u64 route_id_packed
u32 batch_flags
u32 count
u32 descriptor_table_len   // count * 12
per message:
  u32 payload_offset
  u32 payload_len
  u16 message_flags
  u16 reserved
bytes payloads (concatenated)
```

### ACK_BATCH / NACK_BATCH

```text
u64 consumer_id
u32 op_count
u16 flags
per op (16 bytes):
  u8 kind   // 1=One, 2=Range, 3=MultipleUpTo
  u8 disposition (NACK only)
  u16 reserved
  u64 tag
  u32 len_or_zero
  u32 reserved
```

## Error codes

| Code | Meaning |
|------|---------|
| 1 | Malformed frame |
| 2 | Unsupported version |
| 3 | Unknown op |
| 4 | Invalid flags |
| 5 | Body too large |
| 100 | Route not found |
| 101 | Stale route |
| 102 | Queue not found |
| 103 | Consumer not found |
| 104 | Invalid delivery tag |
| 500 | Internal |

## Versioning

HELLO negotiates `major.minor` (`0.1` in PR7). Same major required; capabilities bits gate optional features.

## Non-goals (PR7)

TCP, TLS, auth, persistence, compression, cluster redirects.

## Implementation

- Crate: `aurum-protocol-native` — wire, codec, message bodies, inbound/outbound adapters
- Harness: `aurum-broker::NativeInMemoryHarness` — bytes → broker → bytes (in-process)
- Experiment: `h6-native-protocol`
