# AMQP Adapter Architecture

`aurum-protocol-amqp` is an **edge adapter**. It translates AMQP 0-9-1 frames to AurumMQ internal command batches and encodes broker outputs back to AMQP. It does not own queue state, routing tables, or storage.

## Data flow

```text
AMQP bytes
  → AmqpCodec (wire/)
  → AmqpSession (session/)
  → translate/ (control + outbound + errors)
  → AmqpBrokerPort (port.rs)
  → InMemoryBroker / shard executor (aurum-broker)
  → DeliveryEventBatch / errors / confirms
  → translate/outbound → AMQP frames
```

## Module layout

| Module | Role |
|--------|------|
| `wire/` | Frame codec, `error`, `shortstr`, `longstr`, field tables, basic properties |
| `method/` | Per-class AMQP methods (`connection`, `channel`, `exchange`, `queue`, `basic`, `confirm`) |
| `session/` | Connection/channel state, content assembly, consumers, route cache |
| `translate/` | Control command builders, delivery outbound encoder, `AmqpErrorScope` |
| `harness/` | `AmqpTranscriptHarness` trait; concrete harness in `aurum-broker` |
| `port.rs` | `AmqpBrokerPort` trait for broker integration |

## Dependency rules

```text
aurum-protocol-amqp
  → aurum-types
  → aurum-internal-protocol
  ✗ aurum-core
  ✗ aurum-storage
  ✗ aurum-broker (adapter must not depend on broker; broker depends on adapter)
```

## Route cache

Per-channel cache keyed by `(exchange_hash, routing_key_hash)` + `route_table_version`:

```text
basic.publish
  → cache hit → IngressPublishBatch::Route(route_id)
  → cache miss → resolve_route via broker → cache insert → publish
```

Cache entries are invalidated when `route_table_version` changes.

## Content assembly

Publish is three frames:

```text
basic.publish method
content header (body_size + properties)
one or more body frames
```

`PendingPublishContent` (session/content.rs) accumulates body bytes until `body.len() == body_size`, then emits `IngressPublishBatch`.

## Delivery metadata (Option A)

`DeliveryEventBatch.metadata` carries exchange/routing_key cold fields. The broker harness enriches deliveries from `store_delivery_context` keyed by payload handle.

## Consumer mapping

- AMQP `consumer_tag` ↔ internal `ConsumerId` (`session/consumers.rs`)
- AMQP `delivery_tag` ↔ internal `DeliveryTag` via `delivery_consumers` map per channel
- Publish properties stored by payload handle for outbound `basic.deliver`

## Harness

`AmqpInMemoryHarness` in `aurum-broker` implements `AmqpBrokerPort`:

- Dynamic routing config (declare exchange/queue/bind → recompile `RouteTable`)
- Payload byte store + delivery metadata for publish/deliver roundtrip
- Shared transcripts in `amqp_transcript` module
- Integration tests in `amqp_harness_tests`
- Benchmark workloads in `experiments/h8-amqp-adapter`

## Error scope

| Condition | Response |
|-----------|----------|
| Malformed frame / bad protocol state | `connection.close` (503) |
| Invalid delivery tag / consumer errors | `channel.close` (504) |
| `immediate=true` on publish | `channel.close` |
| Passive declare | `channel.close` (501) |

See `translate/errors.rs` for `AmqpErrorScope` and `command_error_scope`.

## Session API

```rust
AmqpSession::receive_bytes(input, &mut out)?;
AmqpSession::receive_frame(frame, &mut out)?;
AmqpSession::drain_broker_outputs(channel, &mut out)?; // no-op when idle
```
