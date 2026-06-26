# AMQP 0-9-1 Compatibility v0

PR9 introduces a transport-neutral AMQP adapter in `aurum-protocol-amqp`. This document lists supported methods and known limitations.

## Support matrix

| Feature | Status |
|---------|--------|
| Protocol header + connection handshake | Supported |
| `channel.open` / `channel.close` | Supported |
| `exchange.declare` (direct, fanout) | Supported |
| `queue.declare` / `queue.bind` | Supported |
| `basic.qos` | Supported |
| `basic.consume` / `basic.cancel` | Supported |
| `basic.publish` + content header + body (multi-frame) | Supported |
| `basic.deliver` + content (multi-frame) | Supported |
| `basic.ack` / `basic.nack` / `basic.reject` | Supported |
| `confirm.select` | Supported (no publisher confirms yet) |
| `basic.get` | Not supported |
| Transactions (`tx.*`) | Not supported |
| Headers exchange | Not supported |
| TLS / TCP listener | Out of scope (PR10) |

## Module layout (PR9)

```text
aurum-protocol-amqp/
├── wire/          frame codec, shortstr, longstr, field_table, properties, error
├── method/        connection, channel, exchange, queue, basic, confirm
├── session/       connection, channel, content, consumers, route_cache, error
├── translate/     inbound helpers, outbound encoder, control mapping, errors
├── harness/       transcript harness trait (impl in aurum-broker)
└── port.rs        AmqpBrokerPort
```

## Wire format

- AMQP 0-9-1 frame layout: type (1) + channel (2 BE) + size (4 BE) + payload + end `0xCE`
- Method frames use class-id + method-id + arguments (big-endian primitives, shortstr, field tables)
- Content uses `basic` class header (weight=0, body_size u64 BE, property flags) + body frame(s)

## Error handling

- Malformed frames → `connection.close` or `channel.close` with reply code 5xx
- Broker `InvalidDeliveryTag` → `channel.close` (504)
- `immediate=true` on publish → `channel.close` (not supported in v0)
- Passive declare → `channel.close` (not supported in v0)
- Unknown methods → deterministic close, no panic

## Durability mapping

| AMQP | Internal |
|------|----------|
| `delivery_mode=2` | `ConfirmMode::LocalDurable` on ingress publish |
| Other modes | `ConfirmMode::None` |

## Testing

```bash
cargo test --workspace
cargo test -p aurum-broker in_memory::amqp_harness_tests
cargo run -p h8-amqp-adapter -- handshake_only
cargo run -p h8-amqp-adapter -- publish_deliver_ack_many
cargo run -p h8-amqp-adapter -- publish_nack_requeue_ack
cargo run -p h8-amqp-adapter -- fragmented_body_publish
cargo run -p h8-amqp-adapter -- multi_channel_publish_consume
```
