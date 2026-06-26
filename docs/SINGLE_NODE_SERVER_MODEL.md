# Single-Node Server Model (PR10)

PR10 introduces the first runnable AurumMQ broker process. This document describes how the pieces compose and what is intentionally temporary.

## What PR10 is

PR10 turns existing library components into a **single-node networked broker**:

```text
TCP client (native or AMQP)
  → aurum-transport (blocking std::net)
  → protocol session adapter
  → CommandBatch
  → SingleNodeBroker / InMemoryShardExecutor
  → BrokerOutputBatch
  → protocol frames
  → client
```

## What PR10 is not

This is **not** the final production runtime:

- No thread-per-core shard loops yet (PR11)
- No advanced storage recovery on restart yet (PR12)
- No TLS, clustering, or RabbitMQ-full compatibility

Blocking I/O and `Arc<Mutex<SingleNodeBroker>>` are acceptable in PR10 for correctness and composition validation.

## Protocol boundary

Protocol crates remain outside `aurum-core`:

- Sessions only see `CommandBatch` / `BrokerOutputBatch`
- Core queue semantics are unchanged

## Connection output routing

AMQP deliveries are **pushed** to consumer connections. `BrokerService` maintains:

- `ConnectionRegistry`: `ConsumerId → ConnectionId`
- Per-connection outboxes for cross-connection deliveries

When connection A publishes and connection B consumes, the delivery is routed to B's outbox and sent on B's next read cycle.

## Storage modes

| Mode | Backend |
|------|---------|
| `dev-in-memory` | `NoopStorage` |
| `single-node-persistent` | `AppendOnlyStorage` (PR8) |

Storage is selected at broker construction via enum dispatch (not `dyn` in the hot path).

## Configuration

See `examples/single-node.toml` and `docs/CONFIGURATION_V0.md`.

Quick start:

```bash
cargo run -p aurum-cli -- broker dev --native 127.0.0.1:7777 --amqp 127.0.0.1:5672
```

## Replaced in future PRs

| PR10 component | Future replacement |
|----------------|-------------------|
| `Arc<Mutex<SingleNodeBroker>>` | Shard mailboxes / broker thread |
| Blocking `std::net` transport | mio / io_uring / thread-per-core runtime |
| `NoopStorage` default | Production durability policies |
| Static bootstrap routing | Runtime management + clustering |
