# AurumMQ

AurumMQ is a research-first Rust workspace for a RabbitMQ-like broker with a high-performance data plane.

The current goal is not to implement the whole broker. The goal is to validate the foundational hypotheses with small, isolated crates and experiments before the architecture becomes expensive to change.

## Workspace/plugin architecture

The project is intentionally split into small crates so compile artifacts and dependencies can be cached independently:

```text
crates/
  aurum-types           shared IDs, commands, batches, public data types
  aurum-kernels         scalar/SWAR/bitset kernels; SIMD later
  aurum-intrusive       index-intrusive lists and arenas
  aurum-core            queue engine; no protocol/storage dependencies
  aurum-routing         compiled route tables and route IDs
  aurum-plugin-api      cold-path adapter/plugin traits
  aurum-protocol-native native batch protocol adapter
  aurum-protocol-amqp   AMQP compatibility adapter boundary
  aurum-concurrency     SPSC/MPSC/RCU research primitives
  aurum-storage         append-only log abstractions
  aurum-runtime         shard runtime and thread-per-core scaffolding
  aurum-internal-protocol adapter-neutral command/event batches
  aurum-broker          in-memory broker executor (composes core + protocol)
  aurum-cli             CLI entry point
experiments/
  h1-queue-engine       hypothesis 1 benchmark harness
  h2-consumer-session   consumer session benchmark harness
  h3-command-protocol   internal command protocol boundary tests
  h4-in-memory-broker   single-node in-memory broker executor workloads
  h5-routing            compiled routing benchmark harness
  h6-native-protocol    native binary protocol codec/adapter workloads
  h7-storage-engine     append-only storage engine benchmark harness
  h8-amqp-adapter       AMQP 0-9-1 adapter transcript workloads
```

Dependency boundary:

```text
adapter -> aurum-internal-protocol -> aurum-broker -> aurum-core
aurum-core must not depend on protocol crates or aurum-broker.
```

Design rule:

```text
Hot path: concrete types, static dispatch, ranges/masks, no protocol-specific objects.
Cold path: dynamic dispatch is allowed for plugins/adapters/management.
```

## Build

Use the provided standalone toolchain if available:

```bash
PATH=/mnt/data/rust/bin:$PATH cargo check --workspace --all-targets
PATH=/mnt/data/rust/bin:$PATH cargo build --workspace --release
```

Or with your local Rust:

```bash
cargo check --workspace --all-targets
```

## Run H1 experiment

```bash
cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=deliver_ack --variant=both
cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=random_ack --variant=both
cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=nack_retry_ack --variant=both
```

The current H1 implementation is a hybrid range + block-bitset queue:

```text
sequential ready path -> DeliveryRange / AckRange
sparse/retry path     -> DeliveryMask / AckMask
active state          -> block-level lists / masks
```

## Run H4 in-memory broker

```bash
cargo run --release -p h4-in-memory-broker -- \
  --messages=4194304 \
  --batch=128 \
  --prefetch=128 \
  --consumers=1 \
  --workload=publish_deliver_ack
```

Workloads: `publish_deliver_ack`, `publish_deliver_ack_multiple`, `publish_nack_requeue_ack`, `consumer_cancel_requeue`, `multi_consumer_round_robin`.

## Run H5 routing benchmark

```bash
cargo run --release -p h5-routing
```

Compares cold `resolve_direct(exchange, routing_key)` vs hot `get_by_route_id(route_id, version)`.

## Run H6 native protocol benchmark

```bash
cargo run --release -p h6-native-protocol -- \
  --messages=1048576 \
  --batch=128 \
  --workload=publish_route_id_batch
```

Workloads: `resolve_route_only`, `publish_route_id_batch`, `publish_resolve_each_time`.

See [docs/NATIVE_PROTOCOL_V0.md](docs/NATIVE_PROTOCOL_V0.md) for wire format.

## Run H7 storage engine benchmark

```bash
cargo run --release -p h7-storage-engine -- \
  --messages=1048576 \
  --payload-bytes=256 \
  --batch=128 \
  --mode=buffered
```

Workloads: `append_payload_batches` (default), `publish_ack_recover`.

Modes: `buffered`, `fsync-on-flush`.

See [docs/STORAGE_FORMAT_V0.md](docs/STORAGE_FORMAT_V0.md) and [docs/STORAGE_RECOVERY_MODEL.md](docs/STORAGE_RECOVERY_MODEL.md).

## Run H8 AMQP adapter transcript

```bash
cargo run -p h8-amqp-adapter -- handshake_only
cargo test -p aurum-broker in_memory::amqp_harness_tests
```

See [docs/AMQP_COMPATIBILITY_V0.md](docs/AMQP_COMPATIBILITY_V0.md) and [docs/AMQP_ADAPTER_ARCHITECTURE.md](docs/AMQP_ADAPTER_ARCHITECTURE.md).

## Next work

1. Native TCP transport wrapping the PR7 codec/adapter.
2. Single-node server runtime (PR10) with AMQP listener.
3. Keep AMQP and native protocol behind adapter crates so the core never depends on protocol types.
