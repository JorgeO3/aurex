# Workspace Architecture

AurumMQ uses a workspace-as-plugin architecture.

This is not dynamic loading yet. It is a compile-time plugin architecture where protocol adapters, storage engines, routing implementations, kernels, and runtime backends are separate crates composed by `aurum-broker`.

## Goals

- Improve incremental compilation times by isolating crates.
- Cache dependencies and build artifacts at workspace level.
- Keep hot-path crates small and stable.
- Avoid protocol-specific contamination inside `aurum-core`.
- Allow adapters to run embedded, sidecar, or external gateway later.

## Dependency direction

```text
aurum-types
  ↑
aurum-kernels     aurum-intrusive
  ↑                    ↑
aurum-core ────────────┘
  ↑
aurum-routing
  ↑
aurum-broker
  ↑
aurum-cli
```

Adapters depend on shared types and plugin traits, not vice versa:

```text
aurum-plugin-api
  ↑        ↑
native    amqp
```

The core must not depend on:

```text
AMQP frame types
Kafka frame types
HTTP request types
JSON/admin types
storage-specific file formats
Kubernetes/operator types
```

## Hot vs cold path

Hot path crates:

```text
aurum-core
aurum-kernels
aurum-intrusive
aurum-concurrency
```

Cold/control path crates:

```text
aurum-plugin-api
aurum-protocol-amqp
aurum-protocol-native
aurum-routing compiler pieces
aurum-cli
```

## Plugin modes planned

```text
embedded: adapter crate linked into broker process
sidecar: adapter process talks internal command protocol
external: remote gateway, higher latency but isolated
```

AMQP and native are first-class embedded adapters. Kafka/MQTT/STOMP should start as sidecars or external gateways.
