# Configuration v0 (PR10)

Minimal TOML configuration for the single-node broker.

## Example

See [`examples/single-node.toml`](../examples/single-node.toml).

## Sections

### `[broker]`

| Key | Values | Default |
|-----|--------|---------|
| `mode` | `dev-in-memory`, `single-node-persistent` | `dev-in-memory` |

### `[storage]`

| Key | Values | Default |
|-----|--------|---------|
| `backend` | `noop`, `append-only` | `noop` |
| `data_dir` | path | `./data` |

### `[listeners.native]` / `[listeners.amqp]`

| Key | Type | Default |
|-----|------|---------|
| `enabled` | bool | `true` |
| `bind` | socket address | `127.0.0.1:7777` / `127.0.0.1:5672` |

### `[[exchanges]]`, `[[queues]]`, `[[bindings]]`

Bootstrap routing compiled at startup via `RouteCompiler`.

## CLI

```bash
# Validate config
cargo run -p aurum-cli -- broker check-config --config examples/single-node.toml

# Dev mode with overrides
cargo run -p aurum-cli -- broker dev --native 127.0.0.1:7777 --amqp 127.0.0.1:5672

# Start from file
cargo run -p aurum-cli -- broker start --config examples/single-node.toml
```

## Limits (programmatic)

`BrokerLimits` in `SingleNodeBrokerConfig` defines defensive caps:

- `max_frame_size`
- `max_connection_read_buffer`
- `max_connection_write_buffer`
- `max_connections`

These are wired for future enforcement; PR10 uses conservative defaults.
