use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BrokerFeatureFlags: u32 {
        const NATIVE_PROTOCOL = 1 << 0;
        const AMQP_PROTOCOL = 1 << 1;
        const APPEND_ONLY_STORAGE = 1 << 2;
        const ROUTE_ID_FAST_PATH = 1 << 3;
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerMode {
    DevInMemory = 0,
    SingleNodePersistent = 1,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendKind {
    Noop = 0,
    AppendOnly = 1,
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackendKind,
    pub data_dir: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackendKind::Noop,
            data_dir: PathBuf::from("./data"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListenerEndpointConfig {
    pub enabled: bool,
    pub bind: SocketAddr,
}

impl ListenerEndpointConfig {
    #[must_use]
    pub fn localhost(port: u16) -> Self {
        Self {
            enabled: true,
            bind: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ListenerConfigSet {
    pub native: Option<ListenerEndpointConfig>,
    pub amqp: Option<ListenerEndpointConfig>,
}

#[derive(Debug, Clone)]
pub struct ExchangeBootstrap {
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct QueueBootstrap {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct BindingBootstrap {
    pub exchange: String,
    pub queue: String,
    pub routing_key: String,
}

#[derive(Debug, Clone, Default)]
pub struct RoutingBootstrapConfig {
    pub exchanges: Vec<ExchangeBootstrap>,
    pub queues: Vec<QueueBootstrap>,
    pub bindings: Vec<BindingBootstrap>,
}

#[derive(Debug, Clone)]
pub struct BrokerLimits {
    pub max_frame_size: u32,
    pub max_connection_read_buffer: usize,
    pub max_connection_write_buffer: usize,
    pub max_inflight_command_batches: u32,
    pub max_payload_bytes_per_batch: u64,
    pub max_connections: usize,
}

impl Default for BrokerLimits {
    fn default() -> Self {
        Self {
            max_frame_size: 128 * 1024,
            max_connection_read_buffer: 64 * 1024,
            max_connection_write_buffer: 256 * 1024,
            max_inflight_command_batches: 64,
            max_payload_bytes_per_batch: 16 * 1024 * 1024,
            max_connections: 256,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SingleNodeBrokerConfig {
    pub mode: BrokerMode,
    pub storage: StorageConfig,
    pub listeners: ListenerConfigSet,
    pub routing: RoutingBootstrapConfig,
    pub limits: BrokerLimits,
    pub features: BrokerFeatureFlags,
}

impl Default for SingleNodeBrokerConfig {
    fn default() -> Self {
        Self::dev_defaults()
    }
}

impl SingleNodeBrokerConfig {
    #[must_use]
    pub fn dev_defaults() -> Self {
        let mut routing = RoutingBootstrapConfig::default();
        routing.exchanges.push(ExchangeBootstrap {
            name: "amq.direct".into(),
            kind: "direct".into(),
        });
        routing.queues.push(QueueBootstrap {
            name: "test.queue".into(),
        });
        routing.bindings.push(BindingBootstrap {
            exchange: "amq.direct".into(),
            queue: "test.queue".into(),
            routing_key: "test".into(),
        });
        Self {
            mode: BrokerMode::DevInMemory,
            storage: StorageConfig::default(),
            listeners: ListenerConfigSet {
                native: Some(ListenerEndpointConfig::localhost(7777)),
                amqp: Some(ListenerEndpointConfig::localhost(5672)),
            },
            routing,
            limits: BrokerLimits::default(),
            features: BrokerFeatureFlags::NATIVE_PROTOCOL | BrokerFeatureFlags::AMQP_PROTOCOL,
        }
    }

    pub fn from_toml_str(raw: &str) -> Result<Self, ConfigError> {
        let table: toml::Table =
            toml::from_str(raw).map_err(|e: toml::de::Error| ConfigError::Parse(e.to_string()))?;
        let mut config = Self::dev_defaults();

        if let Some(broker) = table.get("broker").and_then(|v| v.as_table()) {
            if let Some(mode) = broker.get("mode").and_then(|v| v.as_str()) {
                config.mode = match mode {
                    "dev-in-memory" | "dev" => BrokerMode::DevInMemory,
                    "single-node-persistent" => BrokerMode::SingleNodePersistent,
                    other => return Err(ConfigError::InvalidField {
                        field: "broker.mode".into(),
                        value: other.into(),
                    }),
                };
            }
        }

        if let Some(storage) = table.get("storage").and_then(|v| v.as_table()) {
            if let Some(backend) = storage.get("backend").and_then(|v| v.as_str()) {
                config.storage.backend = match backend {
                    "noop" => StorageBackendKind::Noop,
                    "append-only" => StorageBackendKind::AppendOnly,
                    other => {
                        return Err(ConfigError::InvalidField {
                            field: "storage.backend".into(),
                            value: other.into(),
                        });
                    }
                };
            }
            if let Some(dir) = storage.get("data_dir").and_then(|v| v.as_str()) {
                config.storage.data_dir = PathBuf::from(dir);
            }
        }

        if let Some(listeners) = table.get("listeners").and_then(|v| v.as_table()) {
            if let Some(native) = listeners.get("native").and_then(|v| v.as_table()) {
                config.listeners.native = Some(parse_listener_endpoint(native, "listeners.native")?);
            }
            if let Some(amqp) = listeners.get("amqp").and_then(|v| v.as_table()) {
                config.listeners.amqp = Some(parse_listener_endpoint(amqp, "listeners.amqp")?);
            }
        }

        config.routing = RoutingBootstrapConfig::default();
        if let Some(exchanges) = table.get("exchanges").and_then(|v| v.as_array()) {
            for item in exchanges {
                let t = item.as_table().ok_or_else(|| ConfigError::InvalidField {
                    field: "exchanges[]".into(),
                    value: "expected table".into(),
                })?;
                config.routing.exchanges.push(ExchangeBootstrap {
                    name: required_str(t, "name", "exchanges[]")?,
                    kind: optional_str(t, "kind").unwrap_or_else(|| "direct".into()),
                });
            }
        }
        if let Some(queues) = table.get("queues").and_then(|v| v.as_array()) {
            for item in queues {
                let t = item.as_table().ok_or_else(|| ConfigError::InvalidField {
                    field: "queues[]".into(),
                    value: "expected table".into(),
                })?;
                config.routing.queues.push(QueueBootstrap {
                    name: required_str(t, "name", "queues[]")?,
                });
            }
        }
        if let Some(bindings) = table.get("bindings").and_then(|v| v.as_array()) {
            for item in bindings {
                let t = item.as_table().ok_or_else(|| ConfigError::InvalidField {
                    field: "bindings[]".into(),
                    value: "expected table".into(),
                })?;
                config.routing.bindings.push(BindingBootstrap {
                    exchange: required_str(t, "exchange", "bindings[]")?,
                    queue: required_str(t, "queue", "bindings[]")?,
                    routing_key: optional_str(t, "routing_key").unwrap_or_default(),
                });
            }
        }

        Ok(config)
    }

    pub fn from_toml_file(path: impl AsRef<std::path::Path>) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path.as_ref())
            .map_err(|e| ConfigError::Io(e.to_string()))?;
        Self::from_toml_str(&raw)
    }
}

fn parse_listener_endpoint(
    table: &toml::Table,
    field: &str,
) -> Result<ListenerEndpointConfig, ConfigError> {
    let enabled = table
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let bind = table
        .get("bind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ConfigError::MissingField {
            field: format!("{field}.bind"),
        })?
        .parse()
        .map_err(|e: std::net::AddrParseError| ConfigError::InvalidField {
            field: format!("{field}.bind"),
            value: e.to_string(),
        })?;
    Ok(ListenerEndpointConfig { enabled, bind })
}

fn required_str(table: &toml::Table, key: &str, field: &str) -> Result<String, ConfigError> {
    table
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| ConfigError::MissingField {
            field: format!("{field}.{key}"),
        })
}

fn optional_str(table: &toml::Table, key: &str) -> Option<String> {
    table.get(key).and_then(|v| v.as_str()).map(str::to_owned)
}

#[derive(Debug)]
pub enum ConfigError {
    Io(String),
    Parse(String),
    MissingField { field: String },
    InvalidField { field: String, value: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "config io error: {e}"),
            Self::Parse(e) => write!(f, "config parse error: {e}"),
            Self::MissingField { field } => write!(f, "missing config field: {field}"),
            Self::InvalidField { field, value } => {
                write!(f, "invalid config field {field}: {value}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_defaults_have_listeners() {
        let cfg = SingleNodeBrokerConfig::dev_defaults();
        assert!(cfg.listeners.native.is_some());
        assert!(cfg.listeners.amqp.is_some());
    }

    #[test]
    fn parse_example_toml() {
        let raw = r#"
[broker]
mode = "single-node-persistent"

[storage]
backend = "append-only"
data_dir = "./data"

[listeners.native]
enabled = true
bind = "127.0.0.1:7777"

[listeners.amqp]
enabled = true
bind = "127.0.0.1:5672"

[[exchanges]]
name = "amq.direct"
kind = "direct"

[[queues]]
name = "test.queue"

[[bindings]]
exchange = "amq.direct"
queue = "test.queue"
routing_key = "test"
"#;
        let cfg = SingleNodeBrokerConfig::from_toml_str(raw).unwrap();
        assert_eq!(cfg.mode, BrokerMode::SingleNodePersistent);
        assert_eq!(cfg.storage.backend, StorageBackendKind::AppendOnly);
        assert_eq!(cfg.routing.bindings.len(), 1);
    }
}
