use std::fmt;

#[derive(Debug)]
pub enum BrokerInitError {
    Storage(String),
    Routing(String),
    Config(crate::single_node::config::ConfigError),
}

impl fmt::Display for BrokerInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "storage init failed: {e}"),
            Self::Routing(e) => write!(f, "routing init failed: {e}"),
            Self::Config(e) => write!(f, "config error: {e}"),
        }
    }
}

impl std::error::Error for BrokerInitError {}

impl From<crate::single_node::config::ConfigError> for BrokerInitError {
    fn from(value: crate::single_node::config::ConfigError) -> Self {
        Self::Config(value)
    }
}
