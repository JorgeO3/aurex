use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::config::ListenerConfig;
use crate::connection_id::ConnectionId;
use crate::error::{TransportError, TransportResult};
use crate::tcp::connection::Connection;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Blocking std::net TCP listener for PR10.
#[derive(Debug)]
pub struct TcpListenerBackend {
    listener: TcpListener,
    config: ListenerConfig,
    active: Arc<AtomicU64>,
}

impl TcpListenerBackend {
    pub fn bind(config: ListenerConfig) -> TransportResult<Self> {
        let listener = TcpListener::bind(config.bind)?;
        Ok(Self {
            listener,
            config,
            active: Arc::new(AtomicU64::new(0)),
        })
    }

    #[must_use]
    pub fn local_addr(&self) -> TransportResult<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    #[must_use]
    pub fn config(&self) -> &ListenerConfig {
        &self.config
    }

    pub fn accept(&self) -> TransportResult<(ConnectionId, Connection)> {
        let active = self.active.load(Ordering::Relaxed);
        if active as usize >= self.config.max_connections {
            return Err(TransportError::ConnectionLimit);
        }
        let (stream, _peer) = self.listener.accept()?;
        self.active.fetch_add(1, Ordering::Relaxed);
        let id = ConnectionId(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed));
        let conn = Connection::from_stream(stream, self.config.flags)?;
        Ok((id, conn))
    }

    pub fn release_connection(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}
