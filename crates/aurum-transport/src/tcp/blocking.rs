use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::config::ListenerConfig;
use crate::connection_id::ConnectionId;
use crate::error::TransportResult;
use crate::tcp::connection::Connection;
use crate::tcp::listener::TcpListenerBackend;

pub type ConnectionHandler = Arc<dyn Fn(ConnectionId, Connection) + Send + Sync + 'static>;

/// Spawn a blocking accept loop on a dedicated thread.
pub fn spawn_blocking_listener(
    config: ListenerConfig,
    handler: ConnectionHandler,
) -> TransportResult<(JoinHandle<()>, SocketAddr)> {
    let backend = Arc::new(TcpListenerBackend::bind(config)?);
    let addr = backend.local_addr()?;
    let backend_thread = Arc::clone(&backend);
    let join = thread::Builder::new()
        .name("aurum-tcp-listener".into())
        .spawn(move || {
            loop {
                match backend_thread.accept() {
                    Ok((id, conn)) => handler(id, conn),
                    Err(_) => break,
                }
            }
        })?;
    Ok((join, addr))
}
