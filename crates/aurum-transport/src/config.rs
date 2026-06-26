use std::net::SocketAddr;

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ListenerFlags: u16 {
        const ENABLED = 1 << 0;
        const TCP_NODELAY = 1 << 1;
        const LOW_LATENCY = 1 << 2;
        const ALLOW_PLAINTEXT = 1 << 3;
    }
}

#[derive(Debug, Clone)]
pub struct ListenerConfig {
    pub bind: SocketAddr,
    pub flags: ListenerFlags,
    pub max_connections: usize,
    pub max_read_buffer: usize,
    pub max_write_buffer: usize,
}

impl ListenerConfig {
    #[must_use]
    pub fn localhost(port: u16) -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], port)),
            flags: ListenerFlags::ENABLED | ListenerFlags::TCP_NODELAY | ListenerFlags::ALLOW_PLAINTEXT,
            max_connections: 256,
            max_read_buffer: 64 * 1024,
            max_write_buffer: 256 * 1024,
        }
    }
}
