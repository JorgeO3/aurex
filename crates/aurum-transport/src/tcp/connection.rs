use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::Duration;

use crate::config::ListenerFlags;
use crate::error::{TransportError, TransportResult};

/// Blocking TCP connection wrapper for PR10 development servers.
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn from_stream(mut stream: TcpStream, flags: ListenerFlags) -> TransportResult<Self> {
        stream.set_nodelay(flags.contains(ListenerFlags::TCP_NODELAY))?;
        stream.set_read_timeout(Some(Duration::from_secs(300)))?;
        Ok(Self { stream })
    }

    #[must_use]
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    pub fn read(&mut self, buf: &mut [u8]) -> TransportResult<usize> {
        Ok(self.stream.read(buf)?)
    }

    pub fn write_all(&mut self, buf: &[u8]) -> TransportResult<()> {
        self.stream.write_all(buf)?;
        Ok(())
    }

    pub fn shutdown(&self) -> TransportResult<()> {
        let _ = self.stream.shutdown(Shutdown::Both);
        Ok(())
    }
}
