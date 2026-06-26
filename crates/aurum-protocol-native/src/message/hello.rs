use bytes::{BufMut, BytesMut};

use crate::codec::cursor::{Cursor, CursorError, write_string, write_u16_le, write_u64_le};
use crate::wire::{NativeCapabilities, NATIVE_PROTOCOL_MAJOR, NATIVE_PROTOCOL_MINOR};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloBody {
    pub client_major: u16,
    pub client_minor: u16,
    pub client_capabilities: NativeCapabilities,
    pub client_name: Vec<u8>,
}

impl HelloBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let client_major = cur.read_u16_le()?;
        let client_minor = cur.read_u16_le()?;
        let caps = cur.read_u64_le()?;
        let client_capabilities =
            NativeCapabilities::from_bits(caps).ok_or(CursorError::InvalidLength)?;
        let client_name = cur.read_string(255)?.to_vec();
        Ok(Self {
            client_major,
            client_minor,
            client_capabilities,
            client_name,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        write_u16_le(dst, self.client_major);
        write_u16_le(dst, self.client_minor);
        write_u64_le(dst, self.client_capabilities.bits());
        write_string(dst, &self.client_name, 255)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloOkBody {
    pub server_major: u16,
    pub server_minor: u16,
    pub server_capabilities: NativeCapabilities,
    pub connection_id: u64,
}

impl HelloOkBody {
    #[must_use]
    pub fn default_server(connection_id: u64) -> Self {
        Self {
            server_major: NATIVE_PROTOCOL_MAJOR,
            server_minor: NATIVE_PROTOCOL_MINOR,
            server_capabilities: NativeCapabilities::ROUTE_ID
                | NativeCapabilities::PUBLISH_BATCH
                | NativeCapabilities::ACK_RANGE
                | NativeCapabilities::NACK_RANGE
                | NativeCapabilities::DELIVERY_BATCH,
            connection_id,
        }
    }

    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let server_major = cur.read_u16_le()?;
        let server_minor = cur.read_u16_le()?;
        let caps = cur.read_u64_le()?;
        let server_capabilities =
            NativeCapabilities::from_bits(caps).ok_or(CursorError::InvalidLength)?;
        let connection_id = cur.read_u64_le()?;
        Ok(Self {
            server_major,
            server_minor,
            server_capabilities,
            connection_id,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u16_le(dst, self.server_major);
        write_u16_le(dst, self.server_minor);
        write_u64_le(dst, self.server_capabilities.bits());
        write_u64_le(dst, self.connection_id);
    }
}
