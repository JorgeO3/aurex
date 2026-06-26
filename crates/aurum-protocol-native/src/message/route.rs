use bytes::{BufMut, BytesMut};

use crate::codec::cursor::{
    pack_route_id, unpack_route_id, Cursor, CursorError, write_u16_le, write_u32_le, write_u64_le,
};
use crate::wire::MAX_EXCHANGE_NAME_LEN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRouteBody {
    pub route_table_version_hint: u64,
    pub exchange_id_hint: u32,
    pub exchange: Vec<u8>,
    pub routing_key: Vec<u8>,
}

impl ResolveRouteBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let route_table_version_hint = cur.read_u64_le()?;
        let exchange_id_hint = cur.read_u32_le()?;
        let exchange_len = cur.read_u16_le()? as usize;
        let routing_key_len = cur.read_u16_le()? as usize;
        if exchange_len > MAX_EXCHANGE_NAME_LEN {
            return Err(CursorError::StringTooLong {
                max: MAX_EXCHANGE_NAME_LEN,
            });
        }
        if routing_key_len > crate::wire::MAX_ROUTING_KEY_LEN {
            return Err(CursorError::StringTooLong {
                max: crate::wire::MAX_ROUTING_KEY_LEN,
            });
        }
        let exchange = cur.read_bytes(exchange_len)?.to_vec();
        let routing_key = cur.read_bytes(routing_key_len)?.to_vec();
        Ok(Self {
            route_table_version_hint,
            exchange_id_hint,
            exchange,
            routing_key,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        if self.exchange.len() > MAX_EXCHANGE_NAME_LEN
            || self.routing_key.len() > crate::wire::MAX_ROUTING_KEY_LEN
        {
            return Err(CursorError::StringTooLong {
                max: MAX_EXCHANGE_NAME_LEN,
            });
        }
        write_u64_le(dst, self.route_table_version_hint);
        write_u32_le(dst, self.exchange_id_hint);
        write_u16_le(dst, self.exchange.len() as u16);
        write_u16_le(dst, self.routing_key.len() as u16);
        dst.put_slice(&self.exchange);
        dst.put_slice(&self.routing_key);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteResolvedBody {
    pub route_table_version: u64,
    pub route_id_packed: u64,
}

impl RouteResolvedBody {
    #[must_use]
    pub fn route_parts(&self) -> (u32, u32) {
        unpack_route_id(self.route_id_packed)
    }

    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        Ok(Self {
            route_table_version: cur.read_u64_le()?,
            route_id_packed: cur.read_u64_le()?,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u64_le(dst, self.route_table_version);
        write_u64_le(dst, self.route_id_packed);
    }

    #[must_use]
    pub fn from_route(route_table_version: u64, index: u32, generation: u32) -> Self {
        Self {
            route_table_version,
            route_id_packed: pack_route_id(index, generation),
        }
    }
}
