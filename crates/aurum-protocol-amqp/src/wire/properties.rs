use bitflags::bitflags;
use bytes::{Buf, BufMut, BytesMut};

use super::{read_bit, read_shortstr, read_u64, write_bits, write_shortstr, ShortStr, WireError};
use super::field_table::FieldTable;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BasicPropertyFlags: u16 {
        const CONTENT_TYPE = 1 << 15;
        const CONTENT_ENCODING = 1 << 14;
        const HEADERS = 1 << 13;
        const DELIVERY_MODE = 1 << 12;
        const PRIORITY = 1 << 11;
        const CORRELATION_ID = 1 << 10;
        const REPLY_TO = 1 << 9;
        const EXPIRATION = 1 << 8;
        const MESSAGE_ID = 1 << 7;
        const TIMESTAMP = 1 << 6;
        const TYPE = 1 << 5;
        const USER_ID = 1 << 4;
        const APP_ID = 1 << 3;
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicProperties {
    pub content_type: Option<ShortStr>,
    pub content_encoding: Option<ShortStr>,
    pub headers: FieldTable,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<ShortStr>,
    pub reply_to: Option<ShortStr>,
    pub expiration: Option<ShortStr>,
    pub message_id: Option<ShortStr>,
    pub timestamp: Option<u64>,
    pub message_type: Option<ShortStr>,
    pub user_id: Option<ShortStr>,
    pub app_id: Option<ShortStr>,
}

impl BasicProperties {
    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), WireError> {
        let mut flags = BasicPropertyFlags::empty();
        if self.content_type.is_some() {
            flags |= BasicPropertyFlags::CONTENT_TYPE;
        }
        if self.content_encoding.is_some() {
            flags |= BasicPropertyFlags::CONTENT_ENCODING;
        }
        if !self.headers.fields.is_empty() {
            flags |= BasicPropertyFlags::HEADERS;
        }
        if self.delivery_mode.is_some() {
            flags |= BasicPropertyFlags::DELIVERY_MODE;
        }
        if self.priority.is_some() {
            flags |= BasicPropertyFlags::PRIORITY;
        }
        if self.correlation_id.is_some() {
            flags |= BasicPropertyFlags::CORRELATION_ID;
        }
        if self.reply_to.is_some() {
            flags |= BasicPropertyFlags::REPLY_TO;
        }
        if self.expiration.is_some() {
            flags |= BasicPropertyFlags::EXPIRATION;
        }
        if self.message_id.is_some() {
            flags |= BasicPropertyFlags::MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            flags |= BasicPropertyFlags::TIMESTAMP;
        }
        if self.message_type.is_some() {
            flags |= BasicPropertyFlags::TYPE;
        }
        if self.user_id.is_some() {
            flags |= BasicPropertyFlags::USER_ID;
        }
        if self.app_id.is_some() {
            flags |= BasicPropertyFlags::APP_ID;
        }
        dst.put_u16(flags.bits());

        if let Some(v) = &self.content_type {
            write_shortstr(dst, v);
        }
        if let Some(v) = &self.content_encoding {
            write_shortstr(dst, v);
        }
        if !self.headers.fields.is_empty() {
            self.headers.encode(dst)?;
        }
        if let Some(v) = self.delivery_mode {
            dst.put_u8(v);
        }
        if let Some(v) = self.priority {
            dst.put_u8(v);
        }
        if let Some(v) = &self.correlation_id {
            write_shortstr(dst, v);
        }
        if let Some(v) = &self.reply_to {
            write_shortstr(dst, v);
        }
        if let Some(v) = &self.expiration {
            write_shortstr(dst, v);
        }
        if let Some(v) = &self.message_id {
            write_shortstr(dst, v);
        }
        if let Some(v) = self.timestamp {
            dst.put_u64(v);
        }
        if let Some(v) = &self.message_type {
            write_shortstr(dst, v);
        }
        if let Some(v) = &self.user_id {
            write_shortstr(dst, v);
        }
        if let Some(v) = &self.app_id {
            write_shortstr(dst, v);
        }
        Ok(())
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, WireError> {
        let flags_raw = u16::from_be_bytes(buf[..2].try_into().expect("flags"));
        buf.advance(2);
        let flags = BasicPropertyFlags::from_bits(flags_raw).unwrap_or(BasicPropertyFlags::empty());
        let mut bit_pos = 0u8;
        let mut props = Self::default();

        macro_rules! read_opt_shortstr {
            ($field:ident, $flag:ident) => {
                if flags.contains(BasicPropertyFlags::$flag) {
                    props.$field = Some(read_shortstr(buf)?);
                }
            };
        }

        read_opt_shortstr!(content_type, CONTENT_TYPE);
        read_opt_shortstr!(content_encoding, CONTENT_ENCODING);
        if flags.contains(BasicPropertyFlags::HEADERS) {
            props.headers = FieldTable::decode(buf)?;
        }
        if flags.contains(BasicPropertyFlags::DELIVERY_MODE) {
            props.delivery_mode = Some(buf[0]);
            buf.advance(1);
        }
        if flags.contains(BasicPropertyFlags::PRIORITY) {
            props.priority = Some(buf[0]);
            buf.advance(1);
        }
        read_opt_shortstr!(correlation_id, CORRELATION_ID);
        read_opt_shortstr!(reply_to, REPLY_TO);
        read_opt_shortstr!(expiration, EXPIRATION);
        read_opt_shortstr!(message_id, MESSAGE_ID);
        if flags.contains(BasicPropertyFlags::TIMESTAMP) {
            props.timestamp = Some(read_u64(buf)?);
        }
        read_opt_shortstr!(message_type, TYPE);
        read_opt_shortstr!(user_id, USER_ID);
        read_opt_shortstr!(app_id, APP_ID);
        let _ = bit_pos;
        Ok(props)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContentHeader {
    pub class_id: u16,
    pub body_size: u64,
    pub properties: BasicProperties,
}

impl ContentHeader {
    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), WireError> {
        dst.put_u16(self.class_id);
        dst.put_u16(0); // weight
        dst.put_u64(self.body_size);
        self.properties.encode(dst)
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, WireError> {
        let class_id = u16::from_be_bytes(buf[..2].try_into().expect("class"));
        buf.advance(2);
        buf.advance(2); // weight
        let body_size = read_u64(buf)?;
        let properties = BasicProperties::decode(buf)?;
        Ok(Self {
            class_id,
            body_size,
            properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_roundtrip() {
        let mut props = BasicProperties::default();
        props.delivery_mode = Some(2);
        props.content_type = Some(ShortStr::from("text/plain"));
        let mut buf = BytesMut::new();
        props.encode(&mut buf).unwrap();
        let mut slice = buf.as_ref();
        let decoded = BasicProperties::decode(&mut slice).unwrap();
        assert_eq!(decoded.delivery_mode, Some(2));
    }
}
