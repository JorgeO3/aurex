use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    read_longstr, read_shortstr, read_u32, read_u64, write_longstr, write_shortstr, ShortStr,
    WireError,
};

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    ShortShortInt(i8),
    ShortShortUInt(u8),
    ShortInt(i16),
    ShortUInt(u16),
    LongInt(i32),
    LongUInt(u32),
    LongLongInt(i64),
    LongLongUInt(u64),
    Float(f32),
    Double(f64),
    ShortString(ShortStr),
    LongString(Bytes),
    FieldTable(FieldTable),
    Void,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldTable {
    pub fields: BTreeMap<String, FieldValue>,
}

impl FieldTable {
  pub fn insert(&mut self, key: impl Into<String>, value: FieldValue) {
        self.fields.insert(key.into(), value);
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), WireError> {
        let mut body = BytesMut::new();
        for (k, v) in &self.fields {
            let key = ShortStr::try_from_bytes(k.as_bytes())?;
            write_shortstr(&mut body, &key);
            encode_field_value(&mut body, v)?;
        }
        write_longstr(dst, &body);
        Ok(())
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, WireError> {
        let table_bytes = read_longstr(buf)?;
        let mut slice = table_bytes.as_ref();
        let mut fields = BTreeMap::new();
        while !slice.is_empty() {
            let key = read_shortstr(&mut slice)?;
            let key_str = String::from_utf8_lossy(key.as_bytes()).into_owned();
            let value = decode_field_value(&mut slice)?;
            fields.insert(key_str, value);
        }
        Ok(Self { fields })
    }
}

fn encode_field_value(dst: &mut BytesMut, value: &FieldValue) -> Result<(), WireError> {
    match value {
        FieldValue::Bool(v) => {
            dst.put_u8(b't');
            dst.put_u8(u8::from(*v));
        }
        FieldValue::ShortShortInt(v) => {
            dst.put_u8(b'b');
            dst.put_i8(*v);
        }
        FieldValue::ShortShortUInt(v) => {
            dst.put_u8(b'B');
            dst.put_u8(*v);
        }
        FieldValue::ShortInt(v) => {
            dst.put_u8(b'U');
            dst.put_i16(*v);
        }
        FieldValue::ShortUInt(v) => {
            dst.put_u8(b'u');
            dst.put_u16(*v);
        }
        FieldValue::LongInt(v) => {
            dst.put_u8(b'I');
            dst.put_i32(*v);
        }
        FieldValue::LongUInt(v) => {
            dst.put_u8(b'i');
            dst.put_u32(*v);
        }
        FieldValue::LongLongInt(v) => {
            dst.put_u8(b'L');
            dst.put_i64(*v);
        }
        FieldValue::LongLongUInt(v) => {
            dst.put_u8(b'l');
            dst.put_u64(*v);
        }
        FieldValue::Float(v) => {
            dst.put_u8(b'f');
            dst.put_f32(*v);
        }
        FieldValue::Double(v) => {
            dst.put_u8(b'd');
            dst.put_f64(*v);
        }
        FieldValue::ShortString(s) => {
            dst.put_u8(b's');
            write_shortstr(dst, s);
        }
        FieldValue::LongString(s) => {
            dst.put_u8(b'S');
            write_longstr(dst, s);
        }
        FieldValue::FieldTable(t) => {
            dst.put_u8(b'F');
            t.encode(dst)?;
        }
        FieldValue::Void => {
            dst.put_u8(b'V');
        }
    }
    Ok(())
}

fn decode_field_value(buf: &mut &[u8]) -> Result<FieldValue, WireError> {
    if buf.is_empty() {
        return Err(WireError::NeedMore);
    }
    let tag = buf[0];
    buf.advance(1);
    Ok(match tag {
        b't' => {
            let v = buf[0] != 0;
            buf.advance(1);
            FieldValue::Bool(v)
        }
        b'b' => {
            let v = buf[0] as i8;
            buf.advance(1);
            FieldValue::ShortShortInt(v)
        }
        b'B' => {
            let v = buf[0];
            buf.advance(1);
            FieldValue::ShortShortUInt(v)
        }
        b'U' => {
            let v = i16::from_be_bytes(buf[..2].try_into().expect("i16"));
            buf.advance(2);
            FieldValue::ShortInt(v)
        }
        b'u' => {
            let v = u16::from_be_bytes(buf[..2].try_into().expect("u16"));
            buf.advance(2);
            FieldValue::ShortUInt(v)
        }
        b'I' => {
            let v = i32::from_be_bytes(buf[..4].try_into().expect("i32"));
            buf.advance(4);
            FieldValue::LongInt(v)
        }
        b'i' => {
            let v = u32::from_be_bytes(buf[..4].try_into().expect("u32"));
            buf.advance(4);
            FieldValue::LongUInt(v)
        }
        b'L' => {
            let v = i64::from_be_bytes(buf[..8].try_into().expect("i64"));
            buf.advance(8);
            FieldValue::LongLongInt(v)
        }
        b'l' => {
            let v = u64::from_be_bytes(buf[..8].try_into().expect("u64"));
            buf.advance(8);
            FieldValue::LongLongUInt(v)
        }
        b'f' => {
            let v = f32::from_be_bytes(buf[..4].try_into().expect("f32"));
            buf.advance(4);
            FieldValue::Float(v)
        }
        b'd' => {
            let v = f64::from_be_bytes(buf[..8].try_into().expect("f64"));
            buf.advance(8);
            FieldValue::Double(v)
        }
        b's' => FieldValue::ShortString(read_shortstr(buf)?),
        b'S' => FieldValue::LongString(read_longstr(buf)?),
        b'F' => FieldValue::FieldTable(FieldTable::decode(buf)?),
        b'V' => FieldValue::Void,
        _ => return Err(WireError::UnknownFrameType),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_roundtrip() {
        let table = FieldTable::default();
        let mut buf = BytesMut::new();
        table.encode(&mut buf).unwrap();
        let mut slice = buf.as_ref();
        let decoded = FieldTable::decode(&mut slice).unwrap();
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn table_with_string() {
        let mut table = FieldTable::default();
        table.insert("x-queue-type", FieldValue::ShortString(ShortStr::from("classic")));
        let mut buf = BytesMut::new();
        table.encode(&mut buf).unwrap();
        let mut slice = buf.as_ref();
        let decoded = FieldTable::decode(&mut slice).unwrap();
        assert_eq!(decoded.fields.len(), 1);
    }
}
