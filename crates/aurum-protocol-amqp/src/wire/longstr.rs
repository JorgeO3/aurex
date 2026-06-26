use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::error::WireError;

pub fn read_longstr(buf: &mut &[u8]) -> Result<Bytes, WireError> {
    if buf.len() < 4 {
        return Err(WireError::NeedMore);
    }
    let len = u32::from_be_bytes(buf[..4].try_into().expect("len")) as usize;
    if buf.len() < 4 + len {
        return Err(WireError::NeedMore);
    }
    buf.advance(4);
    let s = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(s)
}

pub fn write_longstr(dst: &mut BytesMut, s: &[u8]) {
    dst.put_u32(s.len() as u32);
    dst.extend_from_slice(s);
}
