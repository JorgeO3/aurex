use bytes::{Buf, BufMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorError {
    UnexpectedEof,
    StringTooLong { max: usize },
    InvalidLength,
    OffsetOutOfBounds,
    Overflow,
}

pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    #[must_use]
    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn read_u8(&mut self) -> Result<u8, CursorError> {
        if self.pos >= self.buf.len() {
            return Err(CursorError::UnexpectedEof);
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn read_u16_le(&mut self) -> Result<u16, CursorError> {
        if self.remaining() < 2 {
            return Err(CursorError::UnexpectedEof);
        }
        let mut cur = &self.buf[self.pos..];
        let v = cur.get_u16_le();
        self.pos += 2;
        Ok(v)
    }

    pub fn read_u32_le(&mut self) -> Result<u32, CursorError> {
        if self.remaining() < 4 {
            return Err(CursorError::UnexpectedEof);
        }
        let mut cur = &self.buf[self.pos..];
        let v = cur.get_u32_le();
        self.pos += 4;
        Ok(v)
    }

    pub fn read_u64_le(&mut self) -> Result<u64, CursorError> {
        if self.remaining() < 8 {
            return Err(CursorError::UnexpectedEof);
        }
        let mut cur = &self.buf[self.pos..];
        let v = cur.get_u64_le();
        self.pos += 8;
        Ok(v)
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], CursorError> {
        if self.remaining() < len {
            return Err(CursorError::UnexpectedEof);
        }
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    pub fn read_string(&mut self, max_len: usize) -> Result<&'a [u8], CursorError> {
        let len = usize::from(self.read_u16_le()?);
        if len > max_len {
            return Err(CursorError::StringTooLong { max: max_len });
        }
        self.read_bytes(len)
    }

    pub fn check_bounds(&self, offset: u32, len: u32) -> Result<(), CursorError> {
        let end = offset
            .checked_add(len)
            .ok_or(CursorError::Overflow)?;
        if end as usize > self.buf.len() {
            return Err(CursorError::OffsetOutOfBounds);
        }
        Ok(())
    }
}

pub fn write_u16_le(dst: &mut impl BufMut, v: u16) {
    dst.put_u16_le(v);
}

pub fn write_u32_le(dst: &mut impl BufMut, v: u32) {
    dst.put_u32_le(v);
}

pub fn write_u64_le(dst: &mut impl BufMut, v: u64) {
    dst.put_u64_le(v);
}

pub fn write_bytes(dst: &mut impl BufMut, bytes: &[u8]) {
    dst.put_slice(bytes);
}

pub fn write_string(dst: &mut impl BufMut, bytes: &[u8], max_len: usize) -> Result<(), CursorError> {
    if bytes.len() > max_len {
        return Err(CursorError::StringTooLong { max: max_len });
    }
    write_u16_le(dst, bytes.len() as u16);
    write_bytes(dst, bytes);
    Ok(())
}

pub fn pack_route_id(index: u32, generation: u32) -> u64 {
    (u64::from(generation) << 32) | u64::from(index)
}

pub fn unpack_route_id(v: u64) -> (u32, u32) {
    ((v & 0xFFFF_FFFF) as u32, (v >> 32) as u32)
}
