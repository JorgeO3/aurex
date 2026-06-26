use bytes::{Buf, BufMut, BytesMut};
use smallvec::SmallVec;

use super::error::WireError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortStr(SmallVec<[u8; 64]>);

impl ShortStr {
    pub const MAX_LEN: usize = 255;

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() > Self::MAX_LEN {
            return Err(WireError::FrameTooLarge {
                size: bytes.len() as u32,
                max: Self::MAX_LEN as u32,
            });
        }
        Ok(Self(bytes.iter().copied().collect()))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.0).into_owned()
    }
}

impl From<&str> for ShortStr {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().iter().copied().collect())
    }
}

pub fn read_shortstr(buf: &mut &[u8]) -> Result<ShortStr, WireError> {
    if buf.is_empty() {
        return Err(WireError::NeedMore);
    }
    let len = buf[0] as usize;
    if buf.len() < 1 + len {
        return Err(WireError::NeedMore);
    }
    buf.advance(1);
    let s = buf[..len].to_vec();
    buf.advance(len);
    ShortStr::try_from_bytes(&s)
}

pub fn write_shortstr(dst: &mut BytesMut, s: &ShortStr) {
    let b = s.as_bytes();
    dst.put_u8(b.len() as u8);
    dst.extend_from_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shortstr_roundtrip() {
        let s = ShortStr::from("hello");
        let mut buf = BytesMut::new();
        write_shortstr(&mut buf, &s);
        let mut slice = buf.as_ref();
        let decoded = read_shortstr(&mut slice).unwrap();
        assert_eq!(decoded.as_bytes(), b"hello");
    }

    #[test]
    fn shortstr_too_long() {
        let long = vec![b'a'; 256];
        assert!(ShortStr::try_from_bytes(&long).is_err());
    }
}
