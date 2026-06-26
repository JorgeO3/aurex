use bytes::{Buf, BufMut};

use super::constants::{NATIVE_HEADER_LEN, NATIVE_MAGIC, NATIVE_WIRE_VERSION};
use super::error_code::NativeErrorCode;
use super::flags::{validate_frame_flags, FrameFlags};
use super::op::{NativeOp, NativeOpError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct NativeFrameHeader {
    pub magic: u16,
    pub version: u8,
    pub header_len: u8,
    pub op: u16,
    pub flags: u16,
    pub stream_id: u32,
    pub correlation_id: u64,
    pub body_len: u32,
    pub reserved: u32,
    pub reserved2: u32,
}

impl NativeFrameHeader {
    #[must_use]
    pub const fn new(op: NativeOp, flags: FrameFlags, stream_id: u32, correlation_id: u64, body_len: u32) -> Self {
        Self {
            magic: NATIVE_MAGIC,
            version: NATIVE_WIRE_VERSION,
            header_len: NATIVE_HEADER_LEN,
            op: op.as_u16(),
            flags: flags.bits(),
            stream_id,
            correlation_id,
            body_len,
            reserved: 0,
            reserved2: 0,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) {
        dst.put_u16_le(self.magic);
        dst.put_u8(self.version);
        dst.put_u8(self.header_len);
        dst.put_u16_le(self.op);
        dst.put_u16_le(self.flags);
        dst.put_u32_le(self.stream_id);
        dst.put_u64_le(self.correlation_id);
        dst.put_u32_le(self.body_len);
        dst.put_u32_le(self.reserved);
        dst.put_u32_le(self.reserved2);
    }

    pub fn decode(src: &[u8]) -> Result<Self, NativeHeaderError> {
        if src.len() < usize::from(NATIVE_HEADER_LEN) {
            return Err(NativeHeaderError::Truncated);
        }
        let mut cur = &src[..usize::from(NATIVE_HEADER_LEN)];
        let magic = cur.get_u16_le();
        if magic != NATIVE_MAGIC {
            return Err(NativeHeaderError::BadMagic);
        }
        let version = cur.get_u8();
        if version != NATIVE_WIRE_VERSION {
            return Err(NativeHeaderError::UnsupportedVersion);
        }
        let header_len = cur.get_u8();
        if header_len < NATIVE_HEADER_LEN {
            return Err(NativeHeaderError::HeaderTooSmall);
        }
        let op = cur.get_u16_le();
        let flags_raw = cur.get_u16_le();
        let stream_id = cur.get_u32_le();
        let correlation_id = cur.get_u64_le();
        let body_len = cur.get_u32_le();
        let reserved = cur.get_u32_le();
        let reserved2 = cur.get_u32_le();

        let flags = FrameFlags::from_bits(flags_raw).ok_or(NativeHeaderError::InvalidFlags)?;
        validate_frame_flags(flags).map_err(|_| NativeHeaderError::InvalidFlags)?;
        let _op = NativeOp::try_from(op).map_err(|e| match e {
            NativeOpError::Unknown(v) => NativeHeaderError::UnknownOp(v),
        })?;

        Ok(Self {
            magic,
            version,
            header_len,
            op,
            flags: flags.bits(),
            stream_id,
            correlation_id,
            body_len,
            reserved,
            reserved2,
        })
    }

    #[must_use]
    pub fn op(&self) -> Result<NativeOp, NativeOpError> {
        NativeOp::try_from(self.op)
    }

    #[must_use]
    pub fn frame_flags(&self) -> Option<FrameFlags> {
        FrameFlags::from_bits(self.flags)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeHeaderError {
    Truncated,
    BadMagic,
    UnsupportedVersion,
    HeaderTooSmall,
    InvalidFlags,
    UnknownOp(u16),
}

impl From<NativeHeaderError> for NativeErrorCode {
    fn from(value: NativeHeaderError) -> Self {
        match value {
            NativeHeaderError::Truncated | NativeHeaderError::BadMagic | NativeHeaderError::HeaderTooSmall => {
                Self::MalformedFrame
            }
            NativeHeaderError::UnsupportedVersion => Self::UnsupportedVersion,
            NativeHeaderError::InvalidFlags => Self::InvalidFlags,
            NativeHeaderError::UnknownOp(_) => Self::UnknownOp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_32_bytes() {
        let hdr = NativeFrameHeader::new(NativeOp::Heartbeat, FrameFlags::NONE, 0, 0, 0);
        let mut buf = bytes::BytesMut::new();
        hdr.encode(&mut buf);
        assert_eq!(buf.len(), 32);
        let decoded = NativeFrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded, hdr);
    }

    #[test]
    fn rejects_bad_magic() {
        let hdr = NativeFrameHeader::new(NativeOp::Heartbeat, FrameFlags::NONE, 0, 0, 0);
        let mut buf = bytes::BytesMut::new();
        hdr.encode(&mut buf);
        buf[0] = 0;
        assert!(matches!(
            NativeFrameHeader::decode(&buf),
            Err(NativeHeaderError::BadMagic)
        ));
    }
}
