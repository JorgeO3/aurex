use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::constants::{FRAME_END, PROTOCOL_HEADER};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Method,
    Header,
    Body,
    Heartbeat,
}

impl FrameKind {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Method => 1,
            Self::Header => 2,
            Self::Body => 3,
            Self::Heartbeat => 4,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Method),
            2 => Some(Self::Header),
            3 => Some(Self::Body),
            4 => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: FrameKind,
    pub channel: u16,
    pub size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl RawFrame {
    #[must_use]
    pub fn new(kind: FrameKind, channel: u16, payload: Bytes) -> Self {
        Self {
            header: FrameHeader {
                kind,
                channel,
                size: payload.len() as u32,
            },
            payload,
        }
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u8(self.header.kind.as_u8());
        dst.put_u16(self.header.channel);
        dst.put_u32(self.header.size);
        dst.extend_from_slice(&self.payload);
        dst.put_u8(FRAME_END);
    }

    #[must_use]
    pub fn wire_len(&self) -> usize {
        1 + 2 + 4 + self.payload.len() + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolHeaderStatus {
    Complete,
    NeedMore,
    Invalid,
}

pub fn parse_protocol_header(buf: &[u8]) -> ProtocolHeaderStatus {
    if buf.len() < PROTOCOL_HEADER.len() {
        return ProtocolHeaderStatus::NeedMore;
    }
    if &buf[..PROTOCOL_HEADER.len()] == PROTOCOL_HEADER {
        ProtocolHeaderStatus::Complete
    } else {
        ProtocolHeaderStatus::Invalid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::constants::{FRAME_END, PROTOCOL_HEADER};

    #[test]
    fn frame_roundtrip() {
        let frame = RawFrame::new(FrameKind::Method, 1, Bytes::from_static(b"payload"));
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        assert_eq!(buf[buf.len() - 1], FRAME_END);
    }

    #[test]
    fn protocol_header_valid() {
        assert_eq!(
            parse_protocol_header(PROTOCOL_HEADER),
            ProtocolHeaderStatus::Complete
        );
    }

    #[test]
    fn protocol_header_invalid() {
        assert_eq!(
            parse_protocol_header(b"NOTAMQP0"),
            ProtocolHeaderStatus::Invalid
        );
    }
}
