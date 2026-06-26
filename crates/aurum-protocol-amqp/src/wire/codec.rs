use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::constants::FRAME_END;
use super::error::WireError;
use super::frame::{FrameHeader, FrameKind, RawFrame};

pub struct AmqpCodec {
    pub max_frame_size: u32,
    frame_header: Option<PartialHeader>,
}

#[derive(Debug, Clone)]
struct PartialHeader {
    kind: FrameKind,
    channel: u16,
    size: u32,
}

impl Default for AmqpCodec {
    fn default() -> Self {
        Self::new(crate::wire::constants::DEFAULT_FRAME_MAX)
    }
}

impl AmqpCodec {
    #[must_use]
    pub fn new(max_frame_size: u32) -> Self {
        Self {
            max_frame_size,
            frame_header: None,
        }
    }

    pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<RawFrame>, WireError> {
        loop {
            if let Some(hdr) = self.frame_header.take() {
                let need = hdr.size as usize + 1;
                if buf.len() < need {
                    self.frame_header = Some(hdr);
                    return Ok(None);
                }
                let payload = buf.split_to(hdr.size as usize).freeze();
                let end = buf.get_u8();
                if end != FRAME_END {
                    return Err(WireError::BadFrameEnd);
                }
                return Ok(Some(RawFrame {
                    header: FrameHeader {
                        kind: hdr.kind,
                        channel: hdr.channel,
                        size: hdr.size,
                    },
                    payload,
                }));
            }

            if buf.len() < 7 {
                return Ok(None);
            }
            let kind_byte = buf[0];
            let channel = u16::from_be_bytes([buf[1], buf[2]]);
            let size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
            if size > self.max_frame_size {
                return Err(WireError::FrameTooLarge {
                    size,
                    max: self.max_frame_size,
                });
            }
            let Some(kind) = FrameKind::from_u8(kind_byte) else {
                return Err(WireError::UnknownFrameType);
            };
            let _ = buf.split_to(7);
            if kind == FrameKind::Heartbeat && size == 0 {
                if buf.is_empty() {
                    return Ok(None);
                }
                let end = buf.get_u8();
                if end != FRAME_END {
                    return Err(WireError::BadFrameEnd);
                }
                return Ok(Some(RawFrame::new(kind, channel, Bytes::new())));
            }
            self.frame_header = Some(PartialHeader { kind, channel, size });
        }
    }

    pub fn encode(&self, frame: &RawFrame, dst: &mut BytesMut) {
        frame.encode(dst);
    }
}

pub fn read_u16(buf: &mut &[u8]) -> Result<u16, WireError> {
    if buf.len() < 2 {
        return Err(WireError::NeedMore);
    }
    let v = u16::from_be_bytes(buf[..2].try_into().expect("u16"));
    buf.advance(2);
    Ok(v)
}

pub fn read_u32(buf: &mut &[u8]) -> Result<u32, WireError> {
    if buf.len() < 4 {
        return Err(WireError::NeedMore);
    }
    let v = u32::from_be_bytes(buf[..4].try_into().expect("u32"));
    buf.advance(4);
    Ok(v)
}

pub fn read_u64(buf: &mut &[u8]) -> Result<u64, WireError> {
    if buf.len() < 8 {
        return Err(WireError::NeedMore);
    }
    let v = u64::from_be_bytes(buf[..8].try_into().expect("u64"));
    buf.advance(8);
    Ok(v)
}

pub fn write_u16(dst: &mut BytesMut, v: u16) {
    dst.put_u16(v);
}

pub fn write_u32(dst: &mut BytesMut, v: u32) {
    dst.put_u32(v);
}

pub fn write_u64(dst: &mut BytesMut, v: u64) {
    dst.put_u64(v);
}

pub fn read_bit(buf: &mut &[u8], bit_pos: &mut u8) -> Result<bool, WireError> {
    if *bit_pos == 0 {
        if buf.is_empty() {
            return Err(WireError::NeedMore);
        }
        *bit_pos = 8;
    }
    *bit_pos -= 1;
    let byte = buf[0];
    let set = (byte & (1 << *bit_pos)) != 0;
    if *bit_pos == 0 {
        buf.advance(1);
    }
    Ok(set)
}

pub fn write_bits(dst: &mut BytesMut, bits: u8) {
    dst.put_u8(bits);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_frame_end() {
        let mut codec = AmqpCodec::default();
        let mut buf = BytesMut::from(&[1u8, 0, 1, 0, 0, 0, 1, b'x', 0xFF][..]);
        assert!(matches!(codec.decode(&mut buf), Err(WireError::BadFrameEnd)));
    }

    #[test]
    fn oversized_frame() {
        let mut codec = AmqpCodec::new(8);
        let mut buf = BytesMut::from(&[1u8, 0, 1, 0, 0, 0, 16][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(WireError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn heartbeat_frame() {
        let mut codec = AmqpCodec::default();
        let mut buf = BytesMut::from(&[4u8, 0, 0, 0, 0, 0, 0, FRAME_END][..]);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.header.kind, FrameKind::Heartbeat);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn codec_roundtrip() {
        let mut codec = AmqpCodec::default();
        let frame = RawFrame::new(FrameKind::Method, 1, Bytes::from_static(b"abc"));
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf);
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn truncated_frame_needs_more() {
        let mut codec = AmqpCodec::default();
        let mut buf = BytesMut::from(&[1u8, 0, 1, 0, 0, 0, 3][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
