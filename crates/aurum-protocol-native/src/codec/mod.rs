use bytes::Buf;

pub mod cursor;

use bytes::{Bytes, BytesMut};

use crate::wire::{NativeFrameHeader, NativeHeaderError, NativeOp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeFrame {
    pub header: NativeFrameHeader,
    pub body: Bytes,
}

impl NativeFrame {
    #[must_use]
    pub fn new(header: NativeFrameHeader, body: Bytes) -> Self {
        Self { header, body }
    }

    #[must_use]
    pub fn op(&self) -> Option<NativeOp> {
        self.header.op().ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeDecodeError {
    Truncated,
    Header(NativeHeaderError),
    BodyTooLarge,
    FrameTooLarge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEncodeError {
    BodyTooLarge,
}

#[derive(Debug, Clone)]
pub struct NativeCodec {
    max_frame_len: usize,
}

impl Default for NativeCodec {
    fn default() -> Self {
        Self::new(crate::wire::DEFAULT_MAX_FRAME_LEN)
    }
}

impl NativeCodec {
    #[must_use]
    pub fn new(max_frame_len: usize) -> Self {
        Self { max_frame_len }
    }

    pub fn decode(&self, src: &mut BytesMut) -> Result<Option<NativeFrame>, NativeDecodeError> {
        if src.len() < usize::from(crate::wire::NATIVE_HEADER_LEN) {
            return Ok(None);
        }
        let header = NativeFrameHeader::decode(src).map_err(NativeDecodeError::Header)?;
        let total = usize::from(crate::wire::NATIVE_HEADER_LEN) + header.body_len as usize;
        if total > self.max_frame_len {
            return Err(NativeDecodeError::FrameTooLarge);
        }
        if src.len() < total {
            return Ok(None);
        }
        src.advance(usize::from(crate::wire::NATIVE_HEADER_LEN));
        let body = src.split_to(header.body_len as usize).freeze();
        Ok(Some(NativeFrame { header, body }))
    }

    pub fn encode(&self, frame: &NativeFrame, dst: &mut BytesMut) -> Result<(), NativeEncodeError> {
        let total = usize::from(crate::wire::NATIVE_HEADER_LEN) + frame.body.len();
        if total > self.max_frame_len {
            return Err(NativeEncodeError::BodyTooLarge);
        }
        let mut header = frame.header;
        header.body_len = frame.body.len() as u32;
        header.encode(dst);
        dst.extend_from_slice(&frame.body);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{FrameFlags, NATIVE_HEADER_LEN};

    #[test]
    fn heartbeat_roundtrip() {
        let mut codec = NativeCodec::default();
        let frame = NativeFrame::new(
            NativeFrameHeader::new(NativeOp::Heartbeat, FrameFlags::NONE, 0, 7, 0),
            Bytes::new(),
        );
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.correlation_id, 7);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_frame_waits() {
        let mut codec = NativeCodec::default();
        let frame = NativeFrame::new(
            NativeFrameHeader::new(NativeOp::Heartbeat, FrameFlags::NONE, 0, 0, 0),
            Bytes::new(),
        );
        let mut buf = BytesMut::new();
        codec.encode(&frame, &mut buf).unwrap();
        let mut partial = buf.split_to(NATIVE_HEADER_LEN as usize - 1);
        assert!(codec.decode(&mut partial).unwrap().is_none());
    }

    #[test]
    fn two_frames_in_one_buffer() {
        let mut codec = NativeCodec::default();
        let f1 = NativeFrame::new(
            NativeFrameHeader::new(NativeOp::Heartbeat, FrameFlags::NONE, 0, 1, 0),
            Bytes::new(),
        );
        let f2 = NativeFrame::new(
            NativeFrameHeader::new(NativeOp::HeartbeatAck, FrameFlags::RESPONSE, 0, 1, 0),
            Bytes::new(),
        );
        let mut buf = BytesMut::new();
        codec.encode(&f1, &mut buf).unwrap();
        codec.encode(&f2, &mut buf).unwrap();
        let d1 = codec.decode(&mut buf).unwrap().unwrap();
        let d2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(d1.header.op, NativeOp::Heartbeat.as_u16());
        assert_eq!(d2.header.op, NativeOp::HeartbeatAck.as_u16());
    }
}
