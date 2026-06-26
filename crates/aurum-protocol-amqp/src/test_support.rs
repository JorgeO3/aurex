use bytes::BytesMut;

use crate::method::AmqpMethod;
use crate::wire::constants::DEFAULT_FRAME_MAX;
use crate::wire::frame::{FrameKind, RawFrame};
use crate::wire::{AmqpCodec, WireError};

/// Decode every AMQP frame from a wire buffer (for integration tests).
pub fn decode_all_frames(bytes: &[u8]) -> Result<Vec<RawFrame>, WireError> {
    let mut codec = AmqpCodec::new(DEFAULT_FRAME_MAX);
    let mut buf = BytesMut::from(bytes);
    let mut frames = Vec::new();
    while let Some(frame) = codec.decode(&mut buf)? {
        frames.push(frame);
    }
    if !buf.is_empty() {
        return Err(WireError::NeedMore);
    }
    Ok(frames)
}

/// Decode the AMQP method from a method frame payload.
pub fn decode_method_frame(frame: &RawFrame) -> Result<AmqpMethod, WireError> {
    if frame.header.kind != FrameKind::Method {
        return Err(WireError::UnknownFrameType);
    }
    let mut buf = frame.payload.as_ref();
    if buf.len() < 4 {
        return Err(WireError::NeedMore);
    }
    let class_id = u16::from_be_bytes(buf[..2].try_into().expect("class"));
    buf = &buf[2..];
    let method_id = u16::from_be_bytes(buf[..2].try_into().expect("method"));
    buf = &buf[2..];
    crate::method::decode_method(class_id, method_id, buf)
}

/// Collect method frames from a wire buffer.
pub fn decode_methods(bytes: &[u8]) -> Result<Vec<AmqpMethod>, WireError> {
    decode_all_frames(bytes)?
        .iter()
        .filter(|f| f.header.kind == FrameKind::Method)
        .map(decode_method_frame)
        .collect()
}
