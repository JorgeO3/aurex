use bytes::{BufMut, BytesMut};

use crate::codec::cursor::{Cursor, CursorError, write_u32_le, write_u64_le};
use crate::wire::NativeConsumerFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeStartBody {
    pub queue_id: u32,
    pub consumer_id_hint: u32,
    pub prefetch: u32,
    pub consumer_flags: NativeConsumerFlags,
}

impl ConsumeStartBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let queue_id = cur.read_u32_le()?;
        let consumer_id_hint = cur.read_u32_le()?;
        let prefetch = cur.read_u32_le()?;
        let flags_raw = cur.read_u16_le()?;
        let consumer_flags =
            NativeConsumerFlags::from_bits(flags_raw).ok_or(CursorError::InvalidLength)?;
        Ok(Self {
            queue_id,
            consumer_id_hint,
            prefetch,
            consumer_flags,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u32_le(dst, self.queue_id);
        write_u32_le(dst, self.consumer_id_hint);
        write_u32_le(dst, self.prefetch);
        dst.put_u16_le(self.consumer_flags.bits());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerOkBody {
    pub queue_id: u32,
    pub consumer_id: u64,
    pub effective_prefetch: u32,
}

impl ConsumerOkBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        Ok(Self {
            queue_id: cur.read_u32_le()?,
            consumer_id: cur.read_u64_le()?,
            effective_prefetch: cur.read_u32_le()?,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u32_le(dst, self.queue_id);
        write_u64_le(dst, self.consumer_id);
        write_u32_le(dst, self.effective_prefetch);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditUpdateBody {
    pub consumer_id: u64,
    pub credit_delta: u32,
    pub flags: crate::wire::CreditFlags,
}

impl CreditUpdateBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let consumer_id = cur.read_u64_le()?;
        let credit_delta = cur.read_u32_le()?;
        let flags_raw = cur.read_u16_le()?;
        let flags = crate::wire::CreditFlags::from_bits(flags_raw).ok_or(CursorError::InvalidLength)?;
        Ok(Self {
            consumer_id,
            credit_delta,
            flags,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u64_le(dst, self.consumer_id);
        write_u32_le(dst, self.credit_delta);
        dst.put_u16_le(self.flags.bits());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelConsumerBody {
    pub consumer_id: u64,
    pub cancel_disposition: u8,
}

impl CancelConsumerBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        Ok(Self {
            consumer_id: cur.read_u64_le()?,
            cancel_disposition: cur.read_u8()?,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u64_le(dst, self.consumer_id);
        dst.put_u8(self.cancel_disposition);
    }
}
