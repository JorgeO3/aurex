use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::cursor::{Cursor, CursorError, write_u32_le, write_u64_le};
use crate::wire::NativeDeliveryFlags;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryDescriptor {
    pub delivery_tag: u64,
    pub payload_offset: u32,
    pub payload_len: u32,
    pub delivery_flags: NativeDeliveryFlags,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryBatchBody {
    pub consumer_id: u64,
    pub descriptors: Vec<DeliveryDescriptor>,
    pub payloads: Bytes,
}

impl DeliveryBatchBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let consumer_id = cur.read_u64_le()?;
        let count = cur.read_u32_le()?;
        let descriptor_table_len = cur.read_u32_le()?;
        let expected = count.checked_mul(20).ok_or(CursorError::Overflow)?;
        if descriptor_table_len != expected || count == 0 {
            return Err(CursorError::InvalidLength);
        }
        let mut descriptors = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let delivery_tag = cur.read_u64_le()?;
            let payload_offset = cur.read_u32_le()?;
            let payload_len = cur.read_u32_le()?;
            let flags_raw = cur.read_u16_le()?;
            let _reserved = cur.read_u16_le()?;
            cur.check_bounds(payload_offset, payload_len)?;
            let delivery_flags =
                NativeDeliveryFlags::from_bits(flags_raw).ok_or(CursorError::InvalidLength)?;
            descriptors.push(DeliveryDescriptor {
                delivery_tag,
                payload_offset,
                payload_len,
                delivery_flags,
            });
        }
        let payload_start = cur.position();
        let payloads = Bytes::copy_from_slice(&body[payload_start..]);
        Ok(Self {
            consumer_id,
            descriptors,
            payloads,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        let count = self.descriptors.len() as u32;
        if count == 0 {
            return Err(CursorError::InvalidLength);
        }
        write_u64_le(dst, self.consumer_id);
        write_u32_le(dst, count);
        write_u32_le(dst, count * 20);
        for d in &self.descriptors {
            write_u64_le(dst, d.delivery_tag);
            write_u32_le(dst, d.payload_offset);
            write_u32_le(dst, d.payload_len);
            dst.put_u16_le(d.delivery_flags.bits());
            dst.put_u16_le(0);
        }
        dst.put_slice(&self.payloads);
        Ok(())
    }
}
