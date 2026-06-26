use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::cursor::{Cursor, CursorError, write_u32_le, write_u64_le};
use crate::wire::{NativeMessageFlags, MAX_PAYLOAD_SIZE, MAX_PUBLISH_BATCH_MESSAGES};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishDescriptor {
    pub payload_offset: u32,
    pub payload_len: u32,
    pub message_flags: NativeMessageFlags,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatchBody {
    pub route_table_version: u64,
    pub route_id_packed: u64,
    pub batch_flags: u32,
    pub descriptors: Vec<PublishDescriptor>,
    pub payloads: Bytes,
}

impl PublishBatchBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let route_table_version = cur.read_u64_le()?;
        let route_id_packed = cur.read_u64_le()?;
        let batch_flags = cur.read_u32_le()?;
        let count = cur.read_u32_le()?;
        let descriptor_table_len = cur.read_u32_le()?;
        if count == 0 || count > MAX_PUBLISH_BATCH_MESSAGES {
            return Err(CursorError::InvalidLength);
        }
        let expected_desc = count.checked_mul(12).ok_or(CursorError::Overflow)?;
        if descriptor_table_len != expected_desc {
            return Err(CursorError::InvalidLength);
        }
        let mut descriptors = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let payload_offset = cur.read_u32_le()?;
            let payload_len = cur.read_u32_le()?;
            let flags_raw = cur.read_u16_le()?;
            let _reserved = cur.read_u16_le()?;
            if payload_len > MAX_PAYLOAD_SIZE {
                return Err(CursorError::InvalidLength);
            }
            cur.check_bounds(payload_offset, payload_len)?;
            let message_flags =
                NativeMessageFlags::from_bits(flags_raw).ok_or(CursorError::InvalidLength)?;
            descriptors.push(PublishDescriptor {
                payload_offset,
                payload_len,
                message_flags,
            });
        }
        let payload_start = cur.position();
        let payloads = Bytes::copy_from_slice(&body[payload_start..]);
        Ok(Self {
            route_table_version,
            route_id_packed,
            batch_flags,
            descriptors,
            payloads,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        let count = self.descriptors.len() as u32;
        if count == 0 || count > MAX_PUBLISH_BATCH_MESSAGES {
            return Err(CursorError::InvalidLength);
        }
        write_u64_le(dst, self.route_table_version);
        write_u64_le(dst, self.route_id_packed);
        write_u32_le(dst, self.batch_flags);
        write_u32_le(dst, count);
        write_u32_le(dst, count * 12);
        for d in &self.descriptors {
            if d.payload_len > MAX_PAYLOAD_SIZE {
                return Err(CursorError::InvalidLength);
            }
            write_u32_le(dst, d.payload_offset);
            write_u32_le(dst, d.payload_len);
            dst.put_u16_le(d.message_flags.bits());
            dst.put_u16_le(0);
        }
        dst.put_slice(&self.payloads);
        Ok(())
    }

    pub fn payload_slice(&self, d: &PublishDescriptor) -> Result<&[u8], CursorError> {
        let start = d.payload_offset as usize;
        let end = start
            .checked_add(d.payload_len as usize)
            .ok_or(CursorError::Overflow)?;
        self.payloads
            .get(start..end)
            .ok_or(CursorError::OffsetOutOfBounds)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishConfirmBatchBody {
    pub accepted_count: u32,
    pub failed_count: u32,
}

impl PublishConfirmBatchBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        Ok(Self {
            accepted_count: cur.read_u32_le()?,
            failed_count: cur.read_u32_le()?,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u32_le(dst, self.accepted_count);
        write_u32_le(dst, self.failed_count);
    }
}
