use bytes::{BufMut, BytesMut};

use crate::codec::cursor::{Cursor, CursorError, write_u16_le, write_u32_le, write_u64_le};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NativeAckOpKind {
    One = 1,
    Range = 2,
    MultipleUpTo = 3,
}

impl TryFrom<u8> for NativeAckOpKind {
    type Error = CursorError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::One),
            2 => Ok(Self::Range),
            3 => Ok(Self::MultipleUpTo),
            _ => Err(CursorError::InvalidLength),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeAckOp {
    One { tag: u64 },
    Range { first_tag: u64, len: u32 },
    MultipleUpTo { tag: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckBatchBody {
    pub consumer_id: u64,
    pub flags: u16,
    pub ops: Vec<NativeAckOp>,
}

impl AckBatchBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let consumer_id = cur.read_u64_le()?;
        let op_count = cur.read_u32_le()?;
        let flags = cur.read_u16_le()?;
        let mut ops = Vec::with_capacity(op_count as usize);
        for _ in 0..op_count {
            let kind = NativeAckOpKind::try_from(cur.read_u8()?)?;
            let _ = cur.read_u8()?;
            let _ = cur.read_u16_le()?;
            let tag = cur.read_u64_le()?;
            let len_or_zero = cur.read_u32_le()?;
            let _ = cur.read_u32_le()?;
            let op = match kind {
                NativeAckOpKind::One => NativeAckOp::One { tag },
                NativeAckOpKind::Range => NativeAckOp::Range {
                    first_tag: tag,
                    len: len_or_zero,
                },
                NativeAckOpKind::MultipleUpTo => NativeAckOp::MultipleUpTo { tag },
            };
            ops.push(op);
        }
        Ok(Self {
            consumer_id,
            flags,
            ops,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        write_u64_le(dst, self.consumer_id);
        write_u32_le(dst, self.ops.len() as u32);
        dst.put_u16_le(self.flags);
        for op in &self.ops {
            match *op {
                NativeAckOp::One { tag } => {
                    dst.put_u8(NativeAckOpKind::One as u8);
                    dst.put_u8(0);
                    dst.put_u16_le(0);
                    write_u64_le(dst, tag);
                    write_u32_le(dst, 0);
                    write_u32_le(dst, 0);
                }
                NativeAckOp::Range { first_tag, len } => {
                    dst.put_u8(NativeAckOpKind::Range as u8);
                    dst.put_u8(0);
                    dst.put_u16_le(0);
                    write_u64_le(dst, first_tag);
                    write_u32_le(dst, len);
                    write_u32_le(dst, 0);
                }
                NativeAckOp::MultipleUpTo { tag } => {
                    dst.put_u8(NativeAckOpKind::MultipleUpTo as u8);
                    dst.put_u8(0);
                    dst.put_u16_le(0);
                    write_u64_le(dst, tag);
                    write_u32_le(dst, 0);
                    write_u32_le(dst, 0);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NativeNackDisposition {
    Requeue = 1,
    Drop = 2,
    DeadLetter = 3,
}

impl TryFrom<u8> for NativeNackDisposition {
    type Error = CursorError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Requeue),
            2 => Ok(Self::Drop),
            3 => Ok(Self::DeadLetter),
            _ => Err(CursorError::InvalidLength),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeNackOp {
    One { tag: u64, disposition: NativeNackDisposition },
    Range {
        first_tag: u64,
        len: u32,
        disposition: NativeNackDisposition,
    },
    MultipleUpTo {
        tag: u64,
        disposition: NativeNackDisposition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackBatchBody {
    pub consumer_id: u64,
    pub flags: u16,
    pub ops: Vec<NativeNackOp>,
}

impl NackBatchBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let consumer_id = cur.read_u64_le()?;
        let op_count = cur.read_u32_le()?;
        let flags = cur.read_u16_le()?;
        let mut ops = Vec::with_capacity(op_count as usize);
        for _ in 0..op_count {
            let kind = NativeAckOpKind::try_from(cur.read_u8()?)?;
            let disposition = NativeNackDisposition::try_from(cur.read_u8()?)?;
            let _ = cur.read_u16_le()?;
            let tag = cur.read_u64_le()?;
            let len_or_zero = cur.read_u32_le()?;
            let _ = cur.read_u32_le()?;
            let op = match kind {
                NativeAckOpKind::One => NativeNackOp::One { tag, disposition },
                NativeAckOpKind::Range => NativeNackOp::Range {
                    first_tag: tag,
                    len: len_or_zero,
                    disposition,
                },
                NativeAckOpKind::MultipleUpTo => NativeNackOp::MultipleUpTo { tag, disposition },
            };
            ops.push(op);
        }
        Ok(Self {
            consumer_id,
            flags,
            ops,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        write_u64_le(dst, self.consumer_id);
        write_u32_le(dst, self.ops.len() as u32);
        dst.put_u16_le(self.flags);
        for op in &self.ops {
            match *op {
                NativeNackOp::One { tag, disposition } => {
                    dst.put_u8(NativeAckOpKind::One as u8);
                    dst.put_u8(disposition as u8);
                    dst.put_u16_le(0);
                    write_u64_le(dst, tag);
                    write_u32_le(dst, 0);
                    write_u32_le(dst, 0);
                }
                NativeNackOp::Range {
                    first_tag,
                    len,
                    disposition,
                } => {
                    dst.put_u8(NativeAckOpKind::Range as u8);
                    dst.put_u8(disposition as u8);
                    dst.put_u16_le(0);
                    write_u64_le(dst, first_tag);
                    write_u32_le(dst, len);
                    write_u32_le(dst, 0);
                }
                NativeNackOp::MultipleUpTo { tag, disposition } => {
                    dst.put_u8(NativeAckOpKind::MultipleUpTo as u8);
                    dst.put_u8(disposition as u8);
                    dst.put_u16_le(0);
                    write_u64_le(dst, tag);
                    write_u32_le(dst, 0);
                    write_u32_le(dst, 0);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementResultBatchBody {
    pub consumer_id: u64,
    pub settled: u32,
    pub kind: u8,
}

impl SettlementResultBatchBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        Ok(Self {
            consumer_id: cur.read_u64_le()?,
            settled: cur.read_u32_le()?,
            kind: cur.read_u8()?,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        write_u64_le(dst, self.consumer_id);
        write_u32_le(dst, self.settled);
        dst.put_u8(self.kind);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorBody {
    pub error_code: u16,
    pub correlation_id: u64,
    pub message: Vec<u8>,
}

impl ErrorBody {
    pub fn decode(body: &[u8]) -> Result<Self, CursorError> {
        let mut cur = Cursor::new(body);
        let error_code = cur.read_u16_le()?;
        let message_len = cur.read_u16_le()? as usize;
        let correlation_id = cur.read_u64_le()?;
        if message_len > crate::wire::MAX_ERROR_MESSAGE_LEN {
            return Err(CursorError::StringTooLong {
                max: crate::wire::MAX_ERROR_MESSAGE_LEN,
            });
        }
        let message = cur.read_bytes(message_len)?.to_vec();
        Ok(Self {
            error_code,
            correlation_id,
            message,
        })
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<(), CursorError> {
        if self.message.len() > crate::wire::MAX_ERROR_MESSAGE_LEN {
            return Err(CursorError::StringTooLong {
                max: crate::wire::MAX_ERROR_MESSAGE_LEN,
            });
        }
        write_u16_le(dst, self.error_code);
        write_u16_le(dst, self.message.len() as u16);
        write_u64_le(dst, self.correlation_id);
        dst.put_slice(&self.message);
        Ok(())
    }
}
