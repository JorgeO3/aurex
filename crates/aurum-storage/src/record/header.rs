use crate::flags::RecordFlags;

pub const AURUM_RECORD_MAGIC: u32 = 0x4155_524D; // "AURM"
pub const RECORD_VERSION_V0: u16 = 0;
pub const RECORD_HEADER_LEN: u16 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub magic: u32,
    pub version: u16,
    pub kind: u16,
    pub flags: u16,
    pub header_len: u16,
    pub body_len: u32,
    pub record_crc32c: u32,
    pub stream_id: u64,
    pub base_seq: u64,
    pub count: u32,
    pub reserved: u32,
}

impl RecordHeader {
    #[must_use]
    pub fn new(
        kind: crate::record::kind::RecordKind,
        flags: RecordFlags,
        stream_id: u64,
        base_seq: u64,
        count: u32,
        body_len: u32,
    ) -> Self {
        Self {
            magic: AURUM_RECORD_MAGIC,
            version: RECORD_VERSION_V0,
            kind: kind.as_u16(),
            flags: flags.bits(),
            header_len: RECORD_HEADER_LEN,
            body_len,
            record_crc32c: 0,
            stream_id,
            base_seq,
            count,
            reserved: 0,
        }
    }
}
