use crc32c::crc32c;

use super::header::RecordHeader;

pub fn crc32c_record(header: &RecordHeader, body: &[u8]) -> u32 {
    let mut buf = Vec::with_capacity(40 + body.len());
    encode_header_zero_crc(header, &mut buf);
    buf.extend_from_slice(body);
    crc32c(&buf)
}

fn encode_header_zero_crc(header: &RecordHeader, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&header.magic.to_le_bytes());
    dst.extend_from_slice(&header.version.to_le_bytes());
    dst.extend_from_slice(&header.kind.to_le_bytes());
    dst.extend_from_slice(&header.flags.to_le_bytes());
    dst.extend_from_slice(&header.header_len.to_le_bytes());
    dst.extend_from_slice(&header.body_len.to_le_bytes());
    dst.put_u32_zeroed();
    dst.extend_from_slice(&header.stream_id.to_le_bytes());
    dst.extend_from_slice(&header.base_seq.to_le_bytes());
    dst.extend_from_slice(&header.count.to_le_bytes());
}

trait PutZeroed {
    fn put_u32_zeroed(&mut self);
}

impl PutZeroed for Vec<u8> {
    fn put_u32_zeroed(&mut self) {
        self.extend_from_slice(&0u32.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::kind::RecordKind;
    use crate::record::flags::RecordFlags;

    #[test]
    fn crc_is_stable() {
        let hdr = RecordHeader::new(RecordKind::PayloadBatch, RecordFlags::HAS_CRC32C, 1, 0, 1, 4);
        let body = b"test";
        let c1 = crc32c_record(&hdr, body);
        let c2 = crc32c_record(&hdr, body);
        assert_eq!(c1, c2);
        assert_ne!(c1, crc32c_record(&hdr, b"other"));
    }
}
