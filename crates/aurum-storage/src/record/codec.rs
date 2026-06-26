use super::checksum::crc32c_record;
use super::flags::RecordFlags;
use super::header::{RecordHeader, AURUM_RECORD_MAGIC, RECORD_HEADER_LEN, RECORD_VERSION_V0};
use super::kind::{RecordKind, RecordKindError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBody {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordDecodeError {
    Truncated,
    BadMagic,
    UnsupportedVersion,
    HeaderTooSmall,
    InvalidFlags,
    UnknownKind(RecordKindError),
    BodyTooShort,
    CrcMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordEncodeError {
    BodyTooLarge,
}

pub fn encode_record(
    kind: RecordKind,
    stream_id: u64,
    base_seq: u64,
    count: u32,
    body: &[u8],
) -> Result<Vec<u8>, RecordEncodeError> {
    let mut header = RecordHeader::new(
        kind,
        RecordFlags::HAS_CRC32C | RecordFlags::BATCHED,
        stream_id,
        base_seq,
        count,
        body.len() as u32,
    );
    header.record_crc32c = crc32c_record(&header, body);
    let mut out = Vec::with_capacity(usize::from(RECORD_HEADER_LEN) + body.len());
    write_header(&header, &mut out);
    out.extend_from_slice(body);
    Ok(out)
}

pub fn decode_record(buf: &[u8]) -> Result<(RecordHeader, RecordBody), RecordDecodeError> {
    if buf.len() < usize::from(RECORD_HEADER_LEN) {
        return Err(RecordDecodeError::Truncated);
    }
    let header = read_header(&buf[..usize::from(RECORD_HEADER_LEN)])?;
    let total = usize::from(RECORD_HEADER_LEN) + header.body_len as usize;
    if buf.len() < total {
        return Err(RecordDecodeError::BodyTooShort);
    }
    let body_bytes = &buf[usize::from(RECORD_HEADER_LEN)..total];
    let mut zero_crc = header;
    zero_crc.record_crc32c = 0;
    let expected = crc32c_record(&zero_crc, body_bytes);
    if header.record_crc32c != expected {
        return Err(RecordDecodeError::CrcMismatch);
    }
    Ok((
        header,
        RecordBody {
            bytes: body_bytes.to_vec(),
        },
    ))
}

fn read_header(buf: &[u8]) -> Result<RecordHeader, RecordDecodeError> {
    let magic = u32::from_le_bytes(buf[0..4].try_into().expect("magic"));
    if magic != AURUM_RECORD_MAGIC {
        return Err(RecordDecodeError::BadMagic);
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().expect("version"));
    if version != RECORD_VERSION_V0 {
        return Err(RecordDecodeError::UnsupportedVersion);
    }
    let kind = u16::from_le_bytes(buf[6..8].try_into().expect("kind"));
    let flags = u16::from_le_bytes(buf[8..10].try_into().expect("flags"));
    let header_len = u16::from_le_bytes(buf[10..12].try_into().expect("header_len"));
    if header_len < RECORD_HEADER_LEN {
        return Err(RecordDecodeError::HeaderTooSmall);
    }
    let body_len = u32::from_le_bytes(buf[12..16].try_into().expect("body_len"));
    let record_crc32c = u32::from_le_bytes(buf[16..20].try_into().expect("crc"));
    let stream_id = u64::from_le_bytes(buf[20..28].try_into().expect("stream"));
    let base_seq = u64::from_le_bytes(buf[28..36].try_into().expect("base"));
    let count = u32::from_le_bytes(buf[36..40].try_into().expect("count"));
    let _ = RecordKind::try_from(kind).map_err(RecordDecodeError::UnknownKind)?;
    let _ = RecordFlags::from_bits(flags).ok_or(RecordDecodeError::InvalidFlags)?;
    Ok(RecordHeader {
        magic,
        version,
        kind,
        flags,
        header_len,
        body_len,
        record_crc32c,
        stream_id,
        base_seq,
        count,
        reserved: 0,
    })
}

fn write_header(header: &RecordHeader, dst: &mut Vec<u8>) {
    dst.extend_from_slice(&header.magic.to_le_bytes());
    dst.extend_from_slice(&header.version.to_le_bytes());
    dst.extend_from_slice(&header.kind.to_le_bytes());
    dst.extend_from_slice(&header.flags.to_le_bytes());
    dst.extend_from_slice(&header.header_len.to_le_bytes());
    dst.extend_from_slice(&header.body_len.to_le_bytes());
    dst.extend_from_slice(&header.record_crc32c.to_le_bytes());
    dst.extend_from_slice(&header.stream_id.to_le_bytes());
    dst.extend_from_slice(&header.base_seq.to_le_bytes());
    dst.extend_from_slice(&header.count.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let body = b"hello-payload";
        let wire = encode_record(RecordKind::PayloadBatch, 1, 0, 1, body).unwrap();
        let (hdr, decoded) = decode_record(&wire).unwrap();
        assert_eq!(hdr.kind, RecordKind::PayloadBatch.as_u16());
        assert_eq!(decoded.bytes, body);
    }

    #[test]
    fn rejects_bad_magic() {
        let wire = encode_record(RecordKind::PayloadBatch, 1, 0, 1, b"x").unwrap();
        let mut bad = wire;
        bad[0] = 0;
        assert!(matches!(decode_record(&bad), Err(RecordDecodeError::BadMagic)));
    }

    #[test]
    fn rejects_crc_mismatch() {
        let mut wire = encode_record(RecordKind::PayloadBatch, 1, 0, 1, b"x").unwrap();
        let idx = wire.len() - 1;
        wire[idx] ^= 0xFF;
        assert!(matches!(decode_record(&wire), Err(RecordDecodeError::CrcMismatch)));
    }
}
