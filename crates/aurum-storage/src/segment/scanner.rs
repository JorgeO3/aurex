use crate::ids::{LogOffset, SegmentId};
use crate::record::codec::decode_record;
use crate::record::header::RECORD_HEADER_LEN;
use crate::record::kind::RecordKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRecord {
    pub segment_id: SegmentId,
    pub offset: LogOffset,
    pub kind: RecordKind,
    pub stream_id: u64,
    pub base_seq: u64,
    pub count: u32,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentScanResult {
    pub records: Vec<ScannedRecord>,
    pub valid_end: LogOffset,
    pub truncated_tail: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentScanError {
    CorruptRecord { offset: LogOffset },
    TruncatedTail,
}

pub fn scan_segment_bytes(segment_id: SegmentId, buf: &[u8]) -> Result<SegmentScanResult, SegmentScanError> {
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < buf.len() {
        if buf.len() - pos < usize::from(RECORD_HEADER_LEN) {
            return Ok(SegmentScanResult {
                records,
                valid_end: LogOffset(pos as u64),
                truncated_tail: true,
            });
        }
        let start = pos;
        match decode_record(&buf[pos..]) {
            Ok((hdr, body)) => {
                let wire_len = usize::from(RECORD_HEADER_LEN) + body.bytes.len();
                let kind = RecordKind::try_from(hdr.kind).map_err(|_| SegmentScanError::CorruptRecord {
                    offset: LogOffset(start as u64),
                })?;
                records.push(ScannedRecord {
                    segment_id,
                    offset: LogOffset(start as u64),
                    kind,
                    stream_id: hdr.stream_id,
                    base_seq: hdr.base_seq,
                    count: hdr.count,
                    body: body.bytes,
                });
                pos += wire_len;
            }
            Err(crate::record::codec::RecordDecodeError::Truncated)
            | Err(crate::record::codec::RecordDecodeError::BodyTooShort) => {
                return Ok(SegmentScanResult {
                    records,
                    valid_end: LogOffset(start as u64),
                    truncated_tail: true,
                });
            }
            Err(_) => {
                return Err(SegmentScanError::CorruptRecord {
                    offset: LogOffset(start as u64),
                });
            }
        }
    }
    Ok(SegmentScanResult {
        records,
        valid_end: LogOffset(pos as u64),
        truncated_tail: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::codec::encode_record;

    #[test]
    fn scans_multiple_records() {
        let mut buf = Vec::new();
        buf.extend(encode_record(RecordKind::PayloadBatch, 1, 0, 1, b"a").unwrap());
        buf.extend(encode_record(RecordKind::PayloadBatch, 1, 1, 1, b"b").unwrap());
        let result = scan_segment_bytes(SegmentId(1), &buf).unwrap();
        assert_eq!(result.records.len(), 2);
        assert!(!result.truncated_tail);
    }

    #[test]
    fn truncates_partial_tail() {
        let mut buf = encode_record(RecordKind::PayloadBatch, 1, 0, 1, b"a").unwrap();
        buf.truncate(buf.len() - 3);
        let result = scan_segment_bytes(SegmentId(1), &buf).unwrap();
        assert_eq!(result.records.len(), 0);
        assert!(result.truncated_tail);
    }
}
