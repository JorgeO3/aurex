use crate::record::codec::{decode_record, encode_record};
use crate::record::kind::RecordKind;

#[test]
fn record_module_tests() {
    let body = b"batch-body";
    let wire = encode_record(RecordKind::QueueIndexBatch, 99, 10, 2, body).unwrap();
    let (hdr, decoded) = decode_record(&wire).unwrap();
    assert_eq!(hdr.stream_id, 99);
    assert_eq!(hdr.base_seq, 10);
    assert_eq!(decoded.bytes, body);
}
