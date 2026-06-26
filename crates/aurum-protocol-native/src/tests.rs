use bytes::BytesMut;

use crate::message::{AckBatchBody, HelloBody, NativeAckOp, ResolveRouteBody};
use crate::wire::{FrameFlags, NativeCapabilities, NativeFrameHeader, NativeOp};
use crate::{NativeCodec, NativeFrame};

#[test]
fn resolve_route_body_roundtrip() {
    let body = ResolveRouteBody {
        route_table_version_hint: 1,
        exchange_id_hint: 0,
        exchange: b"orders".to_vec(),
        routing_key: b"created".to_vec(),
    };
    let mut buf = BytesMut::new();
    body.encode(&mut buf).unwrap();
    let decoded = ResolveRouteBody::decode(&buf).unwrap();
    assert_eq!(decoded, body);
}

#[test]
fn ack_range_body_roundtrip() {
    let body = AckBatchBody {
        consumer_id: 7,
        flags: 0,
        ops: vec![NativeAckOp::Range {
            first_tag: 10,
            len: 4,
        }],
    };
    let mut buf = BytesMut::new();
    body.encode(&mut buf).unwrap();
    let decoded = AckBatchBody::decode(&buf).unwrap();
    assert_eq!(decoded.ops.len(), 1);
}

#[test]
fn hello_body_roundtrip() {
    let body = HelloBody {
        client_major: 0,
        client_minor: 1,
        client_capabilities: NativeCapabilities::PUBLISH_BATCH,
        client_name: b"aurum".to_vec(),
    };
    let mut buf = BytesMut::new();
    body.encode(&mut buf).unwrap();
    let decoded = HelloBody::decode(&buf).unwrap();
    assert_eq!(decoded.client_name, b"aurum");
}

#[test]
fn rejects_oversized_frame() {
    let codec = NativeCodec::new(64);
    let mut body = BytesMut::new();
    HelloBody {
        client_major: 0,
        client_minor: 1,
        client_capabilities: NativeCapabilities::empty(),
        client_name: vec![0u8; 100],
    }
    .encode(&mut body)
    .unwrap();
    let frame = NativeFrame::new(
        NativeFrameHeader::new(NativeOp::Hello, FrameFlags::NONE, 0, 0, body.len() as u32),
        body.freeze(),
    );
    let mut buf = BytesMut::new();
    assert!(codec.encode(&frame, &mut buf).is_err());
}
