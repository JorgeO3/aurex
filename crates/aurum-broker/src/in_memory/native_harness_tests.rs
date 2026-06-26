use bytes::{Bytes, BytesMut};

use aurum_protocol_native::{
    message::{HelloBody, ResolveRouteBody, RouteResolvedBody},
    NativeCapabilities, NativeCodec, NativeFrame, NativeFrameHeader, NativeOp, FrameFlags,
};

use crate::in_memory::NativeInMemoryHarness;

#[test]
fn native_harness_hello_roundtrip() {
    let mut harness = NativeInMemoryHarness::new();
    let mut body = BytesMut::new();
    HelloBody {
        client_major: 0,
        client_minor: 1,
        client_capabilities: NativeCapabilities::ROUTE_ID,
        client_name: b"test".to_vec(),
    }
    .encode(&mut body)
    .unwrap();
    let mut codec = NativeCodec::default();
    let mut req = BytesMut::new();
    codec
        .encode(
            &NativeFrame::new(
                NativeFrameHeader::new(NativeOp::Hello, FrameFlags::NONE, 0, 1, body.len() as u32),
                body.freeze(),
            ),
            &mut req,
        )
        .unwrap();
    let resp = harness.send_bytes(&req);
    let mut buf = BytesMut::from(resp.as_slice());
    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.op(), Some(NativeOp::HelloOk));
}

#[test]
fn native_harness_resolve_route_end_to_end() {
    let mut harness = NativeInMemoryHarness::with_orders_route();
    let mut body = BytesMut::new();
    ResolveRouteBody {
        route_table_version_hint: 1,
        exchange_id_hint: 0,
        exchange: b"orders".to_vec(),
        routing_key: b"created".to_vec(),
    }
    .encode(&mut body)
    .unwrap();
    let mut codec = NativeCodec::default();
    let mut req = BytesMut::new();
    codec
        .encode(
            &NativeFrame::new(
                NativeFrameHeader::new(
                    NativeOp::ResolveRoute,
                    FrameFlags::NONE,
                    0,
                    42,
                    body.len() as u32,
                ),
                body.freeze(),
            ),
            &mut req,
        )
        .unwrap();
    let resp = harness.send_bytes(&req);
    let mut buf = BytesMut::from(resp.as_slice());
    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.op(), Some(NativeOp::RouteResolved));
    let resolved = RouteResolvedBody::decode(&frame.body).unwrap();
    assert_eq!(resolved.route_table_version, 1);
}
