use aurum_protocol_amqp::{
    method::{AmqpMethod, BasicMethod, ChannelMethod},
    test_support::{decode_all_frames, decode_method_frame, decode_methods},
    wire::frame::{FrameKind, RawFrame},
    ShortStr,
};
use bytes::Bytes;

use super::amqp_transcript::*;

#[test]
fn publish_deliver_ack_transcript() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);

    let body = b"hello-amqp";
    let deliver_resp = h.send_bytes(&publish_frames(1, "orders", "created", body));
    let frames = decode_all_frames(&deliver_resp).expect("deliver frames");
    assert!(frames.len() >= 3, "expected deliver + header + body");

    let deliver = decode_method_frame(&frames[0]).unwrap();
    let AmqpMethod::Basic(BasicMethod::Deliver(d)) = deliver else {
        panic!("expected basic.deliver, got {deliver:?}");
    };
    assert_eq!(d.consumer_tag.as_bytes(), b"ctag-1");
    assert_eq!(d.delivery_tag, 1);
    assert_eq!(d.exchange.as_bytes(), b"orders");
    assert_eq!(d.routing_key.as_bytes(), b"created");
    assert!(!d.redelivered);

    assert_eq!(frames[1].header.kind, FrameKind::Header);
    assert_eq!(frames[2].header.kind, FrameKind::Body);
    assert_eq!(&frames[2].payload[..], body);

    let ack_resp = h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Ack(aurum_protocol_amqp::method::BasicAck {
            delivery_tag: 1,
            multiple: false,
        })),
    ));
    assert!(ack_resp.is_empty(), "ack should not produce frames");
}

#[test]
fn fragmented_body_respects_frame_max() {
    let mut h = super::AmqpInMemoryHarness::new();
    handshake_with_frame_max(&mut h, 64);
    channel_open(&mut h, 1);
    declare_bind_consume(&mut h, 1, "ctag-1");

    let body = vec![b'x'; 100];
    let deliver_resp = h.send_bytes(&publish_frames_with_max(1, "orders", "created", &body, 64));
    let frames = decode_all_frames(&deliver_resp).expect("fragmented deliver");
    assert!(!frames.is_empty());
    let body_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.header.kind == FrameKind::Body)
        .collect();
    assert!(body_frames.len() > 1);
    let reassembled: Vec<u8> = body_frames
        .iter()
        .flat_map(|f| f.payload.iter().copied())
        .collect();
    assert_eq!(reassembled, body);
}

#[test]
fn nack_requeue_redelivers() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);
    h.send_bytes(&publish_frames(1, "orders", "created", b"retry-me"));
    let redeliver_resp = h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Nack(aurum_protocol_amqp::method::BasicNack {
            delivery_tag: 1,
            multiple: false,
            requeue: true,
        })),
    ));
    let frames = decode_all_frames(&redeliver_resp).expect("redeliver");
    let deliver = decode_method_frame(&frames[0]).unwrap();
    let AmqpMethod::Basic(BasicMethod::Deliver(d)) = deliver else {
        panic!("expected basic.deliver");
    };
    assert!(d.redelivered);
}

#[test]
fn invalid_ack_tag_channel_close() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);
    h.send_bytes(&publish_frames(1, "orders", "created", b"x"));
    let resp = h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Ack(aurum_protocol_amqp::method::BasicAck {
            delivery_tag: 99,
            multiple: false,
        })),
    ));
    let methods = decode_methods(&resp).expect("channel close");
    assert!(methods.iter().any(|m| matches!(m, AmqpMethod::Channel(ChannelMethod::Close(_)))));
}

#[test]
fn basic_cancel_ok() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);
    let resp = h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Cancel(aurum_protocol_amqp::method::BasicCancel {
            consumer_tag: ShortStr::from("ctag-1"),
            nowait: false,
        })),
    ));
    let methods = decode_methods(&resp).expect("cancel-ok");
    assert!(matches!(methods[0], AmqpMethod::Basic(BasicMethod::CancelOk)));
}

#[test]
fn method_before_connection_open_returns_close() {
    let mut h = super::AmqpInMemoryHarness::new();
    use aurum_protocol_amqp::{method::ConnectionMethod, wire::constants::PROTOCOL_HEADER};
    h.send_bytes(PROTOCOL_HEADER);
    h.send_bytes(&client_start_ok());
    let resp = h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Channel(ChannelMethod::Open(
            aurum_protocol_amqp::method::ChannelOpen {
                reserved: ShortStr::from(""),
            },
        )),
    ));
    let methods = decode_methods(&resp).expect("connection close");
    assert!(methods
        .iter()
        .any(|m| matches!(m, AmqpMethod::Connection(ConnectionMethod::Close(_)))));
}

#[test]
fn zero_byte_publish_delivers() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);
    let resp = h.send_bytes(&publish_frames(1, "orders", "created", b""));
    let frames = decode_all_frames(&resp).expect("deliver empty");
    assert!(matches!(
        decode_method_frame(&frames[0]).unwrap(),
        AmqpMethod::Basic(BasicMethod::Deliver(_))
    ));
    let body_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.header.kind == FrameKind::Body)
        .collect();
    assert!(body_frames.is_empty() || body_frames[0].payload.is_empty());
}

#[test]
fn second_publish_uses_route_cache() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);
    h.send_bytes(&publish_frames(1, "orders", "created", b"one"));
    let resp = h.send_bytes(&publish_frames(1, "orders", "created", b"two"));
    let frames = decode_all_frames(&resp).expect("second deliver");
    let AmqpMethod::Basic(BasicMethod::Deliver(d)) = decode_method_frame(&frames[0]).unwrap() else {
        panic!("expected deliver");
    };
    assert_eq!(d.delivery_tag, 2);
}

#[test]
fn heartbeat_accepted_when_open() {
    let mut h = super::AmqpInMemoryHarness::new();
    setup_queue(&mut h, 1);
    let mut hb = bytes::BytesMut::new();
    RawFrame::new(FrameKind::Heartbeat, 0, Bytes::new()).encode(&mut hb);
    assert!(h.send_bytes(&hb).is_empty());
}

#[test]
fn multi_channel_consume() {
    let mut h = super::AmqpInMemoryHarness::new();
    handshake(&mut h);
    channel_open(&mut h, 1);
    channel_open(&mut h, 2);
    declare_bind_consume(&mut h, 1, "ctag-1");
    declare_bind_consume(&mut h, 2, "ctag-2");
    let resp = h.send_bytes(&publish_frames(1, "orders", "created", b"mc"));
    assert!(!decode_all_frames(&resp).expect("deliver").is_empty());
}
