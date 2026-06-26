//! Shared AMQP transcript helpers for integration tests and h8 workloads.

use aurum_protocol_amqp::{
    method::{
        encode_method, method_class_id, method_id, AmqpMethod, BasicMethod, ChannelMethod,
        ConnectionMethod, ExchangeMethod, QueueMethod,
    },
    test_support::{decode_all_frames, decode_method_frame, decode_methods},
    wire::constants::{CLASS_BASIC, PROTOCOL_HEADER},
    wire::frame::{FrameKind, RawFrame},
    wire::properties::{BasicProperties, ContentHeader},
    ShortStr,
};
use bytes::{BufMut, Bytes, BytesMut};

use super::AmqpInMemoryHarness;

pub fn encode_method_frame(channel: u16, method: AmqpMethod) -> Vec<u8> {
    let class_id = method_class_id(&method);
    let mid = method_id(&method);
    let mut payload = BytesMut::new();
    payload.put_u16(class_id);
    payload.put_u16(mid);
    encode_method(&method, &mut payload).unwrap();
    let frame = RawFrame::new(FrameKind::Method, channel, payload.freeze());
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    buf.to_vec()
}

pub fn handshake(h: &mut AmqpInMemoryHarness) {
    handshake_with_frame_max(h, 131_072);
}

pub fn handshake_with_frame_max(h: &mut AmqpInMemoryHarness, frame_max: u32) {
    let r1 = h.send_bytes(PROTOCOL_HEADER);
    let frames = decode_all_frames(&r1).expect("connection.start");
    assert!(!frames.is_empty());

    let r2 = h.send_bytes(&client_start_ok());
    let tune = decode_methods(&r2).expect("connection.tune");
    assert!(matches!(tune[0], AmqpMethod::Connection(ConnectionMethod::Tune(_))));

    let mut open = client_tune_ok(frame_max);
    open.extend(client_connection_open());
    let r3 = h.send_bytes(&open);
    let methods = decode_methods(&r3).expect("open-ok");
    assert!(matches!(
        methods.last(),
        Some(AmqpMethod::Connection(ConnectionMethod::OpenOk))
    ));
}

pub fn client_start_ok() -> Vec<u8> {
    encode_method_frame(
        0,
        AmqpMethod::Connection(ConnectionMethod::StartOk(
            aurum_protocol_amqp::method::ConnectionStartOk {
                client_properties: Default::default(),
                mechanism: ShortStr::from("PLAIN"),
                response: b"\0guest\0guest".to_vec(),
                locale: ShortStr::from("en_US"),
            },
        )),
    )
}

pub fn client_tune_ok(frame_max: u32) -> Vec<u8> {
    encode_method_frame(
        0,
        AmqpMethod::Connection(ConnectionMethod::TuneOk(
            aurum_protocol_amqp::method::ConnectionTuneOk {
                channel_max: 2047,
                frame_max,
                heartbeat: 60,
            },
        )),
    )
}

pub fn client_connection_open() -> Vec<u8> {
    encode_method_frame(
        0,
        AmqpMethod::Connection(ConnectionMethod::Open(
            aurum_protocol_amqp::method::ConnectionOpen {
                virtual_host: ShortStr::from("/"),
                insist: false,
            },
        )),
    )
}

pub fn channel_open(h: &mut AmqpInMemoryHarness, channel: u16) {
    let bytes = encode_method_frame(
        channel,
        AmqpMethod::Channel(ChannelMethod::Open(
            aurum_protocol_amqp::method::ChannelOpen {
                reserved: ShortStr::from(""),
            },
        )),
    );
    let resp = h.send_bytes(&bytes);
    let methods = decode_methods(&resp).expect("channel.open-ok");
    assert!(matches!(methods[0], AmqpMethod::Channel(ChannelMethod::OpenOk)));
}

/// Open a channel over a raw TCP stream (for h10 TCP workloads).
pub fn channel_open_tcp(stream: &mut std::net::TcpStream, channel: u16) {
    use std::io::Write;
    stream
        .write_all(&encode_method_frame(
            channel,
            AmqpMethod::Channel(ChannelMethod::Open(
                aurum_protocol_amqp::method::ChannelOpen {
                    reserved: ShortStr::from(""),
                },
            )),
        ))
        .expect("channel.open write");
    let _ = read_tcp_response(stream);
}

fn read_tcp_response(stream: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    stream
        .set_read_timeout(Some(std::time::Duration::from_millis(500)))
        .ok();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or(0);
    buf.truncate(n);
    buf
}

pub fn publish_frames(channel: u16, exchange: &str, routing_key: &str, body: &[u8]) -> Vec<u8> {
    publish_frames_with_max(channel, exchange, routing_key, body, 131_072)
}

pub fn publish_frames_with_max(
    channel: u16,
    exchange: &str,
    routing_key: &str,
    body: &[u8],
    frame_max: u32,
) -> Vec<u8> {
    let mut frames = Vec::new();
    frames.extend(encode_method_frame(
        channel,
        AmqpMethod::Basic(BasicMethod::Publish(aurum_protocol_amqp::method::BasicPublish {
            exchange: ShortStr::from(exchange),
            routing_key: ShortStr::from(routing_key),
            flags: aurum_protocol_amqp::method::BasicPublishFlags::empty(),
        })),
    ));
    let mut header_payload = BytesMut::new();
    ContentHeader {
        class_id: CLASS_BASIC,
        body_size: body.len() as u64,
        properties: BasicProperties::default(),
    }
    .encode(&mut header_payload)
    .unwrap();
    let mut header_frame = BytesMut::new();
    RawFrame::new(FrameKind::Header, channel, header_payload.freeze()).encode(&mut header_frame);
    frames.extend_from_slice(&header_frame);
    if body.is_empty() {
        return frames;
    }
    let max_body = usize::try_from(frame_max).unwrap_or(1).max(1);
    for chunk in body.chunks(max_body) {
        let mut body_frame = BytesMut::new();
        RawFrame::new(FrameKind::Body, channel, Bytes::copy_from_slice(chunk))
            .encode(&mut body_frame);
        frames.extend_from_slice(&body_frame);
    }
    frames
}

pub fn setup_queue(h: &mut AmqpInMemoryHarness, channel: u16) {
    handshake(h);
    channel_open(h, channel);
    declare_bind_consume(h, channel, "ctag-1");
}

pub fn declare_bind_consume_bootstrap(channel: u16, tag: &str) -> Vec<u8> {
    declare_bind_consume_named(channel, "test.queue", "amq.direct", "test", tag)
}

pub fn declare_bind_consume_named(
    channel: u16,
    queue: &str,
    exchange: &str,
    routing_key: &str,
    tag: &str,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(encode_method_frame(
        channel,
        AmqpMethod::Exchange(ExchangeMethod::Declare(
            aurum_protocol_amqp::method::ExchangeDeclare {
                exchange: ShortStr::from(exchange),
                exchange_type: ShortStr::from("direct"),
                passive: false,
                durable: true,
                auto_delete: false,
                internal: false,
                nowait: false,
                arguments: Default::default(),
            },
        )),
    ));
    bytes.extend(encode_method_frame(
        channel,
        AmqpMethod::Queue(QueueMethod::Declare(
            aurum_protocol_amqp::method::QueueDeclare {
                queue: ShortStr::from(queue),
                passive: false,
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: Default::default(),
            },
        )),
    ));
    bytes.extend(encode_method_frame(
        channel,
        AmqpMethod::Queue(QueueMethod::Bind(aurum_protocol_amqp::method::QueueBind {
            queue: ShortStr::from(queue),
            exchange: ShortStr::from(exchange),
            routing_key: ShortStr::from(routing_key),
            nowait: false,
            arguments: Default::default(),
        })),
    ));
    bytes.extend(encode_method_frame(
        channel,
        AmqpMethod::Basic(BasicMethod::Qos(aurum_protocol_amqp::method::BasicQos {
            prefetch_size: 0,
            prefetch_count: 128,
            global: false,
        })),
    ));
    bytes.extend(encode_method_frame(
        channel,
        AmqpMethod::Basic(BasicMethod::Consume(aurum_protocol_amqp::method::BasicConsume {
            queue: ShortStr::from(queue),
            consumer_tag: ShortStr::from(tag),
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            arguments: Default::default(),
        })),
    ));
    bytes
}

/// AMQP handshake over a raw TCP stream.
pub fn handshake_tcp(stream: &mut std::net::TcpStream) {
    use std::io::Write;
    stream.write_all(PROTOCOL_HEADER).expect("protocol header");
    let _ = read_tcp_response(stream);
    stream
        .write_all(&client_start_ok())
        .expect("connection.start-ok");
    let _ = read_tcp_response(stream);
    let mut open = client_tune_ok(131_072);
    open.extend(client_connection_open());
    stream.write_all(&open).expect("tune-ok + open");
    let _ = read_tcp_response(stream);
}

/// Publish → deliver → ack transcript over TCP using bootstrap route names.
pub fn run_publish_deliver_ack_bootstrap_tcp(stream: &mut std::net::TcpStream) {
    use std::io::Write;
    handshake_tcp(stream);
    channel_open_tcp(stream, 1);
    stream
        .write_all(&declare_bind_consume_bootstrap(1, "ctag-1"))
        .expect("declare/bind/consume");
    let _ = read_tcp_response(stream);
    stream
        .write_all(&publish_frames(1, "amq.direct", "test", b"tcp-msg"))
        .expect("publish");
    let deliver = read_tcp_response(stream);
    assert!(!deliver.is_empty(), "expected deliver frames");
    stream
        .write_all(&encode_method_frame(
            1,
            AmqpMethod::Basic(BasicMethod::Ack(aurum_protocol_amqp::method::BasicAck {
                delivery_tag: 1,
                multiple: false,
            })),
        ))
        .expect("ack");
    let _ = read_tcp_response(stream);
}

pub fn declare_bind_consume(h: &mut AmqpInMemoryHarness, channel: u16, tag: &str) {
    h.send_bytes(&declare_bind_consume_named(
        channel,
        "q1",
        "orders",
        "created",
        tag,
    ));
}

pub fn run_publish_deliver_ack_transcript(h: &mut AmqpInMemoryHarness) {
    setup_queue(h, 1);
    let deliver = h.send_bytes(&publish_frames(1, "orders", "created", b"h8-msg"));
    assert!(!deliver.is_empty());
    h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Ack(aurum_protocol_amqp::method::BasicAck {
            delivery_tag: 1,
            multiple: false,
        })),
    ));
}

pub fn run_publish_nack_requeue_ack(h: &mut AmqpInMemoryHarness) {
    setup_queue(h, 1);
    h.send_bytes(&publish_frames(1, "orders", "created", b"nack"));
    let redeliver = h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Nack(aurum_protocol_amqp::method::BasicNack {
            delivery_tag: 1,
            multiple: false,
            requeue: true,
        })),
    ));
    assert!(!redeliver.is_empty());
    h.send_bytes(&encode_method_frame(
        1,
        AmqpMethod::Basic(BasicMethod::Ack(aurum_protocol_amqp::method::BasicAck {
            delivery_tag: 2,
            multiple: false,
        })),
    ));
}

pub fn run_fragmented_body_transcript(h: &mut AmqpInMemoryHarness) {
    handshake_with_frame_max(h, 64);
    channel_open(h, 1);
    declare_bind_consume(h, 1, "ctag-1");
    let body = vec![b'f'; 100];
    let resp = h.send_bytes(&publish_frames_with_max(1, "orders", "created", &body, 64));
    assert!(!resp.is_empty());
}

pub fn run_multi_channel_transcript(h: &mut AmqpInMemoryHarness) {
    handshake(h);
    channel_open(h, 1);
    channel_open(h, 2);
    declare_bind_consume(h, 1, "ctag-1");
    declare_bind_consume(h, 2, "ctag-2");
    h.send_bytes(&publish_frames(1, "orders", "created", b"multi"));
}
