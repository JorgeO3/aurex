use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::single_node::config::SingleNodeBrokerConfig;
use crate::{amqp_transcript, AmqpServerSession, BrokerService, NativeServerSession};
use aurum_protocol_native::{
    message::{
        AckBatchBody, ConsumeStartBody, DeliveryBatchBody, HelloBody, NativeAckOp,
        NackBatchBody, NativeNackDisposition, NativeNackOp, PublishBatchBody, PublishDescriptor,
        ResolveRouteBody, RouteResolvedBody,
    },
    wire::NativeConsumerFlags,
    NativeCapabilities, NativeCodec, NativeFrame, NativeFrameHeader, NativeOp, FrameFlags,
};
use aurum_transport::{config::ListenerConfig, spawn_blocking_listener, ListenerFlags};
use bytes::{BytesMut, BufMut};

fn spawn_native_server() -> (std::thread::JoinHandle<()>, std::net::SocketAddr, Arc<BrokerService>) {
    let service = Arc::new(
        BrokerService::new(SingleNodeBrokerConfig::dev_defaults()).unwrap(),
    );
    service.start();
    let accept_service = Arc::clone(&service);
    let transport_config = ListenerConfig {
        bind: "127.0.0.0:0".parse().unwrap(),
        flags: ListenerFlags::ENABLED | ListenerFlags::TCP_NODELAY,
        max_connections: 8,
        max_read_buffer: 64 * 1024,
        max_write_buffer: 256 * 1024,
    };
    let (listener, addr) = spawn_blocking_listener(
        transport_config,
        Arc::new(move |id, mut conn| {
            let service = Arc::clone(&accept_service);
            let mut session = NativeServerSession::new(id, service.clone());
            service.record_connection_accepted();
            let mut buf = [0u8; 8192];
            loop {
                match conn.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let resp = session.on_bytes(&buf[..n]);
                        if !resp.is_empty() && conn.write_all(&resp).is_err() {
                            break;
                        }
                        service.record_io(
                            u64::try_from(n).unwrap_or(0),
                            u64::try_from(resp.len()).unwrap_or(0),
                            1,
                            u64::from(!resp.is_empty()),
                        );
                    }
                    Err(_) => break,
                }
            }
            service.record_connection_closed();
        }),
    )
    .unwrap();
    (listener, addr, service)
}

fn spawn_amqp_server() -> (std::thread::JoinHandle<()>, std::net::SocketAddr) {
    let service = Arc::new(
        BrokerService::new(SingleNodeBrokerConfig::dev_defaults()).unwrap(),
    );
    service.start();
    let accept_service = Arc::clone(&service);
    let transport_config = ListenerConfig {
        bind: "127.0.0.0:0".parse().unwrap(),
        flags: ListenerFlags::ENABLED,
        max_connections: 8,
        max_read_buffer: 64 * 1024,
        max_write_buffer: 256 * 1024,
    };
    let (listener, addr) = spawn_blocking_listener(
        transport_config,
        Arc::new(move |id, mut conn| {
            let service = Arc::clone(&accept_service);
            let mut session = crate::AmqpServerSession::new(id, service.clone());
            service.record_connection_accepted();
            let mut buf = [0u8; 8192];
            loop {
                match conn.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        let resp = session.on_bytes(&buf[..n]);
                        if !resp.is_empty() && conn.write_all(&resp).is_err() {
                            break;
                        }
                        service.record_io(
                            u64::try_from(n).unwrap_or(0),
                            u64::try_from(resp.len()).unwrap_or(0),
                            1,
                            u64::from(!resp.is_empty()),
                        );
                    }
                    Err(_) => break,
                }
            }
            service.record_connection_closed();
        }),
    )
    .unwrap();
    (listener, addr)
}

#[test]
fn native_tcp_hello_roundtrip() {
    let (listener, addr, _) = spawn_native_server();
    thread::sleep(Duration::from_millis(50));
    let mut stream = TcpStream::connect(addr).unwrap();
    let resp = send_frame(&mut stream, hello_frame(1));
    assert_eq!(frame_with_op(&resp, NativeOp::HelloOk).op(), Some(NativeOp::HelloOk));
    drop(listener);
}

#[test]
fn native_tcp_resolve_route() {
    let (listener, addr, _) = spawn_native_server();
    thread::sleep(Duration::from_millis(50));
    let mut stream = TcpStream::connect(addr).unwrap();
    let resp = send_frame(&mut stream, resolve_frame(2));
    assert_eq!(frame_with_op(&resp, NativeOp::RouteResolved).op(), Some(NativeOp::RouteResolved));
    let body = RouteResolvedBody::decode(&frame_with_op(&resp, NativeOp::RouteResolved).body).unwrap();
    assert!(body.route_id_packed > 0);
    drop(listener);
}

#[test]
fn native_tcp_publish_confirm() {
    let (listener, addr, _) = spawn_native_server();
    thread::sleep(Duration::from_millis(50));
    let mut stream = TcpStream::connect(addr).unwrap();
    let _ = send_frame(&mut stream, hello_frame(1));
    let resolved = RouteResolvedBody::decode(
        &frame_with_op(&send_frame(&mut stream, resolve_frame(2)), NativeOp::RouteResolved).body,
    )
    .unwrap();
    let resp = send_frame(&mut stream, publish_frame(resolved, 4, 3));
    assert_eq!(
        frame_with_op(&resp, NativeOp::PublishConfirmBatch).op(),
        Some(NativeOp::PublishConfirmBatch)
    );
    drop(listener);
}

#[test]
fn native_tcp_consume_ack() {
    let (listener, addr, _) = spawn_native_server();
    thread::sleep(Duration::from_millis(50));
    let mut stream = TcpStream::connect(addr).unwrap();
    let _ = send_frame(&mut stream, hello_frame(1));
    let resolved = RouteResolvedBody::decode(
        &frame_with_op(&send_frame(&mut stream, resolve_frame(2)), NativeOp::RouteResolved).body,
    )
    .unwrap();
    let _ = send_frame(&mut stream, publish_frame(resolved, 3, 1));
    let consume_resp = send_frame(&mut stream, consume_frame(4, 1, 1));
    assert_eq!(
        frame_with_op(&consume_resp, NativeOp::ConsumerOk).op(),
        Some(NativeOp::ConsumerOk)
    );
    let deliver = frame_with_op(&consume_resp, NativeOp::DeliveryBatch);
    let delivery = DeliveryBatchBody::decode(&deliver.body).unwrap();
    let tag = delivery.descriptors[0].delivery_tag;
    let settled_resp = send_frame(&mut stream, ack_frame(5, delivery.consumer_id, tag));
    assert_eq!(
        frame_with_op(&settled_resp, NativeOp::SettlementResultBatch).op(),
        Some(NativeOp::SettlementResultBatch)
    );
    drop(listener);
}

#[test]
fn native_tcp_nack_requeue_ack() {
    let (listener, addr, _) = spawn_native_server();
    thread::sleep(Duration::from_millis(50));
    let mut stream = TcpStream::connect(addr).unwrap();
    let _ = send_frame(&mut stream, hello_frame(1));
    let resolved = RouteResolvedBody::decode(
        &frame_with_op(&send_frame(&mut stream, resolve_frame(2)), NativeOp::RouteResolved).body,
    )
    .unwrap();
    let _ = send_frame(&mut stream, publish_frame(resolved, 3, 1));
    let consumer_ok = send_frame(&mut stream, consume_frame(4, 1, 1));
    let consumer_id = aurum_protocol_native::message::ConsumerOkBody::decode(
        &frame_with_op(&consumer_ok, NativeOp::ConsumerOk).body,
    )
    .unwrap()
    .consumer_id;
    let deliver = frame_with_op(&consumer_ok, NativeOp::DeliveryBatch);
    let tag = DeliveryBatchBody::decode(&deliver.body).unwrap().descriptors[0].delivery_tag;
    let redeliver_resp = send_frame(&mut stream, nack_frame(6, consumer_id, tag));
    let tag2 = DeliveryBatchBody::decode(
        &frame_with_op(&redeliver_resp, NativeOp::DeliveryBatch).body,
    )
    .unwrap()
    .descriptors[0]
    .delivery_tag;
    let settled_resp = send_frame(&mut stream, ack_frame(7, consumer_id, tag2));
    assert_eq!(
        frame_with_op(&settled_resp, NativeOp::SettlementResultBatch).op(),
        Some(NativeOp::SettlementResultBatch)
    );
    drop(listener);
}

#[test]
fn amqp_tcp_transcript_consume_ack() {
    let (listener, addr) = spawn_amqp_server();
    thread::sleep(Duration::from_millis(100));
    let mut stream = TcpStream::connect(addr).unwrap();
    amqp_transcript::run_publish_deliver_ack_bootstrap_tcp(&mut stream);
    drop(listener);
}

#[test]
fn tcp_session_records_metrics() {
    let (listener, addr, service) = spawn_native_server();
    thread::sleep(Duration::from_millis(50));
    let mut stream = TcpStream::connect(addr).unwrap();
    let _ = send_frame(&mut stream, hello_frame(1));
    drop(stream);
    thread::sleep(Duration::from_millis(50));
    let snap = service.shared_broker().lock().unwrap().metrics().snapshot();
    assert!(snap.accepted_connections >= 1);
    assert!(snap.bytes_in > 0);
    assert!(snap.bytes_out > 0);
    drop(listener);
}

fn send_frame(stream: &mut TcpStream, frame: NativeFrame) -> Vec<NativeFrame> {
    let bytes = encode_frame(&frame);
    stream.write_all(&bytes).unwrap();
    read_all_frames(stream)
}

fn frame_with_op(frames: &[NativeFrame], op: NativeOp) -> &NativeFrame {
    frames
        .iter()
        .find(|f| f.op() == Some(op))
        .unwrap_or_else(|| panic!("missing frame op {op:?}, got {:?}", frames.iter().filter_map(|f| f.op()).collect::<Vec<_>>()))
}

fn read_all_frames(stream: &mut TcpStream) -> Vec<NativeFrame> {
    stream.set_read_timeout(Some(Duration::from_millis(500))).ok();
    let mut buf = vec![0u8; 16384];
    let n = stream.read(&mut buf).unwrap_or(0);
    let mut codec = NativeCodec::default();
    let mut bytes = BytesMut::from(&buf[..n]);
    let mut frames = Vec::new();
    while let Ok(Some(frame)) = codec.decode(&mut bytes) {
        frames.push(frame);
    }
    frames
}

fn encode_frame(frame: &NativeFrame) -> Vec<u8> {
    let mut codec = NativeCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();
    buf.to_vec()
}

fn hello_frame(correlation_id: u64) -> NativeFrame {
    let mut body = BytesMut::new();
    HelloBody {
        client_major: 0,
        client_minor: 1,
        client_capabilities: NativeCapabilities::ROUTE_ID,
        client_name: b"tcp-test".to_vec(),
    }
    .encode(&mut body);
    frame(NativeOp::Hello, correlation_id, body.freeze())
}

fn resolve_frame(correlation_id: u64) -> NativeFrame {
    let mut body = BytesMut::new();
    ResolveRouteBody {
        route_table_version_hint: 1,
        exchange_id_hint: 0,
        exchange: b"amq.direct".to_vec(),
        routing_key: b"test".to_vec(),
    }
    .encode(&mut body)
    .unwrap();
    frame(NativeOp::ResolveRoute, correlation_id, body.freeze())
}

fn publish_frame(resolved: RouteResolvedBody, correlation_id: u64, count: u32) -> NativeFrame {
    let mut payloads = BytesMut::new();
    let mut descriptors = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let offset = payloads.len() as u32;
        let payload = b"msg";
        descriptors.push(PublishDescriptor {
            payload_offset: offset,
            payload_len: payload.len() as u32,
            message_flags: Default::default(),
        });
        payloads.extend_from_slice(payload);
    }
    let body = PublishBatchBody {
        route_table_version: resolved.route_table_version,
        route_id_packed: resolved.route_id_packed,
        batch_flags: 0,
        descriptors,
        payloads: payloads.freeze(),
    };
    let mut buf = BytesMut::new();
    body.encode(&mut buf).unwrap();
    frame(NativeOp::PublishBatch, correlation_id, buf.freeze())
}

fn consume_frame(correlation_id: u64, queue_id: u32, consumer_hint: u32) -> NativeFrame {
    let mut body = BytesMut::new();
    ConsumeStartBody {
        queue_id,
        consumer_id_hint: consumer_hint,
        prefetch: 16,
        consumer_flags: NativeConsumerFlags::MANUAL_ACK,
    }
    .encode(&mut body);
    frame(NativeOp::ConsumeStart, correlation_id, body.freeze())
}

fn ack_frame(correlation_id: u64, consumer_id: u64, tag: u64) -> NativeFrame {
    let body = AckBatchBody {
        consumer_id,
        flags: 0,
        ops: vec![NativeAckOp::One { tag }],
    };
    let mut buf = BytesMut::new();
    body.encode(&mut buf).unwrap();
    frame(NativeOp::AckBatch, correlation_id, buf.freeze())
}

fn nack_frame(correlation_id: u64, consumer_id: u64, tag: u64) -> NativeFrame {
    let body = NackBatchBody {
        consumer_id,
        flags: 0,
        ops: vec![NativeNackOp::One {
            tag,
            disposition: NativeNackDisposition::Requeue,
        }],
    };
    let mut buf = BytesMut::new();
    body.encode(&mut buf).unwrap();
    frame(NativeOp::NackBatch, correlation_id, buf.freeze())
}

fn frame(op: NativeOp, correlation_id: u64, body: bytes::Bytes) -> NativeFrame {
    NativeFrame::new(
        NativeFrameHeader::new(op, FrameFlags::NONE, 0, correlation_id, body.len() as u32),
        body,
    )
}
