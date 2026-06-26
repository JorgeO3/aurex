use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use aurum_broker::{
    amqp_transcript, BrokerMode, BrokerServer, ListenerEndpointConfig, SingleNodeBrokerConfig,
    StorageBackendKind,
};
use aurum_protocol_native::{
    message::{
        AckBatchBody, ConsumeStartBody, DeliveryBatchBody, HelloBody, NativeAckOp,
        NackBatchBody, NativeNackDisposition, NativeNackOp, PublishBatchBody, PublishDescriptor,
        ResolveRouteBody, RouteResolvedBody,
    },
    wire::NativeConsumerFlags,
    NativeCapabilities, NativeCodec, NativeFrame, NativeFrameHeader, NativeOp, FrameFlags,
};
use bytes::{BytesMut, BufMut};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let workload = parse_str(&args, "--workload", "");
    let protocol = parse_str(&args, "--protocol", "native");
    let messages = parse_u64(&args, "--messages", 10_000);
    let batch = parse_u64(&args, "--batch", 64) as u32;

    if !workload.is_empty() {
        run_workload(workload, messages, batch);
        return;
    }

    match protocol {
        "native" => run_native_publish_confirm(messages, batch),
        "amqp-transcript" => run_amqp_transcript_publish(messages),
        other => {
            eprintln!("unknown protocol: {other}");
            eprintln!("use --workload= for explicit workloads");
            std::process::exit(1);
        }
    }
}

fn run_workload(workload: &str, messages: u64, batch: u32) {
    let start = Instant::now();
    match workload {
        "native_publish_confirm" => run_native_publish_confirm(messages, batch),
        "native_publish_consume_ack" => run_native_publish_consume_ack(messages),
        "native_nack_requeue_ack" => run_native_nack_requeue_ack(),
        "amqp_transcript_publish" => run_amqp_transcript_publish(messages),
        "amqp_transcript_consume_ack" => run_amqp_transcript_consume_ack(),
        "persistent_restart_smoke" => run_persistent_restart_smoke(),
        other => {
            eprintln!("unknown workload: {other}");
            std::process::exit(1);
        }
    }
    let _ = start;
}

fn run_native_publish_confirm(messages: u64, batch: u32) {
    let (addr, server) = start_native_server();
    thread::sleep(Duration::from_millis(100));
    let mut stream = TcpStream::connect(addr).unwrap();
    let start = Instant::now();
    let _ = send_frames(&mut stream, hello_frame(1));
    let resolved = decode_resolved(&send_frames(&mut stream, resolve_frame(2)));
    let publish = publish_frame(resolved, batch);
    let batches = messages.div_ceil(u64::from(batch));
    for i in 0..batches {
        let mut req = publish.clone();
        req.header.correlation_id = i + 3;
        let _ = send_frames(&mut stream, req);
    }
    report("native_publish_confirm", messages, start.elapsed());
    drop(server);
}

fn run_native_publish_consume_ack(messages: u64) {
    let (addr, server) = start_native_server();
    thread::sleep(Duration::from_millis(100));
    let mut stream = TcpStream::connect(addr).unwrap();
    let start = Instant::now();
    let _ = send_frames(&mut stream, hello_frame(1));
    let resolved = decode_resolved(&send_frames(&mut stream, resolve_frame(2)));
    let _ = send_frames(&mut stream, publish_frame(resolved, 1));
    let _ = send_frames(&mut stream, consume_frame(3, 1, 1));
    for i in 0..messages {
        let mut publish = publish_frame(resolved, 1);
        publish.header.correlation_id = 4 + i;
        let resp = send_frames(&mut stream, publish);
        let deliver = find_op(&resp, NativeOp::DeliveryBatch);
        let delivery = DeliveryBatchBody::decode(&deliver.body).unwrap();
        let tag = delivery.descriptors[0].delivery_tag;
        let _ = send_frames(
            &mut stream,
            ack_frame(1000 + i, delivery.consumer_id, tag),
        );
    }
    report("native_publish_consume_ack", messages, start.elapsed());
    drop(server);
}

fn run_native_nack_requeue_ack() {
    let (addr, server) = start_native_server();
    thread::sleep(Duration::from_millis(100));
    let mut stream = TcpStream::connect(addr).unwrap();
    let start = Instant::now();
    let _ = send_frames(&mut stream, hello_frame(1));
    let resolved = decode_resolved(&send_frames(&mut stream, resolve_frame(2)));
    let _ = send_frames(&mut stream, publish_frame(resolved, 1));
    let consumer_ok = send_frames(&mut stream, consume_frame(3, 1, 1));
    let consumer_id = aurum_protocol_native::message::ConsumerOkBody::decode(
        &find_op(&consumer_ok, NativeOp::ConsumerOk).body,
    )
    .unwrap()
    .consumer_id;
    let deliver = find_op(&consumer_ok, NativeOp::DeliveryBatch);
    let tag = DeliveryBatchBody::decode(&deliver.body).unwrap().descriptors[0].delivery_tag;
    let redeliver = send_frames(&mut stream, nack_frame(4, consumer_id, tag));
    let tag2 = DeliveryBatchBody::decode(&find_op(&redeliver, NativeOp::DeliveryBatch).body)
        .unwrap()
        .descriptors[0]
        .delivery_tag;
    let _ = send_frames(&mut stream, ack_frame(5, consumer_id, tag2));
    report("native_nack_requeue_ack", 1, start.elapsed());
    drop(server);
}

fn run_amqp_transcript_publish(messages: u64) {
    let (addr, server) = start_amqp_server();
    thread::sleep(Duration::from_millis(100));
    let mut stream = TcpStream::connect(addr).unwrap();
    let start = Instant::now();
    amqp_transcript::handshake_tcp(&mut stream);
    amqp_transcript::channel_open_tcp(&mut stream, 1);
    for i in 0..messages {
        let publish = amqp_transcript::publish_frames(1, "amq.direct", "test", format!("msg-{i}").as_bytes());
        std::io::Write::write_all(&mut stream, &publish).unwrap();
        let _ = read_available(&mut stream);
    }
    report("amqp_transcript_publish", messages, start.elapsed());
    drop(server);
}

fn run_amqp_transcript_consume_ack() {
    let (addr, server) = start_amqp_server();
    thread::sleep(Duration::from_millis(100));
    let mut stream = TcpStream::connect(addr).unwrap();
    let start = Instant::now();
    amqp_transcript::run_publish_deliver_ack_bootstrap_tcp(&mut stream);
    report("amqp_transcript_consume_ack", 1, start.elapsed());
    drop(server);
}

fn run_persistent_restart_smoke() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = SingleNodeBrokerConfig::dev_defaults();
    config.mode = BrokerMode::SingleNodePersistent;
    config.storage.backend = StorageBackendKind::AppendOnly;
    config.storage.data_dir = dir.path().to_path_buf();
    config.listeners.native = Some(ListenerEndpointConfig {
        enabled: true,
        bind: "127.0.0.0:0".parse().unwrap(),
    });
    config.listeners.amqp = None;
    let server = BrokerServer::start(config.clone()).expect("start persistent");
    let addr = server.native_addr().expect("native addr");
    thread::sleep(Duration::from_millis(100));
    {
        let mut stream = TcpStream::connect(addr).unwrap();
        let _ = send_frames(&mut stream, hello_frame(1));
        let resolved = decode_resolved(&send_frames(&mut stream, resolve_frame(2)));
        let _ = send_frames(&mut stream, publish_frame(resolved, 1));
    }
    drop(server);
    let server2 = BrokerServer::start(config).expect("restart persistent");
    let health = server2
        .service()
        .shared_broker()
        .lock()
        .unwrap()
        .health();
    println!(
        "workload=persistent_restart_smoke state={:?}",
        health.state
    );
}

fn start_native_server() -> (std::net::SocketAddr, BrokerServer) {
    let mut config = SingleNodeBrokerConfig::dev_defaults();
    config.listeners.native = Some(ListenerEndpointConfig {
        enabled: true,
        bind: "127.0.0.0:0".parse().unwrap(),
    });
    config.listeners.amqp = None;
    let server = BrokerServer::start(config).expect("start");
    (server.native_addr().expect("addr"), server)
}

fn start_amqp_server() -> (std::net::SocketAddr, BrokerServer) {
    let mut config = SingleNodeBrokerConfig::dev_defaults();
    config.listeners.native = None;
    config.listeners.amqp = Some(ListenerEndpointConfig {
        enabled: true,
        bind: "127.0.0.0:0".parse().unwrap(),
    });
    let server = BrokerServer::start(config).expect("start");
    (server.amqp_addr().expect("addr"), server)
}

fn send_frames(stream: &mut TcpStream, frame: NativeFrame) -> Vec<NativeFrame> {
    let mut codec = NativeCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(&frame, &mut buf).unwrap();
    std::io::Write::write_all(stream, &buf).unwrap();
    read_all_native(stream)
}

fn read_all_native(stream: &mut TcpStream) -> Vec<NativeFrame> {
    let bytes = read_available(stream);
    let mut codec = NativeCodec::default();
    let mut buf = BytesMut::from(bytes.as_slice());
    let mut frames = Vec::new();
    while let Ok(Some(frame)) = codec.decode(&mut buf) {
        frames.push(frame);
    }
    frames
}

fn decode_resolved(frames: &[NativeFrame]) -> RouteResolvedBody {
    RouteResolvedBody::decode(&find_op(frames, NativeOp::RouteResolved).body).unwrap()
}

fn find_op<'a>(frames: &'a [NativeFrame], op: NativeOp) -> &'a NativeFrame {
    frames
        .iter()
        .find(|f| f.op() == Some(op))
        .unwrap_or_else(|| panic!("missing {op:?}"))
}

fn read_available(stream: &mut TcpStream) -> Vec<u8> {
    stream.set_read_timeout(Some(Duration::from_millis(500))).ok();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match std::io::Read::read(stream, &mut tmp) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if n < tmp.len() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    buf
}

fn hello_frame(correlation_id: u64) -> NativeFrame {
    let mut body = BytesMut::new();
    HelloBody {
        client_major: 0,
        client_minor: 1,
        client_capabilities: NativeCapabilities::ROUTE_ID,
        client_name: b"h10".to_vec(),
    }
    .encode(&mut body)
    .unwrap();
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

fn publish_frame(resolved: RouteResolvedBody, count: u32) -> NativeFrame {
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
    frame(NativeOp::PublishBatch, 2, buf.freeze())
}

fn consume_frame(correlation_id: u64, queue_id: u32, consumer_hint: u32) -> NativeFrame {
    let mut body = BytesMut::new();
    ConsumeStartBody {
        queue_id,
        consumer_id_hint: consumer_hint,
        prefetch: 128,
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

fn report(name: &str, messages: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    let rate = if secs > 0.0 { messages as f64 / secs } else { 0.0 };
    println!("workload={name} messages={messages} elapsed_s={secs:.3} rate={rate:.0}/s");
}

fn parse_u64(args: &[String], key: &str, default: u64) -> u64 {
    args.iter()
        .find_map(|a| a.strip_prefix(key).map(|v| v.trim_start_matches('=').parse().ok()))
        .flatten()
        .unwrap_or(default)
}

fn parse_str<'a>(args: &'a [String], key: &str, default: &'a str) -> &'a str {
    args.iter()
        .find_map(|a| a.strip_prefix(key).map(|v| v.trim_start_matches('=')))
        .unwrap_or(default)
}
