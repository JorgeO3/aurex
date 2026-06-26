use std::time::Instant;

use aurum_broker::NativeInMemoryHarness;
use aurum_protocol_native::{
    codec::cursor::pack_route_id,
    message::{HelloBody, PublishBatchBody, PublishDescriptor, ResolveRouteBody, RouteResolvedBody},
    NativeCapabilities, NativeCodec, NativeFrame, NativeFrameHeader, NativeOp, FrameFlags,
};
use aurum_types::{ExchangeId, QueueId};
use bytes::{Bytes, BytesMut};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let messages = parse_u64(&args, "--messages", 1_048_576);
    let batch = parse_u64(&args, "--batch", 128) as u32;
    let workload = parse_str(&args, "--workload", "publish_route_id_batch");

    match workload {
        "resolve_route_only" => bench_resolve_only(messages),
        "publish_route_id_batch" => bench_publish_route_id(messages, batch),
        "publish_resolve_each_time" => bench_resolve_each_publish(messages, batch),
        other => {
            eprintln!("unknown workload: {other}");
            std::process::exit(1);
        }
    }
}

fn bench_resolve_only(iterations: u64) {
    let mut harness = NativeInMemoryHarness::with_orders_route();
    let frame = resolve_frame(1);
    let start = Instant::now();
    for i in 0..iterations {
        let mut req = frame.clone();
        req.header.correlation_id = i + 1;
        let _ = harness.send_bytes(&encode(&req));
    }
    report("resolve_route_only", iterations, start.elapsed());
}

fn bench_publish_route_id(messages: u64, batch: u32) {
    let mut harness = NativeInMemoryHarness::with_orders_route();
    let resolved = resolve_once(&mut harness);
    let publish_frame = publish_frame(resolved, batch);
    let batches = messages.div_ceil(u64::from(batch));
    let start = Instant::now();
    for i in 0..batches {
        let mut req = publish_frame.clone();
        req.header.correlation_id = i + 2;
        let _ = harness.send_bytes(&encode(&req));
    }
    report("publish_route_id_batch", messages, start.elapsed());
}

fn bench_resolve_each_publish(messages: u64, batch: u32) {
    let mut harness = NativeInMemoryHarness::with_orders_route();
    let resolve = resolve_frame(1);
    let batches = messages.div_ceil(u64::from(batch));
    let start = Instant::now();
    for i in 0..batches {
        let mut r = resolve.clone();
        r.header.correlation_id = i * 2 + 1;
        let resp = harness.send_bytes(&encode(&r));
        let resolved = decode_route_resolved(&resp);
        let mut p = publish_frame(resolved, batch);
        p.header.correlation_id = i * 2 + 2;
        let _ = harness.send_bytes(&encode(&p));
    }
    report("publish_resolve_each_time", messages, start.elapsed());
}

fn resolve_once(harness: &mut NativeInMemoryHarness) -> RouteResolvedBody {
    let resp = harness.send_bytes(&encode(&resolve_frame(1)));
    decode_route_resolved(&resp)
}

fn decode_route_resolved(bytes: &[u8]) -> RouteResolvedBody {
    let mut codec = NativeCodec::default();
    let mut buf = BytesMut::from(bytes);
    let frame = codec.decode(&mut buf).unwrap().unwrap();
    RouteResolvedBody::decode(&frame.body).unwrap()
}

fn resolve_frame(correlation_id: u64) -> NativeFrame {
    let mut body = BytesMut::new();
    ResolveRouteBody {
        route_table_version_hint: 1,
        exchange_id_hint: 0,
        exchange: b"orders".to_vec(),
        routing_key: b"created".to_vec(),
    }
    .encode(&mut body)
    .unwrap();
    NativeFrame::new(
        NativeFrameHeader::new(
            NativeOp::ResolveRoute,
            FrameFlags::NONE,
            0,
            correlation_id,
            body.len() as u32,
        ),
        body.freeze(),
    )
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
    NativeFrame::new(
        NativeFrameHeader::new(
            NativeOp::PublishBatch,
            FrameFlags::NONE,
            0,
            2,
            buf.len() as u32,
        ),
        buf.freeze(),
    )
}

fn encode(frame: &NativeFrame) -> Vec<u8> {
    let mut codec = NativeCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();
    buf.to_vec()
}

fn report(workload: &str, messages: u64, elapsed: std::time::Duration) {
    let ns_per_msg = elapsed.as_nanos() as f64 / messages as f64;
    println!(
        "workload={workload} messages={messages} elapsed_ms={} ns_per_message={ns_per_msg:.1}",
        elapsed.as_millis()
    );
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
