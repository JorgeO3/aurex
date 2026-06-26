use aurum_broker::{amqp_transcript, AmqpInMemoryHarness};
use aurum_protocol_amqp::{
    test_support::decode_all_frames,
    wire::constants::PROTOCOL_HEADER,
};

fn main() {
    let workload = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "handshake_only".to_string());

    match workload.as_str() {
        "handshake_only" => bench_handshake(),
        "publish_deliver_ack_1" => bench_publish_deliver_ack(1),
        "publish_deliver_ack_many" => bench_publish_deliver_ack(100),
        "publish_nack_requeue_ack" => bench_named("publish_nack_requeue_ack", || {
            let mut h = AmqpInMemoryHarness::new();
            amqp_transcript::run_publish_nack_requeue_ack(&mut h);
        }),
        "fragmented_body_publish" => bench_named("fragmented_body_publish", || {
            let mut h = AmqpInMemoryHarness::new();
            amqp_transcript::run_fragmented_body_transcript(&mut h);
        }),
        "multi_channel_publish_consume" => bench_named("multi_channel_publish_consume", || {
            let mut h = AmqpInMemoryHarness::new();
            amqp_transcript::run_multi_channel_transcript(&mut h);
        }),
        other => {
            eprintln!("unknown workload: {other}");
            std::process::exit(1);
        }
    }
}

fn bench_handshake() {
    let start = std::time::Instant::now();
    for _ in 0..10_000 {
        let mut harness = AmqpInMemoryHarness::new();
        let resp = harness.send_bytes(PROTOCOL_HEADER);
        let frames = decode_all_frames(&resp).expect("start");
        assert!(!frames.is_empty());
    }
    println!(
        "workload=handshake_only iterations=10000 elapsed_ms={}",
        start.elapsed().as_millis()
    );
}

fn bench_publish_deliver_ack(messages: u32) {
    let start = std::time::Instant::now();
    for _ in 0..messages {
        let mut h = AmqpInMemoryHarness::new();
        amqp_transcript::run_publish_deliver_ack_transcript(&mut h);
    }
    let elapsed = start.elapsed();
    let name = if messages == 1 {
        "publish_deliver_ack_1"
    } else {
        "publish_deliver_ack_many"
    };
    println!(
        "workload={name} messages={messages} elapsed_ms={} ns_per_message={:.1}",
        elapsed.as_millis(),
        elapsed.as_nanos() as f64 / f64::from(messages)
    );
}

fn bench_named(name: &str, f: impl FnOnce()) {
    let start = std::time::Instant::now();
    f();
    println!("workload={name} elapsed_ms={}", start.elapsed().as_millis());
}
