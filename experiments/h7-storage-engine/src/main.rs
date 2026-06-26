use std::time::Instant;

use aurum_storage::{AppendOnlyStorageEngine, DurabilityMode, QueueSeq, StorageConfig};
use aurum_types::QueueId;
use tempfile::tempdir;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let messages = parse_u64(&args, "--messages", 1_048_576);
    let payload_bytes = parse_u64(&args, "--payload-bytes", 256) as usize;
    let batch = parse_u64(&args, "--batch", 128) as usize;
    let mode = parse_str(&args, "--mode", "buffered");
    let workload = parse_str(&args, "--workload", "append_payload_batches");

    let durability = match mode {
        "fsync-on-flush" => DurabilityMode::FsyncOnFlush,
        _ => DurabilityMode::Buffered,
    };

    match workload {
        "append_payload_batches" => bench_payload(messages, payload_bytes, batch, durability),
        "publish_ack_recover" => bench_publish_ack_recover(messages, payload_bytes, batch, durability),
        other => {
            eprintln!("unknown workload: {other}");
            std::process::exit(1);
        }
    }
}

fn bench_payload(messages: u64, payload_bytes: usize, batch: usize, durability: DurabilityMode) {
    let dir = tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.durability = durability;
    let mut engine = AppendOnlyStorageEngine::open(config).unwrap();
    let payload = vec![0u8; payload_bytes];
    let batches = messages.div_ceil(batch as u64);
    let start = Instant::now();
    for b in 0..batches {
        let n = if b + 1 == batches {
            (messages - b * batch as u64) as usize
        } else {
            batch
        };
        let slices: Vec<&[u8]> = (0..n).map(|_| payload.as_slice()).collect();
        engine
            .append_publish(QueueId(1), QueueSeq(b * batch as u64), &slices)
            .unwrap();
    }
    engine.flush().unwrap();
    report("append_payload_batches", messages, start.elapsed());
}

fn bench_publish_ack_recover(messages: u64, payload_bytes: usize, batch: usize, durability: DurabilityMode) {
    let dir = tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.durability = durability;
    let mut engine = AppendOnlyStorageEngine::open(config).unwrap();
    let payload = vec![0u8; payload_bytes];
    let start = Instant::now();
    let slices: Vec<&[u8]> = (0..batch).map(|_| payload.as_slice()).collect();
    for i in 0..messages {
        if i % batch as u64 == 0 {
            engine
                .append_publish(QueueId(1), QueueSeq(i), &slices)
                .unwrap();
        }
        if i % 2 == 1 {
            engine
                .append_ack_range(QueueId(1), QueueSeq(i - 1), 1)
                .unwrap();
        }
    }
    engine.flush().unwrap();
    let image = engine.recover_queue(QueueId(1)).unwrap();
    let elapsed = start.elapsed();
    println!(
        "workload=publish_ack_recover messages={messages} ready={} elapsed_ms={}",
        image.ready.len(),
        elapsed.as_millis()
    );
}

fn report(workload: &str, messages: u64, elapsed: std::time::Duration) {
    let ns = elapsed.as_nanos() as f64 / messages as f64;
    println!(
        "workload={workload} messages={messages} elapsed_ms={} ns_per_message={ns:.1}",
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
