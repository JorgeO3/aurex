use std::time::Instant;

use aurum_broker::InMemoryShardExecutor;
use aurum_internal_protocol::{
    batch::ShardEventBatch,
    command::{
        consume::{CancelConsumer, CancelConsumerBatch, ConsumeCommandBatch, ConsumeStart},
        publish::ShardPublishBatch,
        settlement::{AckCommandBatch, NackCommandBatch},
        shard::ShardCommandBatch,
    },
    event::{
        confirm::{ConsumerEventBatch, PublishConfirmBatch, SettlementResultBatch},
        delivery::DeliveryEventBatch,
        error::CommandErrorBatch,
    },
    flags::ConsumeFlags,
    sink::EventSink,
};
use aurum_types::{ChannelId, ConsumerId, DeliveryTag, PayloadHandle, QueueId};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn consumer_id() -> ConsumerId { ConsumerId(1) }
fn queue_id() -> QueueId { QueueId(0) }

fn consume_batch(prefetch: u32) -> ShardCommandBatch<PayloadHandle> {
    ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
        consumer_id(),
        ChannelId(0),
        queue_id(),
        prefetch,
    )))
}

fn publish_batch(n: u32) -> ShardCommandBatch<PayloadHandle> {
    ShardCommandBatch::Publish(ShardPublishBatch::contiguous(queue_id(), n))
}

fn last_delivery_tag(events: &[ShardEventBatch<PayloadHandle>]) -> Option<DeliveryTag> {
    let mut max: Option<DeliveryTag> = None;
    for ev in events {
        if let ShardEventBatch::Delivery(d) = ev {
            if let Some(tag) = d.last_tag() {
                max = Some(match max {
                    Some(prev) if prev.0 >= tag.0 => prev,
                    _ => tag,
                });
            }
        }
    }
    max
}

fn total_delivered(events: &[ShardEventBatch<PayloadHandle>]) -> usize {
    events.iter().filter_map(|e| {
        if let ShardEventBatch::Delivery(d) = e { Some(d.total_count()) } else { None }
    }).sum()
}

fn total_settled(events: &[ShardEventBatch<PayloadHandle>]) -> u32 {
    events.iter().filter_map(|e| {
        if let ShardEventBatch::Settlement(s) = e { Some(s.settled) } else { None }
    }).sum()
}

// ── BenchSink ─────────────────────────────────────────────────────────────────

/// Zero-allocation sink for the hot benchmark path.
/// Captures only the last delivery tag; ignores settlement/error events.
struct BenchSink {
    last_tag: Option<DeliveryTag>,
}

impl BenchSink {
    fn new() -> Self { Self { last_tag: None } }
    fn reset(&mut self) { self.last_tag = None; }
}

impl EventSink<PayloadHandle> for BenchSink {
    #[inline(always)]
    fn on_delivery(&mut self, b: DeliveryEventBatch<PayloadHandle>) {
        if let Some(t) = b.last_tag() {
            self.last_tag = Some(t);
        }
    }
    #[inline(always)]
    fn on_settlement(&mut self, _: SettlementResultBatch) {}
    #[inline(always)]
    fn on_confirm(&mut self, _: PublishConfirmBatch) {}
    #[inline(always)]
    fn on_consumer(&mut self, _: ConsumerEventBatch) {}
    #[inline(always)]
    fn on_error(&mut self, _: CommandErrorBatch) {}
}

// ── Scenarios ─────────────────────────────────────────────────────────────────

fn scenario_publish_consume_ack(n: u32) {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut events: Vec<ShardEventBatch<PayloadHandle>> = Vec::new();

    exec.execute(consume_batch(n), &mut events).unwrap();
    exec.execute(publish_batch(n), &mut events).unwrap();

    let delivered = total_delivered(&events);
    assert_eq!(delivered, n as usize, "expected {n} delivered, got {delivered}");

    let last = last_delivery_tag(&events).expect("no delivery");
    events.clear();
    exec.execute(
        ShardCommandBatch::Ack(AckCommandBatch::multiple(consumer_id(), last)),
        &mut events,
    ).unwrap();

    let settled = total_settled(&events);
    assert_eq!(settled, n, "expected {n} settled, got {settled}");

    let counts = exec.queue(queue_id()).unwrap().debug_counts();
    assert_eq!(counts.acked, n as u64, "queue should show all messages acked");
    assert_eq!(counts.inflight, 0, "no messages should remain inflight");
    println!("  publish_consume_ack({n}): ok — delivered={delivered} settled={settled}");
}

fn scenario_nack_requeue_redeliver(n: u32) {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut events: Vec<ShardEventBatch<PayloadHandle>> = Vec::new();

    exec.execute(consume_batch(n), &mut events).unwrap();
    exec.execute(publish_batch(n), &mut events).unwrap();

    let last = last_delivery_tag(&events).expect("no delivery");
    events.clear();

    exec.execute(
        ShardCommandBatch::Nack(NackCommandBatch::requeue_multiple(consumer_id(), last)),
        &mut events,
    ).unwrap();

    let settled = total_settled(&events);
    assert_eq!(settled, n, "expected {n} nacked");

    events.clear();
    exec.retry_all_and_deliver(&mut events);

    let redelivered = total_delivered(&events);
    assert_eq!(redelivered, n as usize, "expected {n} redelivered");

    let last2 = last_delivery_tag(&events).expect("no redelivery");
    events.clear();
    exec.execute(
        ShardCommandBatch::Ack(AckCommandBatch::multiple(consumer_id(), last2)),
        &mut events,
    ).unwrap();

    let acked = total_settled(&events);
    assert_eq!(acked, n, "expected {n} finally acked");
    println!("  nack_requeue_redeliver({n}): ok — settled={settled} redelivered={redelivered} acked={acked}");
}

fn scenario_cancel_requeue(n: u32) {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut events: Vec<ShardEventBatch<PayloadHandle>> = Vec::new();

    exec.execute(consume_batch(n), &mut events).unwrap();
    exec.execute(publish_batch(n), &mut events).unwrap();

    let delivered = total_delivered(&events);
    assert_eq!(delivered, n as usize);
    events.clear();

    exec.execute(
        ShardCommandBatch::Cancel(CancelConsumerBatch::one(CancelConsumer::requeue(consumer_id()))),
        &mut events,
    ).unwrap();

    exec.execute(consume_batch(n), &mut events).unwrap();
    exec.retry_all_and_deliver(&mut events);

    let redelivered = total_delivered(&events);
    assert_eq!(redelivered, n as usize, "expected {n} redelivered after cancel-requeue");
    println!("  cancel_requeue({n}): ok — delivered={delivered} redelivered={redelivered}");
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

fn bench_publish_consume_ack(messages: u64, batch_size: u32) {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut sink = BenchSink::new();

    exec.execute(
        ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart {
            consumer_id: consumer_id(),
            channel_id: ChannelId(0),
            queue_id: queue_id(),
            prefetch: 0,
            flags: ConsumeFlags::empty(),
        })),
        &mut sink,
    ).unwrap();

    let mut total_acked = 0u64;
    let start = Instant::now();

    let mut remaining = messages;
    while remaining > 0 {
        let this_batch = remaining.min(u64::from(batch_size)) as u32;
        remaining -= u64::from(this_batch);

        sink.reset();
        exec.execute(
            ShardCommandBatch::Publish(ShardPublishBatch::contiguous(queue_id(), this_batch)),
            &mut sink,
        ).unwrap();

        if let Some(last) = sink.last_tag {
            exec.execute(
                ShardCommandBatch::Ack(AckCommandBatch::multiple(consumer_id(), last)),
                &mut sink,
            ).unwrap();
            total_acked += u64::from(this_batch);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let ns = elapsed * 1e9 / messages as f64;
    println!(
        "  bench publish_consume_ack: messages={messages} batch={batch_size} \
         total_acked={total_acked} ns_per_msg={ns:.2}"
    );
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    println!("AurumMQ H3 — Internal Command Protocol");
    println!();

    println!("Correctness scenarios:");
    scenario_publish_consume_ack(100);
    scenario_publish_consume_ack(1024);
    scenario_nack_requeue_redeliver(100);
    scenario_nack_requeue_redeliver(512);
    scenario_cancel_requeue(64);
    println!();

    println!("Benchmarks:");
    bench_publish_consume_ack(1_048_576, 128);
    bench_publish_consume_ack(1_048_576, 256);
    bench_publish_consume_ack(1_048_576, 512);
}
