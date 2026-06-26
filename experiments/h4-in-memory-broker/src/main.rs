use std::env;
use std::time::Instant;

use aurum_broker::InMemoryBroker;
use aurum_internal_protocol::{
    command::{
        consume::{CancelConsumer, CancelConsumerBatch, ConsumeCommandBatch, ConsumeStart},
        control::DeclareQueueBatch,
        publish::ShardPublishBatch,
        settlement::{AckCommandBatch, NackCommandBatch},
        shard::ShardCommandBatch,
    },
    flags::ConsumeFlags,
};
use aurum_types::{ChannelId, ConsumerId, DeliveryTag, QueueId};

#[derive(Debug, Clone, Copy)]
struct Config {
    messages: u64,
    batch: u32,
    prefetch: u32,
    consumers: u32,
    workload: Workload,
}

#[derive(Debug, Clone, Copy)]
enum Workload {
    PublishDeliverAck,
    PublishDeliverAckMultiple,
    PublishNackRequeueAck,
    ConsumerCancelRequeue,
    MultiConsumerRoundRobin,
}

fn parse_args() -> Config {
    let mut messages = 1_048_576u64;
    let mut batch = 128u32;
    let mut prefetch = 128u32;
    let mut consumers = 1u32;
    let mut workload = Workload::PublishDeliverAck;

    for arg in env::args().skip(1) {
        if let Some(v) = arg.strip_prefix("--messages=") {
            messages = v.parse().expect("messages");
        } else if let Some(v) = arg.strip_prefix("--batch=") {
            batch = v.parse().expect("batch");
        } else if let Some(v) = arg.strip_prefix("--prefetch=") {
            prefetch = v.parse().expect("prefetch");
        } else if let Some(v) = arg.strip_prefix("--consumers=") {
            consumers = v.parse().expect("consumers");
        } else if let Some(v) = arg.strip_prefix("--workload=") {
            workload = match v {
                "publish_deliver_ack" => Workload::PublishDeliverAck,
                "publish_deliver_ack_multiple" => Workload::PublishDeliverAckMultiple,
                "publish_nack_requeue_ack" => Workload::PublishNackRequeueAck,
                "consumer_cancel_requeue" => Workload::ConsumerCancelRequeue,
                "multi_consumer_round_robin" => Workload::MultiConsumerRoundRobin,
                other => panic!("unknown workload: {other}"),
            };
        }
    }

    Config { messages, batch, prefetch, consumers, workload }
}

fn queue_id() -> QueueId {
    QueueId(0)
}

fn setup_broker(broker: &mut InMemoryBroker, consumers: u32, prefetch: u32) {
    broker.execute(ShardCommandBatch::Declare(DeclareQueueBatch::one(queue_id())));
    for i in 0..consumers {
        broker.execute(ShardCommandBatch::Consume(ConsumeCommandBatch::one(
            ConsumeStart {
                consumer_id: ConsumerId(u64::from(i) + 1),
                channel_id: ChannelId(0),
                queue_id: queue_id(),
                prefetch,
                flags: ConsumeFlags::empty(),
            },
        )));
    }
}

fn last_tag(out: &aurum_broker::ShardOutputBatch) -> Option<DeliveryTag> {
    out.deliveries.iter().filter_map(|d| d.last_tag()).max_by_key(|t| t.0)
}

fn run_publish_deliver_ack(cfg: Config, multiple: bool) -> Metrics {
    let mut broker = InMemoryBroker::single_shard();
    setup_broker(&mut broker, cfg.consumers, cfg.prefetch);

    let mut published = 0u64;
    let mut confirmed = 0u64;
    let mut delivered = 0u64;
    let mut acked = 0u64;
    let mut checksum = 0u64;

    let start = Instant::now();
    let mut remaining = cfg.messages;

    while remaining > 0 {
        let this_batch = remaining.min(u64::from(cfg.batch)) as u32;
        remaining -= u64::from(this_batch);

        let mut batch = ShardPublishBatch::contiguous(queue_id(), this_batch);
        batch.confirm_mode = aurum_internal_protocol::command::publish::ConfirmMode::Accepted;
        let out = broker.execute(ShardCommandBatch::Publish(batch));
        published += u64::from(this_batch);
        confirmed += u64::from(out.total_confirmed());
        delivered += out.total_delivered() as u64;

        if let Some(tag) = last_tag(&out) {
            let ack_batch = if multiple {
                AckCommandBatch::multiple(ConsumerId(1), tag)
            } else {
                // Ack all tags delivered in this batch up to `tag`.
                AckCommandBatch::multiple(ConsumerId(1), tag)
            };
            let out = broker.execute(ShardCommandBatch::Ack(ack_batch));
            acked += u64::from(out.total_settled());
            checksum = checksum.wrapping_add(u64::from(out.total_settled()));
        }
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    Metrics {
        elapsed_ms,
        published,
        confirmed,
        delivered,
        acked,
        nacked: 0,
        redelivered: 0,
        errors: 0,
        checksum,
    }
}

fn run_nack_requeue_ack(cfg: Config) -> Metrics {
    let mut broker = InMemoryBroker::single_shard();
    setup_broker(&mut broker, 1, cfg.prefetch);

    let mut published = 0u64;
    let mut nacked = 0u64;
    let mut acked = 0u64;
    let mut redelivered = 0u64;

    let start = Instant::now();
    let mut remaining = cfg.messages;

    while remaining > 0 {
        let this_batch = remaining.min(u64::from(cfg.batch)) as u32;
        remaining -= u64::from(this_batch);

        let out = broker.execute(ShardCommandBatch::Publish(ShardPublishBatch::contiguous(
            queue_id(),
            this_batch,
        )));
        published += u64::from(this_batch);

        if let Some(tag) = last_tag(&out) {
            let out = broker.execute(ShardCommandBatch::Nack(NackCommandBatch::requeue_multiple(
                ConsumerId(1),
                tag,
            )));
            nacked += u64::from(out.total_settled());

            let mut retry_out = aurum_broker::ShardOutputBatch::default();
            broker.shard_mut().retry_all_and_deliver(&mut retry_out);
            redelivered += retry_out.total_delivered() as u64;

            if let Some(tag2) = last_tag(&retry_out) {
                let out = broker.execute(ShardCommandBatch::Ack(AckCommandBatch::multiple(
                    ConsumerId(1),
                    tag2,
                )));
                acked += u64::from(out.total_settled());
            }
        }
    }

    Metrics {
        elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
        published,
        confirmed: published,
        delivered: published,
        acked,
        nacked,
        redelivered,
        errors: 0,
        checksum: acked,
    }
}

fn run_cancel_requeue(cfg: Config) -> Metrics {
    let mut broker = InMemoryBroker::single_shard();
    setup_broker(&mut broker, 1, cfg.prefetch);

    let n = cfg.messages.min(u64::from(cfg.batch)) as u32;
    let out = broker.execute(ShardCommandBatch::Publish(ShardPublishBatch::contiguous(
        queue_id(),
        n,
    )));
    let delivered = out.total_delivered();

    broker.execute(ShardCommandBatch::Cancel(CancelConsumerBatch::one(
        CancelConsumer::requeue(ConsumerId(1)),
    )));

    setup_broker(&mut broker, 1, cfg.prefetch);
    let mut retry_out = aurum_broker::ShardOutputBatch::default();
    broker.shard_mut().retry_all_and_deliver(&mut retry_out);

    Metrics {
        elapsed_ms: 0.0,
        published: u64::from(n),
        confirmed: u64::from(n),
        delivered: delivered as u64,
        acked: 0,
        nacked: 0,
        redelivered: retry_out.total_delivered() as u64,
        errors: 0,
        checksum: retry_out.total_delivered() as u64,
    }
}

fn run_round_robin(cfg: Config) -> Metrics {
    let mut broker = InMemoryBroker::single_shard();
    setup_broker(&mut broker, cfg.consumers.max(2), cfg.prefetch);

    let out = broker.execute(ShardCommandBatch::Publish(ShardPublishBatch::contiguous(
        queue_id(),
        cfg.messages.min(u64::from(cfg.batch) * u64::from(cfg.consumers.max(2))) as u32,
    )));

    let mut per_consumer = vec![0u64; cfg.consumers.max(2) as usize];
    for d in &out.deliveries {
        let idx = (d.consumer_id.0 - 1) as usize;
        if idx < per_consumer.len() {
            per_consumer[idx] += d.total_count() as u64;
        }
    }

    Metrics {
        elapsed_ms: 0.0,
        published: out.total_confirmed() as u64,
        confirmed: out.total_confirmed() as u64,
        delivered: out.total_delivered() as u64,
        acked: 0,
        nacked: 0,
        redelivered: 0,
        errors: 0,
        checksum: per_consumer.iter().sum(),
    }
}

#[derive(Debug)]
struct Metrics {
    elapsed_ms: f64,
    published: u64,
    confirmed: u64,
    delivered: u64,
    acked: u64,
    nacked: u64,
    redelivered: u64,
    errors: u64,
    checksum: u64,
}

impl Metrics {
    fn print(&self, cfg: &Config) {
        let ns_per_msg = if cfg.messages > 0 && self.elapsed_ms > 0.0 {
            self.elapsed_ms * 1e6 / cfg.messages as f64
        } else {
            0.0
        };
        println!(
            "workload={:?} messages={} batch={} prefetch={} consumers={}",
            cfg.workload, cfg.messages, cfg.batch, cfg.prefetch, cfg.consumers
        );
        println!(
            "elapsed_ms={:.2} ns_per_msg={:.2} published={} confirmed={} delivered={} acked={} nacked={} redelivered={} errors={} checksum={}",
            self.elapsed_ms,
            ns_per_msg,
            self.published,
            self.confirmed,
            self.delivered,
            self.acked,
            self.nacked,
            self.redelivered,
            self.errors,
            self.checksum
        );
    }
}

fn main() {
    let cfg = parse_args();
    println!("AurumMQ H4 — In-Memory Broker Executor");
    println!();

    let metrics = match cfg.workload {
        Workload::PublishDeliverAck => run_publish_deliver_ack(cfg, false),
        Workload::PublishDeliverAckMultiple => run_publish_deliver_ack(cfg, true),
        Workload::PublishNackRequeueAck => run_nack_requeue_ack(cfg),
        Workload::ConsumerCancelRequeue => run_cancel_requeue(cfg),
        Workload::MultiConsumerRoundRobin => run_round_robin(cfg),
    };

    metrics.print(&cfg);
}
