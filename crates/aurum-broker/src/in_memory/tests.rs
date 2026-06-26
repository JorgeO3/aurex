use aurum_internal_protocol::{
    command::{
        consume::{CancelConsumer, CancelConsumerBatch, ConsumeCommandBatch, ConsumeStart},
        control::DeclareQueueBatch,
        publish::ShardPublishBatch,
        settlement::{AckCommandBatch, NackCommandBatch},
        shard::ShardCommandBatch,
    },
    event::{
        confirm::SettlementKind,
        error::CommandErrorKind,
    },
    flags::ConsumeFlags,
};
use aurum_types::{ChannelId, ConsumerId, DeliveryTag, PayloadHandle, QueueId};

use crate::in_memory::{InMemoryBroker, InMemoryShardExecutor, ShardOutputBatch};

fn queue_id() -> QueueId {
    QueueId(0)
}

fn consumer_id() -> ConsumerId {
    ConsumerId(1)
}

fn setup_queue_consumer(exec: &mut InMemoryShardExecutor, prefetch: u32) -> ShardOutputBatch {
    let mut out = ShardOutputBatch::default();
    if !exec.queues().contains(queue_id()) {
        exec.execute_batch(
            ShardCommandBatch::Declare(DeclareQueueBatch::one(queue_id())),
            &mut out,
        )
        .unwrap();
        out.clear();
    }
    exec.execute_batch(
        ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
            consumer_id(),
            ChannelId(0),
            queue_id(),
            prefetch,
        ))),
        &mut out,
    )
    .unwrap();
    out
}

fn publish_batch(n: u32) -> ShardCommandBatch<PayloadHandle> {
    ShardCommandBatch::Publish(ShardPublishBatch::contiguous(queue_id(), n))
}

fn last_delivery_tag(out: &ShardOutputBatch) -> Option<DeliveryTag> {
    out.deliveries.iter().filter_map(|d| d.last_tag()).max_by_key(|t| t.0)
}

#[test]
fn executor_starts_empty() {
    let exec = InMemoryShardExecutor::new(aurum_types::ShardId(0));
    assert!(exec.queues().queue_ids().is_empty());
}

#[test]
fn unknown_queue_publish_returns_error() {
    let mut exec = InMemoryShardExecutor::new(aurum_types::ShardId(0));
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(publish_batch(10), &mut out).unwrap();
    assert_eq!(out.errors.len(), 1);
    assert_eq!(out.errors[0].kind, CommandErrorKind::QueueNotFound);
}

#[test]
fn create_queue_duplicate_errors() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(
        ShardCommandBatch::Declare(DeclareQueueBatch::one(queue_id())),
        &mut out,
    )
    .unwrap();
    assert_eq!(out.errors.len(), 1);
    assert_eq!(out.errors[0].kind, CommandErrorKind::DuplicateQueue);
}

#[test]
fn publish_to_existing_queue_increases_ready_count() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(publish_batch(100), &mut out).unwrap();
    let counts = exec.queue(queue_id()).unwrap().debug_counts();
    assert_eq!(counts.published, 100);
}

#[test]
fn publish_confirm_count_matches_message_count() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut batch = ShardPublishBatch::contiguous(queue_id(), 50);
    batch.confirm_mode = aurum_internal_protocol::command::publish::ConfirmMode::Accepted;
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(ShardCommandBatch::Publish(batch), &mut out)
        .unwrap();
    assert_eq!(out.total_confirmed(), 50);
}

#[test]
fn publish_triggers_delivery_when_consumer_has_credit() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = setup_queue_consumer(&mut exec, 128);
    out.clear();
    exec.execute_batch(publish_batch(64), &mut out).unwrap();
    assert_eq!(out.total_delivered(), 64);
}

#[test]
fn prefetch_limits_delivery() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = setup_queue_consumer(&mut exec, 16);
    out.clear();
    exec.execute_batch(publish_batch(100), &mut out).unwrap();
    assert_eq!(out.total_delivered(), 16);
}

#[test]
fn ack_one_releases_credit_and_triggers_more_delivery() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = setup_queue_consumer(&mut exec, 16);
    out.clear();
    exec.execute_batch(publish_batch(32), &mut out).unwrap();
    assert_eq!(out.total_delivered(), 16);
    let tag = last_delivery_tag(&out).unwrap();
    out.clear();
    exec.execute_batch(
        ShardCommandBatch::Ack(AckCommandBatch::one(consumer_id(), tag)),
        &mut out,
    )
    .unwrap();
    assert_eq!(out.settlements[0].kind, SettlementKind::Ack);
    assert!(out.total_delivered() >= 1);
}

#[test]
fn ack_multiple_settles_all_tags_up_to_tag() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = setup_queue_consumer(&mut exec, 0);
    out.clear();
    exec.execute_batch(publish_batch(100), &mut out).unwrap();
    let tag = last_delivery_tag(&out).unwrap();
    out.clear();
    exec.execute_batch(
        ShardCommandBatch::Ack(AckCommandBatch::multiple(consumer_id(), tag)),
        &mut out,
    )
    .unwrap();
    assert_eq!(out.total_settled(), 100);
}

#[test]
fn nack_requeue_redelivers() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = setup_queue_consumer(&mut exec, 100);
    out.clear();
    exec.execute_batch(publish_batch(50), &mut out).unwrap();
    let tag = last_delivery_tag(&out).unwrap();
    out.clear();
    exec.execute_batch(
        ShardCommandBatch::Nack(NackCommandBatch::requeue_multiple(consumer_id(), tag)),
        &mut out,
    )
    .unwrap();
    exec.retry_all_and_deliver(&mut out);
    assert!(out.total_delivered() >= 50);
}

#[test]
fn cancel_requeue_unacked_redelivers() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = setup_queue_consumer(&mut exec, 64);
    out.clear();
    exec.execute_batch(publish_batch(64), &mut out).unwrap();
    out.clear();
    exec.execute_batch(
        ShardCommandBatch::Cancel(CancelConsumerBatch::one(CancelConsumer::requeue(consumer_id()))),
        &mut out,
    )
    .unwrap();
    out.clear();
    let mut out2 = setup_queue_consumer(&mut exec, 64);
    exec.retry_all_and_deliver(&mut out2);
    assert_eq!(out2.total_delivered(), 64);
}

#[test]
fn unknown_consumer_ack_returns_error() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(
        ShardCommandBatch::Ack(AckCommandBatch::one(ConsumerId(99), DeliveryTag(1))),
        &mut out,
    )
    .unwrap();
    assert_eq!(out.errors[0].kind, CommandErrorKind::ConsumerNotFound);
}

#[test]
fn in_memory_broker_facade_execute() {
    let mut broker = InMemoryBroker::single_shard();
    let mut batch: ShardPublishBatch<PayloadHandle> = ShardPublishBatch::contiguous(queue_id(), 10);
    batch.confirm_mode = aurum_internal_protocol::command::publish::ConfirmMode::Accepted;
    let out = broker.execute(ShardCommandBatch::Declare(DeclareQueueBatch::one(queue_id())));
    assert!(out.errors.is_empty());
    let out = broker.execute(ShardCommandBatch::Consume(ConsumeCommandBatch::one(
        ConsumeStart {
            consumer_id: consumer_id(),
            channel_id: ChannelId(0),
            queue_id: queue_id(),
            prefetch: 0,
            flags: ConsumeFlags::empty(),
        },
    )));
    assert!(out.consumer_events.len() == 1);
}

#[test]
fn round_robin_two_consumers() {
    let mut exec = InMemoryShardExecutor::single_queue();
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(
        ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
            ConsumerId(1),
            ChannelId(0),
            queue_id(),
            10,
        ))),
        &mut out,
    )
    .unwrap();
    exec.execute_batch(
        ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
            ConsumerId(2),
            ChannelId(0),
            queue_id(),
            10,
        ))),
        &mut out,
    )
    .unwrap();
    out.clear();
    exec.execute_batch(publish_batch(20), &mut out).unwrap();
    let c1: usize = out.deliveries.iter().filter(|d| d.consumer_id == ConsumerId(1)).map(|d| d.total_count()).sum();
    let c2: usize = out.deliveries.iter().filter(|d| d.consumer_id == ConsumerId(2)).map(|d| d.total_count()).sum();
    assert_eq!(c1 + c2, 20);
    assert!(c1 > 0 && c2 > 0);
}
