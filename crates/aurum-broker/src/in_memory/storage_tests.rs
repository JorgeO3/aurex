use aurum_internal_protocol::{
    command::{
        consume::{ConsumeCommandBatch, ConsumeStart},
        control::DeclareQueueBatch,
        publish::ShardPublishBatch,
        settlement::AckCommandBatch,
        shard::ShardCommandBatch,
    },
};
use aurum_types::{ChannelId, ConsumerId, DeliveryTag, PayloadHandle, QueueId, ShardId};
use tempfile::tempdir;

use crate::in_memory::{AppendOnlyShardStorage, InMemoryShardExecutor, ShardOutputBatch};

#[test]
fn durable_publish_ack_recover() {
    let dir = tempdir().unwrap();
    let storage = AppendOnlyShardStorage::open(dir.path()).unwrap();
    let mut exec = InMemoryShardExecutor::with_durable_storage(ShardId(0), storage);

    let q = QueueId(7);
    let mut out = ShardOutputBatch::default();
    exec.execute_batch(
        ShardCommandBatch::Declare(DeclareQueueBatch::one(q)),
        &mut out,
    )
    .unwrap();
    exec.execute_batch(
        ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
            ConsumerId(1),
            ChannelId(0),
            q,
            64,
        ))),
        &mut out,
    )
    .unwrap();

    exec.execute_batch(
        ShardCommandBatch::Publish(ShardPublishBatch::contiguous(q, 4)),
        &mut out,
    )
    .unwrap();
    assert_eq!(out.total_delivered(), 4);

    let tag = out.deliveries[0]
        .segments
        .first()
        .expect("delivery")
        .first_tag();
    exec.execute_batch(
        ShardCommandBatch::Ack(AckCommandBatch::multiple(ConsumerId(1), tag)),
        &mut out,
    )
    .unwrap();

    let ready_before = exec.queue(q).unwrap().sequential_ready_len();
    assert_eq!(ready_before, 0);

    let mut exec2 = InMemoryShardExecutor::with_durable_storage(
        ShardId(0),
        AppendOnlyShardStorage::open(dir.path()).unwrap(),
    );
    exec2
        .execute_batch(ShardCommandBatch::Declare(DeclareQueueBatch::one(q)), &mut out)
        .unwrap();
    let recovered = exec2.recover_queue_from_storage(q).unwrap();
    assert_eq!(recovered, 3);
    assert_eq!(exec2.queue(q).unwrap().sequential_ready_len(), 3);
}
