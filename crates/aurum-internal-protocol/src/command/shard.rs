use aurum_types::PayloadHandle;

use super::consume::{CancelConsumerBatch, ConsumeCommandBatch, CreditCommandBatch};
use super::control::DeclareQueueBatch;
use super::publish::ShardPublishBatch;
use super::settlement::{AckCommandBatch, NackCommandBatch, RejectCommandBatch};

/// Command batch already targeted to one shard/queue owner.  The enum
/// dispatch is once per batch, not once per message.
#[derive(Debug, Clone)]
pub enum ShardCommandBatch<P = PayloadHandle> {
    Declare(DeclareQueueBatch),
    Publish(ShardPublishBatch<P>),
    Consume(ConsumeCommandBatch),
    Credit(CreditCommandBatch),
    Ack(AckCommandBatch),
    Nack(NackCommandBatch),
    Reject(RejectCommandBatch),
    Cancel(CancelConsumerBatch),
}
