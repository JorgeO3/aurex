use aurum_types::PayloadHandle;

use crate::event::confirm::{ConsumerEventBatch, PublishConfirmBatch, SettlementResultBatch};
use crate::event::delivery::DeliveryEventBatch;
use crate::event::error::CommandErrorBatch;

/// All events emitted by a shard executor after processing a command batch.
#[derive(Debug, Clone)]
pub enum ShardEventBatch<P = PayloadHandle> {
    PublishConfirm(PublishConfirmBatch),
    Delivery(DeliveryEventBatch<P>),
    Settlement(SettlementResultBatch),
    Consumer(ConsumerEventBatch),
    Error(CommandErrorBatch),
}

/// Tag used for routing/logging without carrying the full batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandBatchKind {
    Publish = 0,
    Ack = 1,
    Nack = 2,
    Credit = 3,
    Consume = 4,
    Cancel = 5,
    ResolveRoute = 6,
    Reject = 7,
    Declare = 8,
}
