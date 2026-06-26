use smallvec::SmallVec;
use aurum_types::{ChannelId, ConsumerId, QueueId};

use crate::flags::ConsumeFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeStart {
    pub consumer_id: ConsumerId,
    pub channel_id: ChannelId,
    pub queue_id: QueueId,
    pub prefetch: u32,
    pub flags: ConsumeFlags,
}

impl ConsumeStart {
    #[must_use]
    pub fn new(consumer_id: ConsumerId, channel_id: ChannelId, queue_id: QueueId, prefetch: u32) -> Self {
        Self { consumer_id, channel_id, queue_id, prefetch, flags: ConsumeFlags::empty() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditUpdate {
    pub consumer_id: ConsumerId,
    pub delta: i32,
    pub new_prefetch: Option<u32>,
}

impl CreditUpdate {
    #[must_use]
    pub fn delta(consumer_id: ConsumerId, delta: i32) -> Self {
        Self { consumer_id, delta, new_prefetch: None }
    }

    #[must_use]
    pub fn set_prefetch(consumer_id: ConsumerId, prefetch: u32) -> Self {
        Self { consumer_id, delta: 0, new_prefetch: Some(prefetch) }
    }
}

/// Maps to `aurum_core::CancelDisposition` but adds `DeadLetterUnacked`
/// as a protocol-level concept (placeholer; exectutor maps to Drop for PR4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CancelDispositionCommand {
    #[default]
    RequeueUnacked   = 0,
    DropUnacked      = 1,
    DeadLetterUnacked = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelConsumer {
    pub consumer_id: ConsumerId,
    pub disposition: CancelDispositionCommand,
}

impl CancelConsumer {
    #[must_use]
    pub fn requeue(consumer_id: ConsumerId) -> Self {
        Self { consumer_id, disposition: CancelDispositionCommand::RequeueUnacked }
    }

    #[must_use]
    pub fn drop_unacked(consumer_id: ConsumerId) -> Self {
        Self { consumer_id, disposition: CancelDispositionCommand::DropUnacked }
    }
}

/// Batch of `ConsumeStart` commands (usually just one per channel setup).
#[derive(Debug, Clone)]
pub struct ConsumeCommandBatch {
    pub items: SmallVec<[ConsumeStart; 4]>,
}

impl ConsumeCommandBatch {
    #[must_use]
    pub fn one(cmd: ConsumeStart) -> Self {
        let mut items = SmallVec::new();
        items.push(cmd);
        Self { items }
    }
}

#[derive(Debug, Clone)]
pub struct CreditCommandBatch {
    pub items: SmallVec<[CreditUpdate; 4]>,
}

#[derive(Debug, Clone)]
pub struct CancelConsumerBatch {
    pub items: SmallVec<[CancelConsumer; 4]>,
}

impl CancelConsumerBatch {
    #[must_use]
    pub fn one(cmd: CancelConsumer) -> Self {
        let mut items = SmallVec::new();
        items.push(cmd);
        Self { items }
    }
}
