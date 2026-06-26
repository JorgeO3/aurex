use smallvec::SmallVec;
use aurum_types::{ConsumerId, DeliveryTag};

use crate::flags::{AckBatchFlags, NackBatchFlags};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SettlementMode {
    /// Settle exactly the specified tag.
    One      = 0,
    /// Settle the tag and all unacknowledged tags issued before it.
    Multiple = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NackDisposition {
    Requeue    = 0,
    DeadLetter = 1,
    Drop       = 2,
}

// ── Ack ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckCommand {
    /// AMQP-native: ack one tag, or all tags up to and including this tag.
    Tag {
        tag: DeliveryTag,
        mode: SettlementMode,
    },
    /// Pre-coalesced range [start..=end] from an adapter coalescer.
    Range {
        start: DeliveryTag,
        end: DeliveryTag,
    },
    /// Sparse ack mask: each set bit i means ack the tag (base + i).
    Mask {
        base: DeliveryTag,
        mask: u64,
    },
}

impl AckCommand {
    #[must_use]
    pub fn one(tag: DeliveryTag) -> Self {
        Self::Tag { tag, mode: SettlementMode::One }
    }

    #[must_use]
    pub fn multiple(tag: DeliveryTag) -> Self {
        Self::Tag { tag, mode: SettlementMode::Multiple }
    }
}

#[derive(Debug, Clone)]
pub struct AckCommandBatch {
    pub consumer_id: ConsumerId,
    pub flags: AckBatchFlags,
    pub items: SmallVec<[AckCommand; 8]>,
}

impl AckCommandBatch {
    #[must_use]
    pub fn one(consumer_id: ConsumerId, tag: DeliveryTag) -> Self {
        let mut items = SmallVec::new();
        items.push(AckCommand::one(tag));
        Self { consumer_id, flags: AckBatchFlags::empty(), items }
    }

    #[must_use]
    pub fn multiple(consumer_id: ConsumerId, tag: DeliveryTag) -> Self {
        let mut items = SmallVec::new();
        items.push(AckCommand::multiple(tag));
        Self {
            consumer_id,
            flags: AckBatchFlags::MULTIPLE,
            items,
        }
    }
}

// ── Nack ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackCommand {
    Tag {
        tag: DeliveryTag,
        mode: SettlementMode,
        disposition: NackDisposition,
    },
    Range {
        start: DeliveryTag,
        end: DeliveryTag,
        disposition: NackDisposition,
    },
    Mask {
        base: DeliveryTag,
        mask: u64,
        disposition: NackDisposition,
    },
}

impl NackCommand {
    #[must_use]
    pub fn requeue_one(tag: DeliveryTag) -> Self {
        Self::Tag { tag, mode: SettlementMode::One, disposition: NackDisposition::Requeue }
    }

    #[must_use]
    pub fn requeue_multiple(tag: DeliveryTag) -> Self {
        Self::Tag { tag, mode: SettlementMode::Multiple, disposition: NackDisposition::Requeue }
    }
}

#[derive(Debug, Clone)]
pub struct NackCommandBatch {
    pub consumer_id: ConsumerId,
    pub flags: NackBatchFlags,
    pub items: SmallVec<[NackCommand; 8]>,
}

impl NackCommandBatch {
    #[must_use]
    pub fn requeue_multiple(consumer_id: ConsumerId, tag: DeliveryTag) -> Self {
        let mut items = SmallVec::new();
        items.push(NackCommand::requeue_multiple(tag));
        Self {
            consumer_id,
            flags: NackBatchFlags::MULTIPLE | NackBatchFlags::REQUEUE,
            items,
        }
    }
}

// ── Reject ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectCommand {
    pub tag: DeliveryTag,
    pub disposition: NackDisposition,
}

impl RejectCommand {
    #[must_use]
    pub fn drop_tag(tag: DeliveryTag) -> Self {
        Self { tag, disposition: NackDisposition::Drop }
    }

    #[must_use]
    pub fn dead_letter(tag: DeliveryTag) -> Self {
        Self { tag, disposition: NackDisposition::DeadLetter }
    }
}

#[derive(Debug, Clone)]
pub struct RejectCommandBatch {
    pub consumer_id: ConsumerId,
    pub items: SmallVec<[RejectCommand; 8]>,
}
