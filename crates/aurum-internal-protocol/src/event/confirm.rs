use aurum_types::{BatchId, ConsumerId, SourceId};

use crate::command::publish::ConfirmMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishConfirmBatch {
    pub source: SourceId,
    pub batch_id: BatchId,
    pub accepted: u32,
    pub first_seq: Option<u64>,
    pub mode: ConfirmMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SettlementKind {
    #[default]
    Ack = 0,
    Nack = 1,
    Reject = 2,
    Cancel = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementResultBatch {
    pub consumer_id: ConsumerId,
    pub kind: SettlementKind,
    pub settled: u32,
}

impl SettlementResultBatch {
    #[must_use]
    pub fn ack(consumer_id: ConsumerId, settled: u32) -> Self {
        Self { consumer_id, kind: SettlementKind::Ack, settled }
    }

    #[must_use]
    pub fn nack(consumer_id: ConsumerId, settled: u32) -> Self {
        Self { consumer_id, kind: SettlementKind::Nack, settled }
    }

    #[must_use]
    pub fn reject(consumer_id: ConsumerId, settled: u32) -> Self {
        Self { consumer_id, kind: SettlementKind::Reject, settled }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConsumerEventKind {
    Started         = 0,
    Cancelled       = 1,
    CreditExhausted = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerEventBatch {
    pub consumer_id: ConsumerId,
    pub kind: ConsumerEventKind,
}
