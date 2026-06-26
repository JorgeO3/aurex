use aurum_types::{BatchId, PayloadHandle, QueueId, RouteId, SourceId};
use smallvec::SmallVec;

use crate::flags::{MessageFlags, PublishFlags};
use crate::route::RoutePublishTarget;

/// Destination before routing/shard-ownership resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressPublishTarget {
    Route(RoutePublishTarget),
    Queue(QueueId),
    /// Exchange + routing key resolved by the routing layer (cold/warm path).
    ExchangeKey { exchange_id: aurum_types::ExchangeId, routing_key_hash: u64 },
}

/// Destination after routing — always a concrete queue on a known shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardPublishTarget {
    pub queue_id: QueueId,
    pub route_id: Option<RouteId>,
}

/// Whether the publisher expects a confirm and at what durability level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ConfirmMode {
    #[default]
    None = 0,
    Accepted = 1,
    LocalDurable = 2,
    Quorum = 3,
}

/// A single message inside a publish batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishRecord<P = PayloadHandle> {
    pub payload: P,
    pub payload_len: u32,
    pub message_flags: MessageFlags,
    pub priority: u8,
    pub expiration_ms: Option<u32>,
    pub key_hash: u32,
}

impl<P: Default> PublishRecord<P> {
    #[must_use]
    pub fn simple(payload: P, len: u32) -> Self {
        Self {
            payload,
            payload_len: len,
            message_flags: MessageFlags::empty(),
            priority: 0,
            expiration_ms: None,
            key_hash: 0,
        }
    }
}

/// Publish batch at ingress (pre-routing).
#[derive(Debug, Clone)]
pub struct IngressPublishBatch<P = PayloadHandle> {
    pub batch_id: BatchId,
    pub source: SourceId,
    pub target: IngressPublishTarget,
    pub flags: PublishFlags,
    pub confirm_mode: ConfirmMode,
    pub records: SmallVec<[PublishRecord<P>; 16]>,
}

/// Publish batch after routing (targeted to one queue/shard).
#[derive(Debug, Clone)]
pub struct ShardPublishBatch<P = PayloadHandle> {
    pub batch_id: BatchId,
    pub source: SourceId,
    pub queue_id: QueueId,
    pub route_id: Option<RouteId>,
    pub flags: PublishFlags,
    pub confirm_mode: ConfirmMode,
    /// Fast path: if non-zero, executor uses this count and ignores `records`.
    /// Allows callers to bypass constructing `records` when payload metadata
    /// is handled by a separate zero-copy buffer (ring buffer, io_uring, etc.).
    pub record_count: u32,
    pub records: SmallVec<[PublishRecord<P>; 16]>,
}

impl<P: Default + Clone> ShardPublishBatch<P> {
    #[must_use]
    pub fn new(queue_id: QueueId, records: SmallVec<[PublishRecord<P>; 16]>) -> Self {
        Self {
            batch_id: BatchId::default(),
            source: SourceId::default(),
            queue_id,
            route_id: None,
            flags: PublishFlags::empty(),
            confirm_mode: ConfirmMode::None,
            record_count: 0,
            records,
        }
    }

    /// Fast path: publish `count` messages without constructing individual records.
    #[must_use]
    pub fn contiguous(queue_id: QueueId, count: u32) -> Self {
        Self {
            batch_id: BatchId::default(),
            source: SourceId::default(),
            queue_id,
            route_id: None,
            flags: PublishFlags::empty(),
            confirm_mode: ConfirmMode::None,
            record_count: count,
            records: SmallVec::new(),
        }
    }

    /// Effective message count: `record_count` if set, else `records.len()`.
    #[must_use]
    pub fn count(&self) -> u32 {
        if self.record_count > 0 { self.record_count } else { self.records.len() as u32 }
    }
}
