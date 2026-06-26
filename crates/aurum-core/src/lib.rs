#![forbid(unsafe_code)]

pub mod queue;
pub mod consumer;

pub use queue::{
    AckBatch, AckMask, AckRange, HybridRangeBlockQueue, InvariantKind, InvariantViolation,
    MessageState, MsgBlock, NackBatch, NackMask, NackRange, NackReason, QueueCounts, QueueError,
    QueueStats, MSGS_PER_BLOCK, WORDS_PER_BLOCK,
};

pub use consumer::{
    AckApplyResult, AckMode, AckRequest, CancelDisposition, CancelResult, ChannelId, ConsumerCredit,
    ConsumerError, ConsumerFlags, ConsumerId, ConsumerSession, DeliveredSegment, DeliveryFlags,
    DeliveryTag, DeliveryWindowOps, MaskSegment, NackApplyResult, NackMode, NackRequest,
    PrefetchMode, RangeSegment, RejectRequest, SegmentDeliveryWindow, SegmentFlags,
    SessionDeliveryBatch, TaggedDeliverySegment, TaggedMask, TaggedRange,
};

#[cfg(feature = "model")]
pub use queue::model::ModelQueue;

#[cfg(any(test, feature = "model"))]
pub use consumer::{ModelConsumerSession, ModelDelivery};
