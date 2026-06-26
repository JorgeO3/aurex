use smallvec::SmallVec;
use aurum_types::QueueId;

/// Declare an in-memory queue on a shard executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclareQueue {
    pub queue_id: QueueId,
}

impl DeclareQueue {
    #[must_use]
    pub fn new(queue_id: QueueId) -> Self {
        Self { queue_id }
    }
}

#[derive(Debug, Clone)]
pub struct DeclareQueueBatch {
    pub items: SmallVec<[DeclareQueue; 4]>,
}

impl DeclareQueueBatch {
    #[must_use]
    pub fn one(queue_id: QueueId) -> Self {
        let mut items = SmallVec::new();
        items.push(DeclareQueue::new(queue_id));
        Self { items }
    }
}
