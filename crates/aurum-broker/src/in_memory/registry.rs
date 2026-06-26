use std::collections::HashMap;

use aurum_core::{ConsumerSession, HybridRangeBlockQueue, SessionDeliveryBatch};
use aurum_types::{ChannelId, ConsumerId, PayloadHandle, QueueId};

use super::flags::{ConsumerRuntimeFlags, QueueRuntimeFlags};

#[derive(Debug)]
pub struct QueueState {
    pub id: QueueId,
    pub queue: HybridRangeBlockQueue,
    pub consumers: Vec<ConsumerId>,
    pub next_consumer_index: usize,
    pub flags: QueueRuntimeFlags,
    /// Maps queue sequence → payload handle for the last published messages.
    pub seq_payloads: HashMap<u64, PayloadHandle>,
}

#[derive(Debug, Default)]
pub struct QueueRegistry {
    queues: HashMap<QueueId, QueueState>,
}

impl QueueRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_queue(&mut self, queue_id: QueueId) -> Result<(), QueueRegistryError> {
        use std::collections::hash_map::Entry;
        match self.queues.entry(queue_id) {
            Entry::Vacant(e) => {
                e.insert(QueueState {
                    id: queue_id,
                    queue: HybridRangeBlockQueue::empty(),
                    consumers: Vec::new(),
                    next_consumer_index: 0,
                    flags: QueueRuntimeFlags::ACTIVE,
                    seq_payloads: HashMap::new(),
                });
                Ok(())
            }
            Entry::Occupied(_) => Err(QueueRegistryError::Duplicate),
        }
    }

    #[must_use]
    pub fn contains(&self, queue_id: QueueId) -> bool {
        self.queues.contains_key(&queue_id)
    }

    #[must_use]
    pub fn get(&self, queue_id: QueueId) -> Option<&QueueState> {
        self.queues.get(&queue_id)
    }

    pub fn get_mut(&mut self, queue_id: QueueId) -> Option<&mut QueueState> {
        self.queues.get_mut(&queue_id)
    }

    pub fn attach_consumer(
        &mut self,
        queue_id: QueueId,
        consumer_id: ConsumerId,
    ) -> Result<(), QueueRegistryError> {
        let queue = self.queues.get_mut(&queue_id).ok_or(QueueRegistryError::NotFound)?;
        if !queue.consumers.contains(&consumer_id) {
            queue.consumers.push(consumer_id);
        }
        Ok(())
    }

    pub fn detach_consumer(&mut self, queue_id: QueueId, consumer_id: ConsumerId) {
        if let Some(queue) = self.queues.get_mut(&queue_id) {
            queue.consumers.retain(|id| *id != consumer_id);
            if queue.next_consumer_index >= queue.consumers.len() {
                queue.next_consumer_index = 0;
            }
        }
    }

    #[must_use]
    pub fn queue_ids(&self) -> Vec<QueueId> {
        self.queues.keys().copied().collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueRegistryError {
    NotFound,
    Duplicate,
}

#[derive(Debug)]
pub struct ConsumerRuntimeState {
    pub queue_id: QueueId,
    pub channel_id: ChannelId,
    pub session: ConsumerSession,
    pub out: SessionDeliveryBatch,
    pub flags: ConsumerRuntimeFlags,
}

#[derive(Debug, Default)]
pub struct ConsumerRegistry {
    consumers: HashMap<ConsumerId, ConsumerRuntimeState>,
}

impl ConsumerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        consumer_id: ConsumerId,
        state: ConsumerRuntimeState,
    ) -> Result<(), ConsumerRegistryError> {
        use std::collections::hash_map::Entry;
        match self.consumers.entry(consumer_id) {
            Entry::Vacant(e) => {
                e.insert(state);
                Ok(())
            }
            Entry::Occupied(_) => Err(ConsumerRegistryError::Duplicate),
        }
    }

    #[must_use]
    pub fn contains(&self, consumer_id: ConsumerId) -> bool {
        self.consumers.contains_key(&consumer_id)
    }

    #[must_use]
    pub fn get(&self, consumer_id: ConsumerId) -> Option<&ConsumerRuntimeState> {
        self.consumers.get(&consumer_id)
    }

    pub fn get_mut(&mut self, consumer_id: ConsumerId) -> Option<&mut ConsumerRuntimeState> {
        self.consumers.get_mut(&consumer_id)
    }

    #[must_use]
    pub fn queue_id(&self, consumer_id: ConsumerId) -> Option<QueueId> {
        self.consumers.get(&consumer_id).map(|c| c.queue_id)
    }

    pub fn remove(&mut self, consumer_id: ConsumerId) -> Option<ConsumerRuntimeState> {
        self.consumers.remove(&consumer_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerRegistryError {
    NotFound,
    Duplicate,
}
