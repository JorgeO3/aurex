use aurum_types::{ExchangeId, QueueId, RoutingKeyHash, ShardId};

use crate::flags::BindingFlags;
use crate::hash::fnv1a64;

#[derive(Debug, Clone)]
pub struct BindingDecl {
    pub exchange_id: ExchangeId,
    pub queue_id: QueueId,
    pub routing_key: String,
    pub flags: BindingFlags,
    pub target_shard: ShardId,
}

impl BindingDecl {
    #[must_use]
    pub fn direct(
        exchange_id: ExchangeId,
        queue_id: QueueId,
        routing_key: impl Into<String>,
    ) -> Self {
        Self {
            exchange_id,
            queue_id,
            routing_key: routing_key.into(),
            flags: BindingFlags::ACTIVE,
            target_shard: ShardId(0),
        }
    }

    #[must_use]
    pub fn fanout(exchange_id: ExchangeId, queue_id: QueueId) -> Self {
        Self {
            exchange_id,
            queue_id,
            routing_key: String::new(),
            flags: BindingFlags::ACTIVE,
            target_shard: ShardId(0),
        }
    }

    #[must_use]
    pub fn routing_hash(&self) -> RoutingKeyHash {
        RoutingKeyHash(fnv1a64(self.routing_key.as_bytes()))
    }
}
