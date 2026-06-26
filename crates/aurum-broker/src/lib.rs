#![forbid(unsafe_code)]

use std::sync::Arc;

use aurum_core::HybridRangeBlockQueue;
use aurum_routing::RouteTable;
use aurum_types::RouteTableVersion;

pub mod in_memory;
pub mod single_node;

pub use in_memory::{
    amqp_transcript, AmqpInMemoryHarness, InMemoryBroker, InMemoryShardExecutor, NativeInMemoryHarness,
    ShardOutputBatch,
};

pub use single_node::{
    AmqpServerSession, BrokerMode, BrokerServer, BrokerService, ListenerEndpointConfig,
    NativeServerSession, SingleNodeBroker, SingleNodeBrokerConfig, StorageBackendKind,
};
pub mod shard_executor {
    pub use crate::in_memory::{InMemoryShardExecutor, ShardOutputBatch};
}

#[derive(Debug)]
pub struct BrokerPrototype {
    pub route_table: Arc<RouteTable>,
    pub queue: HybridRangeBlockQueue,
}

impl BrokerPrototype {
    #[must_use]
    pub fn single_queue(messages: u64) -> Self {
        Self {
            route_table: Arc::new(RouteTable::new_empty(RouteTableVersion::INITIAL)),
            queue: HybridRangeBlockQueue::with_messages(messages),
        }
    }
}
