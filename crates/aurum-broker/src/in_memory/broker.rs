use std::sync::Arc;

use aurum_internal_protocol::command::shard::ShardCommandBatch;
use aurum_routing::RouteTable;
use aurum_types::{PayloadHandle, RouteTableVersion, ShardId};

use super::output::ShardOutputBatch;
use super::shard::InMemoryShardExecutor;

/// Single-shard in-memory broker facade for tests and experiments.
#[derive(Debug)]
pub struct InMemoryBroker {
    pub(crate) route_table: Arc<RouteTable>,
    shard: InMemoryShardExecutor,
}

impl InMemoryBroker {
    #[must_use]
    pub fn single_shard() -> Self {
        Self::with_route_table(Arc::new(RouteTable::new_empty(RouteTableVersion::INITIAL)))
    }

    #[must_use]
    pub fn with_route_table(route_table: Arc<RouteTable>) -> Self {
        Self {
            route_table,
            shard: InMemoryShardExecutor::new(ShardId(0)),
        }
    }

    #[must_use]
    pub fn shard(&self) -> &InMemoryShardExecutor {
        &self.shard
    }

    pub fn shard_mut(&mut self) -> &mut InMemoryShardExecutor {
        &mut self.shard
    }

    pub fn execute(
        &mut self,
        batch: ShardCommandBatch<PayloadHandle>,
    ) -> ShardOutputBatch<PayloadHandle> {
        let mut out = ShardOutputBatch::default();
        let _ = self.shard.execute_batch(batch, &mut out);
        out
    }
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        Self::single_shard()
    }
}
