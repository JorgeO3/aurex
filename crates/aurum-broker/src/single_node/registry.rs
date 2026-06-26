use std::collections::HashMap;

use aurum_types::ConsumerId;
use aurum_transport::ConnectionId;

#[derive(Debug, Default)]
pub struct ConnectionRegistry {
    by_consumer: HashMap<ConsumerId, ConnectionId>,
}

impl ConnectionRegistry {
    pub fn register_consumer(&mut self, consumer_id: ConsumerId, connection_id: ConnectionId) {
        self.by_consumer.insert(consumer_id, connection_id);
    }

    pub fn unregister_consumer(&mut self, consumer_id: ConsumerId) {
        self.by_consumer.remove(&consumer_id);
    }

    #[must_use]
    pub fn connection_for_consumer(&self, consumer_id: ConsumerId) -> Option<ConnectionId> {
        self.by_consumer.get(&consumer_id).copied()
    }
}
