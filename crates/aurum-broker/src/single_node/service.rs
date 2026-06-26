use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aurum_internal_protocol::{
    command::{
        consume::ConsumeCommandBatch,
        ingress::IngressCommandBatch,
        shard::ShardCommandBatch,
    },
    event::delivery::DeliveryEventBatch,
};
use aurum_types::{ConsumerId, PayloadHandle};
use aurum_transport::ConnectionId;

use crate::in_memory::ShardOutputBatch;
use crate::single_node::broker::SingleNodeBroker;
use crate::single_node::config::SingleNodeBrokerConfig;
use crate::single_node::error::BrokerInitError;
use crate::single_node::lifecycle::ServerState;
use crate::single_node::registry::ConnectionRegistry;

pub type SharedBroker = Arc<Mutex<SingleNodeBroker>>;

/// Output split between immediate response and routed push deliveries.
#[derive(Debug, Default)]
pub struct RoutedOutput {
    pub immediate: ShardOutputBatch<PayloadHandle>,
    pub routed: Vec<(ConnectionId, ShardOutputBatch<PayloadHandle>)>,
}

#[derive(Debug)]
pub struct BrokerService {
    broker: SharedBroker,
    registry: Mutex<ConnectionRegistry>,
    outboxes: Mutex<HashMap<ConnectionId, Vec<ShardOutputBatch<PayloadHandle>>>>,
}

impl BrokerService {
    pub fn new(config: SingleNodeBrokerConfig) -> Result<Self, BrokerInitError> {
        let broker = Arc::new(Mutex::new(SingleNodeBroker::new(&config)?));
        Ok(Self {
            broker,
            registry: Mutex::new(ConnectionRegistry::default()),
            outboxes: Mutex::new(HashMap::new()),
        })
    }

    #[must_use]
    pub fn shared_broker(&self) -> SharedBroker {
        Arc::clone(&self.broker)
    }

    pub fn start(&self) {
        if let Ok(mut broker) = self.broker.lock() {
            broker.set_state(ServerState::Starting);
            broker.set_state(ServerState::Running);
        }
    }

    pub fn stop(&self) {
        if let Ok(mut broker) = self.broker.lock() {
            broker.set_state(ServerState::Draining);
            broker.set_state(ServerState::Stopped);
        }
    }

    pub fn record_connection_accepted(&self) {
        let broker = self.broker.lock().expect("broker lock");
        broker
            .metrics()
            .accepted_connections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        broker
            .metrics()
            .active_connections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_connection_closed(&self) {
        self.broker
            .lock()
            .expect("broker lock")
            .metrics()
            .active_connections
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_io(&self, bytes_in: u64, bytes_out: u64, frames_in: u64, frames_out: u64) {
        let broker = self.broker.lock().expect("broker lock");
        let metrics = broker.metrics();
        metrics
            .bytes_in
            .fetch_add(bytes_in, std::sync::atomic::Ordering::Relaxed);
        metrics
            .bytes_out
            .fetch_add(bytes_out, std::sync::atomic::Ordering::Relaxed);
        metrics
            .frames_in
            .fetch_add(frames_in, std::sync::atomic::Ordering::Relaxed);
        metrics
            .frames_out
            .fetch_add(frames_out, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn drain_connection_outputs(
        &self,
        connection_id: ConnectionId,
    ) -> ShardOutputBatch<PayloadHandle> {
        let mut outboxes = self.outboxes.lock().expect("outboxes lock");
        let batches = outboxes.remove(&connection_id).unwrap_or_default();
        let mut merged = ShardOutputBatch::default();
        for batch in batches {
            merge_output(&mut merged, batch);
        }
        merged
    }

    pub fn register_consumer(&self, consumer_id: ConsumerId, connection_id: ConnectionId) {
        self.registry
            .lock()
            .expect("registry lock")
            .register_consumer(consumer_id, connection_id);
    }

    pub fn unregister_consumer(&self, consumer_id: ConsumerId) {
        self.registry
            .lock()
            .expect("registry lock")
            .unregister_consumer(consumer_id);
    }

    pub fn execute_shard(
        &self,
        connection_id: ConnectionId,
        batch: ShardCommandBatch<PayloadHandle>,
    ) -> RoutedOutput {
        self.track_consumers_from_shard(connection_id, &batch);
        let output = self
            .broker
            .lock()
            .expect("broker lock")
            .execute_shard(batch);
        self.route_output(connection_id, output)
    }

    pub fn execute_ingress(
        &self,
        connection_id: ConnectionId,
        batch: IngressCommandBatch<PayloadHandle>,
    ) -> RoutedOutput {
        let output = self
            .broker
            .lock()
            .expect("broker lock")
            .execute_ingress(batch);
        self.route_output(connection_id, output)
    }

    fn track_consumers_from_shard(
        &self,
        connection_id: ConnectionId,
        batch: &ShardCommandBatch<PayloadHandle>,
    ) {
        if let ShardCommandBatch::Consume(c) = batch {
            for item in &c.items {
                self.register_consumer(item.consumer_id, connection_id);
            }
        }
        if let ShardCommandBatch::Cancel(c) = batch {
            for item in &c.items {
                self.unregister_consumer(item.consumer_id);
            }
        }
    }

    fn route_output(
        &self,
        source: ConnectionId,
        output: ShardOutputBatch<PayloadHandle>,
    ) -> RoutedOutput {
        if output.deliveries.is_empty() {
            return RoutedOutput {
                immediate: output,
                routed: Vec::new(),
            };
        }

        let registry = self.registry.lock().expect("registry lock");
        let mut immediate = ShardOutputBatch::default();
        let mut routed_map: HashMap<ConnectionId, ShardOutputBatch<PayloadHandle>> =
            HashMap::new();

        let mut other = output;
        for delivery in other.deliveries.drain(..) {
            let target = registry
                .connection_for_consumer(delivery.consumer_id)
                .unwrap_or(source);
            if target == source {
                immediate.deliveries.push(delivery);
            } else {
                routed_map
                    .entry(target)
                    .or_default()
                    .deliveries
                    .push(delivery);
            }
        }

        immediate.confirms = other.confirms;
        immediate.settlements = other.settlements;
        immediate.consumer_events = other.consumer_events;
        immediate.route_resolved = other.route_resolved;
        immediate.errors = other.errors;

        let mut routed = Vec::new();
        if !routed_map.is_empty() {
            let mut outboxes = self.outboxes.lock().expect("outboxes lock");
            for (conn, batch) in routed_map {
                outboxes.entry(conn).or_default().push(batch);
                routed.push((conn, ShardOutputBatch::default()));
            }
        }

        RoutedOutput { immediate, routed }
    }
}

fn merge_output(dst: &mut ShardOutputBatch<PayloadHandle>, src: ShardOutputBatch<PayloadHandle>) {
    dst.deliveries.extend(src.deliveries);
    dst.confirms.extend(src.confirms);
    dst.settlements.extend(src.settlements);
    dst.consumer_events.extend(src.consumer_events);
    dst.route_resolved.extend(src.route_resolved);
    dst.errors.extend(src.errors);
}

#[cfg(test)]
mod tests {
    use aurum_internal_protocol::command::{
        consume::{ConsumeCommandBatch, ConsumeStart},
        publish::{IngressPublishBatch, IngressPublishTarget, PublishRecord},
        shard::ShardCommandBatch,
    };
    use aurum_types::{BatchId, ChannelId, ConsumerId, PayloadHandle, QueueId, SourceId};

    use super::*;
    use crate::single_node::config::SingleNodeBrokerConfig;

    #[test]
    fn routes_delivery_to_consumer_connection() {
        let service = BrokerService::new(SingleNodeBrokerConfig::dev_defaults()).unwrap();
        let publisher = ConnectionId(1);
        let consumer_conn = ConnectionId(2);
        let queue_id = QueueId(1);

        service
            .broker
            .lock()
            .unwrap()
            .broker_mut()
            .shard_mut()
            .execute_batch(
                ShardCommandBatch::Declare(
                    aurum_internal_protocol::command::control::DeclareQueueBatch::one(queue_id),
                ),
                &mut ShardOutputBatch::default(),
            )
            .unwrap();

        service.execute_shard(
            consumer_conn,
            ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
                ConsumerId(9),
                ChannelId(1),
                queue_id,
                10,
            ))),
        );

        let routed = service.execute_ingress(
            publisher,
            IngressCommandBatch::Publish(IngressPublishBatch {
                batch_id: BatchId(1),
                source: SourceId(1),
                target: IngressPublishTarget::Queue(queue_id),
                flags: aurum_internal_protocol::flags::PublishFlags::empty(),
                confirm_mode: aurum_internal_protocol::command::publish::ConfirmMode::None,
                records: smallvec::smallvec![PublishRecord::simple(PayloadHandle(1), 4)],
            }),
        );
        assert!(routed.immediate.deliveries.is_empty());
        let pushed = service.drain_connection_outputs(consumer_conn);
        assert_eq!(pushed.total_delivered(), 1);
    }
}
