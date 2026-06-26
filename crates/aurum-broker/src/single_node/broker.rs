use std::collections::HashMap;
use std::sync::Arc;

use aurum_internal_protocol::{
    command::{
        control::DeclareQueueBatch,
        ingress::IngressCommandBatch,
        shard::ShardCommandBatch,
    },
    event::delivery::DeliveryMetadata,
};
use aurum_protocol_amqp::{
    AmqpControlCommand, AmqpControlResult, AmqpRouteResolveRequest, AmqpRouteResolveResult,
    BasicProperties, RouteCacheEntry,
};
use aurum_routing::{
    BindingDecl, ExchangeDecl, ExchangeKind, RouteCompiler, RouteTable, RoutingConfig,
};
use aurum_types::{ExchangeId, PayloadHandle, QueueId, RouteTableVersion, ShardId};
use bytes::Bytes;

use crate::in_memory::{
    broker::InMemoryBroker, output::ShardOutputBatch, storage::AppendOnlyShardStorage,
};
use crate::single_node::config::{
    BindingBootstrap, BrokerMode, ExchangeBootstrap, QueueBootstrap, RoutingBootstrapConfig,
    SingleNodeBrokerConfig, StorageBackendKind,
};
use crate::single_node::error::BrokerInitError;
use crate::single_node::lifecycle::{BrokerHealth, ServerState};
use crate::single_node::metrics::BrokerMetrics;

/// Single-node broker service: routing + in-memory shard executor + optional storage.
#[derive(Debug)]
pub struct SingleNodeBroker {
    state: ServerState,
    broker: InMemoryBroker,
    routing_config: RoutingConfig,
    exchange_names: HashMap<String, ExchangeId>,
    queue_names: HashMap<String, QueueId>,
    next_exchange_id: u32,
    next_queue_id: u32,
    metrics: BrokerMetrics,
}

impl SingleNodeBroker {
    pub fn new(config: &SingleNodeBrokerConfig) -> Result<Self, BrokerInitError> {
        let mut broker = InMemoryBroker::single_shard();
        if config.mode == BrokerMode::SingleNodePersistent
            && config.storage.backend == StorageBackendKind::AppendOnly
        {
            let storage = AppendOnlyShardStorage::open(&config.storage.data_dir)
                .map_err(|e| BrokerInitError::Storage(format!("{e:?}")))?;
            *broker.shard_mut() =
                crate::in_memory::InMemoryShardExecutor::with_durable_storage(ShardId(0), storage);
        }

        let (routing_config, exchange_names, queue_names, next_exchange_id, next_queue_id) =
            bootstrap_routing(&config.routing)?;
        let route_table = Arc::new(
            RouteCompiler::compile(&routing_config)
                .map_err(|e| BrokerInitError::Routing(format!("{e:?}")))?,
        );
        broker.install_route_table(route_table);

        Ok(Self {
            state: ServerState::Running,
            broker,
            routing_config,
            exchange_names,
            queue_names,
            next_exchange_id,
            next_queue_id,
            metrics: BrokerMetrics::default(),
        })
    }

    #[must_use]
    pub fn state(&self) -> ServerState {
        self.state
    }

    pub fn set_state(&mut self, state: ServerState) {
        self.state = state;
    }

    #[must_use]
    pub fn metrics(&self) -> &BrokerMetrics {
        &self.metrics
    }

    #[must_use]
    pub fn route_table_version(&self) -> RouteTableVersion {
        self.broker.route_table().version()
    }

    #[must_use]
    pub fn health(&self) -> BrokerHealth {
        BrokerHealth {
            state: self.state,
            route_table_version: self.route_table_version(),
        }
    }

    #[must_use]
    pub fn broker(&self) -> &InMemoryBroker {
        &self.broker
    }

    #[must_use]
    pub fn broker_mut(&mut self) -> &mut InMemoryBroker {
        &mut self.broker
    }

    pub fn execute_shard(
        &mut self,
        batch: ShardCommandBatch<PayloadHandle>,
    ) -> ShardOutputBatch<PayloadHandle> {
        self.metrics.commands_in.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let out = self.broker.execute(batch);
        self.metrics.record_command_output(&out);
        out
    }

    pub fn execute_ingress(
        &mut self,
        batch: IngressCommandBatch<PayloadHandle>,
    ) -> ShardOutputBatch<PayloadHandle> {
        self.metrics.commands_in.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let out = self.broker.execute_ingress(batch);
        self.metrics.record_command_output(&out);
        out
    }

    pub fn handle_amqp_control(&mut self, command: AmqpControlCommand) -> AmqpControlResult {
        match command {
            AmqpControlCommand::DeclareExchange {
                name,
                exchange_type,
                durable: _,
            } => {
                let id = ExchangeId(self.next_exchange_id);
                self.next_exchange_id += 1;
                let kind = if exchange_type == "fanout" {
                    ExchangeKind::Fanout
                } else {
                    ExchangeKind::Direct
                };
                let decl = match kind {
                    ExchangeKind::Fanout => ExchangeDecl::fanout(id, name.clone()),
                    _ => ExchangeDecl::direct(id, name.clone()),
                };
                self.routing_config.add_exchange(decl);
                self.exchange_names.insert(name, id);
                self.recompile_routes();
                AmqpControlResult::ok()
            }
            AmqpControlCommand::DeclareQueue { name } => {
                let id = if name.is_empty() {
                    let id = QueueId(self.next_queue_id);
                    self.next_queue_id += 1;
                    id
                } else if let Some(&id) = self.queue_names.get(&name) {
                    id
                } else {
                    let id = QueueId(self.next_queue_id);
                    self.next_queue_id += 1;
                    self.queue_names.insert(name.clone(), id);
                    id
                };
                let mut out = ShardOutputBatch::default();
                let _ = self.broker.shard_mut().execute_batch(
                    ShardCommandBatch::Declare(DeclareQueueBatch::one(id)),
                    &mut out,
                );
                let queue_name = if name.is_empty() {
                    format!("amq.gen-{}", id.0)
                } else {
                    name
                };
                AmqpControlResult {
                    ok: true,
                    queue_name,
                    queue_id: Some(id),
                }
            }
            AmqpControlCommand::BindQueue {
                queue,
                exchange,
                routing_key,
            } => {
                let Some(&exchange_id) = self.exchange_names.get(&exchange) else {
                    return AmqpControlResult {
                        ok: false,
                        queue_name: String::new(),
                        queue_id: None,
                    };
                };
                let queue_id = self
                    .queue_names
                    .get(&queue)
                    .copied()
                    .unwrap_or_else(|| {
                        let id = QueueId(self.next_queue_id);
                        self.next_queue_id += 1;
                        self.queue_names.insert(queue.clone(), id);
                        id
                    });
                let binding = if self
                    .routing_config
                    .exchanges
                    .iter()
                    .find(|e| e.id == exchange_id)
                    .is_some_and(|e| e.kind == ExchangeKind::Fanout)
                {
                    BindingDecl::fanout(exchange_id, queue_id)
                } else {
                    BindingDecl::direct(exchange_id, queue_id, &routing_key)
                };
                self.routing_config.add_binding(binding);
                self.recompile_routes();
                AmqpControlResult::ok()
            }
            AmqpControlCommand::ResolveQueueId { name } => {
                let id = self.queue_names.get(&name).copied();
                AmqpControlResult {
                    ok: id.is_some(),
                    queue_name: name,
                    queue_id: id,
                }
            }
        }
    }

    pub fn resolve_amqp_route(
        &self,
        request: AmqpRouteResolveRequest,
    ) -> AmqpRouteResolveResult {
        let resolved = self
            .broker
            .route_table()
            .resolve_direct_by_name(&request.exchange, request.routing_key.as_bytes());
        AmqpRouteResolveResult {
            entry: resolved.ok().map(|r| RouteCacheEntry {
                route_id: r.route_id,
                route_version: r.version,
            }),
        }
    }

    fn recompile_routes(&mut self) {
        let table = Arc::new(
            RouteCompiler::compile(&self.routing_config).expect("compile routes"),
        );
        self.broker.install_route_table(table);
    }
}

fn bootstrap_routing(
    config: &RoutingBootstrapConfig,
) -> Result<
    (
        RoutingConfig,
        HashMap<String, ExchangeId>,
        HashMap<String, QueueId>,
        u32,
        u32,
    ),
    BrokerInitError,
> {
    let mut routing_config = RoutingConfig::new(RouteTableVersion::INITIAL);
    let mut exchange_names = HashMap::new();
    let mut queue_names = HashMap::new();
    let mut next_exchange_id = 1u32;
    let mut next_queue_id = 1u32;

    for ExchangeBootstrap { name, kind } in &config.exchanges {
        let id = ExchangeId(next_exchange_id);
        next_exchange_id += 1;
        let decl = if kind == "fanout" {
            ExchangeDecl::fanout(id, name.clone())
        } else {
            ExchangeDecl::direct(id, name.clone())
        };
        routing_config.add_exchange(decl);
        exchange_names.insert(name.clone(), id);
    }

    for QueueBootstrap { name } in &config.queues {
        let id = QueueId(next_queue_id);
        next_queue_id += 1;
        queue_names.insert(name.clone(), id);
    }

    for BindingBootstrap {
        exchange,
        queue,
        routing_key,
    } in &config.bindings
    {
        let exchange_id = exchange_names.get(exchange).copied().ok_or_else(|| {
            BrokerInitError::Routing(format!("unknown exchange in binding: {exchange}"))
        })?;
        let queue_id = queue_names.get(queue).copied().ok_or_else(|| {
            BrokerInitError::Routing(format!("unknown queue in binding: {queue}"))
        })?;
        let binding = if routing_config
            .exchanges
            .iter()
            .find(|e| e.id == exchange_id)
            .is_some_and(|e| e.kind == ExchangeKind::Fanout)
        {
            BindingDecl::fanout(exchange_id, queue_id)
        } else {
            BindingDecl::direct(exchange_id, queue_id, routing_key)
        };
        routing_config.add_binding(binding);
    }

    Ok((
        routing_config,
        exchange_names,
        queue_names,
        next_exchange_id,
        next_queue_id,
    ))
}

/// Per-connection AMQP payload/context storage with shared broker backend.
#[derive(Debug, Default)]
pub struct AmqpPayloadStore {
    pub payloads: HashMap<u64, Bytes>,
    pub delivery_metadata: HashMap<u64, DeliveryMetadata>,
    pub delivery_properties: HashMap<u64, BasicProperties>,
}

impl AmqpPayloadStore {
    pub fn enrich_delivery(
        &self,
        batch: &mut aurum_internal_protocol::event::delivery::DeliveryEventBatch,
    ) {
        let Some(aurum_internal_protocol::event::delivery::DeliveryEventSegment::Range(r)) =
            batch.segments.first()
        else {
            return;
        };
        let Some(handle) = r.payloads.get(0) else {
            return;
        };
        if let Some(meta) = self.delivery_metadata.get(&handle.0) {
            batch.metadata = meta.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use aurum_internal_protocol::command::{
        consume::{ConsumeCommandBatch, ConsumeStart},
        publish::{IngressPublishBatch, IngressPublishTarget, PublishRecord},
        shard::ShardCommandBatch,
    };
    use aurum_types::{BatchId, ConsumerId, PayloadHandle, QueueId, SourceId};

    use super::*;
    use crate::single_node::config::SingleNodeBrokerConfig;

    #[test]
    fn publish_to_queue_confirms() {
        let mut broker = SingleNodeBroker::new(&SingleNodeBrokerConfig::dev_defaults()).unwrap();
        let queue_id = QueueId(1);
        let _ = broker.broker_mut().shard_mut().execute_batch(
            ShardCommandBatch::Declare(aurum_internal_protocol::command::control::DeclareQueueBatch::one(queue_id)),
            &mut ShardOutputBatch::default(),
        );
        let ingress = IngressCommandBatch::Publish(IngressPublishBatch {
            batch_id: BatchId(1),
            source: SourceId(1),
            target: IngressPublishTarget::Queue(queue_id),
            flags: aurum_internal_protocol::flags::PublishFlags::empty(),
            confirm_mode: aurum_internal_protocol::command::publish::ConfirmMode::Accepted,
            records: smallvec::smallvec![PublishRecord::simple(PayloadHandle(1), 5)],
        });
        let out = broker.execute_ingress(ingress);
        assert_eq!(out.total_confirmed(), 1);
    }

    #[test]
    fn consume_and_deliver() {
        let mut broker = SingleNodeBroker::new(&SingleNodeBrokerConfig::dev_defaults()).unwrap();
        let queue_id = QueueId(1);
        broker.broker_mut().shard_mut().execute_batch(
            ShardCommandBatch::Declare(aurum_internal_protocol::command::control::DeclareQueueBatch::one(queue_id)),
            &mut ShardOutputBatch::default(),
        ).unwrap();
        broker.execute_ingress(IngressCommandBatch::Publish(IngressPublishBatch {
            batch_id: BatchId(1),
            source: SourceId(1),
            target: IngressPublishTarget::Queue(queue_id),
            flags: aurum_internal_protocol::flags::PublishFlags::empty(),
            confirm_mode: aurum_internal_protocol::command::publish::ConfirmMode::None,
            records: smallvec::smallvec![PublishRecord::simple(PayloadHandle(42), 3)],
        }));
        let out = broker.execute_shard(ShardCommandBatch::Consume(ConsumeCommandBatch::one(
            ConsumeStart::new(ConsumerId(7), aurum_types::ChannelId(1), queue_id, 10),
        )));
        assert_eq!(out.total_delivered(), 1);
    }
}
