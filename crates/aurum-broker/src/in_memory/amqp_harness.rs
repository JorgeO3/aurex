use std::collections::HashMap;
use std::sync::Arc;

use aurum_internal_protocol::command::{
    control::DeclareQueueBatch,
    ingress::IngressCommandBatch,
    shard::ShardCommandBatch,
};
use aurum_internal_protocol::event::delivery::{DeliveryEventBatch, DeliveryEventSegment, DeliveryMetadata};
use aurum_protocol_amqp::{
    AmqpBrokerOutput, AmqpBrokerPort, AmqpControlCommand, AmqpControlResult, AmqpOutbound,
    AmqpRouteResolveRequest, AmqpRouteResolveResult, AmqpSession, BasicProperties, RawFrame,
    RouteCacheEntry,
};
use aurum_routing::{BindingDecl, ExchangeDecl, ExchangeKind, RouteCompiler, RouteTable, RoutingConfig};
use aurum_types::{ExchangeId, PayloadHandle, QueueId, RouteTableVersion};
use bytes::{BufMut, Bytes, BytesMut};

use super::broker::InMemoryBroker;
use super::output::ShardOutputBatch;

/// In-process harness: AMQP bytes → broker → AMQP response frames.
pub struct AmqpInMemoryHarness {
    session: AmqpSession<AmqpBrokerAdapter>,
}

impl AmqpInMemoryHarness {
    #[must_use]
    pub fn new() -> Self {
        Self {
            session: AmqpSession::new(AmqpBrokerAdapter::new()),
        }
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut out = AmqpOutbound::default();
        let _ = self.session.receive_bytes(bytes, &mut out);
        encode_frames(&out.frames)
    }

    pub fn send_frames(&mut self, frames: &[RawFrame]) -> Vec<RawFrame> {
        let mut out = AmqpOutbound::default();
        for frame in frames {
            let _ = self.session.receive_frame(frame.clone(), &mut out);
        }
        out.frames
    }

    #[must_use]
    pub fn broker(&self) -> &InMemoryBroker {
        self.session.broker().broker()
    }
}

impl Default for AmqpInMemoryHarness {
    fn default() -> Self {
        Self::new()
    }
}

fn encode_frames(frames: &[RawFrame]) -> Vec<u8> {
    let mut buf = Vec::new();
    for frame in frames {
        let mut tmp = BytesMut::new();
        frame.encode(&mut tmp);
        buf.extend_from_slice(&tmp);
    }
    buf
}

pub struct AmqpBrokerAdapter {
    broker: InMemoryBroker,
    routing_config: RoutingConfig,
    exchange_names: HashMap<String, ExchangeId>,
    queue_names: HashMap<String, QueueId>,
    payloads: HashMap<u64, Bytes>,
    delivery_metadata: HashMap<u64, DeliveryMetadata>,
    delivery_properties: HashMap<u64, BasicProperties>,
    next_exchange_id: u32,
    next_queue_id: u32,
}

impl AmqpBrokerAdapter {
    pub fn broker(&self) -> &InMemoryBroker {
        &self.broker
    }

    fn new() -> Self {
        Self {
            broker: InMemoryBroker::single_shard(),
            routing_config: RoutingConfig::new(RouteTableVersion::INITIAL),
            exchange_names: HashMap::new(),
            queue_names: HashMap::new(),
            payloads: HashMap::new(),
            delivery_metadata: HashMap::new(),
            delivery_properties: HashMap::new(),
            next_exchange_id: 1,
            next_queue_id: 1,
        }
    }

    fn recompile_routes(&mut self) {
        let table = Arc::new(RouteCompiler::compile(&self.routing_config).expect("compile routes"));
        self.broker.install_route_table(table);
    }

    fn enrich_delivery(metadata: &HashMap<u64, DeliveryMetadata>, batch: &mut DeliveryEventBatch) {
        let Some(DeliveryEventSegment::Range(r)) = batch.segments.first() else {
            return;
        };
        let Some(handle) = r.payloads.get(0) else {
            return;
        };
        if let Some(meta) = metadata.get(&handle.0) {
            batch.metadata = meta.clone();
        }
    }

    fn output_from_shard(
        delivery_metadata: &HashMap<u64, DeliveryMetadata>,
        out: ShardOutputBatch,
    ) -> AmqpBrokerOutput {
        let mut deliveries = out.deliveries.to_vec();
        for batch in &mut deliveries {
            Self::enrich_delivery(delivery_metadata, batch);
        }
        AmqpBrokerOutput {
            deliveries,
            confirms: out.confirms.to_vec(),
            settlements: out.settlements.to_vec(),
            consumer_events: out.consumer_events.to_vec(),
            route_resolved: out.route_resolved.to_vec(),
            errors: out.errors.to_vec(),
        }
    }
}

impl AmqpBrokerPort for AmqpBrokerAdapter {
    fn handle_control(&mut self, command: AmqpControlCommand) -> AmqpControlResult {
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

    fn handle_shard_batch(&mut self, batch: ShardCommandBatch) -> AmqpBrokerOutput {
        let out = self.broker.execute(batch);
        Self::output_from_shard(&self.delivery_metadata, out)
    }

    fn handle_ingress_batch(&mut self, batch: IngressCommandBatch) -> AmqpBrokerOutput {
        let out = self.broker.execute_ingress(batch);
        Self::output_from_shard(&self.delivery_metadata, out)
    }

    fn resolve_route(&mut self, request: AmqpRouteResolveRequest) -> AmqpRouteResolveResult {
        let table = self.broker.route_table();
        let resolved = table.resolve_direct_by_name(&request.exchange, request.routing_key.as_bytes());
        AmqpRouteResolveResult {
            entry: resolved.ok().map(|r| RouteCacheEntry {
                route_id: r.route_id,
                route_version: r.version,
            }),
        }
    }

    fn route_table_version(&self) -> RouteTableVersion {
        self.broker.route_table().version()
    }

    fn store_payload(&mut self, handle: PayloadHandle, body: Bytes) {
        self.payloads.insert(handle.0, body);
    }

    fn load_payload(&self, handle: PayloadHandle) -> Option<Bytes> {
        self.payloads.get(&handle.0).cloned()
    }

    fn store_delivery_context(
        &mut self,
        handle: PayloadHandle,
        metadata: DeliveryMetadata,
        properties: BasicProperties,
    ) {
        self.delivery_metadata.insert(handle.0, metadata);
        self.delivery_properties.insert(handle.0, properties);
    }

    fn delivery_properties(&self, handle: PayloadHandle) -> Option<BasicProperties> {
        self.delivery_properties.get(&handle.0).cloned()
    }
}
