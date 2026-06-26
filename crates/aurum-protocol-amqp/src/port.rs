use aurum_internal_protocol::event::delivery::DeliveryMetadata;
use aurum_internal_protocol::command::{
    ingress::IngressCommandBatch,
    shard::ShardCommandBatch,
};
use aurum_types::{PayloadHandle, QueueId, RouteId, RouteTableVersion};

use crate::session::RouteCacheEntry;

#[derive(Debug, Clone)]
pub enum AmqpControlCommand {
    DeclareExchange {
        name: String,
        exchange_type: String,
        durable: bool,
    },
    DeclareQueue {
        name: String,
    },
    BindQueue {
        queue: String,
        exchange: String,
        routing_key: String,
    },
    ResolveQueueId {
        name: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct AmqpControlResult {
    pub ok: bool,
    pub queue_name: String,
    pub queue_id: Option<QueueId>,
}

impl AmqpControlResult {
    #[must_use]
    pub fn is_err(&self) -> bool {
        !self.ok
    }

    #[must_use]
    pub fn ok() -> Self {
        Self {
            ok: true,
            queue_name: String::new(),
            queue_id: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AmqpRouteResolveRequest {
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Clone)]
pub struct AmqpRouteResolveResult {
    pub entry: Option<RouteCacheEntry>,
}

#[derive(Debug, Default)]
pub struct AmqpBrokerOutput {
    pub deliveries: Vec<aurum_internal_protocol::event::delivery::DeliveryEventBatch>,
    pub confirms: Vec<aurum_internal_protocol::event::confirm::PublishConfirmBatch>,
    pub settlements: Vec<aurum_internal_protocol::event::confirm::SettlementResultBatch>,
    pub consumer_events: Vec<aurum_internal_protocol::event::confirm::ConsumerEventBatch>,
    pub route_resolved: Vec<aurum_internal_protocol::route::RouteResolvedEvent>,
    pub errors: Vec<aurum_internal_protocol::event::error::CommandError>,
}

pub trait AmqpBrokerPort {
    fn handle_control(&mut self, command: AmqpControlCommand) -> AmqpControlResult;
    fn handle_shard_batch(&mut self, batch: ShardCommandBatch<PayloadHandle>) -> AmqpBrokerOutput;
    fn handle_ingress_batch(&mut self, batch: IngressCommandBatch<PayloadHandle>) -> AmqpBrokerOutput;
    fn resolve_route(&mut self, request: AmqpRouteResolveRequest) -> AmqpRouteResolveResult;
    fn route_table_version(&self) -> RouteTableVersion;
    fn store_payload(&mut self, handle: PayloadHandle, body: bytes::Bytes);
    fn load_payload(&self, handle: PayloadHandle) -> Option<bytes::Bytes>;
    fn store_delivery_context(
        &mut self,
        handle: PayloadHandle,
        metadata: DeliveryMetadata,
        properties: crate::wire::properties::BasicProperties,
    );
    fn delivery_properties(&self, handle: PayloadHandle) -> Option<crate::wire::properties::BasicProperties>;
}
