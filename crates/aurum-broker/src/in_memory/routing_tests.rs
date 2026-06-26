use std::sync::Arc;

use aurum_internal_protocol::{
    command::{
        consume::{ConsumeCommandBatch, ConsumeStart},
        control::DeclareQueueBatch,
        ingress::{IngressCommandBatch, ResolveRouteBatch},
        publish::{
            ConfirmMode, IngressPublishBatch, IngressPublishTarget, PublishRecord,
        },
        shard::ShardCommandBatch,
    },
    event::error::CommandErrorKind,
    route::{CorrelationId, ResolveRouteCommand, RoutePublishTarget},
};
use aurum_routing::{
    BindingDecl, ExchangeDecl, ResolvedRoute, RouteCompiler, RoutingConfig,
};
use aurum_types::{
    ChannelId, ConsumerId, ExchangeId, PayloadHandle, QueueId, RouteTableVersion,
};

use crate::in_memory::InMemoryBroker;

fn routed_broker() -> (InMemoryBroker, ResolvedRoute) {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::direct(ExchangeId(1), "orders"));
    config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(10), "created"));
    let table = Arc::new(RouteCompiler::compile(&config).unwrap());
    let resolved = table.resolve_direct(ExchangeId(1), b"created").unwrap();
    let broker = InMemoryBroker::with_route_table(table);
    (broker, resolved)
}

#[test]
fn in_memory_broker_resolve_route_returns_route_id() {
    let (mut broker, _) = routed_broker();
    let out = broker.execute_ingress(IngressCommandBatch::ResolveRoute(ResolveRouteBatch::one(
        ResolveRouteCommand::new(CorrelationId(1), ExchangeId(1), b"created"),
    )));
    assert_eq!(out.route_resolved.len(), 1);
    assert!(out.errors.is_empty());
}

#[test]
fn in_memory_broker_publish_by_route_delivers() {
    let (mut broker, resolved) = routed_broker();
    broker.execute(ShardCommandBatch::Declare(DeclareQueueBatch::one(QueueId(10))));
    broker.execute(ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
        ConsumerId(1),
        ChannelId(0),
        QueueId(10),
        64,
    ))));

    let mut records = smallvec::SmallVec::new();
    for _ in 0..32 {
        records.push(PublishRecord::simple(PayloadHandle(0), 0));
    }
    let out = broker.execute_ingress(IngressCommandBatch::Publish(IngressPublishBatch {
        batch_id: Default::default(),
        source: Default::default(),
        target: IngressPublishTarget::Route(RoutePublishTarget::new(
            resolved.route_id,
            resolved.version,
        )),
        flags: Default::default(),
        confirm_mode: ConfirmMode::Accepted,
        records,
    }));
    assert_eq!(out.total_confirmed(), 32);
    assert_eq!(out.total_delivered(), 32);
}

#[test]
fn in_memory_broker_publish_stale_route_errors() {
    let (mut broker, resolved) = routed_broker();
    let out = broker.execute_ingress(IngressCommandBatch::Publish(IngressPublishBatch {
        batch_id: Default::default(),
        source: Default::default(),
        target: IngressPublishTarget::Route(RoutePublishTarget::new(
            resolved.route_id,
            RouteTableVersion(999),
        )),
        flags: Default::default(),
        confirm_mode: ConfirmMode::None,
        records: smallvec::smallvec![],
    }));
    assert_eq!(out.errors[0].kind, CommandErrorKind::StaleRouteEpoch);
}

#[test]
fn in_memory_broker_publish_unroutable_errors() {
    let (mut broker, _) = routed_broker();
    let out = broker.execute_ingress(IngressCommandBatch::ResolveRoute(ResolveRouteBatch::one(
        ResolveRouteCommand::new(CorrelationId(2), ExchangeId(1), b"missing"),
    )));
    assert_eq!(out.errors[0].kind, CommandErrorKind::Unroutable);
}

#[test]
fn fanout_publish_reaches_multiple_queues() {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::fanout(ExchangeId(2), "broadcast"));
    config.add_binding(BindingDecl::fanout(ExchangeId(2), QueueId(1)));
    config.add_binding(BindingDecl::fanout(ExchangeId(2), QueueId(2)));
    let table = Arc::new(RouteCompiler::compile(&config).unwrap());
    let resolved = table.resolve_direct(ExchangeId(2), b"").unwrap();
    let mut broker = InMemoryBroker::with_route_table(table);

    broker.execute(ShardCommandBatch::Declare(DeclareQueueBatch::one(QueueId(1))));
    broker.execute(ShardCommandBatch::Declare(DeclareQueueBatch::one(QueueId(2))));
    broker.execute(ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
        ConsumerId(1), ChannelId(0), QueueId(1), 0,
    ))));
    broker.execute(ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
        ConsumerId(2), ChannelId(0), QueueId(2), 0,
    ))));

    let out = broker.execute_ingress(IngressCommandBatch::Publish(IngressPublishBatch {
        batch_id: Default::default(),
        source: Default::default(),
        target: IngressPublishTarget::Route(RoutePublishTarget::new(
            resolved.route_id,
            resolved.version,
        )),
        flags: Default::default(),
        confirm_mode: ConfirmMode::Accepted,
        records: {
            let mut r = smallvec::SmallVec::new();
            r.push(PublishRecord::simple(PayloadHandle(0), 0));
            r
        },
    }));

    assert_eq!(out.total_confirmed(), 2);
    assert_eq!(out.total_delivered(), 2);
}
