use std::sync::atomic::Ordering;

use aurum_internal_protocol::command::{
    consume::{ConsumeCommandBatch, ConsumeStart},
    ingress::IngressCommandBatch,
    publish::{ConfirmMode, IngressPublishBatch, IngressPublishTarget, PublishRecord},
    settlement::AckCommandBatch,
    shard::ShardCommandBatch,
};
use aurum_internal_protocol::route::RoutePublishTarget;
use aurum_types::{BatchId, ChannelId, ConsumerId, DeliveryTag, PayloadHandle, QueueId, SourceId};
use tempfile::TempDir;

use super::broker::SingleNodeBroker;
use super::config::{SingleNodeBrokerConfig, StorageBackendKind};
use super::lifecycle::ServerState;
use super::service::BrokerService;
use crate::in_memory::ShardOutputBatch;

#[test]
fn metrics_snapshot_without_panic() {
    let broker = SingleNodeBroker::new(&SingleNodeBrokerConfig::dev_defaults()).unwrap();
    let _ = broker.metrics().snapshot();
}

#[test]
fn health_running_after_service_start() {
    let service = BrokerService::new(SingleNodeBrokerConfig::dev_defaults()).unwrap();
    service.start();
    let health = service.shared_broker().lock().unwrap().health();
    assert_eq!(health.state, ServerState::Running);
}

#[test]
fn append_only_init_opens_data_dir() {
    let dir = TempDir::new().unwrap();
    let mut config = SingleNodeBrokerConfig::dev_defaults();
    config.mode = super::config::BrokerMode::SingleNodePersistent;
    config.storage.backend = StorageBackendKind::AppendOnly;
    config.storage.data_dir = dir.path().to_path_buf();
    let broker = SingleNodeBroker::new(&config);
    assert!(broker.is_ok());
}

#[test]
fn publish_by_route_confirms() {
    let mut broker = SingleNodeBroker::new(&SingleNodeBrokerConfig::dev_defaults()).unwrap();
    let resolved = broker
        .broker()
        .route_table()
        .resolve_direct_by_name("amq.direct", b"test")
        .unwrap();
    let ingress = IngressCommandBatch::Publish(IngressPublishBatch {
        batch_id: BatchId(1),
        source: SourceId(1),
        target: IngressPublishTarget::Route(RoutePublishTarget {
            route_id: resolved.route_id,
            route_version: resolved.version,
        }),
        flags: aurum_internal_protocol::flags::PublishFlags::empty(),
        confirm_mode: ConfirmMode::Accepted,
        records: smallvec::smallvec![PublishRecord::simple(PayloadHandle(1), 5)],
    });
    let out = broker.execute_ingress(ingress);
    assert_eq!(out.total_confirmed(), 1);
}

#[test]
fn ack_produces_settlement() {
    let mut broker = SingleNodeBroker::new(&SingleNodeBrokerConfig::dev_defaults()).unwrap();
    let queue_id = QueueId(1);
    broker
        .broker_mut()
        .shard_mut()
        .execute_batch(
            ShardCommandBatch::Declare(
                aurum_internal_protocol::command::control::DeclareQueueBatch::one(queue_id),
            ),
            &mut ShardOutputBatch::default(),
        )
        .unwrap();
    broker.execute_shard(ShardCommandBatch::Consume(ConsumeCommandBatch::one(
        ConsumeStart::new(ConsumerId(1), ChannelId(1), queue_id, 10),
    )));
    let deliver = broker.execute_ingress(IngressCommandBatch::Publish(IngressPublishBatch {
        batch_id: BatchId(2),
        source: SourceId(1),
        target: IngressPublishTarget::Queue(queue_id),
        flags: aurum_internal_protocol::flags::PublishFlags::empty(),
        confirm_mode: ConfirmMode::None,
        records: smallvec::smallvec![PublishRecord::simple(PayloadHandle(9), 3)],
    }));
    assert_eq!(deliver.total_delivered(), 1);
    let tag = deliver
        .deliveries
        .first()
        .and_then(|d| d.last_tag())
        .expect("delivery tag");
    let settled = broker.execute_shard(ShardCommandBatch::Ack(AckCommandBatch::one(
        ConsumerId(1),
        tag,
    )));
    assert!(settled.total_settled() > 0);
    assert!(
        broker
            .metrics()
            .acks_applied
            .load(Ordering::Relaxed)
            > 0
    );
}
