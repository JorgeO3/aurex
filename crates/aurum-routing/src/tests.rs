use aurum_types::{ExchangeId, QueueId, RouteId, RouteTableVersion, ShardId};

use crate::binding::BindingDecl;
use crate::config::RoutingConfig;
use crate::error::{RouteLookupError, RouteResolveError};
use crate::exchange::ExchangeDecl;
use crate::queue_set::QueueSetRef;
use crate::RouteCompiler;

fn sample_direct_table() -> crate::RouteTable {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::direct(ExchangeId(1), "orders"));
    config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(10), "created"));
    config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(11), "created"));
    config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(20), "paid"));
    RouteCompiler::compile(&config).unwrap()
}

#[test]
fn direct_exchange_resolves_single_queue() {
    let table = sample_direct_table();
    let resolved = table.resolve_direct(ExchangeId(1), b"paid").unwrap();
    let set = table
        .get_by_route_id(resolved.route_id, resolved.version)
        .unwrap();
    assert_eq!(set.target_count(), 1);
    let targets = set.targets_vec();
    assert_eq!(targets[0].queue_id, QueueId(20));
}

#[test]
fn direct_exchange_resolves_multiple_queues_for_same_key() {
    let table = sample_direct_table();
    let resolved = table.resolve_direct(ExchangeId(1), b"created").unwrap();
    let set = table
        .get_by_route_id(resolved.route_id, resolved.version)
        .unwrap();
    assert_eq!(set.target_count(), 2);
}

#[test]
fn direct_exchange_deduplicates_duplicate_bindings() {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::direct(ExchangeId(1), "x"));
    config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(1), "k"));
    config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(1), "k"));
    let table = RouteCompiler::compile(&config).unwrap();
    let resolved = table.resolve_direct(ExchangeId(1), b"k").unwrap();
    let set = table
        .get_by_route_id(resolved.route_id, resolved.version)
        .unwrap();
    assert_eq!(set.target_count(), 1);
}

#[test]
fn direct_exchange_unknown_key_is_unroutable() {
    let table = sample_direct_table();
    assert_eq!(
        table.resolve_direct(ExchangeId(1), b"missing"),
        Err(RouteResolveError::Unroutable)
    );
}

#[test]
fn route_id_lookup_matches_resolve() {
    let table = sample_direct_table();
    let resolved = table.resolve_direct(ExchangeId(1), b"created").unwrap();
    let via_id = table
        .get_by_route_id(resolved.route_id, resolved.version)
        .unwrap();
    let direct_set = table
        .get_by_route_id(resolved.route_id, resolved.version)
        .unwrap();
    assert_eq!(via_id.target_count(), direct_set.target_count());
}

#[test]
fn stale_route_version_is_rejected() {
    let table = sample_direct_table();
    let resolved = table.resolve_direct(ExchangeId(1), b"paid").unwrap();
    assert_eq!(
        table.get_by_route_id(resolved.route_id, RouteTableVersion(999)),
        Err(RouteLookupError::RouteTableVersionMismatch)
    );
}

#[test]
fn route_generation_mismatch_is_rejected() {
    let table = sample_direct_table();
    let resolved = table.resolve_direct(ExchangeId(1), b"paid").unwrap();
    let bad = RouteId::new(resolved.route_id.index(), resolved.route_id.generation() + 1);
    assert_eq!(
        table.get_by_route_id(bad, resolved.version),
        Err(RouteLookupError::RouteGenerationMismatch)
    );
}

#[test]
fn fanout_ignores_routing_key() {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::fanout(ExchangeId(2), "broadcast"));
    config.add_binding(BindingDecl::fanout(ExchangeId(2), QueueId(1)));
    config.add_binding(BindingDecl::fanout(ExchangeId(2), QueueId(2)));
    let table = RouteCompiler::compile(&config).unwrap();
    let r1 = table.resolve_direct(ExchangeId(2), b"").unwrap();
    let r2 = table.resolve_direct(ExchangeId(2), b"anything").unwrap();
    assert_eq!(r1.route_id, r2.route_id);
    let set = table.get_by_route_id(r1.route_id, r1.version).unwrap();
    assert_eq!(set.target_count(), 2);
}

#[test]
fn fanout_empty_bindings_is_unroutable() {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::fanout(ExchangeId(3), "empty"));
    let table = RouteCompiler::compile(&config).unwrap();
    assert_eq!(
        table.resolve_direct(ExchangeId(3), b""),
        Err(RouteResolveError::Unroutable)
    );
}

#[test]
fn queue_set_groups_targets_by_shard() {
    let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
    config.add_exchange(ExchangeDecl::direct(ExchangeId(1), "sharded"));
    let mut b1 = BindingDecl::direct(ExchangeId(1), QueueId(1), "k");
    b1.target_shard = ShardId(0);
    let mut b2 = BindingDecl::direct(ExchangeId(1), QueueId(2), "k");
    b2.target_shard = ShardId(1);
    config.add_binding(b1);
    config.add_binding(b2);
    let table = RouteCompiler::compile(&config).unwrap();
    let resolved = table.resolve_direct(ExchangeId(1), b"k").unwrap();
    let set = table
        .get_by_route_id(resolved.route_id, resolved.version)
        .unwrap();
    assert!(matches!(set, QueueSetRef::ShardGrouped(_)));
}
