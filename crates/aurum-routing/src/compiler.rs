use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use aurum_types::ExchangeId;

use crate::config::RoutingConfig;
use crate::error::RouteCompileError;
use crate::exchange::{CompiledExchange, ExchangeDecl, ExchangeKind};
use crate::flags::RouteFlags;
use crate::queue_set::{QueueSetBuilder, QueueSetEntry, QueueTarget};
use crate::table::{RouteEntry, RouteTable};

pub struct RouteCompiler;

impl RouteCompiler {
    pub fn compile(config: &RoutingConfig) -> Result<RouteTable, RouteCompileError> {
        if config.exchanges.is_empty() {
            return Err(RouteCompileError::EmptyConfig);
        }

        let mut seen_exchanges = HashSet::new();
        for ex in &config.exchanges {
            if !seen_exchanges.insert(ex.id) {
                return Err(RouteCompileError::DuplicateExchangeId);
            }
        }

        let mut table = RouteTable::new_empty(config.version);
        let exchange_map: HashMap<ExchangeId, &ExchangeDecl> =
            config.exchanges.iter().map(|e| (e.id, e)).collect();

        for binding in &config.bindings {
            if !exchange_map.contains_key(&binding.exchange_id) {
                return Err(RouteCompileError::ExchangeNotFound);
            }
        }

        for exchange in &config.exchanges {
            match exchange.kind {
                ExchangeKind::Direct => compile_direct(exchange, config, &mut table)?,
                ExchangeKind::Fanout => compile_fanout(exchange, config, &mut table)?,
                ExchangeKind::Topic | ExchangeKind::Headers => {
                    return Err(RouteCompileError::UnsupportedExchangeKind);
                }
            }
        }

        Ok(table)
    }
}

fn dedupe_targets(targets: &mut Vec<QueueTarget>) {
    let mut seen = BTreeSet::new();
    targets.retain(|t| seen.insert(*t));
    targets.sort_by_key(|t| (t.shard_id, t.queue_id));
}

fn compile_direct(
    exchange: &ExchangeDecl,
    config: &RoutingConfig,
    table: &mut RouteTable,
) -> Result<(), RouteCompileError> {
    let mut by_key: BTreeMap<String, Vec<QueueTarget>> = BTreeMap::new();
    for binding in &config.bindings {
        if binding.exchange_id != exchange.id {
            continue;
        }
        by_key
            .entry(binding.routing_key.clone())
            .or_default()
            .push(QueueTarget {
                shard_id: binding.target_shard,
                queue_id: binding.queue_id,
            });
    }

    table.push_exchange(
        CompiledExchange {
            id: exchange.id,
            kind: ExchangeKind::Direct,
            flags: exchange.flags,
            fanout_route_index: None,
        },
        &exchange.name,
    );

    for (routing_key, mut targets) in by_key {
        dedupe_targets(&mut targets);
        let entry = QueueSetBuilder::build(&targets);
        let flags = route_flags_for_entry(&entry, RouteFlags::DIRECT);
        let queue_set_id = table.queue_sets_mut().push(entry);
        let route_entry = RouteEntry {
            generation: 1,
            exchange_id: exchange.id,
            routing_hash: RouteTable::routing_hash_for_key(routing_key.as_bytes()),
            routing_len: routing_key.len().min(u16::MAX as usize) as u16,
            queue_set_id,
            flags,
        };
        let route_id = table.push_route_entry(route_entry);
        table.map_direct(exchange.id, routing_key.as_bytes(), route_id);
    }

    Ok(())
}

fn compile_fanout(
    exchange: &ExchangeDecl,
    config: &RoutingConfig,
    table: &mut RouteTable,
) -> Result<(), RouteCompileError> {
    let mut targets: Vec<QueueTarget> = config
        .bindings
        .iter()
        .filter(|b| b.exchange_id == exchange.id)
        .map(|b| QueueTarget {
            shard_id: b.target_shard,
            queue_id: b.queue_id,
        })
        .collect();
    dedupe_targets(&mut targets);

    let entry = if targets.is_empty() {
        QueueSetEntry::Empty
    } else {
        QueueSetBuilder::build(&targets)
    };
    let mut flags = route_flags_for_entry(&entry, RouteFlags::FANOUT);
    if targets.is_empty() {
        flags |= RouteFlags::UNROUTABLE;
    }
    let queue_set_id = table.queue_sets_mut().push(entry);
    let route_entry = RouteEntry {
        generation: 1,
        exchange_id: exchange.id,
        routing_hash: RouteTable::routing_hash_for_key(b""),
        routing_len: 0,
        queue_set_id,
        flags,
    };
    let route_id = table.push_route_entry(route_entry);

    table.push_exchange(
        CompiledExchange {
            id: exchange.id,
            kind: ExchangeKind::Fanout,
            flags: exchange.flags,
            fanout_route_index: Some(route_id.index()),
        },
        &exchange.name,
    );

    Ok(())
}

fn route_flags_for_entry(entry: &QueueSetEntry, base: RouteFlags) -> RouteFlags {
    let mut flags = base;
    match entry {
        QueueSetEntry::Empty => flags |= RouteFlags::UNROUTABLE,
        QueueSetEntry::One(_) => {}
        QueueSetEntry::Small(s) if s.len > 1 => {
            flags |= RouteFlags::HAS_MULTIPLE_TARGETS;
        }
        QueueSetEntry::ShardGrouped(_) => flags |= RouteFlags::HAS_MULTIPLE_TARGETS,
        QueueSetEntry::Small(_) => {}
    }
    flags
}
