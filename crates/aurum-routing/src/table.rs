use std::collections::HashMap;

use aurum_types::{
    ExchangeId, QueueSetId, RouteId, RouteTableVersion, RoutingKeyHash, ShardId,
};

use crate::error::{RouteLookupError, RouteResolveError};
use crate::exchange::CompiledExchange;
use crate::flags::RouteFlags;
use crate::queue_set::{QueueSetRef, QueueSetStorage};
use crate::hash::fnv1a64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteEntry {
    pub generation: u32,
    pub exchange_id: ExchangeId,
    pub routing_hash: RoutingKeyHash,
    pub routing_len: u16,
    pub queue_set_id: QueueSetId,
    pub flags: RouteFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedRoute {
    pub route_id: RouteId,
    pub version: RouteTableVersion,
    pub flags: RouteFlags,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DirectKey {
    exchange_id: ExchangeId,
    routing_key: Vec<u8>,
}

#[derive(Debug)]
pub struct RouteTable {
    pub version: RouteTableVersion,
    exchanges: Vec<CompiledExchange>,
    route_entries: Vec<RouteEntry>,
    queue_sets: QueueSetStorage,
    direct_index: HashMap<DirectKey, RouteId>,
    exchange_by_id: HashMap<ExchangeId, usize>,
    exchange_names: HashMap<String, ExchangeId>,
}

impl RouteTable {
    #[must_use]
    pub fn version(&self) -> RouteTableVersion {
        self.version
    }

    #[must_use]
    pub fn queue_sets(&self) -> &QueueSetStorage {
        &self.queue_sets
    }

    pub fn new_empty(version: RouteTableVersion) -> Self {
        Self {
            version,
            exchanges: Vec::new(),
            route_entries: Vec::new(),
            queue_sets: QueueSetStorage::new(),
            direct_index: HashMap::new(),
            exchange_by_id: HashMap::new(),
            exchange_names: HashMap::new(),
        }
    }

    #[must_use]
    pub fn exchange_id_by_name(&self, name: &str) -> Option<ExchangeId> {
        self.exchange_names.get(name).copied()
    }

    pub fn resolve_direct_by_name(
        &self,
        exchange_name: &str,
        routing_key: &[u8],
    ) -> Result<ResolvedRoute, RouteResolveError> {
        let exchange_id = self
            .exchange_id_by_name(exchange_name)
            .ok_or(RouteResolveError::ExchangeNotFound)?;
        self.resolve_direct(exchange_id, routing_key)
    }

    pub fn resolve_direct(
        &self,
        exchange_id: ExchangeId,
        routing_key: &[u8],
    ) -> Result<ResolvedRoute, RouteResolveError> {
        let exchange = self
            .exchange_by_id
            .get(&exchange_id)
            .and_then(|&idx| self.exchanges.get(idx))
            .ok_or(RouteResolveError::ExchangeNotFound)?;

        match exchange.kind {
            crate::exchange::ExchangeKind::Direct => {
                let key = DirectKey {
                    exchange_id,
                    routing_key: routing_key.to_vec(),
                };
                let route_id = self
                    .direct_index
                    .get(&key)
                    .copied()
                    .ok_or(RouteResolveError::Unroutable)?;
                let entry = &self.route_entries[route_id.index() as usize];
                if entry.flags.contains(RouteFlags::UNROUTABLE) {
                    return Err(RouteResolveError::Unroutable);
                }
                Ok(ResolvedRoute {
                    route_id,
                    version: self.version,
                    flags: entry.flags,
                })
            }
            crate::exchange::ExchangeKind::Fanout => {
                let idx = exchange
                    .fanout_route_index
                    .ok_or(RouteResolveError::Unroutable)? as usize;
                let entry = &self.route_entries[idx];
                if entry.flags.contains(RouteFlags::UNROUTABLE) {
                    return Err(RouteResolveError::Unroutable);
                }
                Ok(ResolvedRoute {
                    route_id: RouteId::new(idx as u32, entry.generation),
                    version: self.version,
                    flags: entry.flags,
                })
            }
            crate::exchange::ExchangeKind::Topic | crate::exchange::ExchangeKind::Headers => {
                Err(RouteResolveError::UnsupportedExchangeKind)
            }
        }
    }

    #[inline]
    pub fn get_by_route_id(
        &self,
        route_id: RouteId,
        expected_version: RouteTableVersion,
    ) -> Result<QueueSetRef<'_>, RouteLookupError> {
        if self.version != expected_version {
            return Err(RouteLookupError::RouteTableVersionMismatch);
        }
        let entry = self
            .route_entries
            .get(route_id.index() as usize)
            .ok_or(RouteLookupError::RouteIdInvalid)?;
        if entry.generation != route_id.generation() {
            return Err(RouteLookupError::RouteGenerationMismatch);
        }
        if entry.flags.contains(RouteFlags::UNROUTABLE) {
            return Err(RouteLookupError::Unroutable);
        }
        Ok(self.queue_sets.get_ref(entry.queue_set_id))
    }

    pub(crate) fn push_exchange(&mut self, exchange: CompiledExchange, name: &str) {
        self.exchange_names.insert(name.to_string(), exchange.id);
        let idx = self.exchanges.len();
        self.exchange_by_id.insert(exchange.id, idx);
        self.exchanges.push(exchange);
    }

    pub(crate) fn push_route_entry(&mut self, entry: RouteEntry) -> RouteId {
        let index = self.route_entries.len() as u32;
        let route_id = RouteId::new(index, entry.generation);
        self.route_entries.push(entry);
        route_id
    }

    pub(crate) fn map_direct(
        &mut self,
        exchange_id: ExchangeId,
        routing_key: &[u8],
        route_id: RouteId,
    ) {
        self.direct_index.insert(
            DirectKey {
                exchange_id,
                routing_key: routing_key.to_vec(),
            },
            route_id,
        );
    }

    pub(crate) fn routing_hash_for_key(routing_key: &[u8]) -> RoutingKeyHash {
        RoutingKeyHash(fnv1a64(routing_key))
    }

    pub(crate) fn queue_sets_mut(&mut self) -> &mut QueueSetStorage {
        &mut self.queue_sets
    }

    pub(crate) fn exchanges(&self) -> &[CompiledExchange] {
        &self.exchanges
    }

    pub(crate) fn exchange_index(&self, id: ExchangeId) -> Option<usize> {
        self.exchange_by_id.get(&id).copied()
    }
}

/// Iterate targets grouped by shard (stable order).
pub fn targets_by_shard(set: QueueSetRef<'_>) -> Vec<(ShardId, Vec<aurum_types::QueueId>)> {
    let mut map: HashMap<ShardId, Vec<aurum_types::QueueId>> = HashMap::new();
    set.for_each_target(|t| {
        map.entry(t.shard_id).or_default().push(t.queue_id);
    });
    let mut out: Vec<_> = map.into_iter().collect();
    out.sort_by_key(|(shard, _)| *shard);
    out
}
