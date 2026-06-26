use std::collections::HashMap;

use crate::wire::ShortStr;

#[derive(Debug, Default, Clone)]
pub struct RouteCache {
    entries: HashMap<(u64, u64), RouteCacheEntry>,
}

#[derive(Debug, Clone, Copy)]
pub struct RouteCacheEntry {
    pub route_id: aurum_types::RouteId,
    pub route_version: aurum_types::RouteTableVersion,
}

impl RouteCache {
    pub fn get(
        &self,
        exchange: &ShortStr,
        routing_key: &ShortStr,
        version: aurum_types::RouteTableVersion,
    ) -> Option<RouteCacheEntry> {
        let key = (
            hash_bytes(exchange.as_bytes()),
            hash_bytes(routing_key.as_bytes()),
        );
        self.entries.get(&key).and_then(|e| {
            if e.route_version == version {
                Some(*e)
            } else {
                None
            }
        })
    }

    pub fn insert(
        &mut self,
        exchange: &ShortStr,
        routing_key: &ShortStr,
        entry: RouteCacheEntry,
    ) {
        let key = (
            hash_bytes(exchange.as_bytes()),
            hash_bytes(routing_key.as_bytes()),
        );
        self.entries.insert(key, entry);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = 0u64;
    for byte in b {
        h = h.wrapping_mul(31).wrapping_add(u64::from(*byte));
    }
    h
}
