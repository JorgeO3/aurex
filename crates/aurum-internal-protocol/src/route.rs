use aurum_types::{ExchangeId, RouteId, RouteTableVersion};

/// Correlation id for resolve-route request/response pairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct CorrelationId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutePublishTarget {
    pub route_id: RouteId,
    pub route_version: RouteTableVersion,
}

impl RoutePublishTarget {
    #[must_use]
    pub const fn new(route_id: RouteId, route_version: RouteTableVersion) -> Self {
        Self { route_id, route_version }
    }
}

#[derive(Debug, Clone)]
pub struct ResolveRouteCommand {
    pub request_id: CorrelationId,
    pub exchange_id: ExchangeId,
    pub exchange_name: smallvec::SmallVec<[u8; 64]>,
    pub routing_key: smallvec::SmallVec<[u8; 64]>,
}

impl ResolveRouteCommand {
    #[must_use]
    pub fn by_id(request_id: CorrelationId, exchange_id: ExchangeId, routing_key: &[u8]) -> Self {
        let mut key = smallvec::SmallVec::new();
        key.extend_from_slice(routing_key);
        Self {
            request_id,
            exchange_id,
            exchange_name: smallvec::SmallVec::new(),
            routing_key: key,
        }
    }

    #[must_use]
    pub fn by_name(request_id: CorrelationId, exchange_name: &[u8], routing_key: &[u8]) -> Self {
        let mut name = smallvec::SmallVec::new();
        name.extend_from_slice(exchange_name);
        let mut key = smallvec::SmallVec::new();
        key.extend_from_slice(routing_key);
        Self {
            request_id,
            exchange_id: ExchangeId(0),
            exchange_name: name,
            routing_key: key,
        }
    }

    #[must_use]
    pub fn new(request_id: CorrelationId, exchange_id: ExchangeId, routing_key: &[u8]) -> Self {
        Self::by_id(request_id, exchange_id, routing_key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteResolvedEvent {
    pub request_id: CorrelationId,
    pub route_id: RouteId,
    pub route_version: RouteTableVersion,
    pub flags: u16,
}
