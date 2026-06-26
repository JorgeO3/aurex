use std::collections::HashMap;

use super::consumers::ConsumerTagMap;
use super::content::PendingPublishContent;
use super::route_cache::RouteCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelPhase {
    Closed,
    Opening,
    Open,
    Closing,
}

#[derive(Debug)]
pub struct AmqpChannelState {
    pub channel_id: u16,
    pub phase: ChannelPhase,
    pub prefetch_count: u16,
    pub consumers: ConsumerTagMap,
    pub delivery_consumers: HashMap<u64, aurum_types::ConsumerId>,
    pub pending_publish: Option<PendingPublishContent>,
    pub route_cache: RouteCache,
}

impl AmqpChannelState {
    #[must_use]
    pub fn new(channel_id: u16) -> Self {
        Self {
            channel_id,
            phase: ChannelPhase::Closed,
            prefetch_count: 0,
            consumers: ConsumerTagMap::default(),
            delivery_consumers: HashMap::new(),
            pending_publish: None,
            route_cache: RouteCache::default(),
        }
    }
}
