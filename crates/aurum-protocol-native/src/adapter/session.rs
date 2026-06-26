use std::collections::HashMap;

use aurum_types::{ChannelId, ConsumerId, QueueId, RouteId, RouteTableVersion};

use crate::message::HelloOkBody;
use crate::wire::{NativeCapabilities, NATIVE_PROTOCOL_MAJOR, NATIVE_PROTOCOL_MINOR};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerInfo {
    pub queue_id: QueueId,
    pub prefetch: u32,
}

#[derive(Debug, Clone)]
pub struct NativeSessionState {
    pub protocol_major: u16,
    pub protocol_minor: u16,
    pub connection_id: u64,
    pub capabilities: NativeCapabilities,
    pub last_seen_route_table_version: RouteTableVersion,
    pub next_consumer_id: u64,
    pub consumers: HashMap<ConsumerId, ConsumerInfo>,
    hello_done: bool,
}

impl Default for NativeSessionState {
    fn default() -> Self {
        Self::new(1)
    }
}

impl NativeSessionState {
    #[must_use]
    pub fn new(connection_id: u64) -> Self {
        Self {
            protocol_major: NATIVE_PROTOCOL_MAJOR,
            protocol_minor: NATIVE_PROTOCOL_MINOR,
            connection_id,
            capabilities: NativeCapabilities::ROUTE_ID
                | NativeCapabilities::PUBLISH_BATCH
                | NativeCapabilities::ACK_RANGE
                | NativeCapabilities::NACK_RANGE
                | NativeCapabilities::DELIVERY_BATCH,
            last_seen_route_table_version: RouteTableVersion::INITIAL,
            next_consumer_id: 1,
            consumers: HashMap::new(),
            hello_done: false,
        }
    }

    pub fn mark_hello(&mut self, major: u16, minor: u16, caps: NativeCapabilities) {
        self.protocol_major = major;
        self.protocol_minor = minor;
        self.capabilities = caps;
        self.hello_done = true;
    }

    #[must_use]
    pub fn is_hello_done(&self) -> bool {
        self.hello_done
    }

    pub fn assign_consumer_id(&mut self, hint: u32) -> ConsumerId {
        let id = if hint == 0 {
            let v = self.next_consumer_id;
            self.next_consumer_id += 1;
            v
        } else {
            u64::from(hint)
        };
        ConsumerId(id)
    }

    pub fn register_consumer(&mut self, id: ConsumerId, queue_id: QueueId, prefetch: u32) {
        self.consumers.insert(id, ConsumerInfo { queue_id, prefetch });
    }

    #[must_use]
    pub fn consumer_info(&self, id: ConsumerId) -> Option<ConsumerInfo> {
        self.consumers.get(&id).copied()
    }

    #[must_use]
    pub fn hello_ok_body(&self) -> HelloOkBody {
        HelloOkBody {
            server_major: NATIVE_PROTOCOL_MAJOR,
            server_minor: NATIVE_PROTOCOL_MINOR,
            server_capabilities: self.capabilities,
            connection_id: self.connection_id,
        }
    }

    #[must_use]
    pub fn channel_id(&self, stream_id: u32) -> ChannelId {
        ChannelId(stream_id)
    }

    pub fn update_route_version(&mut self, version: RouteTableVersion) {
        self.last_seen_route_table_version = version;
    }

    #[must_use]
    pub fn route_id_from_packed(packed: u64) -> RouteId {
        let (index, generation) = crate::codec::cursor::unpack_route_id(packed);
        RouteId::new(index, generation)
    }
}
