#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    Starting = 0,
    Running = 1,
    Draining = 2,
    Stopping = 3,
    Stopped = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerHealth {
    pub state: ServerState,
    pub route_table_version: aurum_types::RouteTableVersion,
}
