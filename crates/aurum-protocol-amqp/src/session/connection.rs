use crate::wire::ShortStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPhase {
    AwaitProtocolHeader,
    AwaitStartOk,
    AwaitTuneOk,
    AwaitOpen,
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone)]
pub struct AmqpConnectionState {
    pub phase: ConnectionPhase,
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
    pub virtual_host: ShortStr,
}

impl Default for AmqpConnectionState {
    fn default() -> Self {
        Self {
            phase: ConnectionPhase::AwaitProtocolHeader,
            channel_max: crate::wire::constants::DEFAULT_CHANNEL_MAX,
            frame_max: crate::wire::constants::DEFAULT_FRAME_MAX,
            heartbeat: crate::wire::constants::DEFAULT_HEARTBEAT,
            virtual_host: ShortStr::from("/"),
        }
    }
}
