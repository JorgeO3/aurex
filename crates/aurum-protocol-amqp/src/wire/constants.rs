//! AMQP 0-9-1 wire constants.

pub const PROTOCOL_HEADER: &[u8; 8] = b"AMQP\x00\x00\x09\x01";
pub const FRAME_END: u8 = 0xCE;

pub const FRAME_TYPE_METHOD: u8 = 1;
pub const FRAME_TYPE_HEADER: u8 = 2;
pub const FRAME_TYPE_BODY: u8 = 3;
pub const FRAME_TYPE_HEARTBEAT: u8 = 4;

pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;
pub const CLASS_CONFIRM: u16 = 85;

pub mod connection {
    pub const START: u16 = 10;
    pub const START_OK: u16 = 11;
    pub const TUNE: u16 = 30;
    pub const TUNE_OK: u16 = 31;
    pub const OPEN: u16 = 40;
    pub const OPEN_OK: u16 = 41;
    pub const CLOSE: u16 = 50;
    pub const CLOSE_OK: u16 = 51;
}

pub mod channel {
    pub const OPEN: u16 = 10;
    pub const OPEN_OK: u16 = 11;
    pub const CLOSE: u16 = 40;
    pub const CLOSE_OK: u16 = 41;
}

pub mod exchange {
    pub const DECLARE: u16 = 10;
    pub const DECLARE_OK: u16 = 11;
}

pub mod queue {
    pub const DECLARE: u16 = 10;
    pub const DECLARE_OK: u16 = 11;
    pub const BIND: u16 = 20;
    pub const BIND_OK: u16 = 21;
}

pub mod basic {
    pub const QOS: u16 = 10;
    pub const QOS_OK: u16 = 11;
    pub const CONSUME: u16 = 20;
    pub const CONSUME_OK: u16 = 21;
    pub const CANCEL: u16 = 30;
    pub const CANCEL_OK: u16 = 31;
    pub const PUBLISH: u16 = 40;
    pub const DELIVER: u16 = 60;
    pub const ACK: u16 = 80;
    pub const REJECT: u16 = 90;
    pub const NACK: u16 = 120;
}

pub mod confirm {
    pub const SELECT: u16 = 10;
    pub const SELECT_OK: u16 = 11;
}

pub const REPLY_SUCCESS: u16 = 200;
pub const REPLY_CHANNEL_ERROR: u16 = 504;
pub const REPLY_CONNECTION_FORCED: u16 = 320;
pub const REPLY_FRAME_ERROR: u16 = 501;
pub const REPLY_SYNTAX_ERROR: u16 = 502;
pub const REPLY_COMMAND_INVALID: u16 = 503;
pub const REPLY_NOT_IMPLEMENTED: u16 = 540;
pub const REPLY_INTERNAL_ERROR: u16 = 541;

pub const DEFAULT_FRAME_MAX: u32 = 131_072;
pub const DEFAULT_CHANNEL_MAX: u16 = 2047;
pub const DEFAULT_HEARTBEAT: u16 = 60;
