/// Magic bytes `AQ` encoded as little-endian `u16`.
pub const NATIVE_MAGIC: u16 = 0x5141;

pub const NATIVE_HEADER_LEN: u8 = 32;

pub const NATIVE_PROTOCOL_MAJOR: u16 = 0;
pub const NATIVE_PROTOCOL_MINOR: u16 = 1;

/// Wire `version` byte in the fixed header (major revision).
pub const NATIVE_WIRE_VERSION: u8 = 1;

pub const DEFAULT_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

pub const MAX_EXCHANGE_NAME_LEN: usize = 255;
pub const MAX_ROUTING_KEY_LEN: usize = 4096;
pub const MAX_PUBLISH_BATCH_MESSAGES: u32 = 4096;
pub const MAX_PAYLOAD_SIZE: u32 = 16 * 1024 * 1024;
pub const MAX_ERROR_MESSAGE_LEN: usize = 4096;
