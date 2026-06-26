#![forbid(unsafe_code)]

pub mod adapter;
pub mod codec;
pub mod message;
pub mod wire;

#[cfg(test)]
mod tests;

pub use adapter::{
    BrokerCommandBatch, NativeAdapterError, NativeBrokerOutputView, NativeInboundAdapter,
    NativeInboundResult, NativeOutboundAdapter, NativeSessionState,
};
pub use codec::{NativeCodec, NativeDecodeError, NativeEncodeError, NativeFrame};
pub use wire::{
    FrameFlags, NativeCapabilities, NativeErrorCode, NativeFrameHeader, NativeOp,
    NATIVE_MAGIC, NATIVE_PROTOCOL_MAJOR, NATIVE_PROTOCOL_MINOR,
};
