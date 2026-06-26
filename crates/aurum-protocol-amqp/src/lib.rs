#![forbid(unsafe_code)]

pub mod harness;
pub mod method;
pub mod port;
pub mod session;
pub mod translate;
pub mod wire;

pub use harness::AmqpTranscriptHarness;
pub use method::AmqpMethod;
pub use port::{
    AmqpBrokerOutput, AmqpBrokerPort, AmqpControlCommand, AmqpControlResult, AmqpRouteResolveRequest,
    AmqpRouteResolveResult,
};
pub use session::{
    AmqpChannelState, AmqpConnectionState, AmqpOutbound, AmqpSession, ChannelPhase, ConnectionPhase,
    ConsumerTagMap, PendingPublishContent, PublishMetadata, RouteCache, RouteCacheEntry, SessionError,
};
pub use translate::{
    bind_queue_command, command_error_scope, declare_exchange_command, declare_queue_command,
    delivery_context_from_batch, delivery_metadata_from, encode_delivery_batches, send_deliver, send_method_frame,
    shortstr_from_bytes, AmqpErrorScope, REPLY_CHANNEL_ERROR, REPLY_COMMAND_INVALID,
};
pub use wire::{AmqpCodec, BasicProperties, FrameKind, RawFrame, ShortStr, WireError};

pub mod test_support;

// Legacy plugin boundary stub.
use aurum_plugin_api::{AdapterError, AdapterFrame, CommandBatch, ProtocolAdapter};
use aurum_types::CommandKind;

#[derive(Debug, Default)]
pub struct AmqpAdapter;

impl ProtocolAdapter for AmqpAdapter {
    fn name(&self) -> &'static str {
        "amqp-0-9-1"
    }

    fn translate(&mut self, frame: AdapterFrame<'_>, out: &mut CommandBatch) -> Result<(), AdapterError> {
        if frame.bytes.is_empty() {
            return Err(AdapterError::MalformedFrame);
        }
        out.kinds.push(CommandKind::PublishBatch);
        Ok(())
    }
}
