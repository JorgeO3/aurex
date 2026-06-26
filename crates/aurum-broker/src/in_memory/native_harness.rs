use std::sync::Arc;

use bytes::{BytesMut, BufMut};
use aurum_protocol_native::{
    BrokerCommandBatch, NativeBrokerOutputView, NativeCodec, NativeFrame, NativeInboundAdapter,
    NativeInboundResult, NativeOutboundAdapter, NativeSessionState,
    NativeOp, FrameFlags, NativeFrameHeader,
};
use aurum_routing::{RouteTable, RouteCompiler, RoutingConfig, BindingDecl, ExchangeDecl};
use aurum_types::{ExchangeId, PayloadHandle, QueueId, RouteTableVersion};

use super::broker::InMemoryBroker;

/// In-process harness: native bytes → broker → native response bytes.
pub struct NativeInMemoryHarness {
    codec: NativeCodec,
    inbound: NativeInboundAdapter,
    outbound: NativeOutboundAdapter,
    broker: InMemoryBroker,
    read_buf: BytesMut,
}

impl NativeInMemoryHarness {
    #[must_use]
    pub fn new() -> Self {
        Self::with_route_table(Arc::new(RouteTable::new_empty(RouteTableVersion::INITIAL)))
    }

    #[must_use]
    pub fn with_route_table(route_table: Arc<RouteTable>) -> Self {
        Self {
            codec: NativeCodec::default(),
            inbound: NativeInboundAdapter::new(NativeSessionState::new(1)),
            outbound: NativeOutboundAdapter::default(),
            broker: InMemoryBroker::with_route_table(route_table),
            read_buf: BytesMut::new(),
        }
    }

    #[must_use]
    pub fn with_orders_route() -> Self {
        let mut config = RoutingConfig::new(RouteTableVersion::INITIAL);
        config.add_exchange(ExchangeDecl::direct(ExchangeId(1), "orders"));
        config.add_binding(BindingDecl::direct(ExchangeId(1), QueueId(10), "created"));
        let table = Arc::new(RouteCompiler::compile(&config).unwrap());
        Self::with_route_table(table)
    }

    #[must_use]
    pub fn broker(&self) -> &InMemoryBroker {
        &self.broker
    }

    #[must_use]
    pub fn broker_mut(&mut self) -> &mut InMemoryBroker {
        &mut self.broker
    }

    pub fn send_bytes(&mut self, bytes: &[u8]) -> Vec<u8> {
        self.read_buf.extend_from_slice(bytes);
        let mut out_bytes = Vec::new();
        while let Ok(Some(frame)) = self.codec.decode(&mut self.read_buf) {
            let responses = self.send_frame(frame);
            for resp in responses {
                let mut buf = BytesMut::new();
                self.codec.encode(&resp, &mut buf).expect("encode");
                out_bytes.extend_from_slice(&buf);
            }
        }
        out_bytes
    }

    pub fn send_frame(&mut self, frame: NativeFrame) -> Vec<NativeFrame> {
        let correlation_id = frame.header.correlation_id;
        let stream_id = frame.header.stream_id;
        match self.inbound.translate_frame(frame) {
            Ok(NativeInboundResult::ImmediateResponse(resp)) => vec![resp],
            Ok(NativeInboundResult::BrokerCommand(cmd)) => {
                let output = self.execute_command(cmd);
                let view = NativeBrokerOutputView {
                    deliveries: &output.deliveries,
                    confirms: &output.confirms,
                    settlements: &output.settlements,
                    consumer_events: &output.consumer_events,
                    route_resolved: &output.route_resolved,
                    errors: &output.errors,
                };
                let mut frames = smallvec::SmallVec::new();
                self.outbound.translate_outputs(
                    self.inbound.session(),
                    &view,
                    correlation_id,
                    stream_id,
                    &mut frames,
                );
                frames.into_vec()
            }
            Err(_) => {
                vec![error_frame(correlation_id, stream_id)]
            }
        }
    }

    fn execute_command(&mut self, cmd: BrokerCommandBatch) -> super::output::ShardOutputBatch<PayloadHandle> {
        match cmd {
            BrokerCommandBatch::Ingress(ing) => self.broker.execute_ingress(ing),
            BrokerCommandBatch::Shard(sh) => self.broker.execute(sh),
        }
    }
}

impl Default for NativeInMemoryHarness {
    fn default() -> Self {
        Self::new()
    }
}

fn error_frame(correlation_id: u64, stream_id: u32) -> NativeFrame {
    use aurum_protocol_native::message::ErrorBody;
    let body = ErrorBody {
        error_code: aurum_protocol_native::NativeErrorCode::Internal.as_u16(),
        correlation_id,
        message: b"adapter error".to_vec(),
    };
    let mut buf = BytesMut::new();
    let _ = body.encode(&mut buf);
    NativeFrame::new(
        NativeFrameHeader::new(
            NativeOp::ErrorFrame,
            FrameFlags::ERROR | FrameFlags::RESPONSE,
            stream_id,
            correlation_id,
            buf.len() as u32,
        ),
        buf.freeze(),
    )
}
