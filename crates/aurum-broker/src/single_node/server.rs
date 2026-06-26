use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use aurum_internal_protocol::command::{
    ingress::IngressCommandBatch,
    shard::ShardCommandBatch,
};
use aurum_protocol_amqp::{
    AmqpBrokerOutput, AmqpBrokerPort, AmqpControlCommand, AmqpControlResult, AmqpOutbound,
    AmqpRouteResolveRequest, AmqpRouteResolveResult, AmqpSession,
};
use aurum_protocol_native::{
    BrokerCommandBatch, NativeBrokerOutputView, NativeCodec, NativeFrame, NativeInboundAdapter,
    NativeInboundResult, NativeOutboundAdapter, NativeSessionState, NativeOp, FrameFlags,
};
use aurum_types::{PayloadHandle, RouteTableVersion};
use bytes::{BufMut, Bytes, BytesMut};

use crate::in_memory::ShardOutputBatch;
use crate::single_node::broker::AmqpPayloadStore;
use crate::single_node::config::{ListenerEndpointConfig, SingleNodeBrokerConfig};
use crate::single_node::service::BrokerService;
use aurum_transport::{config::ListenerConfig, Connection, ConnectionId, ListenerFlags, spawn_blocking_listener};

pub struct NativeServerSession {
    connection_id: ConnectionId,
    codec: NativeCodec,
    inbound: NativeInboundAdapter,
    outbound: NativeOutboundAdapter,
    read_buf: BytesMut,
    service: Arc<BrokerService>,
}

impl NativeServerSession {
    pub fn new(connection_id: ConnectionId, service: Arc<BrokerService>) -> Self {
        Self {
            connection_id,
            codec: NativeCodec::default(),
            inbound: NativeInboundAdapter::new(NativeSessionState::new(connection_id.0)),
            outbound: NativeOutboundAdapter::default(),
            read_buf: BytesMut::new(),
            service,
        }
    }

    pub fn on_bytes(&mut self, input: &[u8]) -> Vec<u8> {
        self.read_buf.extend_from_slice(input);
        let mut out_bytes = Vec::new();
        while let Ok(Some(frame)) = self.codec.decode(&mut self.read_buf) {
            for resp in self.on_frame(frame) {
                let mut buf = BytesMut::new();
                let _ = self.codec.encode(&resp, &mut buf);
                out_bytes.extend_from_slice(&buf);
            }
        }
        out_bytes
    }

    fn on_frame(&mut self, frame: NativeFrame) -> Vec<NativeFrame> {
        let correlation_id = frame.header.correlation_id;
        let stream_id = frame.header.stream_id;
        match self.inbound.translate_frame(frame) {
            Ok(NativeInboundResult::ImmediateResponse(resp)) => vec![resp],
            Ok(NativeInboundResult::BrokerCommand(cmd)) => {
                let output = self.execute_command(cmd);
                self.encode_output(output, correlation_id, stream_id)
            }
            Err(_) => vec![native_error_frame(correlation_id, stream_id)],
        }
    }

    fn execute_command(&mut self, cmd: BrokerCommandBatch) -> ShardOutputBatch<PayloadHandle> {
        let routed = match cmd {
            BrokerCommandBatch::Ingress(ing) => {
                self.service.execute_ingress(self.connection_id, ing)
            }
            BrokerCommandBatch::Shard(sh) => self.service.execute_shard(self.connection_id, sh),
        };
        let mut merged = routed.immediate;
        let pushed = self.service.drain_connection_outputs(self.connection_id);
        merge_output(&mut merged, pushed);
        merged
    }

    fn encode_output(
        &mut self,
        output: ShardOutputBatch<PayloadHandle>,
        correlation_id: u64,
        stream_id: u32,
    ) -> Vec<NativeFrame> {
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
}

pub struct AmqpServerSession {
    connection_id: ConnectionId,
    session: AmqpSession<AmqpServicePort>,
    service: Arc<BrokerService>,
}

impl AmqpServerSession {
    pub fn new(connection_id: ConnectionId, service: Arc<BrokerService>) -> Self {
        let port = AmqpServicePort::new(connection_id, Arc::clone(&service));
        Self {
            connection_id,
            session: AmqpSession::new(port),
            service,
        }
    }

    pub fn on_bytes(&mut self, input: &[u8]) -> Vec<u8> {
        let mut out = AmqpOutbound::default();
        let _ = self.session.receive_bytes(input, &mut out);
        let mut response = encode_amqp_frames(&out.frames);
        let pushed = self.service.drain_connection_outputs(self.connection_id);
        if pushed.total_delivered() > 0 || !pushed.confirms.is_empty() {
            response.extend(self.encode_broker_output(pushed));
        }
        response
    }

    fn encode_broker_output(&mut self, output: ShardOutputBatch<PayloadHandle>) -> Vec<u8> {
        let amqp_out = AmqpServicePort::shard_to_amqp_output(
            self.session.broker().payload_store(),
            output,
        );
        let mut outbound = AmqpOutbound::default();
        let _ = self
            .session
            .push_broker_output(1, amqp_out, &mut outbound);
        encode_amqp_frames(&outbound.frames)
    }
}

pub struct AmqpServicePort {
    connection_id: ConnectionId,
    service: Arc<BrokerService>,
    payload_store: AmqpPayloadStore,
}

impl AmqpServicePort {
    fn new(connection_id: ConnectionId, service: Arc<BrokerService>) -> Self {
        Self {
            connection_id,
            service,
            payload_store: AmqpPayloadStore::default(),
        }
    }

    fn payload_store(&self) -> &AmqpPayloadStore {
        &self.payload_store
    }

    fn shard_to_amqp_output(
        store: &AmqpPayloadStore,
        out: ShardOutputBatch<PayloadHandle>,
    ) -> AmqpBrokerOutput {
        let mut deliveries = out.deliveries.to_vec();
        for batch in &mut deliveries {
            store.enrich_delivery(batch);
        }
        AmqpBrokerOutput {
            deliveries,
            confirms: out.confirms.to_vec(),
            settlements: out.settlements.to_vec(),
            consumer_events: out.consumer_events.to_vec(),
            route_resolved: out.route_resolved.to_vec(),
            errors: out.errors.to_vec(),
        }
    }
}

impl AmqpBrokerPort for AmqpServicePort {
    fn handle_control(&mut self, command: AmqpControlCommand) -> AmqpControlResult {
        self.service
            .shared_broker()
            .lock()
            .expect("broker lock")
            .handle_amqp_control(command)
    }

    fn handle_shard_batch(&mut self, batch: ShardCommandBatch) -> AmqpBrokerOutput {
        let routed = self.service.execute_shard(self.connection_id, batch);
        Self::shard_to_amqp_output(&self.payload_store, routed.immediate)
    }

    fn handle_ingress_batch(&mut self, batch: IngressCommandBatch) -> AmqpBrokerOutput {
        let routed = self.service.execute_ingress(self.connection_id, batch);
        Self::shard_to_amqp_output(&self.payload_store, routed.immediate)
    }

    fn resolve_route(&mut self, request: AmqpRouteResolveRequest) -> AmqpRouteResolveResult {
        self.service
            .shared_broker()
            .lock()
            .expect("broker lock")
            .resolve_amqp_route(request)
    }

    fn route_table_version(&self) -> RouteTableVersion {
        self.service
            .shared_broker()
            .lock()
            .expect("broker lock")
            .route_table_version()
    }

    fn store_payload(&mut self, handle: PayloadHandle, body: Bytes) {
        self.payload_store.payloads.insert(handle.0, body);
    }

    fn load_payload(&self, handle: PayloadHandle) -> Option<Bytes> {
        self.payload_store.payloads.get(&handle.0).cloned()
    }

    fn store_delivery_context(
        &mut self,
        handle: PayloadHandle,
        metadata: aurum_internal_protocol::event::delivery::DeliveryMetadata,
        properties: aurum_protocol_amqp::BasicProperties,
    ) {
        self.payload_store
            .delivery_metadata
            .insert(handle.0, metadata);
        self.payload_store
            .delivery_properties
            .insert(handle.0, properties);
    }

    fn delivery_properties(
        &self,
        handle: PayloadHandle,
    ) -> Option<aurum_protocol_amqp::BasicProperties> {
        self.payload_store
            .delivery_properties
            .get(&handle.0)
            .cloned()
    }
}

pub struct BrokerServer {
    service: Arc<BrokerService>,
    handles: Vec<JoinHandle<()>>,
    native_addr: Option<std::net::SocketAddr>,
    amqp_addr: Option<std::net::SocketAddr>,
}

impl BrokerServer {
    pub fn start(
        config: SingleNodeBrokerConfig,
    ) -> Result<Self, crate::single_node::error::BrokerInitError> {
        let service = Arc::new(BrokerService::new(config.clone())?);
        service.start();
        let mut handles = Vec::new();
        let mut native_addr = None;
        let mut amqp_addr = None;

        if let Some(native) = config.listeners.native.filter(|l| l.enabled) {
            let (handle, addr) = spawn_protocol_listener(
                Arc::clone(&service),
                native,
                ProtocolKind::Native,
            )?;
            handles.push(handle);
            native_addr = Some(addr);
        }
        if let Some(amqp) = config.listeners.amqp.filter(|l| l.enabled) {
            let (handle, addr) = spawn_protocol_listener(
                Arc::clone(&service),
                amqp,
                ProtocolKind::Amqp,
            )?;
            handles.push(handle);
            amqp_addr = Some(addr);
        }

        Ok(Self {
            service,
            handles,
            native_addr,
            amqp_addr,
        })
    }

    #[must_use]
    pub fn service(&self) -> Arc<BrokerService> {
        Arc::clone(&self.service)
    }

    #[must_use]
    pub fn native_addr(&self) -> Option<std::net::SocketAddr> {
        self.native_addr
    }

    #[must_use]
    pub fn amqp_addr(&self) -> Option<std::net::SocketAddr> {
        self.amqp_addr
    }
}

#[derive(Clone, Copy)]
enum ProtocolKind {
    Native,
    Amqp,
}

fn spawn_protocol_listener(
    service: Arc<BrokerService>,
    endpoint: ListenerEndpointConfig,
    kind: ProtocolKind,
) -> Result<(JoinHandle<()>, std::net::SocketAddr), crate::single_node::error::BrokerInitError> {
    let transport_config = ListenerConfig {
        bind: endpoint.bind,
        flags: ListenerFlags::ENABLED | ListenerFlags::TCP_NODELAY | ListenerFlags::ALLOW_PLAINTEXT,
        max_connections: 256,
        max_read_buffer: 64 * 1024,
        max_write_buffer: 256 * 1024,
    };
    let (listener_handle, addr) = spawn_blocking_listener(
        transport_config,
        Arc::new(move |id, conn| {
            let service = Arc::clone(&service);
            thread::Builder::new()
                .name(format!("aurum-conn-{}", id.0))
                .spawn(move || match kind {
                    ProtocolKind::Native => run_native_connection(id, conn, service),
                    ProtocolKind::Amqp => run_amqp_connection(id, conn, service),
                })
                .expect("spawn connection thread");
        }),
    )
    .map_err(|e| crate::single_node::error::BrokerInitError::Storage(e.to_string()))?;
    Ok((listener_handle, addr))
}

fn run_native_connection(
    connection_id: ConnectionId,
    mut conn: Connection,
    service: Arc<BrokerService>,
) {
    service.record_connection_accepted();
    let mut session = NativeServerSession::new(connection_id, Arc::clone(&service));
    let mut read_buf = [0u8; 64 * 1024];
    loop {
        match conn.read(&mut read_buf) {
            Ok(0) => break,
            Ok(n) => {
                let response = session.on_bytes(&read_buf[..n]);
                if !response.is_empty() && conn.write_all(&response).is_err() {
                    break;
                }
                service.record_io(
                    u64::try_from(n).unwrap_or(0),
                    u64::try_from(response.len()).unwrap_or(0),
                    1,
                    u64::from(!response.is_empty()),
                );
            }
            Err(_) => break,
        }
    }
    let _ = conn.shutdown();
    service.record_connection_closed();
}

fn run_amqp_connection(
    connection_id: ConnectionId,
    mut conn: Connection,
    service: Arc<BrokerService>,
) {
    service.record_connection_accepted();
    let mut session = AmqpServerSession::new(connection_id, Arc::clone(&service));
    let mut read_buf = [0u8; 64 * 1024];
    loop {
        match conn.read(&mut read_buf) {
            Ok(0) => break,
            Ok(n) => {
                let response = session.on_bytes(&read_buf[..n]);
                if !response.is_empty() && conn.write_all(&response).is_err() {
                    break;
                }
                service.record_io(
                    u64::try_from(n).unwrap_or(0),
                    u64::try_from(response.len()).unwrap_or(0),
                    1,
                    u64::from(!response.is_empty()),
                );
            }
            Err(_) => break,
        }
    }
    let _ = conn.shutdown();
    service.record_connection_closed();
}

fn encode_amqp_frames(frames: &[aurum_protocol_amqp::RawFrame]) -> Vec<u8> {
    let mut buf = Vec::new();
    for frame in frames {
        let mut tmp = BytesMut::new();
        frame.encode(&mut tmp);
        buf.extend_from_slice(&tmp);
    }
    buf
}

fn merge_output(dst: &mut ShardOutputBatch<PayloadHandle>, src: ShardOutputBatch<PayloadHandle>) {
    dst.deliveries.extend(src.deliveries);
    dst.confirms.extend(src.confirms);
    dst.settlements.extend(src.settlements);
    dst.consumer_events.extend(src.consumer_events);
    dst.route_resolved.extend(src.route_resolved);
    dst.errors.extend(src.errors);
}

fn native_error_frame(correlation_id: u64, stream_id: u32) -> NativeFrame {
    use aurum_protocol_native::message::ErrorBody;
    let body = ErrorBody {
        error_code: aurum_protocol_native::NativeErrorCode::Internal.as_u16(),
        correlation_id,
        message: b"adapter error".to_vec(),
    };
    let mut buf = BytesMut::new();
    let _ = body.encode(&mut buf);
    NativeFrame::new(
        aurum_protocol_native::NativeFrameHeader::new(
            NativeOp::ErrorFrame,
            FrameFlags::ERROR | FrameFlags::RESPONSE,
            stream_id,
            correlation_id,
            buf.len() as u32,
        ),
        buf.freeze(),
    )
}
