use aurum_internal_protocol::{
    command::{
        consume::{CancelConsumer, CancelConsumerBatch, ConsumeCommandBatch, ConsumeStart},
        ingress::IngressCommandBatch,
        publish::{ConfirmMode, IngressPublishBatch, IngressPublishTarget, PublishRecord},
        settlement::{AckCommandBatch, NackCommand, NackCommandBatch, NackDisposition, SettlementMode},
        shard::ShardCommandBatch,
    },
    flags::MessageFlags,
    route::RoutePublishTarget,
};
use aurum_types::{BatchId, ChannelId, ConsumerId, DeliveryTag, PayloadHandle, QueueId, SourceId};
use bytes::{Buf, BytesMut};
use smallvec::smallvec;
use std::collections::HashMap;

use crate::method::{
    AmqpMethod, BasicMethod, ChannelMethod, ConfirmMethod, ConnectionMethod, ExchangeMethod, QueueMethod,
};
use crate::port::{AmqpBrokerOutput, AmqpBrokerPort, AmqpControlCommand, AmqpRouteResolveRequest};
use crate::session::{
    AmqpChannelState, AmqpConnectionState, ChannelPhase, ConnectionPhase, PendingPublishContent,
    SessionError,
};
use crate::translate::{
    bind_queue_command, command_error_scope, declare_exchange_command, declare_queue_command,
    delivery_metadata_from, encode_delivery_batches, send_method_frame, AmqpErrorScope,
    REPLY_CHANNEL_ERROR, REPLY_COMMAND_INVALID,
};
use crate::wire::constants::{
    CLASS_BASIC, DEFAULT_CHANNEL_MAX, DEFAULT_FRAME_MAX, DEFAULT_HEARTBEAT, REPLY_NOT_IMPLEMENTED,
};
use crate::wire::field_table::{FieldTable, FieldValue};
use crate::wire::frame::{FrameKind, RawFrame};
use crate::wire::properties::{BasicProperties, ContentHeader};
use crate::wire::{AmqpCodec, ShortStr};

#[derive(Debug, Default)]
pub struct AmqpOutbound {
    pub frames: Vec<RawFrame>,
}

pub struct AmqpSession<B> {
    broker: B,
    connection: AmqpConnectionState,
    channels: HashMap<u16, AmqpChannelState>,
    codec: AmqpCodec,
    protocol_header_seen: bool,
    next_payload_handle: u64,
}

impl<B: AmqpBrokerPort> AmqpSession<B> {
    #[must_use]
    pub fn new(broker: B) -> Self {
        Self {
            broker,
            connection: AmqpConnectionState::default(),
            channels: HashMap::new(),
            codec: AmqpCodec::new(DEFAULT_FRAME_MAX),
            protocol_header_seen: false,
            next_payload_handle: 1,
        }
    }

    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    #[must_use]
    pub fn broker_mut(&mut self) -> &mut B {
        &mut self.broker
    }

    pub fn drain_broker_outputs(
        &mut self,
        channel: u16,
        out: &mut AmqpOutbound,
    ) -> Result<(), SessionError> {
        self.emit_broker_output(channel, &AmqpBrokerOutput::default(), out)
    }

    /// Encode broker output batches (e.g. pushed deliveries) to wire frames.
    pub fn push_broker_output(
        &mut self,
        channel: u16,
        output: AmqpBrokerOutput,
        out: &mut AmqpOutbound,
    ) -> Result<(), SessionError> {
        self.emit_broker_output(channel, &output, out)
    }

    pub fn receive_bytes(&mut self, input: &[u8], out: &mut AmqpOutbound) -> Result<(), SessionError> {
        let mut buf = BytesMut::from(input);
        if !self.protocol_header_seen {
            if buf.len() < 8 {
                return Ok(());
            }
            if &buf[..8] != crate::wire::constants::PROTOCOL_HEADER {
                return Err(SessionError::Protocol("invalid protocol header".into()));
            }
            self.protocol_header_seen = true;
            buf.advance(8);
            self.send_connection_start(out)?;
            if buf.is_empty() {
                return Ok(());
            }
        }
        while let Some(frame) = self.codec.decode(&mut buf).map_err(SessionError::Wire)? {
            self.receive_frame(frame, out)?;
        }
        Ok(())
    }

    pub fn receive_frame(&mut self, frame: RawFrame, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        match frame.header.kind {
            FrameKind::Method => self.on_method_frame(frame, out),
            FrameKind::Header => self.on_header_frame(frame, out),
            FrameKind::Body => self.on_body_frame(frame, out),
            FrameKind::Heartbeat => Ok(()),
        }
    }

    fn on_method_frame(&mut self, frame: RawFrame, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        if frame.header.channel == 0 {
            return self.on_connection_method(frame, out);
        }
        let mut buf = frame.payload.as_ref();
        let class_id = u16::from_be_bytes(buf[..2].try_into().expect("class"));
        buf.advance(2);
        let method_id = u16::from_be_bytes(buf[..2].try_into().expect("method"));
        buf.advance(2);
        let method = crate::method::decode_method(class_id, method_id, buf)?;
        self.on_channel_method(frame.header.channel, method, out)
    }

    fn on_connection_method(&mut self, frame: RawFrame, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        let mut buf = frame.payload.as_ref();
        let class_id = u16::from_be_bytes(buf[..2].try_into().expect("class"));
        buf.advance(2);
        let method_id = u16::from_be_bytes(buf[..2].try_into().expect("method"));
        buf.advance(2);
        let method = crate::method::decode_method(class_id, method_id, buf)?;
        let AmqpMethod::Connection(conn) = method else {
            return Err(SessionError::Protocol("expected connection method".into()));
        };
        match conn {
            ConnectionMethod::StartOk(m) => {
                if self.connection.phase != ConnectionPhase::AwaitStartOk {
                    return self.connection_close(out, REPLY_COMMAND_INVALID);
                }
                let _ = m;
                self.connection.phase = ConnectionPhase::AwaitTuneOk;
                self.send_connection_tune(out)?;
            }
            ConnectionMethod::TuneOk(m) => {
                self.connection.channel_max = m.channel_max;
                self.connection.frame_max = m.frame_max;
                self.connection.heartbeat = m.heartbeat;
                self.codec.max_frame_size = m.frame_max;
                self.connection.phase = ConnectionPhase::AwaitOpen;
            }
            ConnectionMethod::Open(m) => {
                self.connection.virtual_host = m.virtual_host;
                self.connection.phase = ConnectionPhase::Open;
                self.send_method(0, AmqpMethod::Connection(ConnectionMethod::OpenOk), out)?;
            }
            ConnectionMethod::CloseOk => {
                self.connection.phase = ConnectionPhase::Closed;
            }
            ConnectionMethod::Close(_) => {
                self.send_method(0, AmqpMethod::Connection(ConnectionMethod::CloseOk), out)?;
                self.connection.phase = ConnectionPhase::Closed;
            }
            _ => return self.connection_close(out, REPLY_COMMAND_INVALID),
        }
        Ok(())
    }

    fn on_channel_method(
        &mut self,
        channel: u16,
        method: AmqpMethod,
        out: &mut AmqpOutbound,
    ) -> Result<(), SessionError> {
        if self.connection.phase != ConnectionPhase::Open {
            return self.connection_close(out, REPLY_COMMAND_INVALID);
        }
        match method {
            AmqpMethod::Channel(ChannelMethod::Open(open)) => {
                let mut ch = AmqpChannelState::new(channel);
                ch.phase = ChannelPhase::Open;
                let _ = open;
                self.channels.insert(channel, ch);
                self.send_method(channel, AmqpMethod::Channel(ChannelMethod::OpenOk), out)?;
            }
            AmqpMethod::Channel(ChannelMethod::Close(close)) => {
                self.send_channel_close(channel, close.reply_code, &close.reply_text, out)?;
                if let Some(ch) = self.channels.get_mut(&channel) {
                    ch.phase = ChannelPhase::Closed;
                }
            }
            AmqpMethod::Channel(ChannelMethod::CloseOk) => {
                if let Some(ch) = self.channels.get_mut(&channel) {
                    ch.phase = ChannelPhase::Closed;
                }
            }
            AmqpMethod::Exchange(ExchangeMethod::Declare(decl)) => {
                self.ensure_channel_open(channel)?;
                let result = self.broker.handle_control(declare_exchange_command(&decl));
                if result.is_err() {
                    return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
                }
                self.send_method(channel, AmqpMethod::Exchange(ExchangeMethod::DeclareOk), out)?;
            }
            AmqpMethod::Queue(QueueMethod::Declare(decl)) => {
                self.ensure_channel_open(channel)?;
                if decl.passive {
                    return self.channel_close(channel, REPLY_NOT_IMPLEMENTED, out);
                }
                let result = self.broker.handle_control(declare_queue_command(&decl));
                let queue_name = if decl.queue.as_bytes().is_empty() {
                    result.queue_name.clone()
                } else {
                    decl.queue.to_string_lossy()
                };
                if result.is_err() {
                    return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
                }
                self.send_method(
                    channel,
                    AmqpMethod::Queue(QueueMethod::DeclareOk(
                        crate::method::QueueDeclareOk {
                            queue: ShortStr::from(queue_name.as_str()),
                            message_count: 0,
                            consumer_count: 0,
                        },
                    )),
                    out,
                )?;
            }
            AmqpMethod::Queue(QueueMethod::Bind(bind)) => {
                self.ensure_channel_open(channel)?;
                let result = self.broker.handle_control(bind_queue_command(&bind));
                if result.is_err() {
                    return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
                }
                self.send_method(channel, AmqpMethod::Queue(QueueMethod::BindOk), out)?;
            }
            AmqpMethod::Basic(BasicMethod::Qos(qos)) => {
                self.ensure_channel_open(channel)?;
                if let Some(ch) = self.channels.get_mut(&channel) {
                    ch.prefetch_count = qos.prefetch_count;
                }
                self.send_method(channel, AmqpMethod::Basic(BasicMethod::QosOk), out)?;
            }
            AmqpMethod::Basic(BasicMethod::Consume(consume)) => {
                self.ensure_channel_open(channel)?;
                if consume.no_ack {
                    return self.channel_close(channel, REPLY_NOT_IMPLEMENTED, out);
                }
                let tag = if consume.consumer_tag.as_bytes().is_empty() {
                    ShortStr::from("ctag-1")
                } else {
                    consume.consumer_tag.clone()
                };
                let consumer_id = {
                    let ch = self.channels.get_mut(&channel).expect("channel");
                    ch.consumers.insert(tag.clone())
                };
                let queue_id = self
                    .broker
                    .handle_control(AmqpControlCommand::ResolveQueueId {
                        name: consume.queue.to_string_lossy(),
                    })
                    .queue_id
                    .unwrap_or(QueueId(0));
                let prefetch = u32::from(
                    self.channels.get(&channel).map(|c| c.prefetch_count).unwrap_or(0),
                );
                let batch = ShardCommandBatch::Consume(ConsumeCommandBatch::one(ConsumeStart::new(
                    consumer_id,
                    ChannelId(channel as u32),
                    queue_id,
                    prefetch,
                )));
                let output = self.broker.handle_shard_batch(batch);
                self.emit_broker_output(channel, &output, out)?;
                self.send_method(
                    channel,
                    AmqpMethod::Basic(BasicMethod::ConsumeOk(crate::method::BasicConsumeOk {
                        consumer_tag: tag,
                    })),
                    out,
                )?;
            }
            AmqpMethod::Basic(BasicMethod::Cancel(cancel)) => {
                self.ensure_channel_open(channel)?;
                let consumer_id = self
                    .channels
                    .get(&channel)
                    .and_then(|c| c.consumers.get(&cancel.consumer_tag))
                    .ok_or(SessionError::ChannelClosed(channel))?;
                let batch = ShardCommandBatch::Cancel(CancelConsumerBatch::one(CancelConsumer::requeue(
                    consumer_id,
                )));
                let _ = self.broker.handle_shard_batch(batch);
                let _ = self
                    .channels
                    .get_mut(&channel)
                    .map(|c| c.consumers.remove(&cancel.consumer_tag));
                self.send_method(channel, AmqpMethod::Basic(BasicMethod::CancelOk), out)?;
            }
            AmqpMethod::Basic(BasicMethod::Publish(publish)) => {
                self.ensure_channel_open(channel)?;
                if publish.flags.contains(crate::method::BasicPublishFlags::IMMEDIATE) {
                    return self.channel_close(channel, REPLY_NOT_IMPLEMENTED, out);
                }
                let ch = self.channels.get_mut(&channel).expect("channel");
                if ch.pending_publish.is_some() {
                    return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
                }
                ch.pending_publish = Some(PendingPublishContent {
                    publish,
                    properties: BasicProperties::default(),
                    expected_body_size: 0,
                    body: BytesMut::new(),
                });
            }
            AmqpMethod::Basic(BasicMethod::Ack(ack)) => {
                self.ensure_channel_open(channel)?;
                let consumer_id = self.consumer_for_delivery_tag(channel, ack.delivery_tag)?;
                let batch = if ack.multiple {
                    AckCommandBatch::multiple(consumer_id, DeliveryTag(ack.delivery_tag))
                } else {
                    AckCommandBatch::one(consumer_id, DeliveryTag(ack.delivery_tag))
                };
                let output = self.broker.handle_shard_batch(ShardCommandBatch::Ack(batch));
                self.emit_broker_output(channel, &output, out)?;
            }
            AmqpMethod::Basic(BasicMethod::Nack(nack)) => {
                self.ensure_channel_open(channel)?;
                let consumer_id = self.consumer_for_delivery_tag(channel, nack.delivery_tag)?;
                let mode = if nack.multiple {
                    SettlementMode::Multiple
                } else {
                    SettlementMode::One
                };
                let disposition = if nack.requeue {
                    NackDisposition::Requeue
                } else {
                    NackDisposition::DeadLetter
                };
                let batch = NackCommandBatch {
                    consumer_id,
                    flags: Default::default(),
                    items: smallvec![NackCommand::Tag {
                        tag: DeliveryTag(nack.delivery_tag),
                        mode,
                        disposition,
                    }],
                };
                let output = self.broker.handle_shard_batch(ShardCommandBatch::Nack(batch));
                self.emit_broker_output(channel, &output, out)?;
            }
            AmqpMethod::Basic(BasicMethod::Reject(reject)) => {
                self.ensure_channel_open(channel)?;
                let consumer_id = self.consumer_for_delivery_tag(channel, reject.delivery_tag)?;
                let batch = if reject.requeue {
                    NackCommandBatch {
                        consumer_id,
                        flags: Default::default(),
                        items: smallvec![NackCommand::Tag {
                            tag: DeliveryTag(reject.delivery_tag),
                            mode: SettlementMode::One,
                            disposition: NackDisposition::Requeue,
                        }],
                    }
                } else {
                    NackCommandBatch {
                        consumer_id,
                        flags: Default::default(),
                        items: smallvec![NackCommand::Tag {
                            tag: DeliveryTag(reject.delivery_tag),
                            mode: SettlementMode::One,
                            disposition: NackDisposition::DeadLetter,
                        }],
                    }
                };
                let output = self.broker.handle_shard_batch(ShardCommandBatch::Nack(batch));
                self.emit_broker_output(channel, &output, out)?;
            }
            AmqpMethod::Confirm(ConfirmMethod::Select { .. }) => {
                self.ensure_channel_open(channel)?;
                self.send_method(channel, AmqpMethod::Confirm(ConfirmMethod::SelectOk), out)?;
            }
            _ => return self.channel_close(channel, REPLY_COMMAND_INVALID, out),
        }
        Ok(())
    }

    fn on_header_frame(&mut self, frame: RawFrame, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        self.ensure_channel_open(frame.header.channel)?;
        let ch = self.channels.get_mut(&frame.header.channel).expect("channel");
        let Some(pending) = ch.pending_publish.as_mut() else {
            return self.channel_close(frame.header.channel, REPLY_CHANNEL_ERROR, out);
        };
        let mut buf = frame.payload.as_ref();
        let header = ContentHeader::decode(&mut buf)?;
        if header.class_id != CLASS_BASIC {
            return self.channel_close(frame.header.channel, REPLY_CHANNEL_ERROR, out);
        }
        pending.properties = header.properties;
        pending.expected_body_size = header.body_size;
        if pending.expected_body_size == 0 {
            self.complete_publish(frame.header.channel, out)?;
        }
        Ok(())
    }

    fn on_body_frame(&mut self, frame: RawFrame, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        let channel = frame.header.channel;
        self.ensure_channel_open(channel)?;
        let complete = {
            let ch = self.channels.get_mut(&channel).expect("channel");
            let Some(pending) = ch.pending_publish.as_mut() else {
                return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
            };
            pending.body.extend_from_slice(&frame.payload);
            if pending.body.len() as u64 > pending.expected_body_size {
                return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
            }
            pending.body.len() as u64 == pending.expected_body_size
        };
        if complete {
            self.complete_publish(channel, out)?;
        }
        Ok(())
    }

    fn complete_publish(&mut self, channel: u16, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        let pending = self
            .channels
            .get_mut(&channel)
            .and_then(|c| c.pending_publish.take())
            .expect("pending publish");
        if pending.publish.flags.contains(crate::method::BasicPublishFlags::IMMEDIATE) {
            return self.channel_close(channel, REPLY_NOT_IMPLEMENTED, out);
        }
        let exchange = pending.publish.exchange.clone();
        let routing_key = pending.publish.routing_key.clone();
        let body = pending.body.freeze();
        let version = self.broker.route_table_version();
        let route = {
            let ch = self.channels.get(&channel).expect("channel");
            ch.route_cache
                .get(&exchange, &routing_key, version)
        };
        let route_target = match route {
            Some(entry) => RoutePublishTarget::new(entry.route_id, entry.route_version),
            None => {
                let resolved = self.broker.resolve_route(AmqpRouteResolveRequest {
                    exchange: exchange.to_string_lossy(),
                    routing_key: routing_key.to_string_lossy(),
                });
                if let Some(entry) = resolved.entry {
                    if let Some(ch) = self.channels.get_mut(&channel) {
                        ch.route_cache.insert(&exchange, &routing_key, entry);
                    }
                    RoutePublishTarget::new(entry.route_id, entry.route_version)
                } else {
                    if pending
                        .publish
                        .flags
                        .contains(crate::method::BasicPublishFlags::MANDATORY)
                    {
                        return self.channel_close(channel, REPLY_CHANNEL_ERROR, out);
                    }
                    return Ok(());
                }
            }
        };
        let handle = self.alloc_payload_handle();
        self.broker.store_payload(handle, body.clone());
        let metadata = delivery_metadata_from(&exchange, &routing_key, &pending.properties);
        self.broker
            .store_delivery_context(handle, metadata, pending.properties.clone());
        let mut records = smallvec::SmallVec::new();
        records.push(PublishRecord {
            payload: handle,
            payload_len: body.len() as u32,
            message_flags: MessageFlags::empty(),
            priority: pending.properties.priority.unwrap_or(0),
            expiration_ms: None,
            key_hash: 0,
        });
        let confirm_mode = if pending.properties.delivery_mode == Some(2) {
            ConfirmMode::LocalDurable
        } else {
            ConfirmMode::None
        };
        let batch = IngressCommandBatch::Publish(IngressPublishBatch {
            batch_id: BatchId::default(),
            source: SourceId::default(),
            target: IngressPublishTarget::Route(route_target),
            flags: Default::default(),
            confirm_mode,
            records,
        });
        let output = self.broker.handle_ingress_batch(batch);
        self.emit_broker_output(channel, &output, out)?;
        Ok(())
    }

    fn emit_broker_output(
        &mut self,
        channel: u16,
        output: &AmqpBrokerOutput,
        out: &mut AmqpOutbound,
    ) -> Result<(), SessionError> {
        for err in &output.errors {
            match command_error_scope(err) {
                AmqpErrorScope::Connection => {
                    self.connection_close(out, REPLY_COMMAND_INVALID)?;
                }
                AmqpErrorScope::Channel(_) => {
                    self.channel_close(channel, REPLY_CHANNEL_ERROR, out)?;
                }
            }
        }
        encode_delivery_batches(
            &self.broker,
            &mut self.channels,
            self.connection.frame_max,
            channel,
            &output.deliveries,
            out,
        )
    }

    fn send_connection_start(&mut self, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        self.connection.phase = ConnectionPhase::AwaitStartOk;
        let mut props = FieldTable::default();
        props.insert("product", FieldValue::ShortString(ShortStr::from("AurumMQ")));
        props.insert("version", FieldValue::ShortString(ShortStr::from("0.1.0")));
        self.send_method(
            0,
            AmqpMethod::Connection(ConnectionMethod::Start(
                crate::method::ConnectionStart {
                    version_major: 0,
                    version_minor: 9,
                    server_properties: props,
                    mechanisms: b"PLAIN".to_vec(),
                    locales: b"en_US".to_vec(),
                },
            )),
            out,
        )
    }

    fn send_connection_tune(&mut self, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        self.send_method(
            0,
            AmqpMethod::Connection(ConnectionMethod::Tune(
                crate::method::ConnectionTune {
                    channel_max: DEFAULT_CHANNEL_MAX,
                    frame_max: DEFAULT_FRAME_MAX,
                    heartbeat: DEFAULT_HEARTBEAT,
                },
            )),
            out,
        )
    }

    fn send_method(&mut self, channel: u16, method: AmqpMethod, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        send_method_frame(channel, method, out)
    }

    fn ensure_channel_open(&self, channel: u16) -> Result<(), SessionError> {
        match self.channels.get(&channel) {
            Some(ch) if ch.phase == ChannelPhase::Open => Ok(()),
            _ => Err(SessionError::ChannelClosed(channel)),
        }
    }

    fn consumer_for_delivery_tag(&self, channel: u16, tag: u64) -> Result<ConsumerId, SessionError> {
        let ch = self.channels.get(&channel).ok_or(SessionError::ChannelClosed(channel))?;
        ch.delivery_consumers
            .get(&tag)
            .copied()
            .or_else(|| ch.consumers.first_consumer_id())
            .ok_or(SessionError::ChannelClosed(channel))
    }

    fn channel_close(&mut self, channel: u16, code: u16, out: &mut AmqpOutbound) -> Result<(), SessionError> {
        self.send_channel_close(channel, code, &ShortStr::from("error"), out)?;
        Ok(())
    }

    fn send_channel_close(
        &mut self,
        channel: u16,
        code: u16,
        text: &ShortStr,
        out: &mut AmqpOutbound,
    ) -> Result<(), SessionError> {
        self.send_method(
            channel,
            AmqpMethod::Channel(ChannelMethod::Close(crate::method::ChannelClose {
                reply_code: code,
                reply_text: text.clone(),
                class_id: 0,
                method_id: 0,
            })),
            out,
        )?;
        if let Some(ch) = self.channels.get_mut(&channel) {
            ch.phase = ChannelPhase::Closing;
        }
        Ok(())
    }

    fn connection_close(&mut self, out: &mut AmqpOutbound, code: u16) -> Result<(), SessionError> {
        self.send_method(
            0,
            AmqpMethod::Connection(ConnectionMethod::Close(crate::method::ConnectionClose {
                reply_code: code,
                reply_text: ShortStr::from("error"),
                class_id: 0,
                method_id: 0,
            })),
            out,
        )?;
        self.connection.phase = ConnectionPhase::Closing;
        Ok(())
    }

    fn alloc_payload_handle(&mut self) -> PayloadHandle {
        let h = PayloadHandle(self.next_payload_handle);
        self.next_payload_handle += 1;
        h
    }
}
