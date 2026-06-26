use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use aurum_internal_protocol::{
    command::{
        consume::{CancelConsumer, CancelConsumerBatch, ConsumeCommandBatch, ConsumeStart, CreditCommandBatch, CreditUpdate},
        ingress::{IngressCommandBatch, ResolveRouteBatch},
        publish::{ConfirmMode, IngressPublishBatch, IngressPublishTarget, PublishRecord},
        settlement::{AckCommand, AckCommandBatch, NackCommand, NackCommandBatch, NackDisposition, SettlementMode},
        shard::ShardCommandBatch,
    },
    flags::{ConsumeFlags, MessageFlags, PublishFlags},
    route::{CorrelationId, ResolveRouteCommand, RoutePublishTarget},
};
use aurum_types::{BatchId, ConsumerId, DeliveryTag, ExchangeId, PayloadHandle, QueueId, RouteTableVersion, SourceId};

use crate::adapter::session::NativeSessionState;
use crate::codec::NativeFrame;
use crate::codec::cursor::CursorError;
use crate::message::{
    AckBatchBody, CancelConsumerBody, ConsumeStartBody, CreditUpdateBody, HelloBody,
    NativeAckOp, NativeNackDisposition, NativeNackOp, NackBatchBody, PublishBatchBody,
    ResolveRouteBody,
};
use crate::wire::{FrameFlags, NativeErrorCode, NativeFrameHeader, NativeOp};

#[derive(Debug, Clone)]
pub enum BrokerCommandBatch<P = PayloadHandle> {
    Ingress(IngressCommandBatch<P>),
    Shard(ShardCommandBatch<P>),
}

#[derive(Debug, Clone)]
pub enum NativeInboundResult {
    BrokerCommand(BrokerCommandBatch),
    ImmediateResponse(NativeFrame),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeAdapterError {
    Protocol(NativeErrorCode),
    Decode(CursorError),
    State(String),
}

pub struct NativeInboundAdapter {
    session: NativeSessionState,
    next_payload_handle: u64,
}

impl Default for NativeInboundAdapter {
    fn default() -> Self {
        Self::new(NativeSessionState::default())
    }
}

impl NativeInboundAdapter {
    #[must_use]
    pub fn new(session: NativeSessionState) -> Self {
        Self {
            session,
            next_payload_handle: 1,
        }
    }

    #[must_use]
    pub fn session(&self) -> &NativeSessionState {
        &self.session
    }

    #[must_use]
    pub fn session_mut(&mut self) -> &mut NativeSessionState {
        &mut self.session
    }

    pub fn translate_frame(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let op = frame.header.op().map_err(|_| NativeAdapterError::Protocol(NativeErrorCode::UnknownOp))?;
        match op {
            NativeOp::Hello => self.translate_hello(frame),
            NativeOp::Heartbeat => Ok(NativeInboundResult::ImmediateResponse(self.heartbeat_ack(&frame))),
            NativeOp::ResolveRoute => self.translate_resolve_route(frame),
            NativeOp::PublishBatch => self.translate_publish(frame),
            NativeOp::ConsumeStart => self.translate_consume_start(frame),
            NativeOp::CreditUpdate => self.translate_credit(frame),
            NativeOp::AckBatch => self.translate_ack(frame),
            NativeOp::NackBatch => self.translate_nack(frame),
            NativeOp::CancelConsumer => self.translate_cancel(frame),
            _ => Err(NativeAdapterError::Protocol(NativeErrorCode::UnknownOp)),
        }
    }

    fn translate_hello(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = HelloBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        if body.client_major != crate::wire::NATIVE_PROTOCOL_MAJOR {
            return Err(NativeAdapterError::Protocol(NativeErrorCode::UnsupportedVersion));
        }
        self.session.mark_hello(body.client_major, body.client_minor, body.client_capabilities);
        let mut buf = BytesMut::new();
        self.session.hello_ok_body().encode(&mut buf);
        Ok(NativeInboundResult::ImmediateResponse(NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::HelloOk,
                FrameFlags::RESPONSE,
                frame.header.stream_id,
                frame.header.correlation_id,
                buf.len() as u32,
            ),
            buf.freeze(),
        )))
    }

    fn heartbeat_ack(&self, frame: &NativeFrame) -> NativeFrame {
        NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::HeartbeatAck,
                FrameFlags::RESPONSE,
                frame.header.stream_id,
                frame.header.correlation_id,
                0,
            ),
            Bytes::new(),
        )
    }

    fn translate_resolve_route(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = ResolveRouteBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let request_id = CorrelationId(frame.header.correlation_id);
        let cmd = if body.exchange.is_empty() && body.exchange_id_hint != 0 {
            ResolveRouteCommand::by_id(
                request_id,
                ExchangeId(body.exchange_id_hint),
                &body.routing_key,
            )
        } else {
            ResolveRouteCommand::by_name(request_id, &body.exchange, &body.routing_key)
        };
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Ingress(
            IngressCommandBatch::ResolveRoute(ResolveRouteBatch::one(cmd)),
        )))
    }

    fn translate_publish(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = PublishBatchBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let route_id = NativeSessionState::route_id_from_packed(body.route_id_packed);
        let route_version = RouteTableVersion(body.route_table_version);
        self.session.update_route_version(route_version);

        let mut records = SmallVec::new();
        for desc in &body.descriptors {
            let payload = body.payload_slice(desc).map_err(NativeAdapterError::Decode)?;
            let handle = PayloadHandle(self.next_payload_handle);
            self.next_payload_handle += 1;
            let mut msg_flags = MessageFlags::empty();
            if desc.message_flags.contains(crate::wire::NativeMessageFlags::PERSISTENT) {
                msg_flags |= MessageFlags::HAS_PAYLOAD_REF;
            }
            records.push(PublishRecord {
                payload: handle,
                payload_len: desc.payload_len,
                message_flags: msg_flags,
                priority: 0,
                expiration_ms: None,
                key_hash: 0,
            });
            let _ = payload;
        }

        let batch = IngressPublishBatch {
            batch_id: BatchId(frame.header.correlation_id),
            source: SourceId(self.session.connection_id),
            target: IngressPublishTarget::Route(RoutePublishTarget::new(route_id, route_version)),
            flags: PublishFlags::ROUTED_BY_ID,
            confirm_mode: ConfirmMode::Accepted,
            records,
        };
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Ingress(
            IngressCommandBatch::Publish(batch),
        )))
    }

    fn translate_consume_start(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = ConsumeStartBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let consumer_id = self.session.assign_consumer_id(body.consumer_id_hint);
        let queue_id = QueueId(body.queue_id);
        self.session
            .register_consumer(consumer_id, queue_id, body.prefetch);
        let mut flags = ConsumeFlags::empty();
        if !body.consumer_flags.contains(crate::wire::NativeConsumerFlags::MANUAL_ACK) {
            flags |= ConsumeFlags::AUTO_ACK;
        }
        let cmd = ConsumeStart {
            consumer_id,
            channel_id: self.session.channel_id(frame.header.stream_id),
            queue_id,
            prefetch: body.prefetch,
            flags,
        };
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Shard(
            ShardCommandBatch::Consume(ConsumeCommandBatch::one(cmd)),
        )))
    }

    fn translate_credit(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = CreditUpdateBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let consumer_id = ConsumerId(body.consumer_id);
        let update = if body.flags.contains(crate::wire::CreditFlags::ABSOLUTE) {
            CreditUpdate::set_prefetch(consumer_id, body.credit_delta)
        } else {
            CreditUpdate::delta(consumer_id, body.credit_delta as i32)
        };
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Shard(
            ShardCommandBatch::Credit(CreditCommandBatch {
                items: smallvec::smallvec![update],
            }),
        )))
    }

    fn translate_ack(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = AckBatchBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let mut items = SmallVec::new();
        for op in body.ops {
            match op {
                NativeAckOp::One { tag } => items.push(AckCommand::one(DeliveryTag(tag))),
                NativeAckOp::Range { first_tag, len } => {
                    if len == 0 {
                        return Err(NativeAdapterError::Protocol(NativeErrorCode::InvalidDeliveryTag));
                    }
                    let end = first_tag
                        .checked_add(u64::from(len) - 1)
                        .ok_or(NativeAdapterError::Protocol(NativeErrorCode::InvalidDeliveryTag))?;
                    items.push(AckCommand::Range {
                        start: DeliveryTag(first_tag),
                        end: DeliveryTag(end),
                    });
                }
                NativeAckOp::MultipleUpTo { tag } => items.push(AckCommand::multiple(DeliveryTag(tag))),
            }
        }
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Shard(
            ShardCommandBatch::Ack(AckCommandBatch {
                consumer_id: ConsumerId(body.consumer_id),
                flags: Default::default(),
                items,
            }),
        )))
    }

    fn translate_nack(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = NackBatchBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let mut items = SmallVec::new();
        for op in body.ops {
            let disposition = map_nack_disposition(match op {
                NativeNackOp::One { disposition, .. } => disposition,
                NativeNackOp::Range { disposition, .. } => disposition,
                NativeNackOp::MultipleUpTo { disposition, .. } => disposition,
            })?;
            match op {
                NativeNackOp::One { tag, .. } => items.push(NackCommand::Tag {
                    tag: DeliveryTag(tag),
                    mode: SettlementMode::One,
                    disposition,
                }),
                NativeNackOp::Range { first_tag, len, .. } => {
                    if len == 0 {
                        return Err(NativeAdapterError::Protocol(NativeErrorCode::InvalidDeliveryTag));
                    }
                    let end = first_tag
                        .checked_add(u64::from(len) - 1)
                        .ok_or(NativeAdapterError::Protocol(NativeErrorCode::InvalidDeliveryTag))?;
                    items.push(NackCommand::Range {
                        start: DeliveryTag(first_tag),
                        end: DeliveryTag(end),
                        disposition,
                    });
                }
                NativeNackOp::MultipleUpTo { tag, .. } => items.push(NackCommand::Tag {
                    tag: DeliveryTag(tag),
                    mode: SettlementMode::Multiple,
                    disposition,
                }),
            }
        }
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Shard(
            ShardCommandBatch::Nack(NackCommandBatch {
                consumer_id: ConsumerId(body.consumer_id),
                flags: Default::default(),
                items,
            }),
        )))
    }

    fn translate_cancel(&mut self, frame: NativeFrame) -> Result<NativeInboundResult, NativeAdapterError> {
        let body = CancelConsumerBody::decode(&frame.body).map_err(NativeAdapterError::Decode)?;
        let consumer_id = ConsumerId(body.consumer_id);
        let cmd = match body.cancel_disposition {
            1 => CancelConsumer::requeue(consumer_id),
            2 => CancelConsumer::drop_unacked(consumer_id),
            _ => CancelConsumer::drop_unacked(consumer_id),
        };
        Ok(NativeInboundResult::BrokerCommand(BrokerCommandBatch::Shard(
            ShardCommandBatch::Cancel(CancelConsumerBatch::one(cmd)),
        )))
    }
}

fn map_nack_disposition(d: NativeNackDisposition) -> Result<NackDisposition, NativeAdapterError> {
    match d {
        NativeNackDisposition::Requeue => Ok(NackDisposition::Requeue),
        NativeNackDisposition::Drop => Ok(NackDisposition::Drop),
        NativeNackDisposition::DeadLetter => Ok(NackDisposition::DeadLetter),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::HelloBody;
    use crate::wire::NativeCapabilities;

    #[test]
    fn hello_returns_immediate_response() {
        let mut adapter = NativeInboundAdapter::default();
        let mut body = BytesMut::new();
        HelloBody {
            client_major: 0,
            client_minor: 1,
            client_capabilities: NativeCapabilities::ROUTE_ID,
            client_name: b"test".to_vec(),
        }
        .encode(&mut body)
        .unwrap();
        let frame = NativeFrame::new(
            NativeFrameHeader::new(NativeOp::Hello, FrameFlags::NONE, 0, 42, body.len() as u32),
            body.freeze(),
        );
        let result = adapter.translate_frame(frame).unwrap();
        assert!(matches!(result, NativeInboundResult::ImmediateResponse(_)));
    }
}
