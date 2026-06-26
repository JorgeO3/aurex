use bytes::{BufMut, BytesMut};
use smallvec::SmallVec;

use aurum_internal_protocol::{
    event::{
        confirm::{ConsumerEventBatch, ConsumerEventKind, PublishConfirmBatch, SettlementKind, SettlementResultBatch},
        delivery::{DeliveryEventSegment, DeliveryEventBatch},
        error::{CommandError, CommandErrorKind},
    },
    route::RouteResolvedEvent,
};
use aurum_types::PayloadHandle;

use crate::adapter::session::NativeSessionState;
use crate::codec::NativeFrame;
use crate::message::{
    ConsumerOkBody, DeliveryBatchBody, DeliveryDescriptor, ErrorBody, PublishConfirmBatchBody,
    RouteResolvedBody, SettlementResultBatchBody,
};
use crate::wire::{FrameFlags, NativeDeliveryFlags, NativeErrorCode, NativeOp, NativeFrameHeader};

/// Broker output slices for the native outbound adapter (no `aurum-broker` dependency).
pub struct NativeBrokerOutputView<'a, P = PayloadHandle> {
    pub deliveries: &'a [DeliveryEventBatch<P>],
    pub confirms: &'a [PublishConfirmBatch],
    pub settlements: &'a [SettlementResultBatch],
    pub consumer_events: &'a [ConsumerEventBatch],
    pub route_resolved: &'a [RouteResolvedEvent],
    pub errors: &'a [CommandError],
}

pub struct NativeOutboundAdapter;

impl Default for NativeOutboundAdapter {
    fn default() -> Self {
        Self
    }
}

impl NativeOutboundAdapter {
    pub fn translate_outputs(
        &self,
        session: &NativeSessionState,
        outputs: &NativeBrokerOutputView<'_, PayloadHandle>,
        request_correlation_id: u64,
        stream_id: u32,
        out: &mut SmallVec<[NativeFrame; 8]>,
    ) {
        for ev in outputs.route_resolved {
            out.push(self.route_resolved_frame(ev, request_correlation_id, stream_id));
        }
        for batch in outputs.confirms {
            out.push(self.confirm_frame(batch.accepted, 0, request_correlation_id, stream_id));
        }
        for batch in outputs.deliveries {
            if let Some(frame) = self.delivery_frame(batch, stream_id) {
                out.push(frame);
            }
        }
        for batch in outputs.settlements {
            out.push(self.settlement_frame(
                batch.consumer_id.0,
                batch.settled,
                batch.kind,
                stream_id,
            ));
        }
        for batch in outputs.consumer_events {
            if batch.kind == ConsumerEventKind::Started {
                if let Some(info) = session.consumer_info(batch.consumer_id) {
                    out.push(self.consumer_ok_frame(
                        info.queue_id.0,
                        batch.consumer_id.0,
                        info.prefetch,
                        request_correlation_id,
                        stream_id,
                    ));
                }
            }
        }
        for err in outputs.errors {
            out.push(self.error_frame(err, request_correlation_id, stream_id));
        }
    }

    fn route_resolved_frame(
        &self,
        ev: &RouteResolvedEvent,
        correlation_id: u64,
        stream_id: u32,
    ) -> NativeFrame {
        let body = RouteResolvedBody::from_route(
            ev.route_version.0,
            ev.route_id.index(),
            ev.route_id.generation(),
        );
        let mut buf = BytesMut::new();
        body.encode(&mut buf);
        NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::RouteResolved,
                FrameFlags::RESPONSE,
                stream_id,
                correlation_id,
                buf.len() as u32,
            ),
            buf.freeze(),
        )
    }

    fn confirm_frame(
        &self,
        accepted: u32,
        failed: u32,
        correlation_id: u64,
        stream_id: u32,
    ) -> NativeFrame {
        let body = PublishConfirmBatchBody {
            accepted_count: accepted,
            failed_count: failed,
        };
        let mut buf = BytesMut::new();
        body.encode(&mut buf);
        NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::PublishConfirmBatch,
                FrameFlags::RESPONSE,
                stream_id,
                correlation_id,
                buf.len() as u32,
            ),
            buf.freeze(),
        )
    }

    fn delivery_frame(
        &self,
        batch: &DeliveryEventBatch<PayloadHandle>,
        stream_id: u32,
    ) -> Option<NativeFrame> {
        let mut descriptors = Vec::new();
        let mut payloads = BytesMut::new();
        for seg in &batch.segments {
            match seg {
                DeliveryEventSegment::Range(r) => {
                    for i in 0..r.len {
                        let tag = r.start_tag.0 + u64::from(i);
                        let offset = payloads.len() as u32;
                        let mut payload_len = 0u32;
                        if let Some(handle) = r.payloads.get(i) {
                            let bytes = handle.0.to_le_bytes();
                            payload_len = bytes.len() as u32;
                            payloads.put_slice(&bytes);
                        }
                        descriptors.push(DeliveryDescriptor {
                            delivery_tag: tag,
                            payload_offset: offset,
                            payload_len,
                            delivery_flags: if r
                                .flags
                                .contains(aurum_internal_protocol::flags::DeliveryEventFlags::REDELIVERED)
                            {
                                NativeDeliveryFlags::REDELIVERED
                            } else {
                                NativeDeliveryFlags::empty()
                            },
                        });
                    }
                }
                DeliveryEventSegment::Mask(m) => {
                    for (tag, handle) in m.iter_handles() {
                        let offset = payloads.len() as u32;
                        let bytes = handle.0.to_le_bytes();
                        descriptors.push(DeliveryDescriptor {
                            delivery_tag: tag.0,
                            payload_offset: offset,
                            payload_len: bytes.len() as u32,
                            delivery_flags: NativeDeliveryFlags::empty(),
                        });
                        payloads.put_slice(&bytes);
                    }
                }
            }
        }
        if descriptors.is_empty() {
            return None;
        }
        let body = DeliveryBatchBody {
            consumer_id: batch.consumer_id.0,
            descriptors,
            payloads: payloads.freeze(),
        };
        let mut buf = BytesMut::new();
        if body.encode(&mut buf).is_err() {
            return None;
        }
        Some(NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::DeliveryBatch,
                FrameFlags::EVENT,
                stream_id,
                0,
                buf.len() as u32,
            ),
            buf.freeze(),
        ))
    }

    fn settlement_frame(
        &self,
        consumer_id: u64,
        settled: u32,
        kind: SettlementKind,
        stream_id: u32,
    ) -> NativeFrame {
        let kind_byte = match kind {
            SettlementKind::Ack => 0,
            SettlementKind::Nack => 1,
            SettlementKind::Reject => 2,
            SettlementKind::Cancel => 3,
        };
        let body = SettlementResultBatchBody {
            consumer_id,
            settled,
            kind: kind_byte,
        };
        let mut buf = BytesMut::new();
        body.encode(&mut buf);
        NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::SettlementResultBatch,
                FrameFlags::RESPONSE,
                stream_id,
                0,
                buf.len() as u32,
            ),
            buf.freeze(),
        )
    }

    fn consumer_ok_frame(
        &self,
        queue_id: u32,
        consumer_id: u64,
        prefetch: u32,
        correlation_id: u64,
        stream_id: u32,
    ) -> NativeFrame {
        let body = ConsumerOkBody {
            queue_id,
            consumer_id,
            effective_prefetch: prefetch,
        };
        let mut buf = BytesMut::new();
        body.encode(&mut buf);
        NativeFrame::new(
            NativeFrameHeader::new(
                NativeOp::ConsumerOk,
                FrameFlags::RESPONSE,
                stream_id,
                correlation_id,
                buf.len() as u32,
            ),
            buf.freeze(),
        )
    }

    fn error_frame(&self, err: &CommandError, correlation_id: u64, stream_id: u32) -> NativeFrame {
        let code = map_command_error(err.kind);
        let body = ErrorBody {
            error_code: code.as_u16(),
            correlation_id,
            message: error_message(err.kind),
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
}

fn map_command_error(kind: CommandErrorKind) -> NativeErrorCode {
    match kind {
        CommandErrorKind::StaleRouteEpoch | CommandErrorKind::RouteGenerationMismatch => {
            NativeErrorCode::RouteStale
        }
        CommandErrorKind::ExchangeNotFound
        | CommandErrorKind::Unroutable
        | CommandErrorKind::InvalidRoute
        | CommandErrorKind::RouteIdInvalid => NativeErrorCode::RouteNotFound,
        CommandErrorKind::QueueNotFound => NativeErrorCode::QueueNotFound,
        CommandErrorKind::ConsumerNotFound
        | CommandErrorKind::ConsumerCancelled
        | CommandErrorKind::DuplicateConsumer => NativeErrorCode::ConsumerNotFound,
        CommandErrorKind::InvalidDeliveryTag | CommandErrorKind::DeliveryTagAlreadySettled => {
            NativeErrorCode::InvalidDeliveryTag
        }
        _ => NativeErrorCode::Internal,
    }
}

fn error_message(kind: CommandErrorKind) -> Vec<u8> {
    format!("{kind:?}").into_bytes()
}
