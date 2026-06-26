use std::collections::HashMap;

use aurum_internal_protocol::event::delivery::{DeliveryEventBatch, DeliveryEventSegment, DeliveryMetadata};
use aurum_types::PayloadHandle;
use bytes::{BufMut, Bytes, BytesMut};

use crate::method::{method_class_id, method_id, AmqpMethod, BasicMethod};
use crate::port::AmqpBrokerPort;
use crate::session::{AmqpChannelState, AmqpOutbound, SessionError};
use crate::wire::constants::CLASS_BASIC;
use crate::wire::frame::{FrameKind, RawFrame};
use crate::wire::properties::{BasicProperties, ContentHeader};
use crate::wire::ShortStr;

pub fn delivery_metadata_from(
    exchange: &ShortStr,
    routing_key: &ShortStr,
    properties: &BasicProperties,
) -> DeliveryMetadata {
    let mut meta = DeliveryMetadata::default();
    meta.exchange.extend_from_slice(exchange.as_bytes());
    meta.routing_key.extend_from_slice(routing_key.as_bytes());
    if let Some(ct) = &properties.content_type {
        meta.content_type.extend_from_slice(ct.as_bytes());
    }
    meta.delivery_mode = properties.delivery_mode.unwrap_or(0);
    meta
}

pub fn shortstr_from_bytes(bytes: &[u8]) -> ShortStr {
    ShortStr::try_from_bytes(bytes).unwrap_or_else(|_| ShortStr::from(""))
}

pub fn delivery_context_from_batch<B: AmqpBrokerPort>(
    batch_meta: &DeliveryMetadata,
    handle: PayloadHandle,
    broker: &B,
) -> (ShortStr, ShortStr, BasicProperties) {
    let properties = broker
        .delivery_properties(handle)
        .unwrap_or_else(|| BasicProperties {
            delivery_mode: if batch_meta.delivery_mode != 0 {
                Some(batch_meta.delivery_mode)
            } else {
                None
            },
            content_type: if batch_meta.content_type.is_empty() {
                None
            } else {
                Some(shortstr_from_bytes(&batch_meta.content_type))
            },
            ..BasicProperties::default()
        });
    (
        shortstr_from_bytes(&batch_meta.exchange),
        shortstr_from_bytes(&batch_meta.routing_key),
        properties,
    )
}

pub fn encode_delivery_batches<B: AmqpBrokerPort>(
    broker: &B,
    channels: &mut HashMap<u16, AmqpChannelState>,
    frame_max: u32,
    channel: u16,
    batches: &[DeliveryEventBatch],
    out: &mut AmqpOutbound,
) -> Result<(), SessionError> {
    for batch in batches {
        let consumer_tag = channels
            .get(&channel)
            .and_then(|c| c.consumers.tag_for(batch.consumer_id))
            .unwrap_or_else(|| ShortStr::from("ctag-1"));
        for seg in &batch.segments {
            match seg {
                DeliveryEventSegment::Range(r) => {
                    for i in 0..r.len {
                        let tag = r.start_tag.0 + u64::from(i);
                        let handle = r.payloads.get(i).unwrap_or(PayloadHandle(0));
                        let body = broker.load_payload(handle).unwrap_or_default();
                        let (exchange, routing_key, properties) =
                            delivery_context_from_batch(&batch.metadata, handle, broker);
                        if let Some(ch) = channels.get_mut(&channel) {
                            ch.delivery_consumers.insert(tag, batch.consumer_id);
                        }
                        send_deliver(
                            frame_max,
                            channel,
                            &consumer_tag,
                            tag,
                            r.flags
                                .contains(aurum_internal_protocol::flags::DeliveryEventFlags::REDELIVERED),
                            &exchange,
                            &routing_key,
                            &properties,
                            &body,
                            out,
                        )?;
                    }
                }
                DeliveryEventSegment::Mask(m) => {
                    for (tag, handle) in m.iter_handles() {
                        let body = broker.load_payload(handle).unwrap_or_default();
                        let (exchange, routing_key, properties) =
                            delivery_context_from_batch(&batch.metadata, handle, broker);
                        if let Some(ch) = channels.get_mut(&channel) {
                            ch.delivery_consumers.insert(tag.0, batch.consumer_id);
                        }
                        send_deliver(
                            frame_max,
                            channel,
                            &consumer_tag,
                            tag.0,
                            m.flags.contains(aurum_internal_protocol::flags::DeliveryEventFlags::REDELIVERED),
                            &exchange,
                            &routing_key,
                            &properties,
                            &body,
                            out,
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn send_deliver(
    frame_max: u32,
    channel: u16,
    consumer_tag: &ShortStr,
    delivery_tag: u64,
    redelivered: bool,
    exchange: &ShortStr,
    routing_key: &ShortStr,
    properties: &BasicProperties,
    body: &Bytes,
    out: &mut AmqpOutbound,
) -> Result<(), SessionError> {
    send_method_frame(
        channel,
        AmqpMethod::Basic(BasicMethod::Deliver(crate::method::BasicDeliver {
            consumer_tag: consumer_tag.clone(),
            delivery_tag,
            redelivered,
            exchange: exchange.clone(),
            routing_key: routing_key.clone(),
        })),
        out,
    )?;
    let header = ContentHeader {
        class_id: CLASS_BASIC,
        body_size: body.len() as u64,
        properties: properties.clone(),
    };
    let mut payload = BytesMut::new();
    header.encode(&mut payload)?;
    send_content_frame(channel, FrameKind::Header, payload.freeze(), out);
    let max_body = usize::try_from(frame_max).unwrap_or(1).max(1);
    if body.is_empty() {
        send_content_frame(channel, FrameKind::Body, Bytes::new(), out);
    } else {
        for chunk in body.chunks(max_body) {
            send_content_frame(channel, FrameKind::Body, Bytes::copy_from_slice(chunk), out);
        }
    }
    Ok(())
}

pub fn send_method_frame(
    channel: u16,
    method: AmqpMethod,
    out: &mut AmqpOutbound,
) -> Result<(), SessionError> {
    let class_id = method_class_id(&method);
    let mid = method_id(&method);
    let mut payload = BytesMut::new();
    payload.put_u16(class_id);
    payload.put_u16(mid);
    crate::method::encode_method(&method, &mut payload)?;
    send_content_frame(channel, FrameKind::Method, payload.freeze(), out);
    Ok(())
}

fn send_content_frame(channel: u16, kind: FrameKind, payload: Bytes, out: &mut AmqpOutbound) {
    out.frames.push(RawFrame::new(kind, channel, payload));
}
