use bitflags::bitflags;
use bytes::BytesMut;

use crate::method::bits::{read_packed_bits, write_packed_bits};
use crate::wire::{
    read_shortstr, read_u16, read_u32, read_u64, write_shortstr, write_u16, write_u32, write_u64,
    ShortStr, WireError,
};
use crate::wire::field_table::FieldTable;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BasicPublishFlags: u8 {
        const MANDATORY = 1 << 0;
        const IMMEDIATE = 1 << 1;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BasicAckFlags: u8 {
        const MULTIPLE = 1 << 0;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BasicNackFlags: u8 {
        const MULTIPLE = 1 << 0;
        const REQUEUE = 1 << 1;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BasicMethod {
    Qos(BasicQos),
    QosOk,
    Consume(BasicConsume),
    ConsumeOk(BasicConsumeOk),
    Cancel(BasicCancel),
    CancelOk,
    Publish(BasicPublish),
    Deliver(BasicDeliver),
    Ack(BasicAck),
    Nack(BasicNack),
    Reject(BasicReject),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicQos {
    pub prefetch_size: u32,
    pub prefetch_count: u16,
    pub global: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicConsume {
    pub queue: ShortStr,
    pub consumer_tag: ShortStr,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicConsumeOk {
    pub consumer_tag: ShortStr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicCancel {
    pub consumer_tag: ShortStr,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicPublish {
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub flags: BasicPublishFlags,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicDeliver {
    pub consumer_tag: ShortStr,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicAck {
    pub delivery_tag: u64,
    pub multiple: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicNack {
    pub delivery_tag: u64,
    pub multiple: bool,
    pub requeue: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicReject {
    pub delivery_tag: u64,
    pub requeue: bool,
}

pub(crate) fn decode_basic(method_id: u16, buf: &mut &[u8]) -> Result<BasicMethod, WireError> {
    use crate::wire::constants::basic;
    Ok(match method_id {
        basic::QOS => {
            let prefetch_size = read_u32(buf)?;
            let prefetch_count = read_u16(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            BasicMethod::Qos(BasicQos {
                prefetch_size,
                prefetch_count,
                global: bits[0],
            })
        }
        basic::QOS_OK => BasicMethod::QosOk,
        basic::CONSUME => {
            read_u16(buf)?;
            let queue = read_shortstr(buf)?;
            let consumer_tag = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 4)?;
            let arguments = FieldTable::decode(buf)?;
            BasicMethod::Consume(BasicConsume {
                queue,
                consumer_tag,
                no_local: bits[0],
                no_ack: bits[1],
                exclusive: bits[2],
                nowait: bits[3],
                arguments,
            })
        }
        basic::CONSUME_OK => BasicMethod::ConsumeOk(BasicConsumeOk {
            consumer_tag: read_shortstr(buf)?,
        }),
        basic::CANCEL => {
            let consumer_tag = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            BasicMethod::Cancel(BasicCancel {
                consumer_tag,
                nowait: bits[0],
            })
        }
        basic::CANCEL_OK => BasicMethod::CancelOk,
        basic::PUBLISH => {
            read_u16(buf)?;
            let exchange = read_shortstr(buf)?;
            let routing_key = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 2)?;
            let mut flags = BasicPublishFlags::empty();
            if bits[0] {
                flags |= BasicPublishFlags::MANDATORY;
            }
            if bits[1] {
                flags |= BasicPublishFlags::IMMEDIATE;
            }
            BasicMethod::Publish(BasicPublish {
                exchange,
                routing_key,
                flags,
            })
        }
        basic::DELIVER => {
            let consumer_tag = read_shortstr(buf)?;
            let delivery_tag = read_u64(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            let exchange = read_shortstr(buf)?;
            let routing_key = read_shortstr(buf)?;
            BasicMethod::Deliver(BasicDeliver {
                consumer_tag,
                delivery_tag,
                redelivered: bits[0],
                exchange,
                routing_key,
            })
        }
        basic::ACK => {
            let delivery_tag = read_u64(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            BasicMethod::Ack(BasicAck {
                delivery_tag,
                multiple: bits[0],
            })
        }
        basic::NACK => {
            let delivery_tag = read_u64(buf)?;
            let bits = read_packed_bits(buf, 2)?;
            BasicMethod::Nack(BasicNack {
                delivery_tag,
                multiple: bits[0],
                requeue: bits[1],
            })
        }
        basic::REJECT => {
            let delivery_tag = read_u64(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            BasicMethod::Reject(BasicReject {
                delivery_tag,
                requeue: bits[0],
            })
        }
        _ => return Err(WireError::UnknownFrameType),
    })
}

pub(crate) fn encode_basic(method: &BasicMethod, dst: &mut BytesMut) -> Result<(), WireError> {
    match method {
        BasicMethod::Qos(m) => {
            write_u32(dst, m.prefetch_size);
            write_u16(dst, m.prefetch_count);
            write_packed_bits(dst, &[m.global]);
        }
        BasicMethod::QosOk => {}
        BasicMethod::Consume(m) => {
            write_u16(dst, 0);
            write_shortstr(dst, &m.queue);
            write_shortstr(dst, &m.consumer_tag);
            write_packed_bits(dst, &[m.no_local, m.no_ack, m.exclusive, m.nowait]);
            m.arguments.encode(dst)?;
        }
        BasicMethod::ConsumeOk(m) => write_shortstr(dst, &m.consumer_tag),
        BasicMethod::Cancel(m) => {
            write_shortstr(dst, &m.consumer_tag);
            write_packed_bits(dst, &[m.nowait]);
        }
        BasicMethod::CancelOk => {}
        BasicMethod::Publish(m) => {
            write_u16(dst, 0);
            write_shortstr(dst, &m.exchange);
            write_shortstr(dst, &m.routing_key);
            write_packed_bits(
                dst,
                &[
                    m.flags.contains(BasicPublishFlags::MANDATORY),
                    m.flags.contains(BasicPublishFlags::IMMEDIATE),
                ],
            );
        }
        BasicMethod::Deliver(m) => {
            write_shortstr(dst, &m.consumer_tag);
            write_u64(dst, m.delivery_tag);
            write_packed_bits(dst, &[m.redelivered]);
            write_shortstr(dst, &m.exchange);
            write_shortstr(dst, &m.routing_key);
        }
        BasicMethod::Ack(m) => {
            write_u64(dst, m.delivery_tag);
            write_packed_bits(dst, &[m.multiple]);
        }
        BasicMethod::Nack(m) => {
            write_u64(dst, m.delivery_tag);
            write_packed_bits(dst, &[m.multiple, m.requeue]);
        }
        BasicMethod::Reject(m) => {
            write_u64(dst, m.delivery_tag);
            write_packed_bits(dst, &[m.requeue]);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    use crate::method::{decode_method, encode_method, AmqpMethod};
    use crate::wire::constants::{basic, CLASS_BASIC};

    #[test]
    fn basic_publish_roundtrip() {
        let method = AmqpMethod::Basic(BasicMethod::Publish(BasicPublish {
            exchange: ShortStr::from("orders"),
            routing_key: ShortStr::from("created"),
            flags: BasicPublishFlags::empty(),
        }));
        let mut body = BytesMut::new();
        encode_method(&method, &mut body).unwrap();
        let decoded = decode_method(CLASS_BASIC, basic::PUBLISH, &body).unwrap();
        assert_eq!(decoded, method);
    }

    #[test]
    fn basic_ack_multiple_roundtrip() {
        let method = AmqpMethod::Basic(BasicMethod::Ack(BasicAck {
            delivery_tag: 42,
            multiple: true,
        }));
        let mut body = BytesMut::new();
        encode_method(&method, &mut body).unwrap();
        let decoded = decode_method(CLASS_BASIC, basic::ACK, &body).unwrap();
        assert_eq!(decoded, method);
    }
}
