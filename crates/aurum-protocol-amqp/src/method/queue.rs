use bytes::BytesMut;

use crate::method::bits::{read_packed_bits, write_packed_bits};
use crate::wire::{read_shortstr, read_u16, read_u32, write_shortstr, write_u16, write_u32, ShortStr, WireError};
use crate::wire::field_table::FieldTable;

#[derive(Debug, Clone, PartialEq)]
pub enum QueueMethod {
    Declare(QueueDeclare),
    DeclareOk(QueueDeclareOk),
    Bind(QueueBind),
    BindOk,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclare {
    pub queue: ShortStr,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclareOk {
    pub queue: ShortStr,
    pub message_count: u32,
    pub consumer_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueBind {
    pub queue: ShortStr,
    pub exchange: ShortStr,
    pub routing_key: ShortStr,
    pub nowait: bool,
    pub arguments: FieldTable,
}

pub(crate) fn decode_queue(method_id: u16, buf: &mut &[u8]) -> Result<QueueMethod, WireError> {
    use crate::wire::constants::queue;
    Ok(match method_id {
        queue::DECLARE => {
            read_u16(buf)?;
            let queue = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 5)?;
            let arguments = FieldTable::decode(buf)?;
            QueueMethod::Declare(QueueDeclare {
                queue,
                passive: bits[0],
                durable: bits[1],
                exclusive: bits[2],
                auto_delete: bits[3],
                nowait: bits[4],
                arguments,
            })
        }
        queue::DECLARE_OK => QueueMethod::DeclareOk(QueueDeclareOk {
            queue: read_shortstr(buf)?,
            message_count: read_u32(buf)?,
            consumer_count: read_u32(buf)?,
        }),
        queue::BIND => {
            read_u16(buf)?;
            let queue = read_shortstr(buf)?;
            let exchange = read_shortstr(buf)?;
            let routing_key = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            let arguments = FieldTable::decode(buf)?;
            QueueMethod::Bind(QueueBind {
                queue,
                exchange,
                routing_key,
                nowait: bits[0],
                arguments,
            })
        }
        queue::BIND_OK => QueueMethod::BindOk,
        _ => return Err(WireError::UnknownFrameType),
    })
}

pub(crate) fn encode_queue(method: &QueueMethod, dst: &mut BytesMut) -> Result<(), WireError> {
    match method {
        QueueMethod::Declare(m) => {
            write_u16(dst, 0);
            write_shortstr(dst, &m.queue);
            write_packed_bits(
                dst,
                &[m.passive, m.durable, m.exclusive, m.auto_delete, m.nowait],
            );
            m.arguments.encode(dst)?;
        }
        QueueMethod::DeclareOk(m) => {
            write_shortstr(dst, &m.queue);
            write_u32(dst, m.message_count);
            write_u32(dst, m.consumer_count);
        }
        QueueMethod::Bind(m) => {
            write_u16(dst, 0);
            write_shortstr(dst, &m.queue);
            write_shortstr(dst, &m.exchange);
            write_shortstr(dst, &m.routing_key);
            write_packed_bits(dst, &[m.nowait]);
            m.arguments.encode(dst)?;
        }
        QueueMethod::BindOk => {}
    }
    Ok(())
}
