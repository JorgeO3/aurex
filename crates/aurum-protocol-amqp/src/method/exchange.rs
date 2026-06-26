use bytes::BytesMut;

use crate::method::bits::{read_packed_bits, write_packed_bits};
use crate::wire::{read_shortstr, read_u16, write_shortstr, write_u16, ShortStr, WireError};
use crate::wire::field_table::FieldTable;

#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeMethod {
    Declare(ExchangeDeclare),
    DeclareOk,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDeclare {
    pub exchange: ShortStr,
    pub exchange_type: ShortStr,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

pub(crate) fn decode_exchange(
    method_id: u16,
    buf: &mut &[u8],
) -> Result<ExchangeMethod, WireError> {
    use crate::wire::constants::exchange;
    Ok(match method_id {
        exchange::DECLARE => {
            read_u16(buf)?; // reserved
            let exchange = read_shortstr(buf)?;
            let exchange_type = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 5)?;
            let arguments = FieldTable::decode(buf)?;
            ExchangeMethod::Declare(ExchangeDeclare {
                exchange,
                exchange_type,
                passive: bits[0],
                durable: bits[1],
                auto_delete: bits[2],
                internal: bits[3],
                nowait: bits[4],
                arguments,
            })
        }
        exchange::DECLARE_OK => ExchangeMethod::DeclareOk,
        _ => return Err(WireError::UnknownFrameType),
    })
}

pub(crate) fn encode_exchange(method: &ExchangeMethod, dst: &mut BytesMut) -> Result<(), WireError> {
    match method {
        ExchangeMethod::Declare(m) => {
            write_u16(dst, 0);
            write_shortstr(dst, &m.exchange);
            write_shortstr(dst, &m.exchange_type);
            write_packed_bits(
                dst,
                &[m.passive, m.durable, m.auto_delete, m.internal, m.nowait],
            );
            m.arguments.encode(dst)?;
        }
        ExchangeMethod::DeclareOk => {}
    }
    Ok(())
}
