use bytes::BytesMut;

use crate::method::bits::{read_packed_bits, write_packed_bits};
use crate::wire::WireError;

#[derive(Debug, Clone, PartialEq)]
pub enum ConfirmMethod {
    Select { nowait: bool },
    SelectOk,
}

pub(crate) fn decode_confirm(method_id: u16, buf: &mut &[u8]) -> Result<ConfirmMethod, WireError> {
    use crate::wire::constants::confirm;
    Ok(match method_id {
        confirm::SELECT => {
            let bits = read_packed_bits(buf, 1)?;
            ConfirmMethod::Select { nowait: bits[0] }
        }
        confirm::SELECT_OK => ConfirmMethod::SelectOk,
        _ => return Err(WireError::UnknownFrameType),
    })
}

pub(crate) fn encode_confirm(method: &ConfirmMethod, dst: &mut BytesMut) -> Result<(), WireError> {
    match method {
        ConfirmMethod::Select { nowait } => write_packed_bits(dst, &[*nowait]),
        ConfirmMethod::SelectOk => {}
    }
    Ok(())
}
