use bytes::BytesMut;

use crate::wire::{read_shortstr, read_u16, write_shortstr, write_u16, ShortStr, WireError};

#[derive(Debug, Clone, PartialEq)]
pub enum ChannelMethod {
    Open(ChannelOpen),
    OpenOk,
    Close(ChannelClose),
    CloseOk,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelOpen {
    pub reserved: ShortStr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelClose {
    pub reply_code: u16,
    pub reply_text: ShortStr,
    pub class_id: u16,
    pub method_id: u16,
}

pub(crate) fn decode_channel(method_id: u16, buf: &mut &[u8]) -> Result<ChannelMethod, WireError> {
    use crate::wire::constants::channel;
    Ok(match method_id {
        channel::OPEN => ChannelMethod::Open(ChannelOpen {
            reserved: read_shortstr(buf)?,
        }),
        channel::OPEN_OK => ChannelMethod::OpenOk,
        channel::CLOSE => ChannelMethod::Close(ChannelClose {
            reply_code: read_u16(buf)?,
            reply_text: read_shortstr(buf)?,
            class_id: read_u16(buf)?,
            method_id: read_u16(buf)?,
        }),
        channel::CLOSE_OK => ChannelMethod::CloseOk,
        _ => return Err(WireError::UnknownFrameType),
    })
}

pub(crate) fn encode_channel(method: &ChannelMethod, dst: &mut BytesMut) -> Result<(), WireError> {
    match method {
        ChannelMethod::Open(m) => write_shortstr(dst, &m.reserved),
        ChannelMethod::OpenOk => {}
        ChannelMethod::Close(m) => {
            write_u16(dst, m.reply_code);
            write_shortstr(dst, &m.reply_text);
            write_u16(dst, m.class_id);
            write_u16(dst, m.method_id);
        }
        ChannelMethod::CloseOk => {}
    }
    Ok(())
}
