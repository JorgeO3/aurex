use bytes::{Buf, BufMut, BytesMut};

use crate::method::bits::{read_packed_bits, write_packed_bits};
use crate::wire::{
    read_shortstr, read_u16, read_u32, write_shortstr, write_u16, write_u32, read_longstr,
    write_longstr, ShortStr, WireError,
};
use crate::wire::field_table::FieldTable;

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionMethod {
    Start(ConnectionStart),
    StartOk(ConnectionStartOk),
    Tune(ConnectionTune),
    TuneOk(ConnectionTuneOk),
    Open(ConnectionOpen),
    OpenOk,
    Close(ConnectionClose),
    CloseOk,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionStart {
    pub version_major: u8,
    pub version_minor: u8,
    pub server_properties: FieldTable,
    pub mechanisms: Vec<u8>,
    pub locales: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionStartOk {
    pub client_properties: FieldTable,
    pub mechanism: ShortStr,
    pub response: Vec<u8>,
    pub locale: ShortStr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTune {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTuneOk {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionOpen {
    pub virtual_host: ShortStr,
    pub insist: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionClose {
    pub reply_code: u16,
    pub reply_text: ShortStr,
    pub class_id: u16,
    pub method_id: u16,
}

pub(crate) fn decode_connection(
    method_id: u16,
    buf: &mut &[u8],
) -> Result<ConnectionMethod, WireError> {
    use crate::wire::constants::connection;
    Ok(match method_id {
        connection::START => {
            let version_major = buf[0];
            buf.advance(1);
            let version_minor = buf[0];
            buf.advance(1);
            let server_properties = FieldTable::decode(buf)?;
            let mechanisms = read_longstr(buf)?.to_vec();
            let locales = read_longstr(buf)?.to_vec();
            ConnectionMethod::Start(ConnectionStart {
                version_major,
                version_minor,
                server_properties,
                mechanisms,
                locales,
            })
        }
        connection::START_OK => {
            let client_properties = FieldTable::decode(buf)?;
            let mechanism = read_shortstr(buf)?;
            let response = read_longstr(buf)?.to_vec();
            let locale = read_shortstr(buf)?;
            ConnectionMethod::StartOk(ConnectionStartOk {
                client_properties,
                mechanism,
                response,
                locale,
            })
        }
        connection::TUNE => ConnectionMethod::Tune(ConnectionTune {
            channel_max: read_u16(buf)?,
            frame_max: read_u32(buf)?,
            heartbeat: read_u16(buf)?,
        }),
        connection::TUNE_OK => ConnectionMethod::TuneOk(ConnectionTuneOk {
            channel_max: read_u16(buf)?,
            frame_max: read_u32(buf)?,
            heartbeat: read_u16(buf)?,
        }),
        connection::OPEN => {
            let virtual_host = read_shortstr(buf)?;
            let bits = read_packed_bits(buf, 1)?;
            ConnectionMethod::Open(ConnectionOpen {
                virtual_host,
                insist: bits[0],
            })
        }
        connection::OPEN_OK => ConnectionMethod::OpenOk,
        connection::CLOSE => ConnectionMethod::Close(ConnectionClose {
            reply_code: read_u16(buf)?,
            reply_text: read_shortstr(buf)?,
            class_id: read_u16(buf)?,
            method_id: read_u16(buf)?,
        }),
        connection::CLOSE_OK => ConnectionMethod::CloseOk,
        _ => return Err(WireError::UnknownFrameType),
    })
}

pub(crate) fn encode_connection(
    method: &ConnectionMethod,
    dst: &mut BytesMut,
) -> Result<(), WireError> {
    match method {
        ConnectionMethod::Start(m) => {
            dst.put_u8(m.version_major);
            dst.put_u8(m.version_minor);
            m.server_properties.encode(dst)?;
            write_longstr(dst, &m.mechanisms);
            write_longstr(dst, &m.locales);
        }
        ConnectionMethod::StartOk(m) => {
            m.client_properties.encode(dst)?;
            write_shortstr(dst, &m.mechanism);
            write_longstr(dst, &m.response);
            write_shortstr(dst, &m.locale);
        }
        ConnectionMethod::Tune(m) => {
            write_u16(dst, m.channel_max);
            write_u32(dst, m.frame_max);
            write_u16(dst, m.heartbeat);
        }
        ConnectionMethod::TuneOk(m) => {
            write_u16(dst, m.channel_max);
            write_u32(dst, m.frame_max);
            write_u16(dst, m.heartbeat);
        }
        ConnectionMethod::Open(m) => {
            write_shortstr(dst, &m.virtual_host);
            write_packed_bits(dst, &[m.insist]);
        }
        ConnectionMethod::OpenOk => {}
        ConnectionMethod::Close(m) => {
            write_u16(dst, m.reply_code);
            write_shortstr(dst, &m.reply_text);
            write_u16(dst, m.class_id);
            write_u16(dst, m.method_id);
        }
        ConnectionMethod::CloseOk => {}
    }
    Ok(())
}
