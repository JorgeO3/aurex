use std::fmt;
use std::io;
use std::net::AddrParseError;

#[derive(Debug)]
pub enum TransportError {
    Io(io::Error),
    AddrParse(AddrParseError),
    ListenerClosed,
    ConnectionLimit,
    ReadBufferFull,
    WriteBufferFull,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::AddrParse(e) => write!(f, "address parse error: {e}"),
            Self::ListenerClosed => write!(f, "listener closed"),
            Self::ConnectionLimit => write!(f, "connection limit reached"),
            Self::ReadBufferFull => write!(f, "read buffer limit exceeded"),
            Self::WriteBufferFull => write!(f, "write buffer limit exceeded"),
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::AddrParse(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<AddrParseError> for TransportError {
    fn from(value: AddrParseError) -> Self {
        Self::AddrParse(value)
    }
}

pub type TransportResult<T> = Result<T, TransportError>;
