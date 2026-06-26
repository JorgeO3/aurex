use crate::wire::WireError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    Wire(WireError),
    Protocol(String),
    ChannelClosed(u16),
    ConnectionClosed,
}

impl From<WireError> for SessionError {
    fn from(e: WireError) -> Self {
        Self::Wire(e)
    }
}
