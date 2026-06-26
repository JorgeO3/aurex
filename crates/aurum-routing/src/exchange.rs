use aurum_types::ExchangeId;

use crate::flags::ExchangeFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExchangeKind {
    Direct = 0,
    Fanout = 1,
    Topic = 2,
    Headers = 3,
}

#[derive(Debug, Clone)]
pub struct ExchangeDecl {
    pub id: ExchangeId,
    pub name: String,
    pub kind: ExchangeKind,
    pub flags: ExchangeFlags,
}

impl ExchangeDecl {
    #[must_use]
    pub fn direct(id: ExchangeId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            kind: ExchangeKind::Direct,
            flags: ExchangeFlags::empty(),
        }
    }

    #[must_use]
    pub fn fanout(id: ExchangeId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            kind: ExchangeKind::Fanout,
            flags: ExchangeFlags::empty(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledExchange {
    pub id: ExchangeId,
    pub kind: ExchangeKind,
    pub flags: ExchangeFlags,
    pub fanout_route_index: Option<u32>,
}
