#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum NativeErrorCode {
    MalformedFrame = 1,
    UnsupportedVersion = 2,
    UnknownOp = 3,
    InvalidFlags = 4,
    BodyTooLarge = 5,
    RouteNotFound = 100,
    RouteStale = 101,
    QueueNotFound = 102,
    ConsumerNotFound = 103,
    InvalidDeliveryTag = 104,
    Internal = 500,
}

impl NativeErrorCode {
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for NativeErrorCode {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::MalformedFrame),
            2 => Ok(Self::UnsupportedVersion),
            3 => Ok(Self::UnknownOp),
            4 => Ok(Self::InvalidFlags),
            5 => Ok(Self::BodyTooLarge),
            100 => Ok(Self::RouteNotFound),
            101 => Ok(Self::RouteStale),
            102 => Ok(Self::QueueNotFound),
            103 => Ok(Self::ConsumerNotFound),
            104 => Ok(Self::InvalidDeliveryTag),
            500 => Ok(Self::Internal),
            _ => Err(()),
        }
    }
}
