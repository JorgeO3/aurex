#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum NativeOp {
    Hello = 1,
    HelloOk = 2,

    ResolveRoute = 10,
    RouteResolved = 11,

    PublishBatch = 20,
    PublishConfirmBatch = 21,

    ConsumeStart = 30,
    ConsumerOk = 31,
    CreditUpdate = 32,
    DeliveryBatch = 33,
    CancelConsumer = 34,
    ConsumerCancelled = 35,

    AckBatch = 40,
    NackBatch = 41,
    SettlementResultBatch = 42,

    Heartbeat = 50,
    HeartbeatAck = 51,

    ErrorFrame = 255,
}

impl NativeOp {
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOpError {
    Unknown(u16),
}

impl TryFrom<u16> for NativeOp {
    type Error = NativeOpError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloOk),
            10 => Ok(Self::ResolveRoute),
            11 => Ok(Self::RouteResolved),
            20 => Ok(Self::PublishBatch),
            21 => Ok(Self::PublishConfirmBatch),
            30 => Ok(Self::ConsumeStart),
            31 => Ok(Self::ConsumerOk),
            32 => Ok(Self::CreditUpdate),
            33 => Ok(Self::DeliveryBatch),
            34 => Ok(Self::CancelConsumer),
            35 => Ok(Self::ConsumerCancelled),
            40 => Ok(Self::AckBatch),
            41 => Ok(Self::NackBatch),
            42 => Ok(Self::SettlementResultBatch),
            50 => Ok(Self::Heartbeat),
            51 => Ok(Self::HeartbeatAck),
            255 => Ok(Self::ErrorFrame),
            other => Err(NativeOpError::Unknown(other)),
        }
    }
}
