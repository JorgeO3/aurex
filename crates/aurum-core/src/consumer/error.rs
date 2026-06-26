use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerError {
    InvalidDeliveryTag,
    DeliveryTagAlreadySettled,
    ConsumerCancelled,
    InsufficientCredit,
    EmptyDelivery,
    InternalInvariantViolation,
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDeliveryTag => write!(f, "delivery tag not found in window"),
            Self::DeliveryTagAlreadySettled => write!(f, "delivery tag already acked or nacked"),
            Self::ConsumerCancelled => write!(f, "consumer session has been cancelled"),
            Self::InsufficientCredit => write!(f, "prefetch credit exhausted"),
            Self::EmptyDelivery => write!(f, "no messages available to deliver"),
            Self::InternalInvariantViolation => write!(f, "internal window invariant violated"),
        }
    }
}
