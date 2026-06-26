use crate::queue::work::NackReason;
use super::id::DeliveryTag;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckRequest {
    pub tag: DeliveryTag,
    pub mode: AckMode,
}

impl AckRequest {
    #[must_use]
    pub fn one(tag: DeliveryTag) -> Self {
        Self { tag, mode: AckMode::One }
    }

    #[must_use]
    pub fn multiple(tag: DeliveryTag) -> Self {
        Self { tag, mode: AckMode::Multiple }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    One,
    Multiple,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackRequest {
    pub tag: DeliveryTag,
    pub mode: NackMode,
    pub reason: NackReason,
}

impl NackRequest {
    #[must_use]
    pub fn one(tag: DeliveryTag, reason: NackReason) -> Self {
        Self { tag, mode: NackMode::One, reason }
    }

    #[must_use]
    pub fn multiple(tag: DeliveryTag, reason: NackReason) -> Self {
        Self { tag, mode: NackMode::Multiple, reason }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackMode {
    One,
    Multiple,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RejectRequest {
    pub tag: DeliveryTag,
    pub reason: NackReason,
}

impl RejectRequest {
    #[must_use]
    pub fn new(tag: DeliveryTag, reason: NackReason) -> Self {
        Self { tag, reason }
    }
}
