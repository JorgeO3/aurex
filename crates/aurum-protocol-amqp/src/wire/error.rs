#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    NeedMore,
    BadFrameEnd,
    UnknownFrameType,
    FrameTooLarge { size: u32, max: u32 },
    InvalidProtocolHeader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeStatus<T> {
    Complete(T),
    NeedMore,
}
