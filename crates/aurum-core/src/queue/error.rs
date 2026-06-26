#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    SeqOutOfRange,
    AckNotInflight,
    NackNotInflight,
    EmptyDelivery,
    InvalidMask,
    CapacityExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvariantKind {
    BitConflict,
    ListMismatch,
    SeqOutOfOrder,
    WordMaskMismatch,
    ListCycle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvariantViolation {
    pub block_idx: usize,
    pub word_idx: usize,
    pub kind: InvariantKind,
}
