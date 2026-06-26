use aurum_types::QueueId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct LogOffset(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct QueueSeq(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct StorageStreamId(pub u64);

impl StorageStreamId {
    #[must_use]
    pub fn payload_log() -> Self {
        Self(1)
    }

    #[must_use]
    pub fn queue_index(queue_id: QueueId) -> Self {
        Self(u64::from(queue_id.0) | (1u64 << 32))
    }

    #[must_use]
    pub fn ack_ledger(queue_id: QueueId) -> Self {
        Self(u64::from(queue_id.0) | (2u64 << 32))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadRef {
    pub segment_id: SegmentId,
    pub offset: LogOffset,
    pub index: u32,
    pub len: u32,
    pub checksum: u32,
}
