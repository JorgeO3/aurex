#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum RecordKind {
    PayloadBatch = 1,
    QueueIndexBatch = 2,
    AckLedgerBatch = 3,
    Checkpoint = 4,
    Manifest = 5,
}

impl RecordKind {
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKindError {
    Unknown(u16),
}

impl TryFrom<u16> for RecordKind {
    type Error = RecordKindError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PayloadBatch),
            2 => Ok(Self::QueueIndexBatch),
            3 => Ok(Self::AckLedgerBatch),
            4 => Ok(Self::Checkpoint),
            5 => Ok(Self::Manifest),
            other => Err(RecordKindError::Unknown(other)),
        }
    }
}
