use crate::record::codec::{RecordDecodeError, RecordEncodeError};
use crate::recovery::RecoveryError;
use crate::segment::scanner::SegmentScanError;

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Encode(RecordEncodeError),
    Decode(RecordDecodeError),
    Scan(SegmentScanError),
    Recovery(RecoveryError),
    StorageFailed,
    UnsupportedVersion { version: u16 },
    UnknownRecordKind { kind: u16 },
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<RecordDecodeError> for StorageError {
    fn from(value: RecordDecodeError) -> Self {
        Self::Decode(value)
    }
}

impl From<RecordEncodeError> for StorageError {
    fn from(value: RecordEncodeError) -> Self {
        Self::Encode(value)
    }
}

impl From<SegmentScanError> for StorageError {
    fn from(value: SegmentScanError) -> Self {
        Self::Scan(value)
    }
}

impl From<RecoveryError> for StorageError {
    fn from(value: RecoveryError) -> Self {
        Self::Recovery(value)
    }
}
