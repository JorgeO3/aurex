#![forbid(unsafe_code)]

pub mod ack_ledger;
pub mod config;
pub mod engine;
pub mod error;
pub mod flags;
pub mod ids;
pub mod io;
pub mod payload;
pub mod queue_index;
pub mod record;
pub mod recovery;
pub mod segment;

#[cfg(test)]
mod tests;

pub use ack_ledger::{AckLedgerEntry, AckLedgerLog};
pub use config::{DurabilityMode, SegmentConfig, StorageConfig};
pub use engine::AppendOnlyStorageEngine;
pub use error::StorageError;
pub use ids::{LogOffset, PayloadRef, QueueSeq, SegmentId, StorageStreamId};
pub use payload::{PayloadBatch, PayloadLog, PayloadRefBatch, PayloadSlice};
pub use queue_index::{QueueIndexEntry, QueueIndexLog};
pub use recovery::{RecoveredMessage, RecoveredQueueImage, RecoveryBuilder, RecoveryError, RecoveryReport};
pub use record::{RecordHeader, RecordKind};

// Legacy trait kept for early experiments.
pub trait AppendLog {
    fn append(&mut self, bytes: &[u8]) -> LogOffset;
}

#[derive(Debug, Default)]
pub struct MemoryAppendLog {
    buf: Vec<u8>,
}

impl AppendLog for MemoryAppendLog {
    fn append(&mut self, bytes: &[u8]) -> LogOffset {
        let off = LogOffset(self.buf.len() as u64);
        self.buf.extend_from_slice(bytes);
        off
    }
}
