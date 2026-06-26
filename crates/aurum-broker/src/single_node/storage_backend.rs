use aurum_storage::StorageError;
use aurum_types::QueueId;

use crate::in_memory::storage::{AppendOnlyShardStorage, NoopStorage};
use aurum_core::AckRange;
use aurum_storage::QueueSeq;

/// Storage backend enum for PR10 (static dispatch, no `dyn`).
#[derive(Debug)]
pub enum StorageBackend {
    Noop(NoopStorage),
    AppendOnly(AppendOnlyShardStorage),
}

impl Default for StorageBackend {
    fn default() -> Self {
        Self::Noop(NoopStorage)
    }
}

impl StorageBackend {
    pub fn open_append_only(data_dir: impl Into<std::path::PathBuf>) -> Result<Self, StorageError> {
        Ok(Self::AppendOnly(AppendOnlyShardStorage::open(data_dir)?))
    }

    pub fn append_publish(
        &mut self,
        queue_id: QueueId,
        base_seq: QueueSeq,
        payloads: &[&[u8]],
    ) -> Result<(), StorageError> {
        match self {
            Self::Noop(s) => s.append_publish(queue_id, base_seq, payloads),
            Self::AppendOnly(s) => s.append_publish(queue_id, base_seq, payloads),
        }
    }

    pub fn append_ack_ranges(
        &mut self,
        queue_id: QueueId,
        ranges: &[AckRange],
    ) -> Result<(), StorageError> {
        match self {
            Self::Noop(s) => s.append_ack_ranges(queue_id, ranges),
            Self::AppendOnly(s) => s.append_ack_ranges(queue_id, ranges),
        }
    }
}
