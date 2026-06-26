use aurum_core::AckRange;
use aurum_storage::{AppendOnlyStorageEngine, DurabilityMode, QueueSeq, StorageConfig, StorageError};
use aurum_types::QueueId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStorageHealth {
    Healthy,
    Failed,
}

#[derive(Debug, Default)]
pub struct NoopStorage;

impl NoopStorage {
    pub fn append_publish(
        &mut self,
        _queue_id: QueueId,
        _base_seq: QueueSeq,
        _payloads: &[&[u8]],
    ) -> Result<(), StorageError> {
        Ok(())
    }

    pub fn append_ack_ranges(
        &mut self,
        _queue_id: QueueId,
        _ranges: &[AckRange],
    ) -> Result<(), StorageError> {
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct AppendOnlyShardStorage {
    engine: AppendOnlyStorageEngine,
    pub health: ShardStorageHealth,
}

impl AppendOnlyShardStorage {
    pub fn open(data_dir: impl Into<std::path::PathBuf>) -> Result<Self, StorageError> {
        let config = StorageConfig::new(data_dir).with_durability(DurabilityMode::Buffered);
        Ok(Self {
            engine: AppendOnlyStorageEngine::open(config)?,
            health: ShardStorageHealth::Healthy,
        })
    }

    pub fn engine(&self) -> &AppendOnlyStorageEngine {
        &self.engine
    }

    pub fn append_publish(
        &mut self,
        queue_id: QueueId,
        base_seq: QueueSeq,
        payloads: &[&[u8]],
    ) -> Result<(), StorageError> {
        if self.health != ShardStorageHealth::Healthy {
            return Err(StorageError::StorageFailed);
        }
        match self.engine.append_publish(queue_id, base_seq, payloads) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.health = ShardStorageHealth::Failed;
                Err(e)
            }
        }
    }

    pub fn append_ack_ranges(
        &mut self,
        queue_id: QueueId,
        ranges: &[AckRange],
    ) -> Result<(), StorageError> {
        if self.health != ShardStorageHealth::Healthy {
            return Err(StorageError::StorageFailed);
        }
        for range in ranges {
            if range.len == 0 {
                continue;
            }
            if let Err(e) = self.engine.append_ack_range(
                queue_id,
                QueueSeq(range.start_seq),
                range.len,
            ) {
                self.health = ShardStorageHealth::Failed;
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.engine.flush()
    }

    pub fn recover_ready_count(&self, queue_id: QueueId) -> Result<u32, StorageError> {
        let image = self.engine.recover_queue(queue_id)?;
        Ok(image.ready.len() as u32)
    }
}

trait StorageConfigExt {
    fn with_durability(self, durability: DurabilityMode) -> Self;
}

impl StorageConfigExt for StorageConfig {
    fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }
}
