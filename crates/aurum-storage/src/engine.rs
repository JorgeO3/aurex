use std::collections::HashMap;
use std::path::PathBuf;

use aurum_types::QueueId;

use crate::ack_ledger::{AckLedgerEntry, AckLedgerLog};
use crate::config::StorageConfig;
use crate::error::StorageError;
use crate::flags::QueueIndexFlags;
use crate::ids::QueueSeq;
use crate::payload::{PayloadBatch, PayloadLog, PayloadSlice};
use crate::queue_index::{QueueIndexEntry, QueueIndexLog};
use crate::recovery::{RecoveryBuilder, RecoveredQueueImage};

#[derive(Debug)]
pub struct AppendOnlyStorageEngine {
    config: StorageConfig,
    payload_log: PayloadLog,
    index_logs: HashMap<QueueId, QueueIndexLog>,
    ack_logs: HashMap<QueueId, AckLedgerLog>,
}

impl AppendOnlyStorageEngine {
    pub fn open(config: StorageConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&config.data_dir)?;
        let payload_dir = config.data_dir.join("payload");
        let payload_log = PayloadLog::open(payload_dir, config.durability)?;
        Ok(Self {
            config,
            payload_log,
            index_logs: HashMap::new(),
            ack_logs: HashMap::new(),
        })
    }

    #[must_use]
    pub fn data_dir(&self) -> &PathBuf {
        &self.config.data_dir
    }

    pub fn append_publish(
        &mut self,
        queue_id: QueueId,
        base_seq: QueueSeq,
        payloads: &[&[u8]],
    ) -> Result<(), StorageError> {
        let slices: Vec<PayloadSlice<'_>> = payloads.iter().map(|b| PayloadSlice { bytes: b }).collect();
        let refs = self
            .payload_log
            .append_batch(PayloadBatch { payloads: &slices })?;

        let mut entries = Vec::with_capacity(refs.refs.len());
        for (i, pref) in refs.refs.iter().enumerate() {
            entries.push(QueueIndexEntry {
                queue_id,
                queue_seq: QueueSeq(base_seq.0 + i as u64),
                payload_ref: *pref,
                flags: QueueIndexFlags::NONE,
            });
        }
        self.index_log_mut(queue_id)?.append_entries(&entries)?;
        Ok(())
    }

    pub fn append_ack_range(
        &mut self,
        queue_id: QueueId,
        start: QueueSeq,
        len: u32,
    ) -> Result<(), StorageError> {
        self.ack_log_mut(queue_id)?
            .append_entries(&[AckLedgerEntry::AckRange { queue_id, start, len }])
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.payload_log.flush()?;
        Ok(())
    }

    pub fn recover_queue(&self, queue_id: QueueId) -> Result<RecoveredQueueImage, StorageError> {
        RecoveryBuilder::new(&self.config.data_dir).recover_queue(queue_id)
    }

    fn index_log_mut(&mut self, queue_id: QueueId) -> Result<&mut QueueIndexLog, StorageError> {
        if !self.index_logs.contains_key(&queue_id) {
            let log = QueueIndexLog::open(&self.config.data_dir, queue_id, self.config.durability)?;
            self.index_logs.insert(queue_id, log);
        }
        Ok(self.index_logs.get_mut(&queue_id).expect("index log"))
    }

    fn ack_log_mut(&mut self, queue_id: QueueId) -> Result<&mut AckLedgerLog, StorageError> {
        if !self.ack_logs.contains_key(&queue_id) {
            let log = AckLedgerLog::open(&self.config.data_dir, queue_id, self.config.durability)?;
            self.ack_logs.insert(queue_id, log);
        }
        Ok(self.ack_logs.get_mut(&queue_id).expect("ack log"))
    }
}
