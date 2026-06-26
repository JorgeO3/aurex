use std::collections::{BTreeMap, BTreeSet};

use aurum_types::QueueId;

use crate::ack_ledger::{AckLedgerEntry, AckLedgerLog};
use crate::error::StorageError;
use crate::ids::{PayloadRef, QueueSeq};
use crate::queue_index::{QueueIndexEntry, QueueIndexLog};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredMessage {
    pub queue_seq: QueueSeq,
    pub payload_ref: PayloadRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredQueueImage {
    pub queue_id: QueueId,
    pub next_seq: QueueSeq,
    pub ready: Vec<RecoveredMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryError {
    CorruptRecord,
    QueueMismatch,
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub queues_recovered: usize,
    pub messages_ready: usize,
    pub truncated_tails: usize,
}

#[derive(Debug)]
pub struct RecoveryBuilder {
    data_dir: std::path::PathBuf,
}

impl RecoveryBuilder {
    #[must_use]
    pub fn new(data_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn recover_queue(&self, queue_id: QueueId) -> Result<RecoveredQueueImage, StorageError> {
        let index_log = QueueIndexLog::open(&self.data_dir, queue_id, crate::config::DurabilityMode::Buffered)?;
        let ack_log = AckLedgerLog::open(&self.data_dir, queue_id, crate::config::DurabilityMode::Buffered)?;
        let indexed = index_log.scan_entries()?;
        let acked = ack_log.scan_entries()?;
        Ok(build_image(queue_id, &indexed, &acked))
    }

    pub fn recover_all(&self) -> Result<(Vec<RecoveredQueueImage>, RecoveryReport), StorageError> {
        let mut queues = Vec::new();
        let mut report = RecoveryReport::default();
        let base = &self.data_dir;
        if !base.exists() {
            return Ok((queues, report));
        }
        for entry in std::fs::read_dir(base)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id_str) = name.strip_prefix("queue-") {
                if let Ok(id) = id_str.parse::<u32>() {
                    let image = self.recover_queue(QueueId(id))?;
                    report.messages_ready += image.ready.len();
                    report.queues_recovered += 1;
                    queues.push(image);
                }
            }
        }
        Ok((queues, report))
    }
}

fn build_image(
    queue_id: QueueId,
    indexed: &[QueueIndexEntry],
    acked: &[AckLedgerEntry],
) -> RecoveredQueueImage {
    let mut by_seq: BTreeMap<u64, PayloadRef> = BTreeMap::new();
    for e in indexed {
        if e.queue_id == queue_id {
            by_seq.insert(e.queue_seq.0, e.payload_ref);
        }
    }

    let mut settled: BTreeSet<u64> = BTreeSet::new();
    for e in acked {
        match *e {
            AckLedgerEntry::AckRange { queue_id: q, start, len }
            | AckLedgerEntry::DeadLetter { queue_id: q, start, len } => {
                if q != queue_id {
                    continue;
                }
                for i in 0..len {
                    settled.insert(start.0 + u64::from(i));
                }
            }
            AckLedgerEntry::NackRequeue { .. } => {}
            AckLedgerEntry::AckMask {
                queue_id: q,
                block_base,
                mask,
                ..
            } => {
                if q != queue_id {
                    continue;
                }
                for bit in 0..64 {
                    if mask & (1u64 << bit) != 0 {
                        settled.insert(block_base.0 + bit);
                    }
                }
            }
        }
    }

    let next_seq = by_seq.keys().next_back().map(|s| QueueSeq(s + 1)).unwrap_or(QueueSeq(0));
    let ready = by_seq
        .into_iter()
        .filter(|(seq, _)| !settled.contains(seq))
        .map(|(seq, payload_ref)| RecoveredMessage {
            queue_seq: QueueSeq(seq),
            payload_ref,
        })
        .collect();

    RecoveredQueueImage {
        queue_id,
        next_seq,
        ready,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DurabilityMode;
    use crate::flags::QueueIndexFlags;
    use crate::ids::SegmentId;
    use crate::payload::{PayloadBatch, PayloadLog, PayloadSlice};
    use tempfile::tempdir;

    #[test]
    fn recover_after_ack_range() {
        let dir = tempdir().unwrap();
        let q = QueueId(10);
        let mut payload_log = PayloadLog::open(dir.path().join("payload"), DurabilityMode::Buffered).unwrap();
        let refs = payload_log
            .append_batch(PayloadBatch {
                payloads: &[PayloadSlice { bytes: b"m1" }, PayloadSlice { bytes: b"m2" }],
            })
            .unwrap();

        let mut index = QueueIndexLog::open(dir.path(), q, DurabilityMode::Buffered).unwrap();
        index
            .append_entries(&[
                QueueIndexEntry {
                    queue_id: q,
                    queue_seq: QueueSeq(0),
                    payload_ref: refs.refs[0],
                    flags: QueueIndexFlags::NONE,
                },
                QueueIndexEntry {
                    queue_id: q,
                    queue_seq: QueueSeq(1),
                    payload_ref: refs.refs[1],
                    flags: QueueIndexFlags::NONE,
                },
            ])
            .unwrap();

        let mut ack = AckLedgerLog::open(dir.path(), q, DurabilityMode::Buffered).unwrap();
        ack.append_entries(&[AckLedgerEntry::AckRange {
            queue_id: q,
            start: QueueSeq(0),
            len: 1,
        }])
        .unwrap();

        let image = RecoveryBuilder::new(dir.path()).recover_queue(q).unwrap();
        assert_eq!(image.ready.len(), 1);
        assert_eq!(image.ready[0].queue_seq, QueueSeq(1));
        assert_eq!(image.next_seq, QueueSeq(2));
    }
}
