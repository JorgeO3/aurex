use aurum_types::QueueId;

use crate::config::DurabilityMode;
use crate::error::StorageError;
use crate::ids::{QueueSeq, SegmentId, StorageStreamId};
use crate::record::kind::RecordKind;
use crate::segment::{SegmentWriter, segment_path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckLedgerEventKind {
    AckRange = 1,
    AckMask = 2,
    NackRequeue = 3,
    DeadLetter = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckLedgerEntry {
    AckRange { queue_id: QueueId, start: QueueSeq, len: u32 },
    AckMask { queue_id: QueueId, block_base: QueueSeq, word_index: u8, mask: u64 },
    NackRequeue { queue_id: QueueId, start: QueueSeq, len: u32 },
    DeadLetter { queue_id: QueueId, start: QueueSeq, len: u32 },
}

#[derive(Debug)]
pub struct AckLedgerLog {
    queue_id: QueueId,
    dir: std::path::PathBuf,
    segment_id: SegmentId,
    writer: SegmentWriter,
    durability: DurabilityMode,
}

impl AckLedgerLog {
    pub fn open(
        base_dir: impl AsRef<std::path::Path>,
        queue_id: QueueId,
        durability: DurabilityMode,
    ) -> Result<Self, StorageError> {
        let dir = base_dir.as_ref().join(format!("queue-{}", queue_id.0));
        std::fs::create_dir_all(&dir)?;
        let segment_id = SegmentId(1);
        let path = segment_path(&dir, "ack", segment_id);
        let writer = SegmentWriter::create(path, segment_id)?;
        Ok(Self { queue_id, dir, segment_id, writer, durability })
    }

    pub fn append_entries(&mut self, entries: &[AckLedgerEntry]) -> Result<(), StorageError> {
        let body = encode_ack_body(entries);
        let base = entries
            .first()
            .map(|e| match e {
                AckLedgerEntry::AckRange { start, .. }
                | AckLedgerEntry::AckMask { block_base: start, .. }
                | AckLedgerEntry::NackRequeue { start, .. }
                | AckLedgerEntry::DeadLetter { start, .. } => start.0,
            })
            .unwrap_or(0);
        self.writer.append_record(
            RecordKind::AckLedgerBatch,
            StorageStreamId::ack_ledger(self.queue_id).0,
            base,
            entries.len() as u32,
            &body,
        )?;
        if matches!(self.durability, DurabilityMode::FsyncOnFlush) {
            self.writer.sync_data()?;
        }
        Ok(())
    }

    pub fn scan_entries(&self) -> Result<Vec<AckLedgerEntry>, StorageError> {
        let path = segment_path(&self.dir, "ack", self.segment_id);
        let mut reader = crate::segment::SegmentReader::open(path, self.segment_id)?;
        let scan = reader.scan()?;
        let mut out = Vec::new();
        for rec in scan.records {
            if rec.kind != RecordKind::AckLedgerBatch {
                continue;
            }
            out.extend(decode_ack_body(&rec.body)?);
        }
        Ok(out)
    }
}

fn encode_ack_body(entries: &[AckLedgerEntry]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        match *e {
            AckLedgerEntry::AckRange { queue_id, start, len } => {
                body.push(AckLedgerEventKind::AckRange as u8);
                body.extend_from_slice(&queue_id.0.to_le_bytes());
                body.extend_from_slice(&start.0.to_le_bytes());
                body.extend_from_slice(&len.to_le_bytes());
            }
            AckLedgerEntry::AckMask { queue_id, block_base, word_index, mask } => {
                body.push(AckLedgerEventKind::AckMask as u8);
                body.extend_from_slice(&queue_id.0.to_le_bytes());
                body.extend_from_slice(&block_base.0.to_le_bytes());
                body.push(word_index);
                body.extend_from_slice(&mask.to_le_bytes());
            }
            AckLedgerEntry::NackRequeue { queue_id, start, len } => {
                body.push(AckLedgerEventKind::NackRequeue as u8);
                body.extend_from_slice(&queue_id.0.to_le_bytes());
                body.extend_from_slice(&start.0.to_le_bytes());
                body.extend_from_slice(&len.to_le_bytes());
            }
            AckLedgerEntry::DeadLetter { queue_id, start, len } => {
                body.push(AckLedgerEventKind::DeadLetter as u8);
                body.extend_from_slice(&queue_id.0.to_le_bytes());
                body.extend_from_slice(&start.0.to_le_bytes());
                body.extend_from_slice(&len.to_le_bytes());
            }
        }
    }
    body
}

pub fn decode_ack_body(body: &[u8]) -> Result<Vec<AckLedgerEntry>, StorageError> {
    if body.len() < 4 {
        return Ok(Vec::new());
    }
    let count = u32::from_le_bytes(body[0..4].try_into().expect("count")) as usize;
    let mut out = Vec::with_capacity(count);
    let mut pos = 4usize;
    for _ in 0..count {
        if pos >= body.len() {
            break;
        }
        let kind = body[pos];
        pos += 1;
        match kind {
            k if k == AckLedgerEventKind::AckRange as u8 => {
                if pos + 16 > body.len() {
                    break;
                }
                let queue_id = read_u32(body, &mut pos);
                let start = QueueSeq(read_u64(body, &mut pos));
                let len = read_u32(body, &mut pos);
                out.push(AckLedgerEntry::AckRange { queue_id: QueueId(queue_id), start, len });
            }
            k if k == AckLedgerEventKind::AckMask as u8 => {
                if pos + 21 > body.len() {
                    break;
                }
                let queue_id = QueueId(read_u32(body, &mut pos));
                let block_base = QueueSeq(read_u64(body, &mut pos));
                let word_index = body[pos];
                pos += 1;
                let mask = read_u64(body, &mut pos);
                out.push(AckLedgerEntry::AckMask { queue_id, block_base, word_index, mask });
            }
            k if k == AckLedgerEventKind::NackRequeue as u8 => {
                if pos + 16 > body.len() {
                    break;
                }
                let queue_id = QueueId(read_u32(body, &mut pos));
                let start = QueueSeq(read_u64(body, &mut pos));
                let len = read_u32(body, &mut pos);
                out.push(AckLedgerEntry::NackRequeue { queue_id, start, len });
            }
            k if k == AckLedgerEventKind::DeadLetter as u8 => {
                if pos + 16 > body.len() {
                    break;
                }
                let queue_id = QueueId(read_u32(body, &mut pos));
                let start = QueueSeq(read_u64(body, &mut pos));
                let len = read_u32(body, &mut pos);
                out.push(AckLedgerEntry::DeadLetter { queue_id, start, len });
            }
            _ => {}
        }
    }
    Ok(out)
}

fn read_u32(body: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes(body[*pos..*pos + 4].try_into().expect("u32"));
    *pos += 4;
    v
}

fn read_u64(body: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_le_bytes(body[*pos..*pos + 8].try_into().expect("u64"));
    *pos += 8;
    v
}
