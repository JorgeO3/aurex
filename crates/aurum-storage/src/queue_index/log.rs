use aurum_types::QueueId;

use crate::config::DurabilityMode;
use crate::error::StorageError;
use crate::flags::QueueIndexFlags;
use crate::ids::{PayloadRef, QueueSeq, SegmentId, StorageStreamId};
use crate::record::kind::RecordKind;
use crate::segment::{segment_path, SegmentWriter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueIndexEntry {
    pub queue_id: QueueId,
    pub queue_seq: QueueSeq,
    pub payload_ref: PayloadRef,
    pub flags: QueueIndexFlags,
}

#[derive(Debug)]
pub struct QueueIndexLog {
    queue_id: QueueId,
    dir: std::path::PathBuf,
    segment_id: SegmentId,
    writer: SegmentWriter,
    durability: DurabilityMode,
}

impl QueueIndexLog {
    pub fn open(
        base_dir: impl AsRef<std::path::Path>,
        queue_id: QueueId,
        durability: DurabilityMode,
    ) -> Result<Self, StorageError> {
        let dir = base_dir.as_ref().join(format!("queue-{}", queue_id.0));
        std::fs::create_dir_all(&dir)?;
        let segment_id = SegmentId(1);
        let path = segment_path(&dir, "index", segment_id);
        let writer = SegmentWriter::create(path, segment_id)?;
        Ok(Self {
            queue_id,
            dir,
            segment_id,
            writer,
            durability,
        })
    }

    pub fn append_entries(&mut self, entries: &[QueueIndexEntry]) -> Result<(), StorageError> {
        let mut body = Vec::new();
        body.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for e in entries {
            body.extend_from_slice(&e.queue_id.0.to_le_bytes());
            body.extend_from_slice(&e.queue_seq.0.to_le_bytes());
            body.extend_from_slice(&e.payload_ref.segment_id.0.to_le_bytes());
            body.extend_from_slice(&e.payload_ref.offset.0.to_le_bytes());
            body.extend_from_slice(&e.payload_ref.index.to_le_bytes());
            body.extend_from_slice(&e.payload_ref.len.to_le_bytes());
            body.extend_from_slice(&e.payload_ref.checksum.to_le_bytes());
            body.extend_from_slice(&e.flags.bits().to_le_bytes());
        }
        let base_seq = entries.first().map(|e| e.queue_seq.0).unwrap_or(0);
        self.writer.append_record(
            RecordKind::QueueIndexBatch,
            StorageStreamId::queue_index(self.queue_id).0,
            base_seq,
            entries.len() as u32,
            &body,
        )?;
        if matches!(self.durability, DurabilityMode::FsyncOnFlush) {
            self.writer.sync_data()?;
        }
        Ok(())
    }

    pub fn scan_entries(&self) -> Result<Vec<QueueIndexEntry>, StorageError> {
        let path = segment_path(&self.dir, "index", self.segment_id);
        let mut reader = crate::segment::SegmentReader::open(path, self.segment_id)?;
        let scan = reader.scan()?;
        let mut out = Vec::new();
        for rec in scan.records {
            if rec.kind != RecordKind::QueueIndexBatch {
                continue;
            }
            out.extend(decode_index_body(&rec.body)?);
        }
        Ok(out)
    }
}

pub fn decode_index_body(body: &[u8]) -> Result<Vec<QueueIndexEntry>, StorageError> {
    if body.len() < 4 {
        return Ok(Vec::new());
    }
    let count = u32::from_le_bytes(body[0..4].try_into().expect("count")) as usize;
    let entry_size = 4 + 8 + 8 + 8 + 4 + 4 + 4 + 2;
    let mut out = Vec::with_capacity(count);
    let mut pos = 4usize;
    for _ in 0..count {
        if pos + entry_size > body.len() {
            break;
        }
        let queue_id = QueueId(u32::from_le_bytes(body[pos..pos + 4].try_into().expect("q")));
        pos += 4;
        let queue_seq = QueueSeq(u64::from_le_bytes(body[pos..pos + 8].try_into().expect("seq")));
        pos += 8;
        let segment_id = SegmentId(u64::from_le_bytes(body[pos..pos + 8].try_into().expect("seg")));
        pos += 8;
        let offset = crate::ids::LogOffset(u64::from_le_bytes(body[pos..pos + 8].try_into().expect("off")));
        pos += 8;
        let index = u32::from_le_bytes(body[pos..pos + 4].try_into().expect("idx"));
        pos += 4;
        let len = u32::from_le_bytes(body[pos..pos + 4].try_into().expect("len"));
        pos += 4;
        let checksum = u32::from_le_bytes(body[pos..pos + 4].try_into().expect("cs"));
        pos += 4;
        let flags_raw = u16::from_le_bytes(body[pos..pos + 2].try_into().expect("fl"));
        pos += 2;
        out.push(QueueIndexEntry {
            queue_id,
            queue_seq,
            payload_ref: PayloadRef {
                segment_id,
                offset,
                index,
                len,
                checksum,
            },
            flags: QueueIndexFlags::from_bits(flags_raw).unwrap_or(QueueIndexFlags::NONE),
        });
    }
    Ok(out)
}
