use crate::config::DurabilityMode;
use crate::error::StorageError;
use crate::ids::{PayloadRef, SegmentId, StorageStreamId};
use crate::record::kind::RecordKind;
use crate::segment::{segment_path, SegmentWriter};

#[derive(Debug, Clone)]
pub struct PayloadSlice<'a> {
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct PayloadBatch<'a> {
    pub payloads: &'a [PayloadSlice<'a>],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadRefBatch {
    pub refs: Vec<PayloadRef>,
}

#[derive(Debug)]
pub struct PayloadLog {
    dir: std::path::PathBuf,
    segment_id: SegmentId,
    writer: SegmentWriter,
    durability: DurabilityMode,
}

impl PayloadLog {
    pub fn open(
        dir: impl Into<std::path::PathBuf>,
        durability: DurabilityMode,
    ) -> Result<Self, StorageError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        let segment_id = SegmentId(1);
        let path = segment_path(&dir, "payload", segment_id);
        let writer = SegmentWriter::create(path, segment_id)?;
        Ok(Self {
            dir,
            segment_id,
            writer,
            durability,
        })
    }

    pub fn append_batch(&mut self, batch: PayloadBatch<'_>) -> Result<PayloadRefBatch, StorageError> {
        let mut body = Vec::new();
        body.extend_from_slice(&(batch.payloads.len() as u32).to_le_bytes());
        let mut payload_region = Vec::new();
        let mut lengths = Vec::with_capacity(batch.payloads.len());
        for p in batch.payloads {
            lengths.push(p.bytes.len() as u32);
            body.extend_from_slice(&(p.bytes.len() as u32).to_le_bytes());
        }
        for p in batch.payloads {
            payload_region.extend_from_slice(p.bytes);
        }
        body.extend_from_slice(&payload_region);

        let offset = self.writer.append_record(
            RecordKind::PayloadBatch,
            StorageStreamId::payload_log().0,
            0,
            batch.payloads.len() as u32,
            &body,
        )?;

        let mut refs = Vec::with_capacity(batch.payloads.len());
        let mut region_off = 0u32;
        for (i, len) in lengths.iter().enumerate() {
            refs.push(PayloadRef {
                segment_id: self.segment_id,
                offset,
                index: i as u32,
                len: *len,
                checksum: 0,
            });
            region_off += *len;
            let _ = region_off;
        }
        self.maybe_sync()?;
        Ok(PayloadRefBatch { refs })
    }

    pub fn read_payload(&self, payload_ref: PayloadRef) -> Result<Vec<u8>, StorageError> {
        let path = segment_path(&self.dir, "payload", payload_ref.segment_id);
        let mut reader = crate::segment::SegmentReader::open(path, payload_ref.segment_id)?;
        let (_hdr, body) = reader.read_record_at(payload_ref.offset)?;
        decode_payload_at(&body, payload_ref.index)
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.writer.flush()?;
        if matches!(self.durability, DurabilityMode::FsyncOnFlush) {
            self.writer.sync_data()?;
        }
        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<(), StorageError> {
        if matches!(self.durability, DurabilityMode::FsyncOnFlush) {
            self.writer.sync_data()?;
        }
        Ok(())
    }
}

fn decode_payload_at(body: &[u8], index: u32) -> Result<Vec<u8>, StorageError> {
    if body.len() < 4 {
        return Err(StorageError::Decode(crate::record::codec::RecordDecodeError::BodyTooShort));
    }
    let count = u32::from_le_bytes(body[0..4].try_into().expect("count")) as usize;
    if index as usize >= count {
        return Err(StorageError::Decode(crate::record::codec::RecordDecodeError::BodyTooShort));
    }
    let mut pos = 4usize;
    let mut lengths = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > body.len() {
            return Err(StorageError::Decode(crate::record::codec::RecordDecodeError::BodyTooShort));
        }
        lengths.push(u32::from_le_bytes(body[pos..pos + 4].try_into().expect("len")));
        pos += 4;
    }
    let region_start = pos;
    let mut off = region_start;
    for (i, len) in lengths.iter().enumerate() {
        if i as u32 == index {
            let end = off + *len as usize;
            return Ok(body[off..end].to_vec());
        }
        off += *len as usize;
    }
    Err(StorageError::Decode(crate::record::codec::RecordDecodeError::BodyTooShort))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_read_payload() {
        let dir = tempdir().unwrap();
        let mut log = PayloadLog::open(dir.path(), DurabilityMode::Buffered).unwrap();
        let slices = [PayloadSlice { bytes: b"hello" }, PayloadSlice { bytes: b"world" }];
        let refs = log.append_batch(PayloadBatch { payloads: &slices }).unwrap();
        assert_eq!(refs.refs.len(), 2);
        assert_eq!(log.read_payload(refs.refs[0]).unwrap(), b"hello");
        assert_eq!(log.read_payload(refs.refs[1]).unwrap(), b"world");
    }
}
