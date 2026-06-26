use crate::ids::{LogOffset, SegmentId};
use crate::io::std_file::StdFileBackend;
use crate::record::codec::{decode_record, encode_record};
use crate::record::header::RecordHeader;
use crate::record::kind::RecordKind;

pub mod scanner;

pub use scanner::{scan_segment_bytes, SegmentScanError, SegmentScanResult};

#[derive(Debug)]
pub struct SegmentWriter {
    backend: StdFileBackend,
    pub segment_id: SegmentId,
}

impl SegmentWriter {
    pub fn create(path: std::path::PathBuf, segment_id: SegmentId) -> std::io::Result<Self> {
        Ok(Self {
            backend: StdFileBackend::open(path)?,
            segment_id,
        })
    }

    pub fn append_record(
        &mut self,
        kind: RecordKind,
        stream_id: u64,
        base_seq: u64,
        count: u32,
        body: &[u8],
    ) -> Result<LogOffset, crate::StorageError> {
        let wire = encode_record(kind, stream_id, base_seq, count, body)?;
        let off = self.backend.append(&wire)?;
        Ok(off)
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.backend.flush()
    }

    pub fn sync_data(&mut self) -> std::io::Result<()> {
        self.backend.sync_data()
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.backend.len()
    }

    pub fn truncate_to(&mut self, offset: LogOffset) -> std::io::Result<()> {
        self.backend.truncate(offset.0)
    }
}

#[derive(Debug)]
pub struct SegmentReader {
    backend: StdFileBackend,
    pub segment_id: SegmentId,
}

impl SegmentReader {
    pub fn open(path: std::path::PathBuf, segment_id: SegmentId) -> std::io::Result<Self> {
        Ok(Self {
            backend: StdFileBackend::open(path)?,
            segment_id,
        })
    }

    pub fn scan(&mut self) -> Result<SegmentScanResult, SegmentScanError> {
        let mut buf = Vec::new();
        let len = self.backend.len() as usize;
        if len > 0 {
            buf = self.backend.read_at(LogOffset(0), len).map_err(|_| SegmentScanError::TruncatedTail)?;
        }
        scan_segment_bytes(self.segment_id, &buf)
    }

    pub fn read_record_at(&mut self, offset: LogOffset) -> Result<(RecordHeader, Vec<u8>), crate::StorageError> {
        let tail = self.backend.read_at(offset, (self.backend.len() - offset.0) as usize)?;
        let (hdr, body) = decode_record(&tail)?;
        Ok((hdr, body.bytes))
    }
}

pub fn segment_path(dir: &std::path::Path, prefix: &str, segment_id: SegmentId) -> std::path::PathBuf {
    dir.join(format!("{prefix}-{:016}.seg", segment_id.0))
}
