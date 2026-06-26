use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::ids::LogOffset;

#[derive(Debug)]
pub struct StdFileBackend {
    file: File,
    path: PathBuf,
    len: u64,
}

impl StdFileBackend {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let len = file.metadata()?.len();
        Ok(Self { file, path, len })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn append(&mut self, bytes: &[u8]) -> std::io::Result<LogOffset> {
        let offset = LogOffset(self.len);
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(bytes)?;
        self.len += bytes.len() as u64;
        Ok(offset)
    }

    pub fn read_at(&mut self, offset: LogOffset, len: usize) -> std::io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset.0))?;
        let mut buf = vec![0u8; len];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn truncate(&mut self, new_len: u64) -> std::io::Result<()> {
        self.file.set_len(new_len)?;
        self.len = new_len;
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    pub fn sync_data(&mut self) -> std::io::Result<()> {
        self.file.sync_data()
    }
}
