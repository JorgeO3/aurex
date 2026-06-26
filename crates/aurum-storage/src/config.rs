#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    Buffered,
    FsyncOnFlush,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentConfig {
    pub max_segment_bytes: u64,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub data_dir: std::path::PathBuf,
    pub durability: DurabilityMode,
    pub segment: SegmentConfig,
}

impl StorageConfig {
    #[must_use]
    pub fn new(data_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            durability: DurabilityMode::Buffered,
            segment: SegmentConfig::default(),
        }
    }
}
