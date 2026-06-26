#![forbid(unsafe_code)]

use aurum_types::ShardId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardConfig {
    pub shard_id: ShardId,
    pub core_id: u16,
    pub numa_node: u16,
}

#[derive(Debug)]
pub struct ShardRuntime {
    pub config: ShardConfig,
}

impl ShardRuntime {
    #[must_use]
    pub const fn new(config: ShardConfig) -> Self {
        Self { config }
    }
}
