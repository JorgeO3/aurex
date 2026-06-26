#![forbid(unsafe_code)]

pub mod amqp_harness;
pub mod amqp_transcript;
pub mod broker;
pub mod flags;
pub mod native_harness;
pub mod output;
pub mod registry;
pub mod routing;
pub mod scheduler;
pub mod shard;
pub mod storage;

#[cfg(test)]
mod amqp_harness_tests;
#[cfg(test)]
mod native_harness_tests;
#[cfg(test)]
mod routing_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod tests;

pub use amqp_harness::AmqpInMemoryHarness;
pub use broker::InMemoryBroker;
pub use native_harness::NativeInMemoryHarness;
pub use output::ShardOutputBatch;
pub use shard::InMemoryShardExecutor;
pub use storage::{AppendOnlyShardStorage, NoopStorage, ShardStorageHealth};
