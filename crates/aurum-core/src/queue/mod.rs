
pub mod constants;
pub mod block;
pub mod stats;
pub mod state;
pub mod error;
pub mod work;

pub(crate) mod hybrid;

#[cfg(any(test, feature = "model"))]
pub mod model;

#[cfg(test)]
mod tests;

pub use constants::{MSGS_PER_BLOCK, WORDS_PER_BLOCK};
pub use block::MsgBlock;
pub use hybrid::HybridRangeBlockQueue;
pub use state::{MessageState, QueueCounts};
pub use error::{InvariantViolation, InvariantKind, QueueError};
pub use stats::QueueStats;
pub use work::{AckBatch, AckMask, AckRange, NackBatch, NackMask, NackRange, NackReason};
