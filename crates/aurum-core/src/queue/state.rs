#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    Ready,
    Inflight,
    Acked,
    Retry,
    SparseReady,
}

/// Structural count of messages currently in each state.
///
/// Unlike `QueueStats` (which tracks cumulative operation totals), this reflects
/// the live state of the queue at the moment of the call.
/// Invariant: `ready + inflight + acked + retry == published`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueCounts {
    pub published: u64,
    pub ready: u64,
    pub inflight: u64,
    pub acked: u64,
    pub retry: u64,
}
