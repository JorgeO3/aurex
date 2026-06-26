use std::collections::VecDeque;

use super::state::{MessageState, QueueCounts};

/// Reference implementation of the queue semantics.
///
/// Deliberately simple and slow — correctness over performance.
/// Mirrors the hybrid queue's delivery priority: sequential range first,
/// then sparse/retry messages, so delivery sequences can be compared exactly.
pub struct ModelQueue {
    states: Vec<MessageState>,
    sequential_head: u64,
    sequential_tail: u64,
    sparse_ready: VecDeque<u64>,
}

impl std::fmt::Debug for ModelQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelQueue")
            .field("published", &self.states.len())
            .field("seq_head", &self.sequential_head)
            .field("seq_tail", &self.sequential_tail)
            .field("sparse_ready", &self.sparse_ready.len())
            .finish()
    }
}

impl ModelQueue {
    #[must_use]
    pub fn new() -> Self {
        Self {
            states: Vec::new(),
            sequential_head: 0,
            sequential_tail: 0,
            sparse_ready: VecDeque::new(),
        }
    }

    #[must_use]
    pub fn with_messages(total: u64) -> Self {
        let mut q = Self::new();
        q.publish(total);
        q
    }

    /// Publish `count` messages as a contiguous sequential range.
    /// Returns the start sequence number.
    pub fn publish_contiguous(&mut self, count: u32) -> u64 {
        let start = self.sequential_tail;
        self.publish(u64::from(count));
        start
    }

    /// Publish `count` messages. Alias for large-count scenarios.
    pub fn publish(&mut self, count: u64) {
        let end = self.sequential_tail + count;
        while (self.states.len() as u64) < end {
            self.states.push(MessageState::Ready);
        }
        self.sequential_tail = end;
    }

    /// Deliver up to `max` messages. Sequential range first, then sparse (retry'd).
    pub fn deliver(&mut self, max: u32) -> Vec<u64> {
        let mut out = Vec::new();
        let mut remaining = max;

        // Sequential first — matches hybrid delivery priority
        while remaining > 0 && self.sequential_head < self.sequential_tail {
            let seq = self.sequential_head;
            self.states[seq as usize] = MessageState::Inflight;
            out.push(seq);
            self.sequential_head += 1;
            remaining -= 1;
        }

        // Sparse / retry'd messages second
        while remaining > 0 {
            match self.sparse_ready.pop_front() {
                Some(seq) => {
                    self.states[seq as usize] = MessageState::Inflight;
                    out.push(seq);
                    remaining -= 1;
                }
                None => break,
            }
        }

        out
    }

    pub fn ack_range(&mut self, start: u64, len: u32) {
        for seq in start..start + u64::from(len) {
            if (seq as usize) < self.states.len() {
                self.states[seq as usize] = MessageState::Acked;
            }
        }
    }

    pub fn ack_id(&mut self, seq: u64) {
        if (seq as usize) < self.states.len() {
            self.states[seq as usize] = MessageState::Acked;
        }
    }

    pub fn nack_range_to_retry(&mut self, start: u64, len: u32) {
        for seq in start..start + u64::from(len) {
            if (seq as usize) < self.states.len() {
                self.states[seq as usize] = MessageState::Retry;
            }
        }
    }

    pub fn nack_id_to_retry(&mut self, seq: u64) {
        if (seq as usize) < self.states.len() {
            self.states[seq as usize] = MessageState::Retry;
        }
    }

    /// Move all Retry messages to sparse-ready. Returns number moved.
    /// Scans in ascending seq order so sparse_ready preserves ascending order.
    pub fn retry_all_now(&mut self) -> u32 {
        let mut count = 0u32;
        for seq in 0..self.states.len() as u64 {
            if self.states[seq as usize] == MessageState::Retry {
                self.states[seq as usize] = MessageState::SparseReady;
                self.sparse_ready.push_back(seq);
                count += 1;
            }
        }
        count
    }

    /// Structural count of messages in each state at this instant.
    #[must_use]
    pub fn counts(&self) -> QueueCounts {
        let mut c = QueueCounts { published: self.states.len() as u64, ..Default::default() };
        for &s in &self.states {
            match s {
                MessageState::Ready | MessageState::SparseReady => c.ready += 1,
                MessageState::Inflight => c.inflight += 1,
                MessageState::Acked => c.acked += 1,
                MessageState::Retry => c.retry += 1,
            }
        }
        c
    }

    #[must_use]
    pub fn total_published(&self) -> u64 {
        self.states.len() as u64
    }

    #[must_use]
    pub fn ready_count(&self) -> u64 {
        self.counts().ready
    }

    #[must_use]
    pub fn inflight_count(&self) -> u64 {
        self.counts().inflight
    }

    #[must_use]
    pub fn acked_count(&self) -> u64 {
        self.counts().acked
    }

    #[must_use]
    pub fn retry_count(&self) -> u64 {
        self.counts().retry
    }

    #[must_use]
    pub fn state_of(&self, seq: u64) -> Option<MessageState> {
        self.states.get(seq as usize).copied()
    }
}

impl Default for ModelQueue {
    fn default() -> Self {
        Self::new()
    }
}
