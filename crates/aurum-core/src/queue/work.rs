use smallvec::SmallVec;

use aurum_types::{BlockIndex, Seq, WordIndex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckRange {
    pub start_seq: Seq,
    pub len: u32,
}

impl AckRange {
    #[must_use]
    pub fn new(start_seq: Seq, len: u32) -> Self {
        Self { start_seq, len }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckMask {
    pub block: BlockIndex,
    pub word: WordIndex,
    pub mask: u64,
}

impl AckMask {
    #[must_use]
    pub fn new(block: BlockIndex, word: WordIndex, mask: u64) -> Self {
        Self { block, word, mask }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AckBatch {
    pub ranges: SmallVec<[AckRange; 4]>,
    pub masks: SmallVec<[AckMask; 4]>,
}

impl AckBatch {
    pub fn clear(&mut self) {
        self.ranges.clear();
        self.masks.clear();
    }

    #[must_use]
    pub fn acked_messages(&self) -> u64 {
        let from_ranges: u64 = self.ranges.iter().map(|r| u64::from(r.len)).sum();
        let from_masks: u64 = self.masks.iter().map(|m| u64::from(m.mask.count_ones())).sum();
        from_ranges + from_masks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackRange {
    pub start_seq: Seq,
    pub len: u32,
}

impl NackRange {
    #[must_use]
    pub fn new(start_seq: Seq, len: u32) -> Self {
        Self { start_seq, len }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NackMask {
    pub block: BlockIndex,
    pub word: WordIndex,
    pub mask: u64,
}

impl NackMask {
    #[must_use]
    pub fn new(block: BlockIndex, word: WordIndex, mask: u64) -> Self {
        Self { block, word, mask }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NackReason {
    #[default]
    Requeue,
    Reject,
    DeadLetter,
}

#[derive(Debug, Clone)]
pub struct NackBatch {
    pub ranges: SmallVec<[NackRange; 4]>,
    pub masks: SmallVec<[NackMask; 4]>,
    pub reason: NackReason,
}

impl NackBatch {
    #[must_use]
    pub fn new(reason: NackReason) -> Self {
        Self { ranges: SmallVec::new(), masks: SmallVec::new(), reason }
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
        self.masks.clear();
    }
}
