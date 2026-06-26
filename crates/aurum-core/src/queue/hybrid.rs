use aurum_kernels::{clear_range, set_range};
use aurum_types::{DeliveryMask, DeliveryRange, DeliveryWork, Seq};
use aurum_intrusive::{Index, IndexList};

use super::block::{MsgBlock, RETRY_LINK, RETRY_TAG, SPARSE_LINK, SPARSE_TAG};
use super::constants::{MSGS_PER_BLOCK, WORDS_PER_BLOCK};
use super::error::{InvariantKind, InvariantViolation};
use super::state::{MessageState, QueueCounts};
use super::stats::QueueStats;
use super::work::{AckBatch, NackBatch};

type RetryList = IndexList<RETRY_LINK, RETRY_TAG>;
type SparseList = IndexList<SPARSE_LINK, SPARSE_TAG>;

#[derive(Debug)]
pub struct HybridRangeBlockQueue {
    next_seq: u64,
    sequential_head: u64,
    sequential_tail: u64,
    blocks: Vec<MsgBlock>,
    sparse_blocks: SparseList,
    retry_blocks: RetryList,
    stats: QueueStats,
}

impl HybridRangeBlockQueue {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            next_seq: 0,
            sequential_head: 0,
            sequential_tail: 0,
            blocks: Vec::new(),
            sparse_blocks: SparseList::new(),
            retry_blocks: RetryList::new(),
            stats: QueueStats::default(),
        }
    }

    #[must_use]
    pub fn with_messages(total_messages: u64) -> Self {
        let mut q = Self::empty();
        let mut remaining = total_messages;
        while remaining > 0 {
            let chunk = remaining.min(u64::from(u32::MAX)) as u32;
            q.publish_contiguous(chunk);
            remaining -= u64::from(chunk);
        }
        q
    }

    pub fn publish_contiguous(&mut self, count: u32) -> DeliveryRange {
        if count == 0 {
            return DeliveryRange::new(self.next_seq, 0);
        }
        let start = self.next_seq;
        let end = start + u64::from(count);

        let needed_blocks = end.div_ceil(MSGS_PER_BLOCK as u64) as usize;
        while self.blocks.len() < needed_blocks {
            let base = self.blocks.len() as u64 * MSGS_PER_BLOCK as u64;
            self.blocks.push(MsgBlock::new(base));
        }

        self.sequential_tail = end;
        self.next_seq = end;
        self.stats.total_published += u64::from(count);

        DeliveryRange::new(start, count)
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.next_seq
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.next_seq == 0
    }

    #[must_use]
    pub fn sequential_ready_len(&self) -> u64 {
        self.sequential_tail.saturating_sub(self.sequential_head)
    }

    #[must_use]
    pub fn stats(&self) -> QueueStats {
        self.stats
    }

    pub fn deliver(&mut self, max_messages: u32, out: &mut DeliveryWork) -> u32 {
        out.clear();
        if max_messages == 0 {
            return 0;
        }

        let mut remaining = max_messages;
        let mut delivered = 0u32;

        let available = self.sequential_ready_len().min(u64::from(remaining)) as u32;
        if available != 0 {
            let start = self.sequential_head;
            self.mark_inflight_range(start, available);
            self.sequential_head += u64::from(available);
            out.ranges.push(DeliveryRange::new(start, available));
            remaining -= available;
            delivered += available;
        }

        if remaining != 0 {
            delivered += self.deliver_sparse_masks(remaining, out);
        }

        self.stats.total_delivered += u64::from(delivered);
        delivered
    }

    pub fn ack_work(&mut self, work: &DeliveryWork) {
        for range in &work.ranges {
            self.ack_range(range.start_seq, range.len);
        }
        for mask in &work.masks {
            self.ack_mask(*mask);
        }
    }

    pub fn nack_work_to_retry(&mut self, work: &DeliveryWork) {
        for range in &work.ranges {
            self.nack_range_to_retry(range.start_seq, range.len);
        }
        for mask in &work.masks {
            self.nack_mask_to_retry(*mask);
        }
    }

    pub fn apply_ack_batch(&mut self, batch: &AckBatch) {
        for range in &batch.ranges {
            self.ack_range(range.start_seq, range.len);
        }
        for mask in &batch.masks {
            let count = u64::from(mask.mask.count_ones());
            {
                let b = &mut self.blocks[mask.block as usize];
                let w = mask.word as usize;
                b.inflight[w] &= !mask.mask;
                b.acked[w] |= mask.mask;
            }
            self.stats.total_acked += count;
        }
    }

    pub fn apply_nack_batch(&mut self, batch: &NackBatch) {
        for range in &batch.ranges {
            self.nack_range_to_retry(range.start_seq, range.len);
        }
        for mask in &batch.masks {
            let count = u64::from(mask.mask.count_ones());
            {
                let b = &mut self.blocks[mask.block as usize];
                let w = mask.word as usize;
                b.inflight[w] &= !mask.mask;
                b.retry[w] |= mask.mask;
                b.retry_word_mask |= 1u8 << w;
            }
            self.ensure_retry_listed(Index::new(mask.block).expect("block index out of range"));
            self.stats.total_nacked += count;
        }
    }

    pub fn ack_range(&mut self, start: Seq, len: u32) {
        if len == 0 {
            return;
        }
        let mut seq = start;
        let end = start + u64::from(len);
        while seq < end {
            let (block_idx, bit) = locate(seq);
            let take = (MSGS_PER_BLOCK - bit).min((end - seq) as usize);
            let block = &mut self.blocks[block_idx];
            set_range(&mut block.acked, bit, take);
            clear_range(&mut block.inflight, bit, take);
            seq += take as u64;
        }
        self.stats.total_acked += u64::from(len);
    }

    pub fn ack_mask(&mut self, mask: DeliveryMask) {
        let b = &mut self.blocks[mask.block as usize];
        let w = mask.word as usize;
        let count = u64::from(mask.mask.count_ones());
        b.inflight[w] &= !mask.mask;
        b.acked[w] |= mask.mask;
        self.stats.total_acked += count;
    }

    pub fn ack_id(&mut self, seq: Seq) {
        if seq >= self.next_seq {
            return;
        }
        let (block_idx, bit) = locate(seq);
        let word = bit >> 6;
        let offset = bit & 63;
        let mask = 1u64 << offset;
        let block = &mut self.blocks[block_idx];
        block.inflight[word] &= !mask;
        block.acked[word] |= mask;
        self.stats.total_acked += 1;
    }

    pub fn nack_range_to_retry(&mut self, start: Seq, len: u32) {
        if len == 0 {
            return;
        }
        let mut seq = start;
        let end = start + u64::from(len);
        while seq < end {
            let (block_idx, bit) = locate(seq);
            let take = (MSGS_PER_BLOCK - bit).min((end - seq) as usize);
            {
                let block = &mut self.blocks[block_idx];
                clear_range(&mut block.inflight, bit, take);
                set_range(&mut block.retry, bit, take);
                let first_word = bit >> 6;
                let last_word = (bit + take - 1) >> 6;
                for w in first_word..=last_word {
                    block.retry_word_mask |= 1u8 << w;
                }
            }
            self.ensure_retry_listed(
                Index::from_usize(block_idx).expect("block index out of range"),
            );
            seq += take as u64;
        }
        self.stats.total_nacked += u64::from(len);
    }

    pub fn nack_mask_to_retry(&mut self, mask: DeliveryMask) {
        let count = u64::from(mask.mask.count_ones());
        {
            let b = &mut self.blocks[mask.block as usize];
            let w = mask.word as usize;
            b.inflight[w] &= !mask.mask;
            b.retry[w] |= mask.mask;
            b.retry_word_mask |= 1u8 << w;
        }
        self.ensure_retry_listed(Index::new(mask.block).expect("block index out of range"));
        self.stats.total_nacked += count;
    }

    pub fn retry_all_now(&mut self) -> u32 {
        let mut moved = 0u32;
        loop {
            let block_index = match self.retry_blocks.pop_front(&mut self.blocks) {
                Some(idx) => idx,
                None => break,
            };
            let b = block_index.as_usize();
            let word_mask = self.blocks[b].retry_word_mask;
            for word_idx in 0..WORDS_PER_BLOCK {
                let mask = self.blocks[b].retry[word_idx];
                if mask == 0 {
                    continue;
                }
                self.blocks[b].retry[word_idx] = 0;
                self.blocks[b].sparse_ready[word_idx] |= mask;
                self.blocks[b].redelivered[word_idx] |= mask;
                moved += mask.count_ones();
            }
            self.blocks[b].retry_word_mask = 0;
            self.blocks[b].sparse_word_mask |= word_mask;
            self.ensure_sparse_listed(block_index);
        }
        self.stats.total_retried += u64::from(moved);
        moved
    }

    #[inline]
    #[must_use]
    pub fn redelivered_mask(&self, block: u32, word: u8, delivery_mask: u64) -> u64 {
        self.blocks[block as usize].redelivered[word as usize] & delivery_mask
    }

    #[must_use]
    pub fn debug_state_of(&self, seq: Seq) -> Option<MessageState> {
        if seq >= self.next_seq {
            return None;
        }
        if seq >= self.sequential_head && seq < self.sequential_tail {
            return Some(MessageState::Ready);
        }
        let (block_idx, bit) = locate(seq);
        if block_idx >= self.blocks.len() {
            return None;
        }
        let block = &self.blocks[block_idx];
        let word = bit >> 6;
        let offset = bit & 63;
        let mask = 1u64 << offset;

        if block.acked[word] & mask != 0 {
            return Some(MessageState::Acked);
        }
        if block.inflight[word] & mask != 0 {
            return Some(MessageState::Inflight);
        }
        if block.retry[word] & mask != 0 {
            return Some(MessageState::Retry);
        }
        if block.sparse_ready[word] & mask != 0 {
            return Some(MessageState::SparseReady);
        }
        None
    }

    /// Structural count of messages in each state at this instant.
    ///
    /// O(blocks × WORDS_PER_BLOCK). Not for hot paths — use in tests and diagnostics.
    #[must_use]
    pub fn debug_counts(&self) -> QueueCounts {
        let published = self.next_seq;
        let sequential_ready = self.sequential_ready_len();
        let mut sparse_ready = 0u64;
        let mut inflight = 0u64;
        let mut acked = 0u64;
        let mut retry = 0u64;

        for block in &self.blocks {
            for w in 0..WORDS_PER_BLOCK {
                inflight += u64::from(block.inflight[w].count_ones());
                acked += u64::from(block.acked[w].count_ones());
                if block.sparse_word_mask & (1u8 << w) != 0 {
                    sparse_ready += u64::from(block.sparse_ready[w].count_ones());
                }
                if block.retry_word_mask & (1u8 << w) != 0 {
                    retry += u64::from(block.retry[w].count_ones());
                }
            }
        }

        QueueCounts { published, ready: sequential_ready + sparse_ready, inflight, acked, retry }
    }

    pub fn validate_invariants(&self) -> Result<(), InvariantViolation> {
        if self.sequential_head > self.sequential_tail {
            return Err(InvariantViolation {
                block_idx: 0,
                word_idx: 0,
                kind: InvariantKind::SeqOutOfOrder,
            });
        }
        if self.sequential_tail > self.next_seq {
            return Err(InvariantViolation {
                block_idx: 0,
                word_idx: 0,
                kind: InvariantKind::SeqOutOfOrder,
            });
        }

        for (block_idx, block) in self.blocks.iter().enumerate() {
            for word_idx in 0..WORDS_PER_BLOCK {
                let inflight = block.inflight[word_idx];
                let acked = block.acked[word_idx];
                let retry = block.retry[word_idx];
                let sparse = block.sparse_ready[word_idx];

                if inflight & acked != 0
                    || inflight & retry != 0
                    || inflight & sparse != 0
                    || acked & retry != 0
                    || acked & sparse != 0
                    || retry & sparse != 0
                {
                    return Err(InvariantViolation {
                        block_idx,
                        word_idx,
                        kind: InvariantKind::BitConflict,
                    });
                }

                let word_bit = 1u8 << word_idx;
                if (retry != 0) != (block.retry_word_mask & word_bit != 0) {
                    return Err(InvariantViolation {
                        block_idx,
                        word_idx,
                        kind: InvariantKind::WordMaskMismatch,
                    });
                }
                if (sparse != 0) != (block.sparse_word_mask & word_bit != 0) {
                    return Err(InvariantViolation {
                        block_idx,
                        word_idx,
                        kind: InvariantKind::WordMaskMismatch,
                    });
                }
            }

            let has_retry = block.retry_word_mask != 0;
            if has_retry != block.is_retry_listed() {
                return Err(InvariantViolation {
                    block_idx,
                    word_idx: 0,
                    kind: InvariantKind::ListMismatch,
                });
            }
            let has_sparse = block.sparse_word_mask != 0;
            if has_sparse != block.is_sparse_listed() {
                return Err(InvariantViolation {
                    block_idx,
                    word_idx: 0,
                    kind: InvariantKind::ListMismatch,
                });
            }
        }

        if self.retry_blocks.validate(&self.blocks).is_err() {
            return Err(InvariantViolation {
                block_idx: 0,
                word_idx: 0,
                kind: InvariantKind::ListCycle,
            });
        }
        if self.sparse_blocks.validate(&self.blocks).is_err() {
            return Err(InvariantViolation {
                block_idx: 0,
                word_idx: 0,
                kind: InvariantKind::ListCycle,
            });
        }

        Ok(())
    }

    fn deliver_sparse_masks(&mut self, max_messages: u32, out: &mut DeliveryWork) -> u32 {
        let mut remaining = max_messages;
        let mut delivered = 0u32;

        while remaining != 0 {
            let block_index = match self.sparse_blocks.pop_front(&mut self.blocks) {
                Some(idx) => idx,
                None => break,
            };
            let b = block_index.as_usize();

            for word_idx in 0..WORDS_PER_BLOCK {
                if remaining == 0 {
                    break;
                }
                let word_bit = 1u8 << word_idx;
                if self.blocks[b].sparse_word_mask & word_bit == 0 {
                    continue;
                }
                let available = self.blocks[b].sparse_ready[word_idx];
                if available == 0 {
                    self.blocks[b].sparse_word_mask &= !word_bit;
                    continue;
                }
                let mask = take_lowest_bits_local(available, remaining);
                self.blocks[b].sparse_ready[word_idx] &= !mask;
                self.blocks[b].inflight[word_idx] |= mask;
                if self.blocks[b].sparse_ready[word_idx] == 0 {
                    self.blocks[b].sparse_word_mask &= !word_bit;
                }
                out.masks.push(DeliveryMask::new(block_index.get(), word_idx as u8, mask));
                let count = mask.count_ones();
                remaining -= count;
                delivered += count;
            }

            if self.blocks[b].sparse_word_mask != 0 {
                // Push to FRONT so remaining seqs in this block are served next,
                // maintaining ascending seq order when multiple sparse blocks exist.
                self.sparse_blocks.push_front(&mut self.blocks, block_index);
            }
        }

        delivered
    }

    fn mark_inflight_range(&mut self, start: u64, len: u32) {
        if len == 0 {
            return;
        }
        let mut seq = start;
        let end = start + u64::from(len);
        while seq < end {
            let (block_idx, bit) = locate(seq);
            let take = (MSGS_PER_BLOCK - bit).min((end - seq) as usize);
            set_range(&mut self.blocks[block_idx].inflight, bit, take);
            seq += take as u64;
        }
    }

    fn ensure_retry_listed(&mut self, block_index: Index) {
        if self.blocks[block_index.as_usize()].is_retry_listed() {
            return;
        }
        self.retry_blocks.push_back(&mut self.blocks, block_index);
    }

    fn ensure_sparse_listed(&mut self, block_index: Index) {
        if self.blocks[block_index.as_usize()].is_sparse_listed() {
            return;
        }
        self.sparse_blocks.push_back(&mut self.blocks, block_index);
    }
}

#[inline(always)]
#[must_use]
pub(super) fn locate(seq: u64) -> (usize, usize) {
    let block = (seq / MSGS_PER_BLOCK as u64) as usize;
    let bit = (seq % MSGS_PER_BLOCK as u64) as usize;
    (block, bit)
}

#[inline(always)]
#[must_use]
fn take_lowest_bits_local(word: u64, max: u32) -> u64 {
    if word == 0 || max == 0 {
        return 0;
    }
    let available = word.count_ones();
    if available <= max {
        return word;
    }
    let mut remaining = max;
    let mut src = word;
    let mut mask = 0u64;
    while remaining != 0 {
        let bit = src.trailing_zeros();
        mask |= 1u64 << bit;
        src &= src - 1;
        remaining -= 1;
    }
    mask
}
