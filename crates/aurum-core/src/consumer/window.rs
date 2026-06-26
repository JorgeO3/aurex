use std::collections::VecDeque;

use aurum_types::{BlockIndex, DeliveryMask, DeliveryRange, WordIndex};

use crate::queue::work::{AckBatch, AckMask, AckRange, NackBatch, NackMask, NackRange, NackReason};

use super::error::ConsumerError;
use super::flags::SegmentFlags;
use super::id::DeliveryTag;
use super::segment::{
    DeliveredSegment, MaskSegment, RangeSegment, bit_for_rank, extract_all_remaining_bits,
    extract_bits_up_to_rank,
};

pub trait DeliveryWindowOps {
    fn push_range(&mut self, first_tag: DeliveryTag, range: DeliveryRange, flags: SegmentFlags);
    fn push_mask(
        &mut self,
        first_tag: DeliveryTag,
        mask: DeliveryMask,
        count: u8,
        flags: SegmentFlags,
    );
    fn ack_one(&mut self, tag: DeliveryTag) -> Result<AckBatch, ConsumerError>;
    fn ack_multiple(&mut self, tag: DeliveryTag) -> Result<AckBatch, ConsumerError>;
    fn nack_one(
        &mut self,
        tag: DeliveryTag,
        reason: NackReason,
    ) -> Result<NackBatch, ConsumerError>;
    fn nack_multiple(
        &mut self,
        tag: DeliveryTag,
        reason: NackReason,
    ) -> Result<NackBatch, ConsumerError>;
    fn drain_all(&mut self, reason: NackReason) -> NackBatch;
    fn unacked_count(&self) -> u32;
    fn contains_tag(&self, tag: DeliveryTag) -> bool;
}

#[derive(Debug, Default)]
pub struct SegmentDeliveryWindow {
    pub(super) segments: VecDeque<DeliveredSegment>,
    unacked: u32,
}

// Internal action for ack_one / nack_one to avoid borrow checker issues.
enum SegAction {
    RemoveSegment,
    RangeTrimFront,
    RangeTrimBack,
    RangeSplitAt { left_len: u32, right: DeliveredSegment },
    MaskClearRank { rank: u32 },
    MaskRemove,
}

impl SegmentDeliveryWindow {
    #[must_use]
    pub fn new() -> Self {
        Self { segments: VecDeque::new(), unacked: 0 }
    }

    fn find_segment_idx(&self, tag: DeliveryTag) -> Option<usize> {
        self.segments.iter().position(|s| s.contains_tag(tag))
    }

    fn compute_one_action(
        seg: &DeliveredSegment,
        tag: DeliveryTag,
    ) -> (OneEntry, SegAction) {
        match seg {
            DeliveredSegment::Range(r) => {
                let offset = (tag.0 - r.first_tag.0) as u32;
                let seq = r.seq_of(tag);
                let entry = OneEntry::Range { seq };
                let action = if r.len == 1 {
                    SegAction::RemoveSegment
                } else if offset == 0 {
                    SegAction::RangeTrimFront
                } else if offset == r.len - 1 {
                    SegAction::RangeTrimBack
                } else {
                    let right = DeliveredSegment::Range(RangeSegment {
                        first_tag: DeliveryTag(tag.0 + 1),
                        start_seq: seq + 1,
                        len: r.len - offset - 1,
                        flags: r.flags,
                    });
                    SegAction::RangeSplitAt { left_len: offset, right }
                };
                (entry, action)
            }
            DeliveredSegment::Mask(m) => {
                let rank = (tag.0 - m.first_tag.0) as u32;
                let bit = bit_for_rank(m.original_mask, rank);
                let entry = OneEntry::Mask { block: m.block, word: m.word, bit };
                let action = if m.remaining_rank_mask.count_ones() == 1 {
                    SegAction::MaskRemove
                } else {
                    SegAction::MaskClearRank { rank }
                };
                (entry, action)
            }
        }
    }

    fn apply_seg_action(&mut self, idx: usize, action: SegAction) {
        match action {
            SegAction::RemoveSegment | SegAction::MaskRemove => {
                self.segments.remove(idx);
            }
            SegAction::RangeTrimFront => {
                if let DeliveredSegment::Range(r) = &mut self.segments[idx] {
                    r.first_tag.0 += 1;
                    r.start_seq += 1;
                    r.len -= 1;
                }
            }
            SegAction::RangeTrimBack => {
                if let DeliveredSegment::Range(r) = &mut self.segments[idx] {
                    r.len -= 1;
                }
            }
            SegAction::RangeSplitAt { left_len, right } => {
                if let DeliveredSegment::Range(r) = &mut self.segments[idx] {
                    r.len = left_len;
                }
                self.segments.insert(idx + 1, right);
            }
            SegAction::MaskClearRank { rank } => {
                if let DeliveredSegment::Mask(m) = &mut self.segments[idx] {
                    m.remaining_rank_mask &= !(1u64 << rank);
                }
            }
        }
    }
}

enum OneEntry {
    Range { seq: u64 },
    Mask { block: BlockIndex, word: WordIndex, bit: u64 },
}

fn full_segment_to_ack(seg: &DeliveredSegment, batch: &mut AckBatch) -> u32 {
    match seg {
        DeliveredSegment::Range(r) => {
            batch.ranges.push(AckRange::new(r.start_seq, r.len));
            r.len
        }
        DeliveredSegment::Mask(m) => {
            let bits = extract_all_remaining_bits(m.original_mask, m.remaining_rank_mask);
            let count = bits.count_ones();
            if bits != 0 {
                batch.masks.push(AckMask::new(m.block, m.word, bits));
            }
            count
        }
    }
}

fn partial_segment_to_ack(
    seg: &mut DeliveredSegment,
    tag: DeliveryTag,
    batch: &mut AckBatch,
) -> u32 {
    match seg {
        DeliveredSegment::Range(r) => {
            let consumed = (tag.0 - r.first_tag.0 + 1) as u32;
            batch.ranges.push(AckRange::new(r.start_seq, consumed));
            r.first_tag.0 += consumed as u64;
            r.start_seq += consumed as u64;
            r.len -= consumed;
            consumed
        }
        DeliveredSegment::Mask(m) => {
            let up_to_rank = (tag.0 - m.first_tag.0) as u32;
            let bits = extract_bits_up_to_rank(m.original_mask, &mut m.remaining_rank_mask, up_to_rank);
            let count = bits.count_ones();
            if bits != 0 {
                batch.masks.push(AckMask::new(m.block, m.word, bits));
            }
            count
        }
    }
}

fn full_segment_to_nack(seg: &DeliveredSegment, batch: &mut NackBatch) -> u32 {
    match seg {
        DeliveredSegment::Range(r) => {
            batch.ranges.push(NackRange::new(r.start_seq, r.len));
            r.len
        }
        DeliveredSegment::Mask(m) => {
            let bits = extract_all_remaining_bits(m.original_mask, m.remaining_rank_mask);
            let count = bits.count_ones();
            if bits != 0 {
                batch.masks.push(NackMask::new(m.block, m.word, bits));
            }
            count
        }
    }
}

fn partial_segment_to_nack(
    seg: &mut DeliveredSegment,
    tag: DeliveryTag,
    batch: &mut NackBatch,
) -> u32 {
    match seg {
        DeliveredSegment::Range(r) => {
            let consumed = (tag.0 - r.first_tag.0 + 1) as u32;
            batch.ranges.push(NackRange::new(r.start_seq, consumed));
            r.first_tag.0 += consumed as u64;
            r.start_seq += consumed as u64;
            r.len -= consumed;
            consumed
        }
        DeliveredSegment::Mask(m) => {
            let up_to_rank = (tag.0 - m.first_tag.0) as u32;
            let bits = extract_bits_up_to_rank(m.original_mask, &mut m.remaining_rank_mask, up_to_rank);
            let count = bits.count_ones();
            if bits != 0 {
                batch.masks.push(NackMask::new(m.block, m.word, bits));
            }
            count
        }
    }
}

impl DeliveryWindowOps for SegmentDeliveryWindow {
    fn push_range(&mut self, first_tag: DeliveryTag, range: DeliveryRange, flags: SegmentFlags) {
        self.segments.push_back(DeliveredSegment::Range(RangeSegment::new(
            first_tag,
            range.start_seq,
            range.len,
            flags,
        )));
        self.unacked += range.len;
    }

    fn push_mask(
        &mut self,
        first_tag: DeliveryTag,
        mask: DeliveryMask,
        count: u8,
        flags: SegmentFlags,
    ) {
        self.segments.push_back(DeliveredSegment::Mask(MaskSegment::new(
            first_tag,
            mask.block,
            mask.word,
            mask.mask,
            flags,
        )));
        self.unacked += u32::from(count);
    }

    fn ack_one(&mut self, tag: DeliveryTag) -> Result<AckBatch, ConsumerError> {
        let idx = self.find_segment_idx(tag).ok_or(ConsumerError::InvalidDeliveryTag)?;

        let (entry, action) = Self::compute_one_action(&self.segments[idx], tag);

        self.apply_seg_action(idx, action);
        self.unacked -= 1;

        let mut batch = AckBatch::default();
        match entry {
            OneEntry::Range { seq } => batch.ranges.push(AckRange::new(seq, 1)),
            OneEntry::Mask { block, word, bit } => batch.masks.push(AckMask::new(block, word, bit)),
        }
        Ok(batch)
    }

    fn ack_multiple(&mut self, tag: DeliveryTag) -> Result<AckBatch, ConsumerError> {
        if !self.contains_tag(tag) {
            return Err(ConsumerError::InvalidDeliveryTag);
        }

        let mut batch = AckBatch::default();
        let mut settled = 0u32;

        loop {
            let last = match self.segments.front() {
                None => break,
                Some(s) => s.last_tag(),
            };

            if last.0 <= tag.0 {
                let seg = self.segments.pop_front().expect("checked above");
                settled += full_segment_to_ack(&seg, &mut batch);
                if last.0 == tag.0 {
                    break;
                }
            } else {
                let front = self.segments.front_mut().expect("checked above");
                settled += partial_segment_to_ack(front, tag, &mut batch);
                break;
            }
        }

        self.unacked -= settled;
        Ok(batch)
    }

    fn nack_one(
        &mut self,
        tag: DeliveryTag,
        reason: NackReason,
    ) -> Result<NackBatch, ConsumerError> {
        let idx = self.find_segment_idx(tag).ok_or(ConsumerError::InvalidDeliveryTag)?;

        let (entry, action) = Self::compute_one_action(&self.segments[idx], tag);

        self.apply_seg_action(idx, action);
        self.unacked -= 1;

        let mut batch = NackBatch::new(reason);
        match entry {
            OneEntry::Range { seq } => batch.ranges.push(NackRange::new(seq, 1)),
            OneEntry::Mask { block, word, bit } => {
                batch.masks.push(NackMask::new(block, word, bit));
            }
        }
        Ok(batch)
    }

    fn nack_multiple(
        &mut self,
        tag: DeliveryTag,
        reason: NackReason,
    ) -> Result<NackBatch, ConsumerError> {
        if !self.contains_tag(tag) {
            return Err(ConsumerError::InvalidDeliveryTag);
        }

        let mut batch = NackBatch::new(reason);
        let mut settled = 0u32;

        loop {
            let last = match self.segments.front() {
                None => break,
                Some(s) => s.last_tag(),
            };

            if last.0 <= tag.0 {
                let seg = self.segments.pop_front().expect("checked above");
                settled += full_segment_to_nack(&seg, &mut batch);
                if last.0 == tag.0 {
                    break;
                }
            } else {
                let front = self.segments.front_mut().expect("checked above");
                settled += partial_segment_to_nack(front, tag, &mut batch);
                break;
            }
        }

        self.unacked -= settled;
        Ok(batch)
    }

    fn drain_all(&mut self, reason: NackReason) -> NackBatch {
        let mut batch = NackBatch::new(reason);
        while let Some(seg) = self.segments.pop_front() {
            full_segment_to_nack(&seg, &mut batch);
        }
        self.unacked = 0;
        batch
    }

    fn unacked_count(&self) -> u32 {
        self.unacked
    }

    fn contains_tag(&self, tag: DeliveryTag) -> bool {
        self.segments.iter().any(|s| s.contains_tag(tag))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aurum_types::DeliveryRange;

    fn range(first_tag: u64, start_seq: u64, len: u32) -> (DeliveryTag, DeliveryRange) {
        (DeliveryTag(first_tag), DeliveryRange::new(start_seq, len))
    }

    #[test]
    fn ack_one_single_range() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft, r) = range(0, 100, 3);
        w.push_range(ft, r, SegmentFlags::empty());
        assert_eq!(w.unacked_count(), 3);

        let batch = w.ack_one(DeliveryTag(1)).unwrap();
        assert_eq!(batch.ranges.len(), 1);
        assert_eq!(batch.ranges[0].start_seq, 101);
        assert_eq!(batch.ranges[0].len, 1);
        assert_eq!(w.unacked_count(), 2);
        // Segment should have been split: [0] and [2]
        assert_eq!(w.segments.len(), 2);
    }

    #[test]
    fn ack_one_front_of_range() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft, r) = range(0, 100, 3);
        w.push_range(ft, r, SegmentFlags::empty());

        w.ack_one(DeliveryTag(0)).unwrap();
        assert_eq!(w.segments.len(), 1);
        if let DeliveredSegment::Range(r) = &w.segments[0] {
            assert_eq!(r.first_tag, DeliveryTag(1));
            assert_eq!(r.start_seq, 101);
            assert_eq!(r.len, 2);
        } else {
            panic!("expected range segment");
        }
    }

    #[test]
    fn ack_multiple_full_range() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft, r) = range(0, 100, 5);
        w.push_range(ft, r, SegmentFlags::empty());

        let batch = w.ack_multiple(DeliveryTag(4)).unwrap();
        assert_eq!(batch.ranges[0].start_seq, 100);
        assert_eq!(batch.ranges[0].len, 5);
        assert_eq!(w.unacked_count(), 0);
        assert!(w.segments.is_empty());
    }

    #[test]
    fn ack_multiple_partial_range() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft, r) = range(0, 100, 5);
        w.push_range(ft, r, SegmentFlags::empty());

        let batch = w.ack_multiple(DeliveryTag(2)).unwrap();
        assert_eq!(batch.ranges[0].start_seq, 100);
        assert_eq!(batch.ranges[0].len, 3); // tags 0,1,2 → seqs 100,101,102
        assert_eq!(w.unacked_count(), 2);

        if let DeliveredSegment::Range(r) = &w.segments[0] {
            assert_eq!(r.first_tag, DeliveryTag(3));
            assert_eq!(r.start_seq, 103);
            assert_eq!(r.len, 2);
        } else {
            panic!();
        }
    }

    #[test]
    fn invalid_tag_returns_error() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft, r) = range(5, 100, 3);
        w.push_range(ft, r, SegmentFlags::empty());

        assert!(matches!(w.ack_one(DeliveryTag(0)), Err(ConsumerError::InvalidDeliveryTag)));
        assert!(matches!(
            w.ack_multiple(DeliveryTag(9)),
            Err(ConsumerError::InvalidDeliveryTag)
        ));
    }

    #[test]
    fn nack_multiple_full_drain() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft1, r1) = range(0, 100, 3);
        let (ft2, r2) = range(3, 200, 2);
        w.push_range(ft1, r1, SegmentFlags::empty());
        w.push_range(ft2, r2, SegmentFlags::empty());
        assert_eq!(w.unacked_count(), 5);

        let batch = w.nack_multiple(DeliveryTag(4), NackReason::Requeue).unwrap();
        assert_eq!(batch.ranges.len(), 2);
        assert_eq!(w.unacked_count(), 0);
    }

    #[test]
    fn drain_all_empties_window() {
        let mut w = SegmentDeliveryWindow::new();
        let (ft, r) = range(0, 0, 10);
        w.push_range(ft, r, SegmentFlags::empty());
        let batch = w.drain_all(NackReason::Requeue);
        assert_eq!(batch.ranges[0].len, 10);
        assert_eq!(w.unacked_count(), 0);
    }

    #[test]
    fn mask_segment_ack_one() {
        let mask_bits = 0b0010_1010u64; // bits 1,3,5 set → 3 messages, ranks 0,1,2
        let dmask = DeliveryMask::new(0, 0, mask_bits);
        let mut w = SegmentDeliveryWindow::new();
        w.push_mask(DeliveryTag(10), dmask, 3, SegmentFlags::empty());
        assert_eq!(w.unacked_count(), 3);

        // Ack rank 1 (tag=11) → bit 3
        let batch = w.ack_one(DeliveryTag(11)).unwrap();
        assert_eq!(batch.masks.len(), 1);
        assert_eq!(batch.masks[0].mask, 1 << 3);
        assert_eq!(w.unacked_count(), 2);
    }

    #[test]
    fn mask_segment_ack_multiple() {
        let mask_bits = 0b0010_1010u64; // bits 1,3,5 → ranks 0,1,2
        let dmask = DeliveryMask::new(0, 0, mask_bits);
        let mut w = SegmentDeliveryWindow::new();
        w.push_mask(DeliveryTag(10), dmask, 3, SegmentFlags::empty());

        // ack_multiple(tag=11) = ack ranks 0,1
        let batch = w.ack_multiple(DeliveryTag(11)).unwrap();
        assert_eq!(batch.masks.len(), 1);
        assert_eq!(batch.masks[0].mask, (1 << 1) | (1 << 3)); // bits 1 and 3
        assert_eq!(w.unacked_count(), 1); // rank 2 (bit 5) remains
    }
}
