use aurum_types::{BlockIndex, Seq, WordIndex};
use super::id::DeliveryTag;
use super::flags::SegmentFlags;

#[derive(Debug, Clone)]
pub struct RangeSegment {
    pub first_tag: DeliveryTag,
    pub start_seq: Seq,
    pub len: u32,
    pub flags: SegmentFlags,
}

impl RangeSegment {
    #[must_use]
    pub fn new(first_tag: DeliveryTag, start_seq: Seq, len: u32, flags: SegmentFlags) -> Self {
        Self { first_tag, start_seq, len, flags }
    }

    #[must_use]
    pub fn last_tag(&self) -> DeliveryTag {
        DeliveryTag(self.first_tag.0 + self.len as u64 - 1)
    }

    #[must_use]
    pub fn contains_tag(&self, tag: DeliveryTag) -> bool {
        tag.0 >= self.first_tag.0 && tag.0 < self.first_tag.0 + self.len as u64
    }

    /// Seq for a tag known to be in this segment.
    #[must_use]
    pub fn seq_of(&self, tag: DeliveryTag) -> Seq {
        self.start_seq + (tag.0 - self.first_tag.0)
    }
}

/// Tracks a sparse (mask) delivery batch.
///
/// Tag→bit mapping uses delivery rank (0-based index over `original_mask` bits):
/// rank r corresponds to the (r+1)-th set bit of `original_mask`.
/// `remaining_rank_mask` is a bitmask of which ranks haven't been settled yet.
#[derive(Debug, Clone)]
pub struct MaskSegment {
    pub first_tag: DeliveryTag,
    pub block: BlockIndex,
    pub word: WordIndex,
    pub original_mask: u64,
    pub remaining_rank_mask: u64,
    pub flags: SegmentFlags,
}

impl MaskSegment {
    #[must_use]
    pub fn new(
        first_tag: DeliveryTag,
        block: BlockIndex,
        word: WordIndex,
        original_mask: u64,
        flags: SegmentFlags,
    ) -> Self {
        let count = original_mask.count_ones();
        let remaining_rank_mask = if count == 64 { u64::MAX } else { (1u64 << count) - 1 };
        Self { first_tag, block, word, original_mask, remaining_rank_mask, flags }
    }

    #[must_use]
    pub fn original_count(&self) -> u32 {
        self.original_mask.count_ones()
    }

    #[must_use]
    pub fn remaining_count(&self) -> u32 {
        self.remaining_rank_mask.count_ones()
    }

    #[must_use]
    pub fn last_tag(&self) -> DeliveryTag {
        DeliveryTag(self.first_tag.0 + self.original_count() as u64 - 1)
    }

    /// Returns true if `tag` corresponds to a rank still present in `remaining_rank_mask`.
    #[must_use]
    pub fn contains_tag(&self, tag: DeliveryTag) -> bool {
        if tag.0 < self.first_tag.0 {
            return false;
        }
        let rank = tag.0 - self.first_tag.0;
        rank < self.original_count() as u64 && self.remaining_rank_mask & (1u64 << rank) != 0
    }
}

#[derive(Debug, Clone)]
pub enum DeliveredSegment {
    Range(RangeSegment),
    Mask(MaskSegment),
}

impl DeliveredSegment {
    #[must_use]
    pub fn contains_tag(&self, tag: DeliveryTag) -> bool {
        match self {
            Self::Range(r) => r.contains_tag(tag),
            Self::Mask(m) => m.contains_tag(tag),
        }
    }

    #[must_use]
    pub fn last_tag(&self) -> DeliveryTag {
        match self {
            Self::Range(r) => r.last_tag(),
            Self::Mask(m) => m.last_tag(),
        }
    }

    #[must_use]
    pub fn remaining_count(&self) -> u32 {
        match self {
            Self::Range(r) => r.len,
            Self::Mask(m) => m.remaining_count(),
        }
    }
}

/// Returns the bit position (within the word) corresponding to delivery rank `rank`
/// in `original_mask`. Panics if rank >= count_ones(original_mask).
#[must_use]
pub(super) fn bit_for_rank(original_mask: u64, rank: u32) -> u64 {
    let mut om = original_mask;
    let mut r = 0u32;
    loop {
        let bit_pos = om.trailing_zeros();
        if r == rank {
            return 1u64 << bit_pos;
        }
        om &= om - 1;
        r += 1;
    }
}

/// Extracts bits from `original_mask` for all ranks in `0..=up_to_rank` that are
/// still set in `remaining_rank_mask`. Clears those rank bits from `remaining_rank_mask`.
#[must_use]
pub(super) fn extract_bits_up_to_rank(
    original_mask: u64,
    remaining_rank_mask: &mut u64,
    up_to_rank: u32,
) -> u64 {
    let mut result = 0u64;
    let mut om = original_mask;
    let mut rank = 0u32;
    while om != 0 && rank <= up_to_rank {
        let bit_pos = om.trailing_zeros();
        let rank_bit = 1u64 << rank;
        if *remaining_rank_mask & rank_bit != 0 {
            result |= 1u64 << bit_pos;
            *remaining_rank_mask &= !rank_bit;
        }
        om &= om - 1;
        rank += 1;
    }
    result
}

/// Extracts bits from `original_mask` for all ranks still set in `remaining_rank_mask`.
#[must_use]
pub(super) fn extract_all_remaining_bits(original_mask: u64, remaining_rank_mask: u64) -> u64 {
    let mut result = 0u64;
    let mut om = original_mask;
    let mut rank = 0u32;
    while om != 0 {
        let bit_pos = om.trailing_zeros();
        if remaining_rank_mask & (1u64 << rank) != 0 {
            result |= 1u64 << bit_pos;
        }
        om &= om - 1;
        rank += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_segment_tag_mapping() {
        let seg = RangeSegment::new(DeliveryTag(10), 100, 5, SegmentFlags::empty());
        assert!(seg.contains_tag(DeliveryTag(10)));
        assert!(seg.contains_tag(DeliveryTag(14)));
        assert!(!seg.contains_tag(DeliveryTag(9)));
        assert!(!seg.contains_tag(DeliveryTag(15)));
        assert_eq!(seg.seq_of(DeliveryTag(12)), 102);
        assert_eq!(seg.last_tag(), DeliveryTag(14));
    }

    #[test]
    fn mask_segment_rank_mapping() {
        // original_mask: bits 0, 2, 4, 5 set (ranks 0,1,2,3)
        let mask = 0b0011_0101u64;
        let seg = MaskSegment::new(DeliveryTag(0), 1, 0, mask, SegmentFlags::empty());
        assert_eq!(seg.original_count(), 4);
        assert_eq!(seg.remaining_count(), 4);
        assert!(seg.contains_tag(DeliveryTag(0)));
        assert!(seg.contains_tag(DeliveryTag(3)));
        assert!(!seg.contains_tag(DeliveryTag(4)));

        assert_eq!(bit_for_rank(mask, 0), 1 << 0);
        assert_eq!(bit_for_rank(mask, 1), 1 << 2);
        assert_eq!(bit_for_rank(mask, 2), 1 << 4);
        assert_eq!(bit_for_rank(mask, 3), 1 << 5);
    }

    #[test]
    fn extract_bits_partial() {
        let mask = 0b0011_0101u64; // bits 0,2,4,5 = ranks 0,1,2,3
        let mut remaining = 0b1111u64;
        let bits = extract_bits_up_to_rank(mask, &mut remaining, 1); // ranks 0,1
        assert_eq!(bits, (1 << 0) | (1 << 2));
        assert_eq!(remaining, 0b1100); // ranks 2,3 still remain
    }

    #[test]
    fn extract_bits_skips_settled_ranks() {
        let mask = 0b0011_0101u64; // bits 0,2,4,5 → ranks 0,1,2,3
        let mut remaining = 0b1101u64; // rank 1 already settled; ranks 0,2,3 remain
        let bits = extract_bits_up_to_rank(mask, &mut remaining, 2); // up to rank 2
        // rank 0: set in remaining → extract bit 0
        // rank 1: already settled → skip
        // rank 2: set in remaining → extract bit 4
        assert_eq!(bits, (1 << 0) | (1 << 4));
        assert_eq!(remaining, 0b1000); // only rank 3 remains
    }

    #[test]
    fn mask_segment_64_bits() {
        let full_mask = u64::MAX;
        let seg = MaskSegment::new(DeliveryTag(0), 0, 0, full_mask, SegmentFlags::empty());
        assert_eq!(seg.original_count(), 64);
        assert_eq!(seg.remaining_rank_mask, u64::MAX);
    }
}
