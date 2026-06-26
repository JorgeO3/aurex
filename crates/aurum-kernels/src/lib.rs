#![forbid(unsafe_code)]

/// Scalar/SWAR-friendly bit operations used by the queue engine.
///
/// The design goal is to keep loops simple enough that LLVM can autovectorize
/// when target flags allow it, while preserving a scalar baseline.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScalarKernel;

pub trait BitKernel {
    fn set_range(words: &mut [u64], start_bit: usize, len: usize);
    fn clear_range(words: &mut [u64], start_bit: usize, len: usize);
    fn take_lowest_bits(word: &mut u64, max: u32) -> u64;
}

impl BitKernel for ScalarKernel {
    #[inline(always)]
    fn set_range(words: &mut [u64], start_bit: usize, len: usize) {
        set_range(words, start_bit, len);
    }

    #[inline(always)]
    fn clear_range(words: &mut [u64], start_bit: usize, len: usize) {
        clear_range(words, start_bit, len);
    }

    #[inline(always)]
    fn take_lowest_bits(word: &mut u64, max: u32) -> u64 {
        take_lowest_bits(word, max)
    }
}

#[inline(always)]
#[must_use]
pub fn low_mask(bits: u32) -> u64 {
    match bits {
        0 => 0,
        64.. => u64::MAX,
        n => (1u64 << n) - 1,
    }
}

#[inline(always)]
#[must_use]
pub fn take_lowest_bits(word: &mut u64, max: u32) -> u64 {
    if *word == 0 || max == 0 {
        return 0;
    }

    let available = word.count_ones();
    let take = available.min(max);
    if take == available {
        let mask = *word;
        *word = 0;
        return mask;
    }

    let mut remaining = take;
    let mut src = *word;
    let mut mask = 0u64;
    while remaining != 0 {
        let bit = src.trailing_zeros();
        let one = 1u64 << bit;
        mask |= one;
        src &= src - 1;
        remaining -= 1;
    }
    *word &= !mask;
    mask
}

pub fn set_range(words: &mut [u64], start_bit: usize, len: usize) {
    if len == 0 {
        return;
    }

    let mut idx = start_bit;
    let end = start_bit + len;
    while idx < end {
        let word_idx = idx >> 6;
        let bit = idx & 63;
        let take = (64 - bit).min(end - idx);
        let mask = low_mask(take as u32) << bit;
        words[word_idx] |= mask;
        idx += take;
    }
}

pub fn clear_range(words: &mut [u64], start_bit: usize, len: usize) {
    if len == 0 {
        return;
    }

    let mut idx = start_bit;
    let end = start_bit + len;
    while idx < end {
        let word_idx = idx >> 6;
        let bit = idx & 63;
        let take = (64 - bit).min(end - idx);
        let mask = low_mask(take as u32) << bit;
        words[word_idx] &= !mask;
        idx += take;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_clear_range_cross_word() {
        let mut words = [0u64; 3];
        set_range(&mut words, 60, 10);
        assert_eq!(words[0] >> 60, 0b1111);
        assert_eq!(words[1] & 0b11_1111, 0b11_1111);
        clear_range(&mut words, 62, 4);
        assert_eq!((words[0] >> 60) & 0b1111, 0b0011);
        assert_eq!(words[1] & 0b11, 0);
    }

    #[test]
    fn take_lowest_bits_sparse() {
        let mut word = 0b10110100u64;
        let mask = take_lowest_bits(&mut word, 2);
        assert_eq!(mask, 0b00010100);
        assert_eq!(word, 0b10100000);
    }
}
