use smallvec::SmallVec;
use aurum_types::{ChannelId, ConsumerId, DeliveryTag, PayloadHandle, QueueId};

use crate::flags::DeliveryEventFlags;

// ── PayloadSpan ───────────────────────────────────────────────────────────────

/// Describes how to obtain the payload handles for a delivery segment without
/// materializing them into a Vec.
///
/// - `Contiguous`: handles are `base, base+1, ..., base+(len-1)`.
///   Zero allocation for the common sequential-delivery path.
/// - `Sparse`: handles don't follow a contiguous pattern (non-sequential backend
///   buffer IDs, io_uring registered slots, etc.).
#[derive(Debug, Clone)]
pub enum PayloadSpan<P = PayloadHandle> {
    Contiguous { base: P, len: u32 },
    Sparse(SmallVec<[P; 8]>),
}

impl<P: Copy> PayloadSpan<P> {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Contiguous { len, .. } => *len as usize,
            Self::Sparse(v) => v.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PayloadSpan<PayloadHandle> {
    /// Iterate over payload handles. For `Contiguous`, derives each handle
    /// arithmetically — no allocation.
    pub fn iter(&self) -> impl Iterator<Item = PayloadHandle> + '_ {
        let (base, len, sparse) = match self {
            Self::Contiguous { base, len } => (base.0, *len, None),
            Self::Sparse(v) => (0, 0, Some(v.as_slice())),
        };
        PayloadSpanIter { base, pos: 0, len, sparse }
    }

    /// Get handle at index `i`. O(1) for both variants.
    #[must_use]
    pub fn get(&self, i: u32) -> Option<PayloadHandle> {
        match self {
            Self::Contiguous { base, len } => {
                if i < *len { Some(PayloadHandle(base.0 + u64::from(i))) } else { None }
            }
            Self::Sparse(v) => v.get(i as usize).copied(),
        }
    }
}

struct PayloadSpanIter<'a> {
    base: u64,
    pos: u32,
    len: u32,
    sparse: Option<&'a [PayloadHandle]>,
}

impl Iterator for PayloadSpanIter<'_> {
    type Item = PayloadHandle;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(slice) = self.sparse {
            if self.pos as usize >= slice.len() {
                return None;
            }
            let h = slice[self.pos as usize];
            self.pos += 1;
            return Some(h);
        }
        if self.pos >= self.len {
            return None;
        }
        let h = PayloadHandle(self.base + u64::from(self.pos));
        self.pos += 1;
        Some(h)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = if let Some(s) = self.sparse {
            s.len().saturating_sub(self.pos as usize)
        } else {
            (self.len - self.pos) as usize
        };
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PayloadSpanIter<'_> {}

// ── Delivery segments ─────────────────────────────────────────────────────────

/// A range of consecutive messages delivered to a consumer.
/// `payloads` is O(1) for the common sequential path via `PayloadSpan::Contiguous`.
#[derive(Debug, Clone)]
pub struct DeliveryRangeSegment<P = PayloadHandle> {
    pub start_tag: DeliveryTag,
    pub start_seq: u64,
    pub len: u32,
    pub payloads: PayloadSpan<P>,
    pub flags: DeliveryEventFlags,
}

/// A sparse set of messages (retry/redelivery path) delivered to a consumer.
/// Payload handles are derived from `(block, word, mask)` by the adapter —
/// no redundant storage needed.
#[derive(Debug, Clone)]
pub struct DeliveryMaskSegment {
    pub base_tag: DeliveryTag,
    pub block: u32,
    pub word: u8,
    pub mask: u64,
    pub flags: DeliveryEventFlags,
}

impl DeliveryMaskSegment {
    /// Iterate over `(tag, PayloadHandle)` pairs for each set bit.
    /// Handle = `block * MSGS_PER_BLOCK + word * 64 + bit_pos`.
    pub fn iter_handles(&self) -> impl Iterator<Item = (DeliveryTag, PayloadHandle)> + '_ {
        const MSGS_PER_BLOCK: u64 = 256; // WORDS_PER_BLOCK(4) × 64 bits
        let block_base = u64::from(self.block) * MSGS_PER_BLOCK + u64::from(self.word) * 64;
        let base_tag = self.base_tag;
        let mut bits = self.mask;
        let mut tag_offset = 0u64;
        std::iter::from_fn(move || {
            if bits == 0 {
                return None;
            }
            let tz = bits.trailing_zeros();
            bits &= bits - 1;
            let handle = PayloadHandle(block_base + u64::from(tz));
            let tag = DeliveryTag(base_tag.0 + tag_offset);
            tag_offset += 1;
            Some((tag, handle))
        })
    }
}

#[derive(Debug, Clone)]
pub enum DeliveryEventSegment<P = PayloadHandle> {
    Range(DeliveryRangeSegment<P>),
    Mask(DeliveryMaskSegment),
}

impl<P> DeliveryEventSegment<P> {
    #[must_use]
    pub fn first_tag(&self) -> DeliveryTag {
        match self {
            Self::Range(r) => r.start_tag,
            Self::Mask(m) => m.base_tag,
        }
    }

    #[must_use]
    pub fn count(&self) -> usize {
        match self {
            Self::Range(r) => r.len as usize,
            Self::Mask(m) => m.mask.count_ones() as usize,
        }
    }

    #[must_use]
    pub fn last_tag(&self) -> DeliveryTag {
        let first = self.first_tag();
        DeliveryTag(first.0 + self.count() as u64 - 1)
    }
}

// ── Delivery metadata (protocol-neutral, for adapter roundtrip) ───────────────

/// Cold metadata attached to a delivery batch so edge adapters (AMQP, etc.)
/// can encode protocol-specific deliver frames without adapter-local tables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryMetadata {
    pub exchange: SmallVec<[u8; 64]>,
    pub routing_key: SmallVec<[u8; 64]>,
    pub content_type: SmallVec<[u8; 32]>,
    pub delivery_mode: u8,
}

// ── Batch ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeliveryEventBatch<P = PayloadHandle> {
    pub consumer_id: ConsumerId,
    pub queue_id: QueueId,
    pub channel_id: ChannelId,
    pub segments: SmallVec<[DeliveryEventSegment<P>; 8]>,
    pub metadata: DeliveryMetadata,
}

impl<P> DeliveryEventBatch<P> {
    #[must_use]
    pub fn total_count(&self) -> usize {
        self.segments.iter().map(|s| s.count()).sum()
    }

    #[must_use]
    pub fn last_tag(&self) -> Option<DeliveryTag> {
        self.segments.last().map(|s| s.last_tag())
    }
}
