use smallvec::SmallVec;

use aurum_types::{DeliveryMask, DeliveryRange, DeliveryWork};

use crate::queue::work::{AckBatch, AckMask, AckRange, NackBatch, NackReason};
use crate::queue::HybridRangeBlockQueue;

use super::credit::{ConsumerCredit, PrefetchMode};
use super::error::ConsumerError;
use super::flags::{ConsumerFlags, DeliveryFlags, SegmentFlags};
use super::id::{ChannelId, ConsumerId, DeliveryTag};
use super::request::{AckMode, AckRequest, NackMode, NackRequest, RejectRequest};
use super::result::{AckApplyResult, CancelDisposition, CancelResult, NackApplyResult};
use super::window::{DeliveryWindowOps, SegmentDeliveryWindow};

#[derive(Debug)]
pub struct TaggedRange {
    pub first_tag: DeliveryTag,
    pub range: DeliveryRange,
    pub flags: DeliveryFlags,
}

#[derive(Debug)]
pub struct TaggedMask {
    pub first_tag: DeliveryTag,
    pub mask: DeliveryMask,
    pub count: u8,
    pub flags: DeliveryFlags,
}

#[derive(Debug)]
pub enum TaggedDeliverySegment {
    Range(TaggedRange),
    Mask(TaggedMask),
}

#[derive(Debug, Default)]
pub struct SessionDeliveryBatch {
    pub segments: SmallVec<[TaggedDeliverySegment; 4]>,
}

impl SessionDeliveryBatch {
    pub fn clear(&mut self) {
        self.segments.clear();
    }
}

#[derive(Debug)]
pub struct ConsumerSession<W = SegmentDeliveryWindow> {
    pub id: ConsumerId,
    pub channel_id: ChannelId,
    next_tag: u64,
    credit: ConsumerCredit,
    window: W,
    flags: ConsumerFlags,
}

impl ConsumerSession {
    #[must_use]
    pub fn new(id: ConsumerId, channel_id: ChannelId, prefetch: PrefetchMode) -> Self {
        Self {
            id,
            channel_id,
            next_tag: DeliveryTag::FIRST.0,
            credit: ConsumerCredit::new(prefetch),
            window: SegmentDeliveryWindow::new(),
            flags: ConsumerFlags::empty(),
        }
    }
}

impl<W: DeliveryWindowOps> ConsumerSession<W> {
    #[must_use]
    pub fn with_window(
        id: ConsumerId,
        channel_id: ChannelId,
        prefetch: PrefetchMode,
        window: W,
    ) -> Self {
        Self {
            id,
            channel_id,
            next_tag: DeliveryTag::FIRST.0,
            credit: ConsumerCredit::new(prefetch),
            window,
            flags: ConsumerFlags::empty(),
        }
    }

    pub fn deliver_from_queue(
        &mut self,
        queue: &mut HybridRangeBlockQueue,
        max: u32,
        out: &mut SessionDeliveryBatch,
    ) -> Result<u32, ConsumerError> {
        out.clear();

        if self.flags.contains(ConsumerFlags::CANCELLED) {
            return Err(ConsumerError::ConsumerCancelled);
        }

        let available = self.credit.available().min(max);
        if available == 0 {
            return Ok(0);
        }

        let mut work = DeliveryWork::default();
        let n = queue.deliver(available, &mut work);
        if n == 0 {
            return Ok(0);
        }

        for range in &work.ranges {
            let first_tag = DeliveryTag(self.next_tag);
            self.next_tag += range.len as u64;
            self.window.push_range(first_tag, *range, SegmentFlags::empty());
            out.segments.push(TaggedDeliverySegment::Range(TaggedRange {
                first_tag,
                range: *range,
                flags: DeliveryFlags::empty(),
            }));
        }

        for mask in &work.masks {
            let count = mask.mask.count_ones();
            let first_tag = DeliveryTag(self.next_tag);
            self.next_tag += count as u64;

            let redeliv = queue.redelivered_mask(mask.block, mask.word, mask.mask);
            let seg_flags =
                if redeliv != 0 { SegmentFlags::REDELIVERED } else { SegmentFlags::empty() };
            let del_flags =
                if redeliv != 0 { DeliveryFlags::REDELIVERED } else { DeliveryFlags::empty() };

            self.window.push_mask(first_tag, *mask, count as u8, seg_flags);
            out.segments.push(TaggedDeliverySegment::Mask(TaggedMask {
                first_tag,
                mask: *mask,
                count: count as u8,
                flags: del_flags,
            }));
        }

        // Reserve is guaranteed to succeed since n <= available.
        let _ = self.credit.reserve(n);
        Ok(n)
    }

    pub fn ack(
        &mut self,
        req: AckRequest,
        queue: &mut HybridRangeBlockQueue,
    ) -> Result<AckApplyResult, ConsumerError> {
        if self.flags.contains(ConsumerFlags::CANCELLED) {
            return Err(ConsumerError::ConsumerCancelled);
        }
        if req.tag.0 < DeliveryTag::FIRST.0 || req.tag.0 >= self.next_tag {
            return Err(ConsumerError::InvalidDeliveryTag);
        }

        let batch = match req.mode {
            AckMode::One => self.window.ack_one(req.tag)
                .map_err(|_| ConsumerError::DeliveryTagAlreadySettled)?,
            AckMode::Multiple => self.window.ack_multiple(req.tag)
                .map_err(|_| ConsumerError::DeliveryTagAlreadySettled)?,
        };

        let acked = batch.acked_messages() as u32;
        let ranges: SmallVec<[AckRange; 4]> = batch.ranges.iter().copied().collect();
        queue.apply_ack_batch(&batch);
        self.credit.release(acked);
        Ok(AckApplyResult {
            acked,
            released_credit: acked,
            ranges,
        })
    }

    pub fn nack(
        &mut self,
        req: NackRequest,
        queue: &mut HybridRangeBlockQueue,
    ) -> Result<NackApplyResult, ConsumerError> {
        if self.flags.contains(ConsumerFlags::CANCELLED) {
            return Err(ConsumerError::ConsumerCancelled);
        }
        if req.tag.0 < DeliveryTag::FIRST.0 || req.tag.0 >= self.next_tag {
            return Err(ConsumerError::InvalidDeliveryTag);
        }

        let batch = match req.mode {
            NackMode::One => self.window.nack_one(req.tag, req.reason)
                .map_err(|_| ConsumerError::DeliveryTagAlreadySettled)?,
            NackMode::Multiple => self.window.nack_multiple(req.tag, req.reason)
                .map_err(|_| ConsumerError::DeliveryTagAlreadySettled)?,
        };

        let nacked = batch_nacked_count(&batch);
        let (requeued, dropped, dead_lettered) = apply_nack_batch(queue, &batch);
        self.credit.release(nacked);
        Ok(NackApplyResult { nacked, requeued, dropped, dead_lettered, released_credit: nacked })
    }

    pub fn reject(
        &mut self,
        req: RejectRequest,
        queue: &mut HybridRangeBlockQueue,
    ) -> Result<NackApplyResult, ConsumerError> {
        self.nack(NackRequest::one(req.tag, req.reason), queue)
    }

    pub fn cancel(
        &mut self,
        disp: CancelDisposition,
        queue: &mut HybridRangeBlockQueue,
    ) -> CancelResult {
        self.flags |= ConsumerFlags::CANCELLED;
        let batch = self.window.drain_all(NackReason::Requeue);
        let total = batch_nacked_count(&batch);
        match disp {
            CancelDisposition::RequeueUnacked => {
                queue.apply_nack_batch(&batch);
                self.credit.release(total);
                CancelResult { requeued: total, dropped: 0 }
            }
            CancelDisposition::DropUnacked => {
                let ack = nack_batch_as_ack(&batch);
                queue.apply_ack_batch(&ack);
                self.credit.release(total);
                CancelResult { requeued: 0, dropped: total }
            }
        }
    }

    #[must_use]
    pub fn unacked_count(&self) -> u32 {
        self.window.unacked_count()
    }

    #[must_use]
    pub fn in_flight_credit(&self) -> u32 {
        self.credit.in_flight()
    }

    #[must_use]
    pub fn prefetch_mode(&self) -> PrefetchMode {
        self.credit.prefetch_mode()
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flags.contains(ConsumerFlags::CANCELLED)
    }

    #[must_use]
    pub fn next_delivery_tag(&self) -> DeliveryTag {
        DeliveryTag(self.next_tag)
    }
}

fn batch_nacked_count(batch: &NackBatch) -> u32 {
    let from_ranges: u32 = batch.ranges.iter().map(|r| r.len).sum();
    let from_masks: u32 = batch.masks.iter().map(|m| m.mask.count_ones()).sum();
    from_ranges + from_masks
}

fn apply_nack_batch(
    queue: &mut HybridRangeBlockQueue,
    batch: &NackBatch,
) -> (u32, u32, u32) {
    let count = batch_nacked_count(batch);
    match batch.reason {
        NackReason::Requeue => {
            queue.apply_nack_batch(batch);
            (count, 0, 0)
        }
        NackReason::Reject => {
            queue.apply_ack_batch(&nack_batch_as_ack(batch));
            (0, count, 0)
        }
        NackReason::DeadLetter => {
            queue.apply_ack_batch(&nack_batch_as_ack(batch));
            (0, 0, count)
        }
    }
}

fn nack_batch_as_ack(batch: &NackBatch) -> AckBatch {
    AckBatch {
        ranges: batch.ranges.iter().map(|r| AckRange::new(r.start_seq, r.len)).collect(),
        masks: batch.masks.iter().map(|m| AckMask::new(m.block, m.word, m.mask)).collect(),
    }
}
