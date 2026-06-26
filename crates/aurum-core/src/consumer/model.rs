use std::collections::{HashSet, VecDeque};

use aurum_types::Seq;
use smallvec::SmallVec;

use crate::queue::model::ModelQueue;
use crate::queue::work::NackReason;

use super::credit::PrefetchMode;
use super::id::DeliveryTag;
use super::request::{AckMode, AckRequest, NackMode, NackRequest, RejectRequest};
use super::result::{AckApplyResult, CancelDisposition, CancelResult, NackApplyResult};

#[derive(Debug, Clone)]
pub struct ModelDelivery {
    pub tag: DeliveryTag,
    pub seq: Seq,
    pub redelivered: bool,
}

#[derive(Debug)]
pub struct ModelConsumerSession {
    next_tag: u64,
    prefetch: PrefetchMode,
    in_flight: u32,
    unacked: VecDeque<ModelDelivery>,
    /// Seqs that were nacked with Requeue and will be redelivered.
    redelivered_seqs: HashSet<u64>,
}

impl ModelConsumerSession {
    #[must_use]
    pub fn new(prefetch: PrefetchMode) -> Self {
        Self {
            next_tag: DeliveryTag::FIRST.0,
            prefetch,
            in_flight: 0,
            unacked: VecDeque::new(),
            redelivered_seqs: HashSet::new(),
        }
    }

    pub fn deliver_from_queue(
        &mut self,
        queue: &mut ModelQueue,
        max: u32,
    ) -> Vec<ModelDelivery> {
        let available = match self.prefetch {
            PrefetchMode::Unlimited => max,
            PrefetchMode::Limited(n) => max.min(n.saturating_sub(self.in_flight)),
        };

        if available == 0 {
            return Vec::new();
        }

        let seqs = queue.deliver(available);
        let mut delivered = Vec::with_capacity(seqs.len());

        for seq in seqs {
            let tag = DeliveryTag(self.next_tag);
            self.next_tag += 1;
            self.in_flight += 1;

            let redelivered = self.redelivered_seqs.contains(&seq);
            let d = ModelDelivery { tag, seq, redelivered };
            self.unacked.push_back(d.clone());
            delivered.push(d);
        }

        delivered
    }

    pub fn ack(&mut self, req: AckRequest, queue: &mut ModelQueue) -> Result<AckApplyResult, ()> {
        match req.mode {
            AckMode::One => self.ack_one_inner(req.tag, queue),
            AckMode::Multiple => self.ack_multiple_inner(req.tag, queue),
        }
    }

    pub fn nack(
        &mut self,
        req: NackRequest,
        queue: &mut ModelQueue,
    ) -> Result<NackApplyResult, ()> {
        match req.mode {
            NackMode::One => self.nack_one_inner(req.tag, req.reason, queue),
            NackMode::Multiple => self.nack_multiple_inner(req.tag, req.reason, queue),
        }
    }

    pub fn reject(
        &mut self,
        req: RejectRequest,
        queue: &mut ModelQueue,
    ) -> Result<NackApplyResult, ()> {
        self.nack_one_inner(req.tag, req.reason, queue)
    }

    pub fn cancel(&mut self, disp: CancelDisposition, queue: &mut ModelQueue) -> CancelResult {
        let all: Vec<_> = self.unacked.drain(..).collect();
        let total = all.len() as u32;
        self.in_flight -= total;

        for d in &all {
            match disp {
                CancelDisposition::RequeueUnacked => {
                    queue.nack_id_to_retry(d.seq);
                    self.redelivered_seqs.insert(d.seq);
                }
                CancelDisposition::DropUnacked => {
                    queue.ack_id(d.seq);
                    self.redelivered_seqs.remove(&d.seq);
                }
            }
        }

        match disp {
            CancelDisposition::RequeueUnacked => CancelResult { requeued: total, dropped: 0 },
            CancelDisposition::DropUnacked => CancelResult { requeued: 0, dropped: total },
        }
    }

    #[must_use]
    pub fn unacked_count(&self) -> u32 {
        self.unacked.len() as u32
    }

    #[must_use]
    pub fn in_flight_count(&self) -> u32 {
        self.in_flight
    }

    fn ack_one_inner(
        &mut self,
        tag: DeliveryTag,
        queue: &mut ModelQueue,
    ) -> Result<AckApplyResult, ()> {
        let pos = self.unacked.iter().position(|d| d.tag == tag).ok_or(())?;
        let d = self.unacked.remove(pos).expect("just found it");
        queue.ack_id(d.seq);
        self.redelivered_seqs.remove(&d.seq);
        self.in_flight -= 1;
        Ok(AckApplyResult {
            acked: 1,
            released_credit: 1,
            ranges: SmallVec::new(),
        })
    }

    fn ack_multiple_inner(
        &mut self,
        tag: DeliveryTag,
        queue: &mut ModelQueue,
    ) -> Result<AckApplyResult, ()> {
        if !self.unacked.iter().any(|d| d.tag == tag) {
            return Err(());
        }
        let mut count = 0u32;
        while let Some(front) = self.unacked.front() {
            if front.tag.0 > tag.0 {
                break;
            }
            let d = self.unacked.pop_front().expect("just peeked");
            queue.ack_id(d.seq);
            self.redelivered_seqs.remove(&d.seq);
            self.in_flight -= 1;
            count += 1;
        }
        Ok(AckApplyResult {
            acked: count,
            released_credit: count,
            ranges: SmallVec::new(),
        })
    }

    fn nack_one_inner(
        &mut self,
        tag: DeliveryTag,
        reason: NackReason,
        queue: &mut ModelQueue,
    ) -> Result<NackApplyResult, ()> {
        let pos = self.unacked.iter().position(|d| d.tag == tag).ok_or(())?;
        let d = self.unacked.remove(pos).expect("just found it");
        self.in_flight -= 1;

        let (requeued, dropped, dead_lettered) = match reason {
            NackReason::Requeue => {
                queue.nack_id_to_retry(d.seq);
                self.redelivered_seqs.insert(d.seq);
                (1, 0, 0)
            }
            NackReason::Reject => {
                queue.ack_id(d.seq);
                self.redelivered_seqs.remove(&d.seq);
                (0, 1, 0)
            }
            NackReason::DeadLetter => {
                queue.ack_id(d.seq);
                self.redelivered_seqs.remove(&d.seq);
                (0, 0, 1)
            }
        };

        Ok(NackApplyResult { nacked: 1, requeued, dropped, dead_lettered, released_credit: 1 })
    }

    fn nack_multiple_inner(
        &mut self,
        tag: DeliveryTag,
        reason: NackReason,
        queue: &mut ModelQueue,
    ) -> Result<NackApplyResult, ()> {
        if !self.unacked.iter().any(|d| d.tag == tag) {
            return Err(());
        }

        let mut count = 0u32;
        while let Some(front) = self.unacked.front() {
            if front.tag.0 > tag.0 {
                break;
            }
            let d = self.unacked.pop_front().expect("just peeked");
            self.in_flight -= 1;
            match reason {
                NackReason::Requeue => {
                    queue.nack_id_to_retry(d.seq);
                    self.redelivered_seqs.insert(d.seq);
                }
                NackReason::Reject | NackReason::DeadLetter => {
                    queue.ack_id(d.seq);
                    self.redelivered_seqs.remove(&d.seq);
                }
            }
            count += 1;
        }

        let (requeued, dropped, dead_lettered) = match reason {
            NackReason::Requeue => (count, 0, 0),
            NackReason::Reject => (0, count, 0),
            NackReason::DeadLetter => (0, 0, count),
        };

        Ok(NackApplyResult {
            nacked: count,
            requeued,
            dropped,
            dead_lettered,
            released_credit: count,
        })
    }
}
