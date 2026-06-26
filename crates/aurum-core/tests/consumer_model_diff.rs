/// Differential tests: ConsumerSession + HybridRangeBlockQueue vs
/// ModelConsumerSession + ModelQueue. After every operation the observable
/// state must match: unacked_count, in-flight credit, and queue.debug_counts().
use aurum_core::{
    AckRequest, CancelDisposition, ConsumerId, ConsumerSession, HybridRangeBlockQueue,
    ChannelId, ModelConsumerSession, ModelQueue, NackRequest, PrefetchMode,
    SessionDeliveryBatch, NackReason,
};

fn consumer_id() -> ConsumerId {
    ConsumerId(1)
}

fn channel_id() -> ChannelId {
    ChannelId(0)
}

fn assert_sync(
    session: &ConsumerSession,
    model: &ModelConsumerSession,
    queue: &HybridRangeBlockQueue,
    mq: &ModelQueue,
    ctx: &str,
) {
    let hc = queue.debug_counts();
    let mc = mq.counts();
    assert_eq!(hc, mc, "[{ctx}] queue counts diverged");
    assert_eq!(
        session.unacked_count(),
        model.unacked_count(),
        "[{ctx}] unacked_count diverged"
    );
    assert_eq!(
        session.in_flight_credit(),
        model.in_flight_count(),
        "[{ctx}] in_flight diverged"
    );
}

// ── §1 Basic sequential deliver + ack_one ─────────────────────────────────

#[test]
fn deliver_then_ack_one_sequential() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(10);
    mq.publish(10);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 5, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 5);
    assert_sync(&s, &ms, &q, &mq, "after deliver 5");

    let t0 = out.segments[0].first_tag();
    s.ack(AckRequest::one(t0), &mut q).unwrap();
    ms.ack(AckRequest::one(t0), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after ack_one(first)");
}

// ── §2 ack_multiple ───────────────────────────────────────────────────────

#[test]
fn deliver_then_ack_multiple_all() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(8);
    mq.publish(8);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 8, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 8);

    let last_tag = out.last_tag();
    s.ack(AckRequest::multiple(last_tag), &mut q).unwrap();
    ms.ack(AckRequest::multiple(last_tag), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after ack_multiple(all)");
    assert_eq!(s.unacked_count(), 0);
}

// ── §3 Partial ack_multiple ───────────────────────────────────────────────

#[test]
fn deliver_then_ack_multiple_partial() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(10);
    mq.publish(10);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 10);
    assert_sync(&s, &ms, &q, &mq, "after deliver 10");

    // Ack first 4 (tags 0..3)
    let tag3 = out.tag_at(3);
    s.ack(AckRequest::multiple(tag3), &mut q).unwrap();
    ms.ack(AckRequest::multiple(tag3), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after ack_multiple(3)");
    assert_eq!(s.unacked_count(), 6);
}

// ── §4 nack_one with Requeue ──────────────────────────────────────────────

#[test]
fn nack_one_requeue_then_retry_redeliver() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(5);
    mq.publish(5);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 5, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 5);

    // Nack tag 2 with Requeue
    let tag2 = out.tag_at(2);
    s.nack(NackRequest::one(tag2, NackReason::Requeue), &mut q).unwrap();
    ms.nack(NackRequest::one(tag2, NackReason::Requeue), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after nack_one(2)");

    // Ack the rest
    let last = out.last_tag();
    s.ack(AckRequest::multiple(last), &mut q).unwrap();
    ms.ack(AckRequest::multiple(last), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after ack_multiple(rest)");

    // Retry all
    q.retry_all_now();
    mq.retry_all_now();

    // Redeliver
    s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 10);
    assert_sync(&s, &ms, &q, &mq, "after redeliver");
    assert_eq!(s.unacked_count(), 1); // just the requeued one
}

// ── §5 nack_multiple + retry ──────────────────────────────────────────────

#[test]
fn nack_multiple_requeue_and_retry() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(10);
    mq.publish(10);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 10);

    // Ack first 5
    let tag4 = out.tag_at(4);
    s.ack(AckRequest::multiple(tag4), &mut q).unwrap();
    ms.ack(AckRequest::multiple(tag4), &mut mq).unwrap();

    // Nack remaining 5 with Requeue
    let tag9 = out.last_tag();
    s.nack(NackRequest::multiple(tag9, NackReason::Requeue), &mut q).unwrap();
    ms.nack(NackRequest::multiple(tag9, NackReason::Requeue), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after nack_multiple(rest)");
    assert_eq!(s.unacked_count(), 0);

    q.retry_all_now();
    mq.retry_all_now();
    assert_sync(&s, &ms, &q, &mq, "after retry_all");

    s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 10);
    assert_sync(&s, &ms, &q, &mq, "after redeliver");
    assert_eq!(s.unacked_count(), 5);
}

// ── §6 nack Reject (drop) ─────────────────────────────────────────────────

#[test]
fn nack_reject_drops_messages() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(5);
    mq.publish(5);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 5, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 5);

    let last = out.last_tag();
    let res = s.nack(NackRequest::multiple(last, NackReason::Reject), &mut q).unwrap();
    let mres = ms.nack(NackRequest::multiple(last, NackReason::Reject), &mut mq).unwrap();

    assert_eq!(res.nacked, 5);
    assert_eq!(res.dropped, 5);
    assert_eq!(mres.nacked, 5);
    assert_sync(&s, &ms, &q, &mq, "after nack Reject");

    // No retry should produce messages
    q.retry_all_now();
    mq.retry_all_now();
    let n = s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 10);
    assert_eq!(n, 0);
    assert_sync(&s, &ms, &q, &mq, "after retry (should be empty)");
}

// ── §7 cancel RequeueUnacked ──────────────────────────────────────────────

#[test]
fn cancel_requeue_unacked() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(8);
    mq.publish(8);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 8, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 8);

    let res = s.cancel(CancelDisposition::RequeueUnacked, &mut q);
    let mres = ms.cancel(CancelDisposition::RequeueUnacked, &mut mq);
    assert_eq!(res.requeued, 8);
    assert_eq!(mres.requeued, 8);
    assert!(s.is_cancelled());
    assert_sync(&s, &ms, &q, &mq, "after cancel requeue");

    // After retry, messages come back
    q.retry_all_now();
    mq.retry_all_now();
    assert_sync(&s, &ms, &q, &mq, "after retry_all_now");
    assert_eq!(q.debug_counts().ready, 8);
}

// ── §8 cancel DropUnacked ─────────────────────────────────────────────────

#[test]
fn cancel_drop_unacked() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(6);
    mq.publish(6);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 6, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 6);

    let res = s.cancel(CancelDisposition::DropUnacked, &mut q);
    let mres = ms.cancel(CancelDisposition::DropUnacked, &mut mq);
    assert_eq!(res.dropped, 6);
    assert_eq!(mres.dropped, 6);
    assert_sync(&s, &ms, &q, &mq, "after cancel drop");
    assert_eq!(q.debug_counts().acked, 6);
}

// ── §9 Limited prefetch blocks excess delivery ────────────────────────────

#[test]
fn limited_prefetch_caps_delivery() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(20);
    mq.publish(20);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Limited(5));
    let mut ms = ModelConsumerSession::new(PrefetchMode::Limited(5));
    let mut out = SessionDeliveryBatch::default();

    let n = s.deliver_from_queue(&mut q, 20, &mut out).unwrap();
    let md = ms.deliver_from_queue(&mut mq, 20);
    assert_eq!(n, 5);
    assert_eq!(md.len(), 5);
    assert_sync(&s, &ms, &q, &mq, "after capped deliver");

    // Second deliver: credit exhausted, should return 0
    let n2 = s.deliver_from_queue(&mut q, 20, &mut out).unwrap();
    let md2 = ms.deliver_from_queue(&mut mq, 20);
    assert_eq!(n2, 0);
    assert_eq!(md2.len(), 0);
}

// ── §10 Credit released on ack ────────────────────────────────────────────

#[test]
fn credit_released_on_ack_allows_more_delivery() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(10);
    mq.publish(10);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Limited(5));
    let mut ms = ModelConsumerSession::new(PrefetchMode::Limited(5));
    let mut out = SessionDeliveryBatch::default();

    s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 10);

    // Ack all 5 to free credit
    let last = out.last_tag();
    s.ack(AckRequest::multiple(last), &mut q).unwrap();
    ms.ack(AckRequest::multiple(last), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after ack all");

    // Should be able to deliver 5 more
    let n = s.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    let md = ms.deliver_from_queue(&mut mq, 10);
    assert_eq!(n, 5);
    assert_eq!(md.len(), 5);
    assert_sync(&s, &ms, &q, &mq, "after second batch");
}

// ── §11 Invalid tag returns error ─────────────────────────────────────────

#[test]
fn ack_invalid_tag_returns_error() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(5);
    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    use aurum_core::{ConsumerError, DeliveryTag};

    // INVALID sentinel (0) always rejected before any delivery
    let err0 = s.ack(AckRequest::one(DeliveryTag::INVALID), &mut q);
    assert!(matches!(err0, Err(ConsumerError::InvalidDeliveryTag)));

    // Tag beyond next_tag (never issued)
    let err = s.ack(AckRequest::one(DeliveryTag(99)), &mut q);
    assert!(matches!(err, Err(ConsumerError::InvalidDeliveryTag)));

    // Deliver 3 → next_tag becomes FIRST + 3
    s.deliver_from_queue(&mut q, 3, &mut out).unwrap();

    // Tag still beyond next_tag
    let err2 = s.ack(AckRequest::one(DeliveryTag(99)), &mut q);
    assert!(matches!(err2, Err(ConsumerError::InvalidDeliveryTag)));
}

// ── §12 Cancelled session rejects further operations ─────────────────────

#[test]
fn cancelled_session_rejects_deliver() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(10);
    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    s.cancel(CancelDisposition::DropUnacked, &mut q);
    use aurum_core::ConsumerError;
    let err = s.deliver_from_queue(&mut q, 5, &mut out);
    assert!(matches!(err, Err(ConsumerError::ConsumerCancelled)));
}

// ── §13 Redelivery flag set after nack+retry+redeliver ───────────────────

#[test]
fn redelivery_flag_set_after_retry() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(5);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    // First delivery — no redelivery flag
    s.deliver_from_queue(&mut q, 5, &mut out).unwrap();
    for seg in &out.segments {
        use aurum_core::{DeliveryFlags, TaggedDeliverySegment};
        if let TaggedDeliverySegment::Range(r) = seg {
            assert!(!r.flags.contains(DeliveryFlags::REDELIVERED));
        }
    }

    // Nack all with Requeue
    let last = out.last_tag();
    s.nack(NackRequest::multiple(last, NackReason::Requeue), &mut q).unwrap();

    // Retry + redeliver
    q.retry_all_now();
    s.deliver_from_queue(&mut q, 10, &mut out).unwrap();

    // Segments should be masks (sparse_ready) with REDELIVERED flag
    for seg in &out.segments {
        use aurum_core::{DeliveryFlags, TaggedDeliverySegment};
        match seg {
            TaggedDeliverySegment::Mask(m) => {
                assert!(m.flags.contains(DeliveryFlags::REDELIVERED), "expected REDELIVERED flag");
            }
            TaggedDeliverySegment::Range(_) => {
                // Sequential shouldn't appear after retry
                panic!("unexpected range segment after redelivery");
            }
        }
    }
}

// ── §14 Two sessions share the same queue independently ──────────────────

#[test]
fn two_sessions_independent() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(10);

    let mut s1 = ConsumerSession::new(ConsumerId(1), channel_id(), PrefetchMode::Limited(5));
    let mut s2 = ConsumerSession::new(ConsumerId(2), channel_id(), PrefetchMode::Limited(5));
    let mut out = SessionDeliveryBatch::default();

    s1.deliver_from_queue(&mut q, 10, &mut out).unwrap();
    s2.deliver_from_queue(&mut q, 10, &mut out).unwrap();

    let counts = q.debug_counts();
    assert_eq!(counts.inflight, 10);
    assert_eq!(s1.unacked_count(), 5);
    assert_eq!(s2.unacked_count(), 5);
}

// ── §15 Multi-cycle: deliver → nack → retry → redeliver × 2 ─────────────

#[test]
fn multi_cycle_nack_requeue() {
    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();
    q.publish_contiguous(6);
    mq.publish(6);

    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut ms = ModelConsumerSession::new(PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();

    // Cycle 1: deliver all 6, ack first 3, nack last 3
    s.deliver_from_queue(&mut q, 6, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 6);
    assert_sync(&s, &ms, &q, &mq, "deliver cycle 1");

    let tag2 = out.tag_at(2);
    let tag5 = out.last_tag();
    s.ack(AckRequest::multiple(tag2), &mut q).unwrap();
    ms.ack(AckRequest::multiple(tag2), &mut mq).unwrap();
    s.nack(NackRequest::multiple(tag5, NackReason::Requeue), &mut q).unwrap();
    ms.nack(NackRequest::multiple(tag5, NackReason::Requeue), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after nack cycle 1");
    assert_eq!(s.unacked_count(), 0);

    q.retry_all_now();
    mq.retry_all_now();
    assert_sync(&s, &ms, &q, &mq, "after retry cycle 1");

    // Cycle 2: only 3 messages come back (the nacked ones)
    s.deliver_from_queue(&mut q, 6, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 6);
    assert_sync(&s, &ms, &q, &mq, "deliver cycle 2");
    assert_eq!(s.unacked_count(), 3);

    // Ack 1 (first of the 3), nack remaining 2
    let tag_first = out.tag_at(0);
    let tag_last = out.last_tag();
    s.ack(AckRequest::multiple(tag_first), &mut q).unwrap();
    ms.ack(AckRequest::multiple(tag_first), &mut mq).unwrap();
    s.nack(NackRequest::multiple(tag_last, NackReason::Requeue), &mut q).unwrap();
    ms.nack(NackRequest::multiple(tag_last, NackReason::Requeue), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "after nack cycle 2");

    q.retry_all_now();
    mq.retry_all_now();
    assert_sync(&s, &ms, &q, &mq, "after retry cycle 2");

    // Final: ack the remaining 2
    s.deliver_from_queue(&mut q, 6, &mut out).unwrap();
    ms.deliver_from_queue(&mut mq, 6);
    assert_eq!(s.unacked_count(), 2);
    let last = out.last_tag();
    s.ack(AckRequest::multiple(last), &mut q).unwrap();
    ms.ack(AckRequest::multiple(last), &mut mq).unwrap();
    assert_sync(&s, &ms, &q, &mq, "final ack");
    assert_eq!(q.debug_counts().acked, 6);
}

// ── Randomized tests ──────────────────────────────────────────────────────

struct XorShift64(u64);
impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn run_random_session(seed: u64, num_ops: usize, prefetch: u32) {
    let mut rng = XorShift64::new(seed);

    let mut q = HybridRangeBlockQueue::empty();
    let mut mq = ModelQueue::new();

    // Publish enough messages to work with
    q.publish_contiguous(1024);
    mq.publish(1024);

    let mode = PrefetchMode::Limited(prefetch);
    let mut s = ConsumerSession::new(consumer_id(), channel_id(), mode);
    let mut ms = ModelConsumerSession::new(mode);
    let mut out = SessionDeliveryBatch::default();

    // Track tags for ack/nack
    let mut active_tags: Vec<aurum_core::DeliveryTag> = Vec::new();

    for i in 0..num_ops {
        let op = rng.next() % 7;
        match op {
            0 | 1 => {
                // Deliver
                let max = ((rng.next() % 32) + 1) as u32;
                let n = s.deliver_from_queue(&mut q, max, &mut out).unwrap_or(0);
                ms.deliver_from_queue(&mut mq, max);
                for seg in &out.segments {
                    let (ft, cnt) = seg.first_tag_and_count();
                    for j in 0..cnt {
                        active_tags.push(aurum_core::DeliveryTag(ft.0 + j as u64));
                    }
                }
                let _ = n;
            }
            2 => {
                // ack_one
                if !active_tags.is_empty() {
                    let idx = (rng.next() as usize) % active_tags.len();
                    let tag = active_tags[idx];
                    if s.ack(AckRequest::one(tag), &mut q).is_ok() {
                        ms.ack(AckRequest::one(tag), &mut mq).ok();
                        active_tags.remove(idx);
                    }
                }
            }
            3 => {
                // ack_multiple
                if !active_tags.is_empty() {
                    // Pick the frontmost tag (lowest tag = oldest delivery)
                    let min_tag = active_tags.iter().copied().min().unwrap();
                    if s.ack(AckRequest::multiple(min_tag), &mut q).is_ok() {
                        ms.ack(AckRequest::multiple(min_tag), &mut mq).ok();
                        active_tags.retain(|t| t.0 > min_tag.0);
                    }
                }
            }
            4 => {
                // nack_one Requeue
                if !active_tags.is_empty() {
                    let idx = (rng.next() as usize) % active_tags.len();
                    let tag = active_tags[idx];
                    if s.nack(NackRequest::one(tag, NackReason::Requeue), &mut q).is_ok() {
                        ms.nack(NackRequest::one(tag, NackReason::Requeue), &mut mq).ok();
                        active_tags.remove(idx);
                    }
                }
            }
            5 => {
                // nack_multiple Requeue
                if !active_tags.is_empty() {
                    let min_tag = active_tags.iter().copied().min().unwrap();
                    if s
                        .nack(NackRequest::multiple(min_tag, NackReason::Requeue), &mut q)
                        .is_ok()
                    {
                        ms.nack(NackRequest::multiple(min_tag, NackReason::Requeue), &mut mq)
                            .ok();
                        active_tags.retain(|t| t.0 > min_tag.0);
                    }
                }
            }
            _ => {
                // retry_all_now
                q.retry_all_now();
                mq.retry_all_now();
            }
        }

        assert_sync(&s, &ms, &q, &mq, &format!("op {i} (seed={seed})"));
    }
}

#[test]
fn random_session_seed_1() {
    run_random_session(1, 500, 64);
}

#[test]
fn random_session_seed_2() {
    run_random_session(42, 500, 128);
}

#[test]
fn random_session_seed_3() {
    run_random_session(12345, 500, 32);
}

#[test]
fn random_session_seed_4() {
    run_random_session(999_999, 1000, 256);
}

// ── §16 Error policy: InvalidDeliveryTag vs DeliveryTagAlreadySettled ────

#[test]
fn ack_zero_tag_returns_invalid() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(3);
    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();
    s.deliver_from_queue(&mut q, 3, &mut out).unwrap();

    use aurum_core::{ConsumerError, DeliveryTag};
    // DeliveryTag(0) = INVALID sentinel — always rejected even after delivery
    let err = s.ack(AckRequest::one(DeliveryTag::INVALID), &mut q);
    assert!(matches!(err, Err(ConsumerError::InvalidDeliveryTag)));
}

#[test]
fn ack_never_issued_tag_returns_invalid() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(5);
    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();
    s.deliver_from_queue(&mut q, 3, &mut out).unwrap(); // next_tag = FIRST + 3

    use aurum_core::{ConsumerError, DeliveryTag};
    let next = s.next_delivery_tag();
    // Tag at next_tag (not yet issued)
    let err = s.ack(AckRequest::one(next), &mut q);
    assert!(matches!(err, Err(ConsumerError::InvalidDeliveryTag)));
    // Tag far beyond
    let err2 = s.ack(AckRequest::one(DeliveryTag(u64::MAX)), &mut q);
    assert!(matches!(err2, Err(ConsumerError::InvalidDeliveryTag)));
}

#[test]
fn double_ack_returns_already_settled() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(5);
    let mut s = ConsumerSession::new(consumer_id(), channel_id(), PrefetchMode::Unlimited);
    let mut out = SessionDeliveryBatch::default();
    s.deliver_from_queue(&mut q, 5, &mut out).unwrap();

    use aurum_core::ConsumerError;
    let t0 = out.tag_at(0);
    // First ack succeeds
    s.ack(AckRequest::one(t0), &mut q).unwrap();
    // Second ack of same tag → already settled
    let err = s.ack(AckRequest::one(t0), &mut q);
    assert!(matches!(err, Err(ConsumerError::DeliveryTagAlreadySettled)));

    // ack_multiple of already-settled tag → already settled
    let err2 = s.ack(AckRequest::multiple(t0), &mut q);
    assert!(matches!(err2, Err(ConsumerError::DeliveryTagAlreadySettled)));
}

// ── Helpers ───────────────────────────────────────────────────────────────

use aurum_core::{DeliveryTag, TaggedDeliverySegment};

trait SessionDeliveryBatchExt {
    fn last_tag(&self) -> DeliveryTag;
    fn tag_at(&self, n: usize) -> DeliveryTag;
}

impl SessionDeliveryBatchExt for SessionDeliveryBatch {
    fn last_tag(&self) -> DeliveryTag {
        let mut max = DeliveryTag(0);
        for seg in &self.segments {
            let (ft, cnt) = seg.first_tag_and_count();
            let last = DeliveryTag(ft.0 + cnt as u64 - 1);
            if last.0 >= max.0 {
                max = last;
            }
        }
        max
    }

    fn tag_at(&self, n: usize) -> DeliveryTag {
        let mut remaining = n;
        for seg in &self.segments {
            let (ft, cnt) = seg.first_tag_and_count();
            if remaining < cnt {
                return DeliveryTag(ft.0 + remaining as u64);
            }
            remaining -= cnt;
        }
        panic!("tag_at({n}) out of bounds in batch with {} total segments", self.segments.len());
    }
}

trait TaggedSegmentExt {
    fn first_tag(&self) -> DeliveryTag;
    fn first_tag_and_count(&self) -> (DeliveryTag, usize);
}

impl TaggedSegmentExt for TaggedDeliverySegment {
    fn first_tag(&self) -> DeliveryTag {
        match self {
            TaggedDeliverySegment::Range(r) => r.first_tag,
            TaggedDeliverySegment::Mask(m) => m.first_tag,
        }
    }

    fn first_tag_and_count(&self) -> (DeliveryTag, usize) {
        match self {
            TaggedDeliverySegment::Range(r) => (r.first_tag, r.range.len as usize),
            TaggedDeliverySegment::Mask(m) => (m.first_tag, m.count as usize),
        }
    }
}
