/// Invariant tests using only the public API.

use aurum_core::{HybridRangeBlockQueue, MessageState, QueueCounts};
use aurum_types::DeliveryWork;

fn check_count_invariant(q: &HybridRangeBlockQueue) {
    let c = q.debug_counts();
    assert_eq!(
        c.ready + c.inflight + c.acked + c.retry,
        c.published,
        "count invariant violated: {c:?}"
    );
}

#[test]
fn invariant_empty_queue() {
    let q = HybridRangeBlockQueue::empty();
    q.validate_invariants().unwrap();
    let c = q.debug_counts();
    assert_eq!(c, QueueCounts::default());
}

#[test]
fn invariant_after_publish() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(256);
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    let c = q.debug_counts();
    assert_eq!(c.published, 256);
    assert_eq!(c.ready, 256);
    assert_eq!(c.inflight, 0);
}

#[test]
fn invariant_after_deliver() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    let c = q.debug_counts();
    assert_eq!(c.inflight, 128);
    assert_eq!(c.ready, 384);
}

#[test]
fn invariant_after_ack() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.ack_work(&work);
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    let c = q.debug_counts();
    assert_eq!(c.acked, 128);
    assert_eq!(c.inflight, 0);
    assert_eq!(c.ready, 128);
}

#[test]
fn invariant_after_nack_retry() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.nack_work_to_retry(&work);
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    let c = q.debug_counts();
    assert_eq!(c.retry, 128);
    assert_eq!(c.inflight, 0);
    assert_eq!(c.ready, 128);
    assert_eq!(c.acked, 0);
}

#[test]
fn invariant_after_retry_all_now() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.nack_work_to_retry(&work);
    let moved = q.retry_all_now();
    assert_eq!(moved, 128);
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    let c = q.debug_counts();
    assert_eq!(c.retry, 0);
    assert_eq!(c.ready, 256); // 128 sparse + 128 sequential
    assert_eq!(c.inflight, 0);
}

#[test]
fn invariant_full_cycle() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    // Deliver all
    loop {
        let n = q.deliver(64, &mut work);
        if n == 0 { break; }
        q.ack_work(&work);
    }
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    let c = q.debug_counts();
    assert_eq!(c.acked, 512);
    assert_eq!(c.ready, 0);
    assert_eq!(c.inflight, 0);
}

#[test]
fn invariant_at_block_boundaries() {
    // Test around seq 63, 64, 65, 255, 256, 257
    for &boundary in &[63u64, 64, 65, 255, 256, 257, 511, 512] {
        let mut q = HybridRangeBlockQueue::with_messages(boundary + 2);
        let mut work = DeliveryWork::default();
        q.deliver((boundary + 2) as u32, &mut work);
        q.validate_invariants().unwrap();
        check_count_invariant(&q);
        q.ack_range(boundary.saturating_sub(1), 3.min((boundary + 2) as u32));
        q.validate_invariants().unwrap();
        check_count_invariant(&q);
    }
}

#[test]
fn invariant_multiple_publish_batches() {
    let mut q = HybridRangeBlockQueue::empty();
    for count in [63u32, 1, 64, 128, 255, 1] {
        q.publish_contiguous(count);
        q.validate_invariants().unwrap();
        check_count_invariant(&q);
    }
    assert_eq!(q.debug_counts().published, 512);
}

#[test]
fn invariant_debug_counts_vs_state_of() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.nack_range_to_retry(0, 64);
    q.ack_range(64, 64);
    q.retry_all_now();

    let c = q.debug_counts();
    // Count by scanning debug_state_of
    let (mut ready, mut inflight, mut acked, mut retry) = (0u64, 0u64, 0u64, 0u64);
    for seq in 0..256 {
        match q.debug_state_of(seq) {
            Some(MessageState::Ready) | Some(MessageState::SparseReady) => ready += 1,
            Some(MessageState::Inflight) => inflight += 1,
            Some(MessageState::Acked) => acked += 1,
            Some(MessageState::Retry) => retry += 1,
            None => {}
        }
    }
    assert_eq!(c.ready, ready, "ready mismatch");
    assert_eq!(c.inflight, inflight, "inflight mismatch");
    assert_eq!(c.acked, acked, "acked mismatch");
    assert_eq!(c.retry, retry, "retry mismatch");
}

#[test]
fn invariant_nack_retry_many_cycles() {
    let mut q = HybridRangeBlockQueue::with_messages(128);
    let mut work = DeliveryWork::default();
    // Put messages through 5 nack/retry cycles
    for _ in 0..5 {
        let n = q.deliver(128, &mut work);
        if n == 0 { break; }
        q.nack_work_to_retry(&work);
        q.retry_all_now();
        q.validate_invariants().unwrap();
        check_count_invariant(&q);
    }
    q.deliver(128, &mut work);
    q.ack_work(&work);
    q.validate_invariants().unwrap();
    check_count_invariant(&q);
    assert_eq!(q.debug_counts().acked, 128);
}
