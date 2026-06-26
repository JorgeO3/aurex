use aurum_types::{DeliveryWork, DeliveryRange};

use super::hybrid::{HybridRangeBlockQueue, locate};
use super::model::ModelQueue;
use super::state::MessageState;
use super::work::{AckBatch, AckRange, NackBatch, NackReason};

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

fn count_states(q: &HybridRangeBlockQueue) -> (u64, u64, u64, u64) {
    let published = q.len();
    let (mut ready, mut inflight, mut acked, mut retry) = (0u64, 0u64, 0u64, 0u64);
    for seq in 0..published {
        match q.debug_state_of(seq) {
            Some(MessageState::Ready) | Some(MessageState::SparseReady) => ready += 1,
            Some(MessageState::Inflight) => inflight += 1,
            Some(MessageState::Acked) => acked += 1,
            Some(MessageState::Retry) => retry += 1,
            None => {}
        }
    }
    (ready, inflight, acked, retry)
}

// ---- publish tests ----

#[test]
fn publish_empty_queue() {
    let mut q = HybridRangeBlockQueue::empty();
    assert_eq!(q.len(), 0);
    assert_eq!(q.sequential_ready_len(), 0);
    assert!(q.is_empty());

    let range = q.publish_contiguous(256);
    assert_eq!(range, DeliveryRange::new(0, 256));
    assert_eq!(q.len(), 256);
    assert_eq!(q.sequential_ready_len(), 256);
    assert!(!q.is_empty());

    q.validate_invariants().unwrap();
}

#[test]
fn publish_zero_is_noop() {
    let mut q = HybridRangeBlockQueue::empty();
    let range = q.publish_contiguous(0);
    assert_eq!(range.len, 0);
    assert_eq!(q.len(), 0);
    q.validate_invariants().unwrap();
}

#[test]
fn publish_multiple_batches() {
    let mut q = HybridRangeBlockQueue::empty();
    let r1 = q.publish_contiguous(100);
    let r2 = q.publish_contiguous(200);
    let r3 = q.publish_contiguous(300);

    assert_eq!(r1, DeliveryRange::new(0, 100));
    assert_eq!(r2, DeliveryRange::new(100, 200));
    assert_eq!(r3, DeliveryRange::new(300, 300));
    assert_eq!(q.len(), 600);
    assert_eq!(q.sequential_ready_len(), 600);
    q.validate_invariants().unwrap();
}

#[test]
fn publish_crosses_block_boundary() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(300); // crosses block 0 (256 msgs) into block 1
    assert_eq!(q.len(), 300);
    assert_eq!(q.sequential_ready_len(), 300);
    q.validate_invariants().unwrap();
}

#[test]
fn with_messages_is_equivalent_to_publish() {
    let mut q1 = HybridRangeBlockQueue::with_messages(1024);
    let mut q2 = HybridRangeBlockQueue::empty();
    q2.publish_contiguous(1024);

    assert_eq!(q1.len(), q2.len());
    assert_eq!(q1.sequential_ready_len(), q2.sequential_ready_len());

    let mut work1 = DeliveryWork::default();
    let mut work2 = DeliveryWork::default();
    assert_eq!(q1.deliver(128, &mut work1), q2.deliver(128, &mut work2));
    assert_eq!(work1.ranges, work2.ranges);
    assert_eq!(work1.masks, work2.masks);

    q1.validate_invariants().unwrap();
    q2.validate_invariants().unwrap();
}

// ---- deliver tests ----

#[test]
fn deliver_single_range() {
    let mut q = HybridRangeBlockQueue::with_messages(1024);
    let mut work = DeliveryWork::default();

    let n = q.deliver(128, &mut work);
    assert_eq!(n, 128);
    assert_eq!(work.ranges.len(), 1);
    assert_eq!(work.masks.len(), 0);
    assert_eq!(work.ranges[0], DeliveryRange::new(0, 128));
    assert_eq!(q.sequential_ready_len(), 896);

    q.validate_invariants().unwrap();
}

#[test]
fn deliver_zero_is_noop() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    let n = q.deliver(0, &mut work);
    assert_eq!(n, 0);
    assert_eq!(work.ranges.len(), 0);
    assert_eq!(q.sequential_ready_len(), 256);
    q.validate_invariants().unwrap();
}

#[test]
fn deliver_more_than_available() {
    let mut q = HybridRangeBlockQueue::with_messages(64);
    let mut work = DeliveryWork::default();
    let n = q.deliver(128, &mut work);
    assert_eq!(n, 64);
    assert_eq!(work.ranges[0], DeliveryRange::new(0, 64));
    assert_eq!(q.sequential_ready_len(), 0);
    q.validate_invariants().unwrap();
}

#[test]
fn deliver_cross_block_range() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();

    // Deliver 300 messages spanning blocks 0 and 1
    let n = q.deliver(300, &mut work);
    assert_eq!(n, 300);
    assert_eq!(work.ranges.len(), 1);
    assert_eq!(work.ranges[0], DeliveryRange::new(0, 300));
    q.validate_invariants().unwrap();
}

#[test]
fn deliver_all_sequential() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    let mut total = 0u32;
    loop {
        let n = q.deliver(128, &mut work);
        if n == 0 {
            break;
        }
        total += n;
    }
    assert_eq!(total, 512);
    assert_eq!(q.sequential_ready_len(), 0);
    q.validate_invariants().unwrap();
}

// ---- ack tests ----

#[test]
fn ack_single_range() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.ack_work(&work);

    let (ready, inflight, acked, retry) = count_states(&q);
    assert_eq!(acked, 128);
    assert_eq!(inflight, 0);
    assert_eq!(ready, 384);
    assert_eq!(retry, 0);

    q.validate_invariants().unwrap();
}

#[test]
fn ack_cross_block_range() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    q.deliver(300, &mut work); // spans blocks 0 and 1
    q.ack_work(&work);

    let (_, inflight, acked, _) = count_states(&q);
    assert_eq!(acked, 300);
    assert_eq!(inflight, 0);
    q.validate_invariants().unwrap();
}

#[test]
fn ack_id_fallback() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(64, &mut work);

    // Ack individual IDs out of order
    q.ack_id(63);
    q.ack_id(0);
    q.ack_id(32);

    assert_eq!(q.debug_state_of(0), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(1), Some(MessageState::Inflight));
    assert_eq!(q.debug_state_of(32), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(63), Some(MessageState::Acked));
    q.validate_invariants().unwrap();
}

#[test]
fn ack_id_out_of_range_is_noop() {
    let mut q = HybridRangeBlockQueue::with_messages(64);
    q.ack_id(999);
    assert_eq!(q.stats().total_acked, 0);
    q.validate_invariants().unwrap();
}

#[test]
fn apply_ack_batch() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);

    let batch = AckBatch {
        ranges: vec![AckRange::new(0, 64)].into(),
        masks: vec![].into(),
    };
    q.apply_ack_batch(&batch);

    assert_eq!(q.debug_state_of(0), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(63), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(64), Some(MessageState::Inflight));
    q.validate_invariants().unwrap();
}

// ---- nack/retry tests ----

#[test]
fn nack_range_to_retry() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.nack_work_to_retry(&work);

    let (_, inflight, _, retry) = count_states(&q);
    assert_eq!(retry, 128);
    assert_eq!(inflight, 0);
    q.validate_invariants().unwrap();
}

#[test]
fn retry_all_now_moves_to_sparse_ready() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.nack_work_to_retry(&work);
    let moved = q.retry_all_now();
    assert_eq!(moved, 128);

    // Now they're in sparse_ready
    for seq in 0..128 {
        assert!(matches!(
            q.debug_state_of(seq),
            Some(MessageState::SparseReady) | Some(MessageState::Ready)
        ));
    }
    q.validate_invariants().unwrap();
}

#[test]
fn nack_retry_ack_full_cycle() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();

    q.deliver(128, &mut work);
    q.nack_work_to_retry(&work);
    assert_eq!(q.retry_all_now(), 128);

    q.deliver(128, &mut work);
    assert_eq!(work.delivered_messages(), 128);
    q.ack_work(&work);

    let (_, inflight, acked, retry) = count_states(&q);
    assert_eq!(acked, 128);
    assert_eq!(inflight, 0);
    assert_eq!(retry, 0);
    q.validate_invariants().unwrap();
}

#[test]
fn nack_cross_block_range() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();
    q.deliver(300, &mut work); // spans 2 blocks
    q.nack_work_to_retry(&work);

    let (_, inflight, _, retry) = count_states(&q);
    assert_eq!(retry, 300);
    assert_eq!(inflight, 0);
    q.validate_invariants().unwrap();
}

#[test]
fn deliver_sparse_after_retry() {
    // Use exactly 256 messages so sequential is fully exhausted before testing sparse
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();

    // Deliver and nack first 128
    q.deliver(128, &mut work);
    q.nack_work_to_retry(&work);

    // Deliver the remaining 128 sequential messages and ack them
    q.deliver(128, &mut work);
    q.ack_work(&work);

    // Sequential is now exhausted. Move retry to sparse.
    let moved = q.retry_all_now();
    assert_eq!(moved, 128);
    assert_eq!(q.sequential_ready_len(), 0);

    // Next delivery must come from sparse (masks), not sequential
    let n = q.deliver(128, &mut work);
    assert_eq!(n, 128);
    assert_eq!(work.ranges.len(), 0, "should deliver via masks, not ranges");
    assert!(!work.masks.is_empty());

    q.ack_work(&work);
    q.validate_invariants().unwrap();
}

#[test]
fn apply_nack_batch_to_retry() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);

    let mut batch = NackBatch::new(NackReason::Requeue);
    batch.ranges.push(super::work::NackRange::new(0, 64));
    q.apply_nack_batch(&batch);

    let (_, inflight, _, retry) = count_states(&q);
    assert_eq!(retry, 64);
    assert_eq!(inflight, 64);
    q.validate_invariants().unwrap();
}

// ---- edge cases at block boundaries ----

#[test]
fn seq_at_block_boundary() {
    let mut q = HybridRangeBlockQueue::with_messages(512);
    let mut work = DeliveryWork::default();

    // Deliver exactly to the block boundary (256 messages = block 0)
    q.deliver(256, &mut work);
    q.ack_work(&work);

    assert_eq!(q.debug_state_of(255), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(256), Some(MessageState::Ready));
    q.validate_invariants().unwrap();
}

#[test]
fn seq_63_64_65_boundary() {
    let mut q = HybridRangeBlockQueue::with_messages(128);
    let mut work = DeliveryWork::default();

    q.deliver(128, &mut work);
    q.ack_id(63);
    q.ack_id(64);
    q.ack_id(65);

    assert_eq!(q.debug_state_of(63), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(64), Some(MessageState::Acked));
    assert_eq!(q.debug_state_of(65), Some(MessageState::Acked));
    q.validate_invariants().unwrap();
}

#[test]
fn ack_range_crossing_word_boundary() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);

    // Ack range crossing word boundary (bits 60-67, spanning words 0 and 1)
    q.ack_range(60, 10);
    for seq in 60..70 {
        assert_eq!(q.debug_state_of(seq), Some(MessageState::Acked), "seq={seq}");
    }
    q.validate_invariants().unwrap();
}

#[test]
fn nack_range_crossing_word_boundary() {
    let mut q = HybridRangeBlockQueue::with_messages(256);
    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    q.nack_range_to_retry(60, 10);

    for seq in 60..70 {
        assert_eq!(q.debug_state_of(seq), Some(MessageState::Retry), "seq={seq}");
    }
    q.validate_invariants().unwrap();
}

// ---- stats tests ----

#[test]
fn stats_tracking() {
    let mut q = HybridRangeBlockQueue::empty();
    q.publish_contiguous(256);
    assert_eq!(q.stats().total_published, 256);

    let mut work = DeliveryWork::default();
    q.deliver(128, &mut work);
    assert_eq!(q.stats().total_delivered, 128);

    q.ack_work(&work);
    assert_eq!(q.stats().total_acked, 128);
}

// ---- invariant validation tests ----

#[test]
fn validate_invariants_clean_state() {
    let mut q = HybridRangeBlockQueue::with_messages(1024);
    q.validate_invariants().unwrap();

    let mut work = DeliveryWork::default();
    q.deliver(256, &mut work);
    q.validate_invariants().unwrap();

    q.ack_work(&work);
    q.validate_invariants().unwrap();
}

#[test]
fn debug_state_of_out_of_range_returns_none() {
    let q = HybridRangeBlockQueue::with_messages(64);
    assert_eq!(q.debug_state_of(64), None);
    assert_eq!(q.debug_state_of(u64::MAX), None);
}

#[test]
fn debug_state_initial_is_ready() {
    let q = HybridRangeBlockQueue::with_messages(256);
    for seq in 0..256 {
        assert_eq!(q.debug_state_of(seq), Some(MessageState::Ready), "seq={seq}");
    }
}

// ---- differential tests ----

#[test]
fn differential_sequential_publish_deliver_ack() {
    let total = 1024u64;
    let batch = 128u32;

    let mut model = ModelQueue::new();
    let mut hybrid = HybridRangeBlockQueue::empty();
    let mut work = DeliveryWork::default();

    model.publish(total);
    hybrid.publish_contiguous(total as u32);

    loop {
        let model_delivered = model.deliver(batch);
        let n = hybrid.deliver(batch, &mut work);

        assert_eq!(model_delivered.len() as u32, n, "deliver count mismatch");
        if n == 0 {
            break;
        }

        // Ack all in model
        for &seq in &model_delivered {
            model.ack_id(seq);
        }
        hybrid.ack_work(&work);

        assert_counts_match(&model, &hybrid);
        hybrid.validate_invariants().unwrap();
    }

    assert_eq!(model.acked_count(), total);
    assert_eq!(hybrid.stats().total_acked, total);
}

#[test]
fn differential_nack_retry() {
    let total = 512u64;
    let mut model = ModelQueue::new();
    let mut hybrid = HybridRangeBlockQueue::empty();
    let mut work = DeliveryWork::default();

    model.publish(total);
    hybrid.publish_contiguous(total as u32);

    // Deliver 128, nack all
    let model_delivered = model.deliver(128);
    hybrid.deliver(128, &mut work);
    assert_eq!(model_delivered.len(), 128);

    for &seq in &model_delivered {
        model.nack_id_to_retry(seq);
    }
    hybrid.nack_work_to_retry(&work);

    assert_counts_match(&model, &hybrid);
    hybrid.validate_invariants().unwrap();

    // Retry
    let m_retry = model.retry_all_now();
    let h_retry = hybrid.retry_all_now();
    assert_eq!(m_retry, h_retry, "retry_all_now count mismatch");

    // Deliver and ack all
    loop {
        let model_d = model.deliver(128);
        let n = hybrid.deliver(128, &mut work);
        assert_eq!(model_d.len() as u32, n);
        if n == 0 {
            break;
        }
        for &seq in &model_d {
            model.ack_id(seq);
        }
        hybrid.ack_work(&work);
    }

    assert_eq!(model.acked_count(), total);
    assert_eq!(hybrid.stats().total_acked, total);
    hybrid.validate_invariants().unwrap();
}

#[test]
fn differential_random_ops_10k() {
    let mut model = ModelQueue::new();
    let mut hybrid = HybridRangeBlockQueue::empty();
    let mut work = DeliveryWork::default();
    let mut rng = XorShift64::new(0xA1B2_C3D4_E5F6_7890);
    let mut last_delivered_model: Vec<u64> = Vec::new();

    for _ in 0..10_000 {
        let op = rng.next_u64() % 6;
        match op {
            0 => {
                // Publish a small batch
                let count = (rng.next_u64() % 64 + 1) as u32;
                model.publish(u64::from(count));
                hybrid.publish_contiguous(count);
            }
            1 => {
                // Deliver
                let max = (rng.next_u64() % 64 + 1) as u32;
                let model_d = model.deliver(max);
                let n = hybrid.deliver(max, &mut work);
                assert_eq!(model_d.len() as u32, n, "deliver count mismatch");
                last_delivered_model = model_d;
            }
            2 if !last_delivered_model.is_empty() => {
                // Ack all from last delivery
                for &seq in &last_delivered_model {
                    model.ack_id(seq);
                }
                hybrid.ack_work(&work);
                last_delivered_model.clear();
                work.clear();
            }
            3 if !last_delivered_model.is_empty() => {
                // Nack all from last delivery to retry
                for &seq in &last_delivered_model {
                    model.nack_id_to_retry(seq);
                }
                hybrid.nack_work_to_retry(&work);
                last_delivered_model.clear();
                work.clear();
            }
            4 => {
                // Retry all
                let m = model.retry_all_now();
                let h = hybrid.retry_all_now();
                assert_eq!(m, h, "retry_all_now mismatch");
            }
            _ => {}
        }

        assert_counts_match(&model, &hybrid);
        hybrid.validate_invariants().unwrap();
    }
}

fn assert_counts_match(model: &ModelQueue, hybrid: &HybridRangeBlockQueue) {
    let mc = model.counts();
    let hc = hybrid.debug_counts();
    assert_eq!(mc.published, hc.published, "published mismatch");
    assert_eq!(mc.ready, hc.ready, "ready mismatch");
    assert_eq!(mc.inflight, hc.inflight, "inflight mismatch");
    assert_eq!(mc.acked, hc.acked, "acked mismatch");
    assert_eq!(mc.retry, hc.retry, "retry mismatch");
}

// ---- locate helper tests ----

#[test]
fn locate_boundaries() {
    assert_eq!(locate(0), (0, 0));
    assert_eq!(locate(255), (0, 255));
    assert_eq!(locate(256), (1, 0));
    assert_eq!(locate(257), (1, 1));
    assert_eq!(locate(511), (1, 255));
    assert_eq!(locate(512), (2, 0));
}
