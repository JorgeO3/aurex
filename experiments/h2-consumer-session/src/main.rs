use std::collections::VecDeque;
use std::time::Instant;

use aurum_core::{
    AckRequest, CancelDisposition, ChannelId, ConsumerId, ConsumerSession, DeliveryTag,
    HybridRangeBlockQueue, NackRequest, NackReason, PrefetchMode, SessionDeliveryBatch,
};
use aurum_types::DeliveryWork;

// ── Workloads & CLI ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum Workload {
    DeliverAckOne,
    DeliverAckMultiple,
    DeliverNackRequeue,
    RandomAckPrefetch128,
    AckOneRandomPrefetch1024,
    MaskRedeliveryAckOne,
    CancelRequeue,
}

#[derive(Debug, Clone, Copy)]
enum Variant {
    PerMessage,
    Session,
    Both,
}

struct Args {
    messages: u64,
    batch: u32,
    workload: Workload,
    variant: Variant,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            messages: 1_048_576,
            batch: 128,
            workload: Workload::DeliverAckMultiple,
            variant: Variant::Both,
        }
    }
}

// ── Baseline: per-message session ────────────────────────────────────────────
//
// Tracks inflight messages in a VecDeque<u64> (one seq per slot). Tags are
// implicit: `base_tag + index`. This is what a naive broker session layer
// looks like without compact range/mask tracking.

struct PerMessageSession {
    window: VecDeque<Option<u64>>,
    base_tag: u64,
}

impl PerMessageSession {
    fn new() -> Self {
        Self { window: VecDeque::new(), base_tag: 0 }
    }

    fn deliver(&mut self, q: &mut HybridRangeBlockQueue, max: u32, work: &mut DeliveryWork) -> u32 {
        work.clear();
        let n = q.deliver(max, work);
        for r in &work.ranges {
            for i in 0..r.len {
                self.window.push_back(Some(r.start_seq + i as u64));
            }
        }
        for m in &work.masks {
            let base = u64::from(m.block) * 256 + u64::from(m.word) * 64;
            let mut mask = m.mask;
            while mask != 0 {
                let bit = mask.trailing_zeros();
                self.window.push_back(Some(base + u64::from(bit)));
                mask &= mask - 1;
            }
        }
        n
    }

    fn ack_one(&mut self, tag: u64, q: &mut HybridRangeBlockQueue) {
        let idx = (tag - self.base_tag) as usize;
        if let Some(slot) = self.window.get_mut(idx) {
            if let Some(seq) = slot.take() {
                q.ack_id(seq);
            }
        }
        self.drain_front();
    }

    fn ack_multiple(&mut self, tag: u64, q: &mut HybridRangeBlockQueue) {
        let n = (tag - self.base_tag + 1) as usize;
        for slot in self.window.drain(..n.min(self.window.len())) {
            if let Some(seq) = slot {
                q.ack_id(seq);
            }
        }
        self.base_tag += n as u64;
    }

    fn nack_multiple_requeue(&mut self, tag: u64, q: &mut HybridRangeBlockQueue) {
        let n = (tag - self.base_tag + 1) as usize;
        for slot in self.window.drain(..n.min(self.window.len())) {
            if let Some(seq) = slot {
                q.nack_range_to_retry(seq, 1);
            }
        }
        self.base_tag += n as u64;
    }

    fn cancel_requeue(&mut self, q: &mut HybridRangeBlockQueue) -> u32 {
        let mut count = 0u32;
        for slot in self.window.drain(..) {
            if let Some(seq) = slot {
                q.nack_range_to_retry(seq, 1);
                count += 1;
            }
        }
        self.base_tag += count as u64;
        count
    }

    fn last_tag(&self) -> u64 {
        self.base_tag + self.window.len() as u64 - 1
    }

    fn first_tag(&self) -> u64 {
        self.base_tag
    }

    fn tag_at(&self, n: usize) -> u64 {
        self.base_tag + n as u64
    }

    fn len(&self) -> usize {
        self.window.iter().filter(|s| s.is_some()).count()
    }

    fn drain_front(&mut self) {
        while self.window.front() == Some(&None) {
            self.window.pop_front();
            self.base_tag += 1;
        }
    }
}

// ── XorShift RNG ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn consumer() -> ConsumerSession {
    ConsumerSession::new(ConsumerId(0), ChannelId(0), PrefetchMode::Unlimited)
}

fn consumer_limited(n: u32) -> ConsumerSession {
    ConsumerSession::new(ConsumerId(0), ChannelId(0), PrefetchMode::Limited(n))
}

fn out_tags(out: &SessionDeliveryBatch) -> Vec<DeliveryTag> {
    let mut tags = Vec::new();
    for seg in &out.segments {
        let (ft, cnt) = match seg {
            aurum_core::TaggedDeliverySegment::Range(r) => (r.first_tag, r.range.len as usize),
            aurum_core::TaggedDeliverySegment::Mask(m) => (m.first_tag, m.count as usize),
        };
        for i in 0..cnt {
            tags.push(DeliveryTag(ft.0 + i as u64));
        }
    }
    tags
}

fn last_tag(out: &SessionDeliveryBatch) -> DeliveryTag {
    let mut max = DeliveryTag(0);
    for seg in &out.segments {
        let (ft, cnt) = match seg {
            aurum_core::TaggedDeliverySegment::Range(r) => (r.first_tag, r.range.len as usize),
            aurum_core::TaggedDeliverySegment::Mask(m) => (m.first_tag, m.count as usize),
        };
        if cnt > 0 {
            let last = DeliveryTag(ft.0 + cnt as u64 - 1);
            if last.0 >= max.0 {
                max = last;
            }
        }
    }
    max
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_per_message(args: &Args) -> (f64, u64) {
    let mut work = DeliveryWork::default();
    let start = Instant::now();
    let checksum = match args.workload {
        Workload::DeliverAckOne => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let first = sess.first_tag();
                for i in 0..n as u64 {
                    sess.ack_one(first + i, &mut q);
                }
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::DeliverAckMultiple => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let last = sess.last_tag();
                sess.ack_multiple(last, &mut q);
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::DeliverNackRequeue => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let last = sess.last_tag();
                sess.nack_multiple_requeue(last, &mut q);
                cs = cs.wrapping_add(u64::from(n));
            }
            q.retry_all_now();
            loop {
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let last = sess.last_tag();
                sess.ack_multiple(last, &mut q);
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::RandomAckPrefetch128 => {
            let prefetch = 128usize;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut rng = Rng::new(0xDEAD_BEEF_C0FFE);
            let mut cs = 0u64;
            loop {
                while sess.len() < prefetch {
                    let n = sess.deliver(&mut q, args.batch, &mut work);
                    if n == 0 { break; }
                }
                if sess.len() == 0 { break; }
                let count = prefetch.min(sess.len());
                let tag = sess.tag_at((rng.next() as usize) % count);
                sess.ack_one(tag, &mut q);
                cs = cs.wrapping_add(1);
            }
            cs
        }
        Workload::AckOneRandomPrefetch1024 => {
            let prefetch = 1024usize;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut rng = Rng::new(0xC0FFEE_DEAD_BEEF);
            let mut cs = 0u64;
            loop {
                while sess.len() < prefetch {
                    let n = sess.deliver(&mut q, args.batch, &mut work);
                    if n == 0 { break; }
                }
                if sess.len() == 0 { break; }
                let count = prefetch.min(sess.len());
                let tag = sess.tag_at((rng.next() as usize) % count);
                sess.ack_one(tag, &mut q);
                cs = cs.wrapping_add(1);
            }
            cs
        }
        Workload::MaskRedeliveryAckOne => {
            // deliver → nack all → retry → redeliver (now masks) → ack_one per tag
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let last = sess.last_tag();
                sess.nack_multiple_requeue(last, &mut q);
                cs = cs.wrapping_add(u64::from(n));
            }
            q.retry_all_now();
            // Redeliver (now from retry pool) and ack individually
            loop {
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let first = sess.first_tag();
                for i in 0..n as u64 {
                    sess.ack_one(first + i, &mut q);
                }
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::CancelRequeue => {
            let prefetch = args.batch;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = PerMessageSession::new();
            let mut cs = 0u64;
            loop {
                while (sess.len() as u32) < prefetch {
                    let n = sess.deliver(&mut q, args.batch, &mut work);
                    if n == 0 { break; }
                }
                if sess.len() == 0 { break; }
                sess.cancel_requeue(&mut q);
                q.retry_all_now();
                let n = sess.deliver(&mut q, args.batch, &mut work);
                if n == 0 { break; }
                let last = sess.last_tag();
                sess.ack_multiple(last, &mut q);
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
    };
    let elapsed = start.elapsed().as_secs_f64();
    (elapsed * 1e9 / args.messages as f64, checksum)
}

fn bench_session(args: &Args) -> (f64, u64) {
    let mut out = SessionDeliveryBatch::default();
    let start = Instant::now();
    let checksum = match args.workload {
        Workload::DeliverAckOne => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = consumer();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let tags = out_tags(&out);
                for tag in tags {
                    sess.ack(AckRequest::one(tag), &mut q).unwrap();
                }
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::DeliverAckMultiple => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = consumer();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let last = last_tag(&out);
                sess.ack(AckRequest::multiple(last), &mut q).unwrap();
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::DeliverNackRequeue => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = consumer();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let last = last_tag(&out);
                sess.nack(NackRequest::multiple(last, NackReason::Requeue), &mut q).unwrap();
                cs = cs.wrapping_add(u64::from(n));
            }
            q.retry_all_now();
            loop {
                let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let last = last_tag(&out);
                sess.ack(AckRequest::multiple(last), &mut q).unwrap();
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::RandomAckPrefetch128 => {
            let prefetch = 128u32;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = consumer_limited(prefetch);
            let mut rng = Rng::new(0xDEAD_BEEF_C0FFE);
            let mut inflight: VecDeque<DeliveryTag> = VecDeque::new();
            let mut cs = 0u64;
            loop {
                while (inflight.len() as u32) < prefetch {
                    let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                    if n == 0 { break; }
                    for &t in out_tags(&out).iter() {
                        inflight.push_back(t);
                    }
                }
                if inflight.is_empty() { break; }
                let idx = (rng.next() as usize) % inflight.len();
                let tag = inflight.remove(idx).unwrap();
                if sess.ack(AckRequest::one(tag), &mut q).is_ok() {
                    cs = cs.wrapping_add(1);
                }
            }
            cs
        }
        Workload::AckOneRandomPrefetch1024 => {
            let prefetch = 1024u32;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = consumer_limited(prefetch);
            let mut rng = Rng::new(0xC0FFEE_DEAD_BEEF);
            let mut inflight: VecDeque<DeliveryTag> = VecDeque::new();
            let mut cs = 0u64;
            loop {
                while (inflight.len() as u32) < prefetch {
                    let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                    if n == 0 { break; }
                    for &t in out_tags(&out).iter() {
                        inflight.push_back(t);
                    }
                }
                if inflight.is_empty() { break; }
                let idx = (rng.next() as usize) % inflight.len();
                let tag = inflight.remove(idx).unwrap();
                if sess.ack(AckRequest::one(tag), &mut q).is_ok() {
                    cs = cs.wrapping_add(1);
                }
            }
            cs
        }
        Workload::MaskRedeliveryAckOne => {
            // deliver → nack all → retry → redeliver (masks) → ack_one per tag
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut sess = consumer();
            let mut cs = 0u64;
            loop {
                let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let last = last_tag(&out);
                sess.nack(NackRequest::multiple(last, NackReason::Requeue), &mut q).unwrap();
                cs = cs.wrapping_add(u64::from(n));
            }
            q.retry_all_now();
            // Redeliver (now MaskSegments) and ack individually
            loop {
                let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let tags = out_tags(&out);
                for tag in tags {
                    sess.ack(AckRequest::one(tag), &mut q).unwrap();
                }
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
        Workload::CancelRequeue => {
            let prefetch = args.batch;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut cs = 0u64;
            loop {
                let mut sess = consumer_limited(prefetch);
                while sess.unacked_count() < prefetch {
                    let n = sess.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                    if n == 0 { break; }
                }
                if sess.unacked_count() == 0 { break; }
                sess.cancel(CancelDisposition::RequeueUnacked, &mut q);
                q.retry_all_now();
                let mut sess2 = consumer_limited(prefetch);
                let n = sess2.deliver_from_queue(&mut q, args.batch, &mut out).unwrap();
                if n == 0 { break; }
                let last = last_tag(&out);
                sess2.ack(AckRequest::multiple(last), &mut q).unwrap();
                cs = cs.wrapping_add(u64::from(n));
            }
            cs
        }
    };
    let elapsed = start.elapsed().as_secs_f64();
    (elapsed * 1e9 / args.messages as f64, checksum)
}

// ── CLI ───────────────────────────────────────────────────────────────────────

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let (key, inline_val) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg, None),
        };
        let mut val = || {
            inline_val
                .clone()
                .unwrap_or_else(|| it.next().expect("missing argument value"))
        };
        match key.as_str() {
            "--messages" => args.messages = val().parse().expect("messages u64"),
            "--batch" => args.batch = val().parse().expect("batch u32"),
            "--workload" => {
                args.workload = match val().as_str() {
                    "deliver_ack_one" => Workload::DeliverAckOne,
                    "deliver_ack_multiple" => Workload::DeliverAckMultiple,
                    "deliver_nack_requeue" => Workload::DeliverNackRequeue,
                    "random_ack_prefetch_128" => Workload::RandomAckPrefetch128,
                    "ack_one_random_prefetch_1024" => Workload::AckOneRandomPrefetch1024,
                    "mask_redelivery_ack_one" => Workload::MaskRedeliveryAckOne,
                    "cancel_requeue" => Workload::CancelRequeue,
                    other => panic!("unknown workload: {other}"),
                };
            }
            "--variant" => {
                args.variant = match val().as_str() {
                    "per" | "per_message" => Variant::PerMessage,
                    "session" => Variant::Session,
                    "both" => Variant::Both,
                    other => panic!("unknown variant: {other}"),
                };
            }
            "--help" | "-h" => {
                println!(
                    "Usage: h2-consumer-session \
                    --messages N --batch N \
                    --workload deliver_ack_one|deliver_ack_multiple|deliver_nack_requeue|\
                    random_ack_prefetch_128|ack_one_random_prefetch_1024|\
                    mask_redelivery_ack_one|cancel_requeue \
                    --variant per_message|session|both"
                );
                std::process::exit(0);
            }
            other => panic!("unknown arg: {other}"),
        }
    }
    args
}

fn main() {
    let args = parse_args();
    println!(
        "AurumMQ H2 consumer-session bench: messages={} batch={} workload={:?} variant={:?}",
        args.messages, args.batch, args.workload, args.variant
    );
    if matches!(args.variant, Variant::PerMessage | Variant::Both) {
        let (ns, cs) = bench_per_message(&args);
        println!("per_message_session  ns_per_msg={ns:.3}  checksum={cs}");
    }
    if matches!(args.variant, Variant::Session | Variant::Both) {
        let (ns, cs) = bench_session(&args);
        println!("consumer_session     ns_per_msg={ns:.3}  checksum={cs}");
    }
}
