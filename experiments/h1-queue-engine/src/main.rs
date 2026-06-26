use std::collections::VecDeque;
use std::time::Instant;

use aurum_core::HybridRangeBlockQueue;
use aurum_types::DeliveryWork;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    DeliverAck,
    RandomAck,
    NackRetryAck,
    AckMultiple,
    WindowedRandomAck,
    MixedInterleaved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Per,
    Hybrid,
    Both,
}

#[derive(Debug, Clone)]
struct Args {
    messages: u64,
    batch: u32,
    workload: Workload,
    variant: Variant,
}

impl Default for Args {
    fn default() -> Self {
        Self { messages: 4_194_304, batch: 128, workload: Workload::DeliverAck, variant: Variant::Both }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Ready,
    Inflight,
    Acked,
    Retry,
}

struct PerMessageQueue {
    states: Vec<State>,
    ready: VecDeque<u32>,
    retry: VecDeque<u32>,
}

impl PerMessageQueue {
    fn new(messages: u64) -> Self {
        let mut ready = VecDeque::with_capacity(messages as usize);
        for id in 0..messages as u32 {
            ready.push_back(id);
        }
        Self { states: vec![State::Ready; messages as usize], ready, retry: VecDeque::new() }
    }

    fn deliver(&mut self, batch: u32, out: &mut Vec<u32>) -> u32 {
        out.clear();
        let mut n = 0;
        while n < batch {
            let Some(id) = self.ready.pop_front() else { break };
            self.states[id as usize] = State::Inflight;
            out.push(id);
            n += 1;
        }
        n
    }

    fn ack_ids(&mut self, ids: &[u32]) {
        for &id in ids {
            self.states[id as usize] = State::Acked;
        }
    }

    fn ack_range(&mut self, start: u32, len: u32) {
        for id in start..start + len {
            self.states[id as usize] = State::Acked;
        }
    }

    fn ack_id(&mut self, id: u32) {
        self.states[id as usize] = State::Acked;
    }

    fn nack_to_retry(&mut self, ids: &[u32]) {
        for &id in ids {
            self.states[id as usize] = State::Retry;
            self.retry.push_back(id);
        }
    }

    fn retry_all_now(&mut self) -> u32 {
        let mut n = 0;
        while let Some(id) = self.retry.pop_front() {
            self.states[id as usize] = State::Ready;
            self.ready.push_back(id);
            n += 1;
        }
        n
    }
}

#[derive(Clone, Copy)]
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

fn shuffle(ids: &mut [u32]) {
    let mut rng = XorShift64::new(0xA11C_E5EE_D15E_A5E5);
    for i in (1..ids.len()).rev() {
        let j = (rng.next_u64() as usize) % (i + 1);
        ids.swap(i, j);
    }
}

fn bench_per(args: &Args) -> (f64, u64) {
    let start = Instant::now();
    let checksum = match args.workload {
        Workload::DeliverAck => {
            let mut q = PerMessageQueue::new(args.messages);
            let mut batch = Vec::with_capacity(args.batch as usize);
            let mut checksum = 0u64;
            loop {
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 { break; }
                checksum ^= u64::from(n);
                q.ack_ids(&batch);
            }
            checksum
        }
        Workload::RandomAck => {
            let mut q = PerMessageQueue::new(args.messages);
            let mut delivered = Vec::with_capacity(args.messages as usize);
            let mut batch = Vec::with_capacity(args.batch as usize);
            loop {
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 { break; }
                delivered.extend_from_slice(&batch);
            }
            shuffle(&mut delivered);
            let mut checksum = 0u64;
            for id in delivered {
                checksum = checksum.wrapping_add(u64::from(id & 63));
                q.ack_id(id);
            }
            checksum
        }
        Workload::NackRetryAck => {
            let mut q = PerMessageQueue::new(args.messages);
            let mut batch = Vec::with_capacity(args.batch as usize);
            let mut checksum = 0u64;
            loop {
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 { break; }
                if (checksum & 3) == 0 {
                    q.nack_to_retry(&batch);
                } else {
                    q.ack_ids(&batch);
                }
                checksum = checksum.wrapping_add(u64::from(n));
            }
            let moved = q.retry_all_now();
            checksum = checksum.wrapping_add(u64::from(moved));
            loop {
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 { break; }
                q.ack_ids(&batch);
                checksum = checksum.wrapping_add(u64::from(n));
            }
            checksum
        }
        Workload::AckMultiple => {
            // Simulates basic.ack(multiple=true): ack whole ranges at once
            let mut q = PerMessageQueue::new(args.messages);
            let mut batch = Vec::with_capacity(args.batch as usize);
            let mut checksum = 0u64;
            let mut ack_start = 0u32;
            loop {
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 { break; }
                checksum ^= u64::from(n);
                q.ack_range(ack_start, n);
                ack_start += n;
            }
            checksum
        }
        Workload::WindowedRandomAck => {
            // Prefetch window of args.batch * 8; acks arrive randomly within window
            let prefetch = args.batch * 8;
            let mut q = PerMessageQueue::new(args.messages);
            let mut window: Vec<u32> = Vec::with_capacity(prefetch as usize);
            let mut batch = Vec::with_capacity(args.batch as usize);
            let mut rng = XorShift64::new(0xC0FFEE_CAFE_BABE);
            let mut checksum = 0u64;
            loop {
                // Fill window
                while window.len() < prefetch as usize {
                    let n = q.deliver(args.batch, &mut batch);
                    if n == 0 { break; }
                    window.extend_from_slice(&batch);
                }
                if window.is_empty() { break; }
                // Ack a random batch from within the window
                let ack_n = (args.batch as usize).min(window.len());
                // Shuffle first ack_n with Fisher-Yates
                for i in (1..ack_n).rev() {
                    let j = (rng.next_u64() as usize) % (i + 1);
                    window.swap(i, j);
                }
                for &id in &window[..ack_n] {
                    checksum = checksum.wrapping_add(u64::from(id & 63));
                    q.ack_id(id);
                }
                window.drain(..ack_n);
            }
            checksum
        }
        Workload::MixedInterleaved => {
            // Simulates real broker: publish/deliver/ack/nack interleaved
            let chunk = (args.messages / 10).max(1) as u32;
            let mut q = PerMessageQueue::new(0);
            let mut batch = Vec::with_capacity(args.batch as usize);
            let mut checksum = 0u64;
            let mut published = 0u64;
            let mut iter = 0u32;
            loop {
                // Publish chunk
                if published < args.messages {
                    let n = chunk.min((args.messages - published) as u32);
                    let start = published as u32;
                    for id in start..start + n {
                        q.states.push(State::Ready);
                        q.ready.push_back(id);
                    }
                    published += u64::from(n);
                }
                // Deliver batch
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 && published >= args.messages { break; }
                // Ack 75%, nack 25%
                let ack_n = (n * 3 / 4) as usize;
                q.ack_ids(&batch[..ack_n]);
                q.nack_to_retry(&batch[ack_n..]);
                checksum = checksum.wrapping_add(u64::from(n));
                // Retry every 10 iterations
                iter += 1;
                if iter % 10 == 0 {
                    let moved = q.retry_all_now();
                    checksum = checksum.wrapping_add(u64::from(moved));
                }
            }
            // Drain remaining
            q.retry_all_now();
            loop {
                let n = q.deliver(args.batch, &mut batch);
                if n == 0 { break; }
                q.ack_ids(&batch);
                checksum = checksum.wrapping_add(u64::from(n));
            }
            checksum
        }
    };
    let elapsed = start.elapsed().as_secs_f64();
    (elapsed * 1e9 / args.messages as f64, checksum)
}

fn bench_hybrid(args: &Args) -> (f64, u64) {
    let start = Instant::now();
    let checksum = match args.workload {
        Workload::DeliverAck => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut work = DeliveryWork::default();
            let mut checksum = 0u64;
            loop {
                let n = q.deliver(args.batch, &mut work);
                if n == 0 { break; }
                checksum ^= u64::from(n);
                q.ack_work(&work);
            }
            checksum
        }
        Workload::RandomAck => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut work = DeliveryWork::default();
            let mut delivered = Vec::with_capacity(args.messages as usize);
            loop {
                let n = q.deliver(args.batch, &mut work);
                if n == 0 { break; }
                for r in &work.ranges {
                    for id in r.start_seq..r.start_seq + u64::from(r.len) {
                        delivered.push(id as u32);
                    }
                }
                for m in &work.masks {
                    let base = u64::from(m.block) * 256 + u64::from(m.word) * 64;
                    let mut mask = m.mask;
                    while mask != 0 {
                        let bit = mask.trailing_zeros();
                        delivered.push((base + u64::from(bit)) as u32);
                        mask &= mask - 1;
                    }
                }
            }
            shuffle(&mut delivered);
            let mut checksum = 0u64;
            for id in delivered {
                checksum = checksum.wrapping_add(u64::from(id & 63));
                q.ack_id(u64::from(id));
            }
            checksum
        }
        Workload::NackRetryAck => {
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut work = DeliveryWork::default();
            let mut checksum = 0u64;
            loop {
                let n = q.deliver(args.batch, &mut work);
                if n == 0 { break; }
                if (checksum & 3) == 0 {
                    q.nack_work_to_retry(&work);
                } else {
                    q.ack_work(&work);
                }
                checksum = checksum.wrapping_add(u64::from(n));
            }
            let moved = q.retry_all_now();
            checksum = checksum.wrapping_add(u64::from(moved));
            loop {
                let n = q.deliver(args.batch, &mut work);
                if n == 0 { break; }
                q.ack_work(&work);
                checksum = checksum.wrapping_add(u64::from(n));
            }
            checksum
        }
        Workload::AckMultiple => {
            // Uses ack_range: ack whole range at once (basic.ack multiple=true)
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut work = DeliveryWork::default();
            let mut checksum = 0u64;
            loop {
                let n = q.deliver(args.batch, &mut work);
                if n == 0 { break; }
                checksum ^= u64::from(n);
                // Ack using range (one call instead of N ack_id calls)
                for r in &work.ranges {
                    q.ack_range(r.start_seq, r.len);
                }
                for m in &work.masks {
                    q.ack_mask(*m);
                }
            }
            checksum
        }
        Workload::WindowedRandomAck => {
            let prefetch = args.batch * 8;
            let mut q = HybridRangeBlockQueue::with_messages(args.messages);
            let mut work = DeliveryWork::default();
            let mut window: Vec<u64> = Vec::with_capacity(prefetch as usize);
            let mut rng = XorShift64::new(0xC0FFEE_CAFE_BABE);
            let mut checksum = 0u64;
            loop {
                while window.len() < prefetch as usize {
                    let n = q.deliver(args.batch, &mut work);
                    if n == 0 { break; }
                    for r in &work.ranges {
                        for id in r.start_seq..r.start_seq + u64::from(r.len) {
                            window.push(id);
                        }
                    }
                    for m in &work.masks {
                        let base = u64::from(m.block) * 256 + u64::from(m.word) * 64;
                        let mut mask = m.mask;
                        while mask != 0 {
                            let bit = mask.trailing_zeros();
                            window.push(base + u64::from(bit));
                            mask &= mask - 1;
                        }
                    }
                }
                if window.is_empty() { break; }
                let ack_n = (args.batch as usize).min(window.len());
                for i in (1..ack_n).rev() {
                    let j = (rng.next_u64() as usize) % (i + 1);
                    window.swap(i, j);
                }
                for &id in &window[..ack_n] {
                    checksum = checksum.wrapping_add(id & 63);
                    q.ack_id(id);
                }
                window.drain(..ack_n);
            }
            checksum
        }
        Workload::MixedInterleaved => {
            let chunk = (args.messages / 10).max(1) as u32;
            let mut q = HybridRangeBlockQueue::empty();
            let mut work = DeliveryWork::default();
            let mut checksum = 0u64;
            let mut published = 0u64;
            let mut iter = 0u32;
            loop {
                if published < args.messages {
                    let n = chunk.min((args.messages - published) as u32);
                    q.publish_contiguous(n);
                    published += u64::from(n);
                }
                let n = q.deliver(args.batch, &mut work);
                if n == 0 && published >= args.messages { break; }
                // Ack 75% (front of work), nack 25% (back of work) using ranges
                let ack_ranges = work.ranges.len() * 3 / 4;
                let ack_masks = work.masks.len() * 3 / 4;
                for r in &work.ranges[..ack_ranges] {
                    q.ack_range(r.start_seq, r.len);
                }
                for m in &work.masks[..ack_masks] {
                    q.ack_mask(*m);
                }
                for r in &work.ranges[ack_ranges..] {
                    q.nack_range_to_retry(r.start_seq, r.len);
                }
                for m in &work.masks[ack_masks..] {
                    q.nack_mask_to_retry(*m);
                }
                checksum = checksum.wrapping_add(u64::from(n));
                iter += 1;
                if iter % 10 == 0 {
                    let moved = q.retry_all_now();
                    checksum = checksum.wrapping_add(u64::from(moved));
                }
            }
            q.retry_all_now();
            loop {
                let n = q.deliver(args.batch, &mut work);
                if n == 0 { break; }
                q.ack_work(&work);
                checksum = checksum.wrapping_add(u64::from(n));
            }
            checksum
        }
    };
    let elapsed = start.elapsed().as_secs_f64();
    (elapsed * 1e9 / args.messages as f64, checksum)
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let (key, inline_value) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg, None),
        };
        let mut value = || inline_value.clone().unwrap_or_else(|| it.next().expect("missing argument value"));
        match key.as_str() {
            "--messages" => args.messages = value().parse().expect("messages u64"),
            "--batch" => args.batch = value().parse().expect("batch u32"),
            "--workload" => {
                args.workload = match value().as_str() {
                    "deliver_ack" => Workload::DeliverAck,
                    "random_ack" => Workload::RandomAck,
                    "nack_retry_ack" => Workload::NackRetryAck,
                    "ack_multiple" => Workload::AckMultiple,
                    "windowed_random_ack" => Workload::WindowedRandomAck,
                    "mixed_interleaved" => Workload::MixedInterleaved,
                    other => panic!("unknown workload: {other}"),
                };
            }
            "--variant" => {
                args.variant = match value().as_str() {
                    "per" => Variant::Per,
                    "hybrid" | "block" | "mask" => Variant::Hybrid,
                    "both" => Variant::Both,
                    other => panic!("unknown variant: {other}"),
                };
            }
            "--help" | "-h" => {
                println!("Usage: h1-queue-engine --messages N --batch N \
                    --workload deliver_ack|random_ack|nack_retry_ack|ack_multiple|windowed_random_ack|mixed_interleaved \
                    --variant per|hybrid|both");
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
        "AurumMQ H1 workspace bench: messages={} batch={} workload={:?} variant={:?}",
        args.messages, args.batch, args.workload, args.variant
    );
    if matches!(args.variant, Variant::Per | Variant::Both) {
        let (ns, checksum) = bench_per(&args);
        println!("per_message_vecdeque     ns_per_msg={ns:.3} checksum={checksum}");
    }
    if matches!(args.variant, Variant::Hybrid | Variant::Both) {
        let (ns, checksum) = bench_hybrid(&args);
        println!("hybrid_range_block      ns_per_msg={ns:.3} checksum={checksum}");
    }
}
