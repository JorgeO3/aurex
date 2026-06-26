use smallvec::SmallVec;

use aurum_core::{
    AckRequest, CancelDisposition, ConsumerError, ConsumerId, ConsumerSession, DeliveryFlags,
    DeliveryTag, HybridRangeBlockQueue, NackReason, NackRequest, PrefetchMode, SessionDeliveryBatch,
    TaggedDeliverySegment,
};
use aurum_internal_protocol::{
    command::{
        consume::{CancelDispositionCommand, ConsumeStart, CreditUpdate},
        control::DeclareQueue,
        publish::ShardPublishBatch,
        settlement::{AckCommand, NackCommand, NackDisposition, RejectCommand, SettlementMode},
        shard::ShardCommandBatch,
    },
    error::SubmitError,
    event::{
        confirm::{ConsumerEventBatch, ConsumerEventKind, PublishConfirmBatch, SettlementResultBatch},
        delivery::{
            DeliveryEventBatch, DeliveryEventSegment, DeliveryMaskSegment, DeliveryRangeSegment,
            PayloadSpan,
        },
        error::{CommandError, CommandErrorBatch, CommandErrorKind},
    },
    flags::DeliveryEventFlags,
    sink::EventSink,
};
use aurum_types::{PayloadHandle, QueueId, ShardId};

use super::flags::ConsumerRuntimeFlags;
use super::output::ShardOutputBatch;
use super::registry::{ConsumerRegistry, ConsumerRegistryError, ConsumerRuntimeState, QueueRegistry};
use super::scheduler::SimpleDeliveryScheduler;
use super::storage::AppendOnlyShardStorage;

/// Single-shard in-memory broker executor. Commands enter, events leave.
#[derive(Debug)]
pub struct InMemoryShardExecutor {
    pub shard_id: ShardId,
    queues: QueueRegistry,
    consumers: ConsumerRegistry,
    scheduler: SimpleDeliveryScheduler,
    durable: Option<AppendOnlyShardStorage>,
}

impl InMemoryShardExecutor {
    #[must_use]
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            queues: QueueRegistry::new(),
            consumers: ConsumerRegistry::new(),
            scheduler: SimpleDeliveryScheduler::default(),
            durable: None,
        }
    }

    #[must_use]
    pub fn with_durable_storage(shard_id: ShardId, storage: AppendOnlyShardStorage) -> Self {
        Self {
            shard_id,
            queues: QueueRegistry::new(),
            consumers: ConsumerRegistry::new(),
            scheduler: SimpleDeliveryScheduler::default(),
            durable: Some(storage),
        }
    }

    pub fn recover_queue_from_storage(&mut self, queue_id: QueueId) -> Result<u32, aurum_storage::StorageError> {
        let Some(store) = &self.durable else {
            return Ok(0);
        };
        let ready = store.recover_ready_count(queue_id)?;
        if ready > 0 {
            if !self.queues.contains(queue_id) {
                let _ = self.queues.create_queue(queue_id);
            }
            if let Some(q) = self.queues.get_mut(queue_id) {
                q.queue.publish_contiguous(ready);
            }
        }
        Ok(ready)
    }

    /// Convenience constructor for single-queue tests (queue 0, shard 0).
    #[must_use]
    pub fn single_queue() -> Self {
        let mut exec = Self::new(ShardId(0));
        exec.queues.create_queue(QueueId(0)).expect("queue 0");
        exec
    }

    #[must_use]
    pub fn queues(&self) -> &QueueRegistry {
        &self.queues
    }

    #[must_use]
    pub fn consumers(&self) -> &ConsumerRegistry {
        &self.consumers
    }

    #[must_use]
    pub fn queue(&self, queue_id: QueueId) -> Option<&HybridRangeBlockQueue> {
        self.queues.get(queue_id).map(|q| &q.queue)
    }

    pub fn queue_mut(&mut self, queue_id: QueueId) -> Option<&mut HybridRangeBlockQueue> {
        self.queues.get_mut(queue_id).map(|q| &mut q.queue)
    }

    /// Execute one shard command batch into a reusable output buffer.
    pub fn execute_batch(
        &mut self,
        batch: ShardCommandBatch<PayloadHandle>,
        out: &mut ShardOutputBatch,
    ) -> Result<(), SubmitError> {
        self.execute(batch, out)
    }

    /// Execute one shard command batch, dispatching events to `sink`.
    #[inline(always)]
    pub fn execute<S: EventSink<PayloadHandle>>(
        &mut self,
        batch: ShardCommandBatch<PayloadHandle>,
        sink: &mut S,
    ) -> Result<(), SubmitError> {
        match batch {
            ShardCommandBatch::Declare(b) => {
                for cmd in b.items {
                    self.exec_declare_queue(cmd, sink);
                }
            }
            ShardCommandBatch::Publish(b) => self.exec_publish(b, sink)?,
            ShardCommandBatch::Consume(b) => {
                for cmd in b.items {
                    self.exec_consume_start(cmd, sink);
                }
            }
            ShardCommandBatch::Credit(b) => {
                for cmd in b.items {
                    self.exec_credit_update(cmd, sink);
                }
            }
            ShardCommandBatch::Ack(b) => self.exec_ack_batch(b, sink),
            ShardCommandBatch::Nack(b) => self.exec_nack_batch(b, sink),
            ShardCommandBatch::Reject(b) => self.exec_reject_batch(b, sink),
            ShardCommandBatch::Cancel(b) => {
                for cmd in b.items {
                    self.exec_cancel(cmd.consumer_id, cmd.disposition, sink);
                }
            }
        }
        Ok(())
    }

    /// Deliver pending messages to all consumers with available credit.
    pub fn deliver_pending<S: EventSink<PayloadHandle>>(&mut self, sink: &mut S) {
        for qid in self.queues.queue_ids() {
            self.drive_queue(qid, sink);
        }
    }

    /// Retry all nacked messages on every queue and re-deliver.
    pub fn retry_all_and_deliver<S: EventSink<PayloadHandle>>(&mut self, sink: &mut S) {
        for qid in self.queues.queue_ids() {
            if let Some(q) = self.queues.get_mut(qid) {
                q.queue.retry_all_now();
            }
        }
        self.deliver_pending(sink);
    }

    // ── Command handlers ──────────────────────────────────────────────────────

    fn exec_declare_queue<S: EventSink<PayloadHandle>>(
        &mut self,
        cmd: DeclareQueue,
        sink: &mut S,
    ) {
        match self.queues.create_queue(cmd.queue_id) {
            Ok(()) => {}
            Err(_) => sink.on_error(CommandErrorBatch::one(CommandError::queue(
                CommandErrorKind::DuplicateQueue,
                cmd.queue_id,
            ))),
        }
    }

    fn exec_publish<S: EventSink<PayloadHandle>>(
        &mut self,
        batch: ShardPublishBatch<PayloadHandle>,
        sink: &mut S,
    ) -> Result<(), SubmitError> {
        let count = batch.count();
        let queue_id = batch.queue_id;

        let Some(queue_state) = self.queues.get_mut(queue_id) else {
            sink.on_error(CommandErrorBatch::one(CommandError::queue(
                CommandErrorKind::QueueNotFound,
                queue_id,
            )));
            return Ok(());
        };

        let base_seq = queue_state.queue.len();

        if let Some(store) = &mut self.durable {
            let payload_bufs: Vec<Vec<u8>> = if batch.records.is_empty() {
                (0..count).map(|_| vec![0u8]).collect()
            } else {
                batch
                    .records
                    .iter()
                    .map(|r| vec![0u8; r.payload_len as usize])
                    .collect()
            };
            let payload_refs: Vec<&[u8]> = payload_bufs.iter().map(|b| b.as_slice()).collect();
            if let Err(e) = store.append_publish(queue_id, aurum_storage::QueueSeq(base_seq), &payload_refs) {
                sink.on_error(CommandErrorBatch::one(CommandError::global(
                    CommandErrorKind::InternalInvariantViolation,
                )));
                let _ = e;
                return Ok(());
            }
        }

        queue_state.queue.publish_contiguous(count);

        if !batch.records.is_empty() {
            for (i, rec) in batch.records.iter().enumerate() {
                queue_state
                    .seq_payloads
                    .insert(base_seq + i as u64, rec.payload);
            }
        }

        if batch.confirm_mode != aurum_internal_protocol::command::publish::ConfirmMode::None {
            sink.on_confirm(PublishConfirmBatch {
                source: batch.source,
                batch_id: batch.batch_id,
                accepted: count,
                first_seq: None,
                mode: batch.confirm_mode,
            });
        }

        self.drive_queue(queue_id, sink);
        Ok(())
    }

    fn exec_consume_start<S: EventSink<PayloadHandle>>(
        &mut self,
        cmd: ConsumeStart,
        sink: &mut S,
    ) {
        if !self.queues.contains(cmd.queue_id) {
            sink.on_error(CommandErrorBatch::one(CommandError::queue(
                CommandErrorKind::QueueNotFound,
                cmd.queue_id,
            )));
            return;
        }

        let prefetch = if cmd.prefetch == 0 {
            PrefetchMode::Unlimited
        } else {
            PrefetchMode::Limited(cmd.prefetch)
        };

        let state = ConsumerRuntimeState {
            queue_id: cmd.queue_id,
            channel_id: cmd.channel_id,
            session: ConsumerSession::new(cmd.consumer_id, cmd.channel_id, prefetch),
            out: SessionDeliveryBatch::default(),
            flags: ConsumerRuntimeFlags::ACTIVE,
        };

        match self.consumers.insert(cmd.consumer_id, state) {
            Ok(()) => {
                if let Err(_) = self.queues.attach_consumer(cmd.queue_id, cmd.consumer_id) {
                    sink.on_error(CommandErrorBatch::one(CommandError::queue(
                        CommandErrorKind::QueueNotFound,
                        cmd.queue_id,
                    )));
                    self.consumers.remove(cmd.consumer_id);
                    return;
                }
                sink.on_consumer(ConsumerEventBatch {
                    consumer_id: cmd.consumer_id,
                    kind: ConsumerEventKind::Started,
                });
                self.drive_queue(cmd.queue_id, sink);
            }
            Err(ConsumerRegistryError::Duplicate) => sink.on_error(CommandErrorBatch::one(
                CommandError::consumer(CommandErrorKind::DuplicateConsumer, cmd.consumer_id),
            )),
            Err(ConsumerRegistryError::NotFound) => {}
        }
    }

    fn exec_credit_update<S: EventSink<PayloadHandle>>(
        &mut self,
        cmd: CreditUpdate,
        sink: &mut S,
    ) {
        let Some(consumer) = self.consumers.get(cmd.consumer_id) else {
            sink.on_error(CommandErrorBatch::one(CommandError::consumer(
                CommandErrorKind::ConsumerNotFound,
                cmd.consumer_id,
            )));
            return;
        };
        let queue_id = consumer.queue_id;
        let _ = cmd;
        self.drive_queue(queue_id, sink);
    }

    fn exec_ack_batch<S: EventSink<PayloadHandle>>(
        &mut self,
        batch: aurum_internal_protocol::command::settlement::AckCommandBatch,
        sink: &mut S,
    ) {
        let cid = batch.consumer_id;
        let mut settled = 0u32;
        let mut errs: SmallVec<[CommandError; 4]> = SmallVec::new();
        for cmd in &batch.items {
            match self.exec_ack_command(cid, cmd) {
                Ok(result) => settled += result.acked,
                Err(e) => errs.push(e),
            }
        }
        if settled > 0 {
            sink.on_settlement(SettlementResultBatch::ack(cid, settled));
            if let Some(qid) = self.consumers.queue_id(cid) {
                self.drive_queue(qid, sink);
            }
        }
        if !errs.is_empty() {
            sink.on_error(CommandErrorBatch { errors: errs });
        }
    }

    fn exec_nack_batch<S: EventSink<PayloadHandle>>(
        &mut self,
        batch: aurum_internal_protocol::command::settlement::NackCommandBatch,
        sink: &mut S,
    ) {
        let cid = batch.consumer_id;
        let mut settled = 0u32;
        let mut errs: SmallVec<[CommandError; 4]> = SmallVec::new();
        for cmd in &batch.items {
            match self.exec_nack_command(cid, cmd) {
                Ok(n) => settled += n,
                Err(e) => errs.push(e),
            }
        }
        if settled > 0 {
            sink.on_settlement(SettlementResultBatch::nack(cid, settled));
            if let Some(qid) = self.consumers.queue_id(cid) {
                if let Some(q) = self.queues.get_mut(qid) {
                    q.queue.retry_all_now();
                }
                self.drive_queue(qid, sink);
            }
        }
        if !errs.is_empty() {
            sink.on_error(CommandErrorBatch { errors: errs });
        }
    }

    fn exec_reject_batch<S: EventSink<PayloadHandle>>(
        &mut self,
        batch: aurum_internal_protocol::command::settlement::RejectCommandBatch,
        sink: &mut S,
    ) {
        let cid = batch.consumer_id;
        let mut settled = 0u32;
        for cmd in &batch.items {
            if let Ok(n) = self.exec_reject_command(cid, cmd) {
                settled += n;
            }
        }
        if settled > 0 {
            sink.on_settlement(SettlementResultBatch::reject(cid, settled));
            if let Some(qid) = self.consumers.queue_id(cid) {
                self.drive_queue(qid, sink);
            }
        }
    }

    fn exec_cancel<S: EventSink<PayloadHandle>>(
        &mut self,
        consumer_id: ConsumerId,
        disposition: CancelDispositionCommand,
        sink: &mut S,
    ) {
        let Some(state) = self.consumers.get_mut(consumer_id) else {
            sink.on_error(CommandErrorBatch::one(CommandError::consumer(
                CommandErrorKind::ConsumerNotFound,
                consumer_id,
            )));
            return;
        };

        if state.flags.contains(ConsumerRuntimeFlags::CANCELLED) {
            sink.on_error(CommandErrorBatch::one(CommandError::consumer(
                CommandErrorKind::ConsumerCancelled,
                consumer_id,
            )));
            return;
        }

        let queue_id = state.queue_id;
        let disp = match disposition {
            CancelDispositionCommand::RequeueUnacked => CancelDisposition::RequeueUnacked,
            CancelDispositionCommand::DropUnacked
            | CancelDispositionCommand::DeadLetterUnacked => CancelDisposition::DropUnacked,
        };

        {
            let Self { queues, consumers, .. } = self;
            let queue = match queues.get_mut(queue_id) {
                Some(q) => q,
                None => return,
            };
            let consumer = match consumers.get_mut(consumer_id) {
                Some(c) => c,
                None => return,
            };
            consumer.session.cancel(disp, &mut queue.queue);
            consumer.flags |= ConsumerRuntimeFlags::CANCELLED;
        }

        self.queues.detach_consumer(queue_id, consumer_id);
        self.consumers.remove(consumer_id);

        sink.on_consumer(ConsumerEventBatch {
            consumer_id,
            kind: ConsumerEventKind::Cancelled,
        });

        if matches!(disposition, CancelDispositionCommand::RequeueUnacked) {
            self.drive_queue(queue_id, sink);
        }
    }

    // ── Settlement helpers ────────────────────────────────────────────────────

    fn exec_ack_command(
        &mut self,
        consumer_id: ConsumerId,
        cmd: &AckCommand,
    ) -> Result<aurum_core::AckApplyResult, CommandError> {
        let queue_id = self.consumers.queue_id(consumer_id).ok_or_else(|| {
            CommandError::consumer(CommandErrorKind::ConsumerNotFound, consumer_id)
        })?;

        let map_err = |e: ConsumerError| match e {
            ConsumerError::InvalidDeliveryTag => {
                CommandError::consumer(CommandErrorKind::InvalidDeliveryTag, consumer_id)
            }
            ConsumerError::DeliveryTagAlreadySettled => {
                CommandError::consumer(CommandErrorKind::DeliveryTagAlreadySettled, consumer_id)
            }
            ConsumerError::ConsumerCancelled => {
                CommandError::consumer(CommandErrorKind::ConsumerCancelled, consumer_id)
            }
            _ => CommandError::consumer(CommandErrorKind::InternalInvariantViolation, consumer_id),
        };

        let result = {
            let Self { queues, consumers, .. } = self;
            let queue = queues.get_mut(queue_id).ok_or_else(|| {
                CommandError::queue(CommandErrorKind::QueueNotFound, queue_id)
            })?;
            let state = consumers.get_mut(consumer_id).ok_or_else(|| {
                CommandError::consumer(CommandErrorKind::ConsumerNotFound, consumer_id)
            })?;

            match cmd {
                AckCommand::Tag { tag, mode: SettlementMode::One } => state
                    .session
                    .ack(AckRequest::one(*tag), &mut queue.queue)
                    .map_err(map_err)?,
                AckCommand::Tag { tag, mode: SettlementMode::Multiple } => state
                    .session
                    .ack(AckRequest::multiple(*tag), &mut queue.queue)
                    .map_err(map_err)?,
                AckCommand::Range { end, .. } => state
                    .session
                    .ack(AckRequest::multiple(*end), &mut queue.queue)
                    .map_err(map_err)?,
                AckCommand::Mask { base, mask } => {
                    let mut total = aurum_core::AckApplyResult::default();
                    let mut remaining = *mask;
                    let mut seq = 0u64;
                    while remaining != 0 {
                        remaining &= remaining - 1;
                        let tag = DeliveryTag(base.0 + seq);
                        seq += 1;
                        if let Ok(r) =
                            state.session.ack(AckRequest::one(tag), &mut queue.queue)
                        {
                            total.acked += r.acked;
                            total.ranges.extend(r.ranges.iter().copied());
                        }
                    }
                    total
                }
            }
        };

        if let Some(store) = &mut self.durable {
            if let Err(_e) = store.append_ack_ranges(queue_id, &result.ranges) {
                return Err(CommandError::global(CommandErrorKind::InternalInvariantViolation));
            }
        }

        Ok(result)
    }

    fn exec_nack_command(
        &mut self,
        consumer_id: ConsumerId,
        cmd: &NackCommand,
    ) -> Result<u32, CommandError> {
        let queue_id = self.consumers.queue_id(consumer_id).ok_or_else(|| {
            CommandError::consumer(CommandErrorKind::ConsumerNotFound, consumer_id)
        })?;

        let to_reason = |d: NackDisposition| match d {
            NackDisposition::Requeue => NackReason::Requeue,
            NackDisposition::DeadLetter => NackReason::DeadLetter,
            NackDisposition::Drop => NackReason::Reject,
        };

        let nacked = {
            let Self { queues, consumers, .. } = self;
            let queue = queues.get_mut(queue_id).ok_or_else(|| {
                CommandError::queue(CommandErrorKind::QueueNotFound, queue_id)
            })?;
            let state = consumers.get_mut(consumer_id).ok_or_else(|| {
                CommandError::consumer(CommandErrorKind::ConsumerNotFound, consumer_id)
            })?;

            let map_err = |_| {
                CommandError::consumer(CommandErrorKind::InvalidDeliveryTag, consumer_id)
            };

            match cmd {
                NackCommand::Tag { tag, mode: SettlementMode::One, disposition } => state
                    .session
                    .nack(NackRequest::one(*tag, to_reason(*disposition)), &mut queue.queue)
                    .map_err(map_err)?
                    .nacked,
                NackCommand::Tag { tag, mode: SettlementMode::Multiple, disposition } => state
                    .session
                    .nack(NackRequest::multiple(*tag, to_reason(*disposition)), &mut queue.queue)
                    .map_err(map_err)?
                    .nacked,
                NackCommand::Range { end, disposition, .. } => state
                    .session
                    .nack(NackRequest::multiple(*end, to_reason(*disposition)), &mut queue.queue)
                    .map_err(map_err)?
                    .nacked,
                NackCommand::Mask { base, mask, disposition } => {
                    let mut total = 0u32;
                    let mut remaining = *mask;
                    let mut bit_pos = 0u64;
                    let reason = to_reason(*disposition);
                    while remaining != 0 {
                        let tz = remaining.trailing_zeros() as u64;
                        bit_pos += tz;
                        remaining >>= tz;
                        let tag = DeliveryTag(base.0 + bit_pos);
                        if let Ok(r) =
                            state.session.nack(NackRequest::one(tag, reason), &mut queue.queue)
                        {
                            total += r.nacked;
                        }
                        remaining >>= 1;
                        bit_pos += 1;
                    }
                    total
                }
            }
        };

        Ok(nacked)
    }

    fn exec_reject_command(
        &mut self,
        consumer_id: ConsumerId,
        cmd: &RejectCommand,
    ) -> Result<u32, CommandError> {
        let queue_id = self.consumers.queue_id(consumer_id).ok_or_else(|| {
            CommandError::consumer(CommandErrorKind::ConsumerNotFound, consumer_id)
        })?;

        let reason = match cmd.disposition {
            NackDisposition::Requeue => NackReason::Requeue,
            NackDisposition::DeadLetter => NackReason::DeadLetter,
            NackDisposition::Drop => NackReason::Reject,
        };

        let Self { queues, consumers, .. } = self;
        let queue = queues.get_mut(queue_id).ok_or_else(|| {
            CommandError::queue(CommandErrorKind::QueueNotFound, queue_id)
        })?;
        let state = consumers.get_mut(consumer_id).ok_or_else(|| {
            CommandError::consumer(CommandErrorKind::ConsumerNotFound, consumer_id)
        })?;

        let r = state
            .session
            .nack(NackRequest::one(cmd.tag, reason), &mut queue.queue)
            .map_err(|_| CommandError::consumer(CommandErrorKind::InvalidDeliveryTag, consumer_id))?;

        Ok(r.nacked)
    }

    // ── Delivery ──────────────────────────────────────────────────────────────

    fn drive_queue<S: EventSink<PayloadHandle>>(&mut self, queue_id: QueueId, sink: &mut S) {
        let consumer_ids: Vec<ConsumerId> = match self.queues.get(queue_id) {
            Some(q) => q.consumers.clone(),
            None => return,
        };
        if consumer_ids.is_empty() {
            return;
        }

        let mut passes = 0u32;
        loop {
            if passes >= self.scheduler.max_delivery_passes {
                break;
            }

            let start = self.queues.get(queue_id).map(|q| q.next_consumer_index).unwrap_or(0);
            let mut delivered_this_pass = 0u32;

            for i in 0..consumer_ids.len() {
                let cid = consumer_ids[(start + i) % consumer_ids.len()];
                delivered_this_pass += self.deliver_consumer(cid, sink);
            }

            if delivered_this_pass == 0 {
                break;
            }

            if let Some(q) = self.queues.get_mut(queue_id) {
                q.next_consumer_index = (start + 1) % consumer_ids.len();
            }
            passes += 1;
        }
    }

    fn deliver_consumer<S: EventSink<PayloadHandle>>(
        &mut self,
        consumer_id: ConsumerId,
        sink: &mut S,
    ) -> u32 {
        let queue_id = match self.consumers.queue_id(consumer_id) {
            Some(q) => q,
            None => return 0,
        };

        let mut total = 0u32;
        loop {
            let n = {
                let Self { queues, consumers, .. } = self;
                let queue = match queues.get_mut(queue_id) {
                    Some(q) => q,
                    None => return total,
                };
                let consumer = match consumers.get_mut(consumer_id) {
                    Some(c) => c,
                    None => return total,
                };
                match consumer.session.deliver_from_queue(&mut queue.queue, 256, &mut consumer.out)
                {
                    Ok(n) => n,
                    Err(_) => return total,
                }
            };

            if n == 0 {
                break;
            }
            total += n;
            self.emit_delivery(consumer_id, queue_id, sink);
        }
        total
    }

    fn emit_delivery<S: EventSink<PayloadHandle>>(
        &mut self,
        consumer_id: ConsumerId,
        queue_id: QueueId,
        sink: &mut S,
    ) {
        let (channel_id, segments) = {
            let consumer = match self.consumers.get_mut(consumer_id) {
                Some(c) => c,
                None => return,
            };
            if consumer.out.segments.is_empty() {
                return;
            }
            let channel_id = consumer.channel_id;
            let mut batch = DeliveryEventBatch {
                consumer_id,
                queue_id,
                channel_id,
                segments: SmallVec::new(),
                metadata: Default::default(),
            };

            let seq_payloads = self
                .queues
                .get(queue_id)
                .map(|q| &q.seq_payloads);

            for seg in &consumer.out.segments {
                match seg {
                    TaggedDeliverySegment::Range(r) => {
                        let merged = if let Some(DeliveryEventSegment::Range(prev)) =
                            batch.segments.last_mut()
                        {
                            if prev.start_seq + u64::from(prev.len) == r.range.start_seq {
                                prev.len += r.range.len;
                                if let PayloadSpan::Contiguous { len, .. } = &mut prev.payloads {
                                    *len += r.range.len;
                                }
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if !merged {
                            let base_handle = seq_payloads
                                .and_then(|m| m.get(&r.range.start_seq).copied())
                                .unwrap_or(PayloadHandle(r.range.start_seq));
                            let flags = if r.flags.contains(DeliveryFlags::REDELIVERED) {
                                DeliveryEventFlags::REDELIVERED
                            } else {
                                DeliveryEventFlags::empty()
                            };
                            batch.segments.push(DeliveryEventSegment::Range(DeliveryRangeSegment {
                                start_tag: r.first_tag,
                                start_seq: r.range.start_seq,
                                len: r.range.len,
                                payloads: PayloadSpan::Contiguous {
                                    base: base_handle,
                                    len: r.range.len,
                                },
                                flags,
                            }));
                        }
                    }
                    TaggedDeliverySegment::Mask(m) => {
                        let flags = if m.flags.contains(DeliveryFlags::REDELIVERED) {
                            DeliveryEventFlags::REDELIVERED
                        } else {
                            DeliveryEventFlags::empty()
                        };
                        batch.segments.push(DeliveryEventSegment::Mask(DeliveryMaskSegment {
                            base_tag: m.first_tag,
                            block: m.mask.block,
                            word: m.mask.word,
                            mask: m.mask.mask,
                            flags,
                        }));
                    }
                }
            }
            consumer.out.clear();
            (channel_id, batch)
        };

        let _ = channel_id;
        sink.on_delivery(segments);
    }
}

impl Default for InMemoryShardExecutor {
    fn default() -> Self {
        Self::new(ShardId(0))
    }
}
