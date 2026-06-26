use smallvec::SmallVec;

use aurum_internal_protocol::{
    event::{
        confirm::{ConsumerEventBatch, PublishConfirmBatch, SettlementResultBatch},
        delivery::DeliveryEventBatch,
        error::{CommandError, CommandErrorBatch},
    },
    route::RouteResolvedEvent,
    sink::EventSink,
};
use aurum_types::PayloadHandle;

/// Reusable output buffer for shard command execution.
#[derive(Debug, Default)]
pub struct ShardOutputBatch<P = PayloadHandle> {
    pub deliveries: SmallVec<[DeliveryEventBatch<P>; 8]>,
    pub confirms: SmallVec<[PublishConfirmBatch; 8]>,
    pub settlements: SmallVec<[SettlementResultBatch; 8]>,
    pub consumer_events: SmallVec<[ConsumerEventBatch; 8]>,
    pub route_resolved: SmallVec<[RouteResolvedEvent; 4]>,
    pub errors: SmallVec<[CommandError; 8]>,
}

impl<P> ShardOutputBatch<P> {
    pub fn clear(&mut self) {
        self.deliveries.clear();
        self.confirms.clear();
        self.settlements.clear();
        self.consumer_events.clear();
        self.route_resolved.clear();
        self.errors.clear();
    }

    pub fn push_errors(&mut self, batch: CommandErrorBatch) {
        self.errors.extend(batch.errors);
    }

    #[must_use]
    pub fn total_delivered(&self) -> usize {
        self.deliveries.iter().map(|d| d.total_count()).sum()
    }

    #[must_use]
    pub fn total_settled(&self) -> u32 {
        self.settlements.iter().map(|s| s.settled).sum()
    }

    #[must_use]
    pub fn total_confirmed(&self) -> u32 {
        self.confirms.iter().map(|c| c.accepted).sum()
    }
}

impl<P> EventSink<P> for ShardOutputBatch<P> {
    #[inline(always)]
    fn on_delivery(&mut self, batch: DeliveryEventBatch<P>) {
        self.deliveries.push(batch);
    }

    #[inline(always)]
    fn on_settlement(&mut self, result: SettlementResultBatch) {
        self.settlements.push(result);
    }

    #[inline(always)]
    fn on_confirm(&mut self, batch: PublishConfirmBatch) {
        self.confirms.push(batch);
    }

    #[inline(always)]
    fn on_consumer(&mut self, event: ConsumerEventBatch) {
        self.consumer_events.push(event);
    }

    #[inline(always)]
    fn on_error(&mut self, batch: CommandErrorBatch) {
        self.push_errors(batch);
    }
}
