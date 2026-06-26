use aurum_types::PayloadHandle;

use crate::batch::ShardEventBatch;
use crate::event::{
    confirm::{ConsumerEventBatch, PublishConfirmBatch, SettlementResultBatch},
    delivery::DeliveryEventBatch,
    error::CommandErrorBatch,
};

/// Typed callback interface for events emitted by a shard executor.
///
/// Implement this trait to receive executor events without allocating a
/// `Vec<ShardEventBatch>`. The executor calls the appropriate method for
/// each event as it is produced. Implementations are monomorphized —
/// zero virtual dispatch overhead on the hot path.
///
/// `Vec<ShardEventBatch<P>>` implements this trait for backwards compatibility.
pub trait EventSink<P = PayloadHandle> {
    fn on_delivery(&mut self, batch: DeliveryEventBatch<P>);
    fn on_settlement(&mut self, result: SettlementResultBatch);
    fn on_confirm(&mut self, batch: PublishConfirmBatch);
    fn on_consumer(&mut self, event: ConsumerEventBatch);
    fn on_error(&mut self, batch: CommandErrorBatch);
}

/// `Vec<ShardEventBatch<P>>` implements `EventSink` for backwards compatibility.
/// Existing callers can pass `&mut Vec<ShardEventBatch>` unchanged.
impl<P> EventSink<P> for Vec<ShardEventBatch<P>> {
    #[inline(always)]
    fn on_delivery(&mut self, b: DeliveryEventBatch<P>) {
        self.push(ShardEventBatch::Delivery(b));
    }

    #[inline(always)]
    fn on_settlement(&mut self, r: SettlementResultBatch) {
        self.push(ShardEventBatch::Settlement(r));
    }

    #[inline(always)]
    fn on_confirm(&mut self, b: PublishConfirmBatch) {
        self.push(ShardEventBatch::PublishConfirm(b));
    }

    #[inline(always)]
    fn on_consumer(&mut self, e: ConsumerEventBatch) {
        self.push(ShardEventBatch::Consumer(e));
    }

    #[inline(always)]
    fn on_error(&mut self, b: CommandErrorBatch) {
        self.push(ShardEventBatch::Error(b));
    }
}
