use std::sync::Arc;

use aurum_internal_protocol::{
    command::{
        control::DeclareQueueBatch,
        ingress::{IngressCommandBatch, ResolveRouteBatch},
        publish::{IngressPublishBatch, IngressPublishTarget, ShardPublishBatch},
        shard::ShardCommandBatch,
    },
    event::error::{CommandError, CommandErrorBatch, CommandErrorKind},
    route::{RoutePublishTarget, RouteResolvedEvent},
};
use aurum_routing::{RouteLookupError, RouteResolveError, RouteTable};
use aurum_types::{PayloadHandle, QueueId, RouteTableVersion};

use super::broker::InMemoryBroker;
use super::output::ShardOutputBatch;

pub fn map_resolve_error(err: RouteResolveError) -> CommandErrorKind {
    match err {
        RouteResolveError::ExchangeNotFound => CommandErrorKind::ExchangeNotFound,
        RouteResolveError::UnsupportedExchangeKind => CommandErrorKind::InvalidRoute,
        RouteResolveError::Unroutable => CommandErrorKind::Unroutable,
    }
}

pub fn map_lookup_error(err: RouteLookupError) -> CommandErrorKind {
    match err {
        RouteLookupError::RouteTableVersionMismatch => CommandErrorKind::StaleRouteEpoch,
        RouteLookupError::RouteGenerationMismatch => CommandErrorKind::RouteGenerationMismatch,
        RouteLookupError::RouteIdInvalid => CommandErrorKind::RouteIdInvalid,
        RouteLookupError::Unroutable => CommandErrorKind::Unroutable,
    }
}

impl InMemoryBroker {
  pub fn route_table(&self) -> &Arc<RouteTable> {
        &self.route_table
    }

    pub fn install_route_table(&mut self, table: Arc<RouteTable>) {
        self.route_table = table;
    }

    pub fn execute_ingress(
        &mut self,
        batch: IngressCommandBatch<PayloadHandle>,
    ) -> ShardOutputBatch<PayloadHandle> {
        let mut out = ShardOutputBatch::default();
        match batch {
            IngressCommandBatch::Publish(p) => self.exec_ingress_publish(p, &mut out),
            IngressCommandBatch::ResolveRoute(r) => self.exec_resolve_route(r, &mut out),
            IngressCommandBatch::Control(_) => {}
        }
        out
    }

    fn exec_resolve_route(
        &mut self,
        batch: ResolveRouteBatch,
        out: &mut ShardOutputBatch<PayloadHandle>,
    ) {
        for cmd in batch.items {
            let resolved = if !cmd.exchange_name.is_empty() {
                let name = std::str::from_utf8(&cmd.exchange_name).map_err(|_| ()).ok();
                match name {
                    Some(n) => self.route_table.resolve_direct_by_name(n, &cmd.routing_key),
                    None => Err(RouteResolveError::ExchangeNotFound),
                }
            } else {
                self.route_table
                    .resolve_direct(cmd.exchange_id, &cmd.routing_key)
            };
            match resolved {
                Ok(resolved) => {
                    out.route_resolved.push(RouteResolvedEvent {
                        request_id: cmd.request_id,
                        route_id: resolved.route_id,
                        route_version: resolved.version,
                        flags: resolved.flags.bits(),
                    });
                }
                Err(err) => out.push_errors(CommandErrorBatch::one(CommandError::global(
                    map_resolve_error(err),
                ))),
            }
        }
    }

    fn exec_ingress_publish(
        &mut self,
        batch: IngressPublishBatch<PayloadHandle>,
        out: &mut ShardOutputBatch<PayloadHandle>,
    ) {
        match batch.target {
            IngressPublishTarget::Queue(queue_id) => {
                self.publish_to_queue(batch, queue_id, None, out);
            }
            IngressPublishTarget::Route(target) => {
                self.publish_by_route(batch, target, out);
            }
            IngressPublishTarget::ExchangeKey { .. } => {
                out.push_errors(CommandErrorBatch::one(CommandError::global(
                    CommandErrorKind::InvalidRoute,
                )));
            }
        }
    }

    fn publish_by_route(
        &mut self,
        batch: IngressPublishBatch<PayloadHandle>,
        target: RoutePublishTarget,
        out: &mut ShardOutputBatch<PayloadHandle>,
    ) {
        let set = match self
            .route_table
            .get_by_route_id(target.route_id, target.route_version)
        {
            Ok(s) => s,
            Err(err) => {
                out.push_errors(CommandErrorBatch::one(CommandError::global(
                    map_lookup_error(err),
                )));
                return;
            }
        };

        if set.target_count() == 0 {
            out.push_errors(CommandErrorBatch::one(CommandError::global(
                CommandErrorKind::Unroutable,
            )));
            return;
        }

        let route_id = Some(target.route_id);
        let local_shard = self.shard().shard_id;
        let targets = set.targets_vec();
        for t in targets {
            if t.shard_id != local_shard {
                out.push_errors(CommandErrorBatch::one(CommandError::global(
                    CommandErrorKind::InvalidRoute,
                )));
                continue;
            }
            self.ensure_queue(t.queue_id);
            self.publish_to_queue(batch.clone(), t.queue_id, route_id, out);
        }
    }

    fn publish_to_queue(
        &mut self,
        batch: IngressPublishBatch<PayloadHandle>,
        queue_id: QueueId,
        route_id: Option<aurum_types::RouteId>,
        out: &mut ShardOutputBatch<PayloadHandle>,
    ) {
        self.ensure_queue(queue_id);
        let shard_batch = ShardPublishBatch {
            batch_id: batch.batch_id,
            source: batch.source,
            queue_id,
            route_id,
            flags: batch.flags,
            confirm_mode: batch.confirm_mode,
            record_count: batch.records.len() as u32,
            records: batch.records.clone(),
        };
        if shard_batch.record_count == 0 && !batch.records.is_empty() {
            // records path
        }
        let count = if shard_batch.record_count > 0 {
            shard_batch.record_count
        } else {
            shard_batch.records.len() as u32
        };
        let mut shard_batch = shard_batch;
        shard_batch.record_count = count;

        let mut scratch = ShardOutputBatch::default();
        if self
            .shard_mut()
            .execute_batch(ShardCommandBatch::Publish(shard_batch), &mut scratch)
            .is_err()
        {
            out.push_errors(CommandErrorBatch::one(CommandError::queue(
                CommandErrorKind::QueueNotFound,
                queue_id,
            )));
            return;
        }
        merge_output(out, scratch);
    }

    fn ensure_queue(&mut self, queue_id: QueueId) {
        if !self.shard().queues().contains(queue_id) {
            let mut out = ShardOutputBatch::default();
            let _ = self.shard_mut().execute_batch(
                ShardCommandBatch::Declare(DeclareQueueBatch::one(queue_id)),
                &mut out,
            );
        }
    }
}

fn merge_output(dst: &mut ShardOutputBatch, src: ShardOutputBatch) {
    dst.deliveries.extend(src.deliveries);
    dst.confirms.extend(src.confirms);
    dst.settlements.extend(src.settlements);
    dst.consumer_events.extend(src.consumer_events);
    dst.route_resolved.extend(src.route_resolved);
    dst.errors.extend(src.errors);
}

/// Build a broker preloaded with an empty route table at the initial version.
pub fn broker_with_table(table: Arc<RouteTable>) -> InMemoryBroker {
    InMemoryBroker::with_route_table(table)
}

pub fn empty_route_table() -> Arc<RouteTable> {
    Arc::new(RouteTable::new_empty(RouteTableVersion::INITIAL))
}
