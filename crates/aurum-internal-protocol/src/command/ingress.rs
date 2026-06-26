use smallvec::SmallVec;
use aurum_types::PayloadHandle;

use super::publish::IngressPublishBatch;
use crate::route::ResolveRouteCommand;

/// Batch of resolve-route commands (usually one per publish setup).
#[derive(Debug, Clone)]
pub struct ResolveRouteBatch {
    pub items: SmallVec<[ResolveRouteCommand; 4]>,
}

impl ResolveRouteBatch {
    #[must_use]
    pub fn one(cmd: ResolveRouteCommand) -> Self {
        let mut items = SmallVec::new();
        items.push(cmd);
        Self { items }
    }
}

/// Placeholder for control-plane commands (connection open, auth, etc.).
#[derive(Debug, Clone)]
pub struct ControlCommandBatch {
    _reserved: (),
}

/// Top-level command batch emitted by a protocol adapter before routing and
/// shard ownership are fully resolved.
#[derive(Debug, Clone)]
pub enum IngressCommandBatch<P = PayloadHandle> {
    Publish(IngressPublishBatch<P>),
    ResolveRoute(ResolveRouteBatch),
    Control(ControlCommandBatch),
}
