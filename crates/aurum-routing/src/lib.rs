#![forbid(unsafe_code)]

pub mod binding;
pub mod compiler;
pub mod config;
pub mod error;
pub mod exchange;
pub mod flags;
pub mod hash;
pub mod queue_set;
pub mod table;

#[cfg(test)]
mod tests;

pub use binding::BindingDecl;
pub use compiler::RouteCompiler;
pub use config::RoutingConfig;
pub use error::{RouteCompileError, RouteLookupError, RouteResolveError};
pub use exchange::{ExchangeDecl, ExchangeKind};
pub use flags::{BindingFlags, ExchangeFlags, RouteFlags};
pub use queue_set::{QueueSetEntry, QueueSetRef, QueueTarget};
pub use table::{ResolvedRoute, RouteTable, targets_by_shard};
