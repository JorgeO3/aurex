use aurum_types::RouteTableVersion;

use crate::binding::BindingDecl;
use crate::exchange::ExchangeDecl;

#[derive(Debug, Clone, Default)]
pub struct RoutingConfig {
    pub version: RouteTableVersion,
    pub exchanges: Vec<ExchangeDecl>,
    pub bindings: Vec<BindingDecl>,
}

impl RoutingConfig {
    #[must_use]
    pub fn new(version: RouteTableVersion) -> Self {
        Self { version, exchanges: Vec::new(), bindings: Vec::new() }
    }

    pub fn add_exchange(&mut self, exchange: ExchangeDecl) {
        self.exchanges.push(exchange);
    }

    pub fn add_binding(&mut self, binding: BindingDecl) {
        self.bindings.push(binding);
    }
}
