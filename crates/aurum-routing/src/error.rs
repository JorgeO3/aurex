#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteCompileError {
    DuplicateExchangeId,
    ExchangeNotFound,
    UnsupportedExchangeKind,
    EmptyConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteResolveError {
    ExchangeNotFound,
    UnsupportedExchangeKind,
    Unroutable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteLookupError {
    RouteTableVersionMismatch,
    RouteGenerationMismatch,
    RouteIdInvalid,
    Unroutable,
}
