#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitError {
    Backpressure,
    Closed,
    StaleEpoch,
    ShardUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandResult {
    Accepted,
    RejectedBackpressure,
    StaleEpoch,
    Redirect,
}
