//! Central error-mapping seam for PR10 (adapters remain protocol-specific).

use aurum_internal_protocol::event::error::{CommandError, CommandErrorKind};
use aurum_protocol_native::NativeErrorCode;

/// Map internal command errors to native wire error codes.
#[must_use]
pub fn command_error_to_native_code(kind: CommandErrorKind) -> NativeErrorCode {
    match kind {
        CommandErrorKind::ExchangeNotFound | CommandErrorKind::Unroutable => {
            NativeErrorCode::RouteNotFound
        }
        CommandErrorKind::StaleRouteEpoch | CommandErrorKind::RouteGenerationMismatch => {
            NativeErrorCode::RouteStale
        }
        CommandErrorKind::RouteIdInvalid | CommandErrorKind::InvalidRoute => {
            NativeErrorCode::RouteNotFound
        }
        CommandErrorKind::QueueNotFound => NativeErrorCode::QueueNotFound,
        CommandErrorKind::ConsumerNotFound => NativeErrorCode::ConsumerNotFound,
        _ => NativeErrorCode::Internal,
    }
}

/// Classify whether a command error should close the whole connection (AMQP scope).
#[must_use]
pub fn is_connection_fatal(err: &CommandError) -> bool {
    matches!(
        err.kind,
        CommandErrorKind::StaleRouteEpoch | CommandErrorKind::RouteGenerationMismatch
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use aurum_internal_protocol::event::error::CommandError;

    #[test]
    fn maps_stale_route() {
        assert_eq!(
            command_error_to_native_code(CommandErrorKind::StaleRouteEpoch),
            NativeErrorCode::RouteStale
        );
    }

    #[test]
    fn stale_route_is_connection_fatal() {
        let err = CommandError::global(CommandErrorKind::StaleRouteEpoch);
        assert!(is_connection_fatal(&err));
    }
}
