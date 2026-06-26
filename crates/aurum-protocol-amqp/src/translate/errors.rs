use aurum_internal_protocol::event::error::{CommandError, CommandErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmqpErrorScope {
    Connection,
    Channel(u16),
}

pub const REPLY_CHANNEL_ERROR: u16 = 504;
pub const REPLY_COMMAND_INVALID: u16 = 503;

pub fn command_error_scope(err: &CommandError) -> AmqpErrorScope {
    match err.kind {
        CommandErrorKind::InvalidDeliveryTag
        | CommandErrorKind::DeliveryTagAlreadySettled
        | CommandErrorKind::ConsumerNotFound
        | CommandErrorKind::ConsumerCancelled
        | CommandErrorKind::DuplicateConsumer => AmqpErrorScope::Channel(0),
        _ => AmqpErrorScope::Connection,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aurum_types::ConsumerId;

    #[test]
    fn invalid_delivery_tag_is_channel_scope() {
        let err = CommandError::consumer(CommandErrorKind::InvalidDeliveryTag, ConsumerId(1));
        assert!(matches!(command_error_scope(&err), AmqpErrorScope::Channel(_)));
    }

    #[test]
    fn unroutable_is_connection_scope() {
        let err = CommandError::global(CommandErrorKind::Unroutable);
        assert!(matches!(command_error_scope(&err), AmqpErrorScope::Connection));
    }
}
