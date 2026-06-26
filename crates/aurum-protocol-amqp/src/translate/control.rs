use crate::method::{ExchangeDeclare, QueueBind, QueueDeclare};
use crate::port::AmqpControlCommand;

pub fn declare_exchange_command(decl: &ExchangeDeclare) -> AmqpControlCommand {
    AmqpControlCommand::DeclareExchange {
        name: decl.exchange.to_string_lossy(),
        exchange_type: decl.exchange_type.to_string_lossy(),
        durable: decl.durable,
    }
}

pub fn declare_queue_command(decl: &QueueDeclare) -> AmqpControlCommand {
    AmqpControlCommand::DeclareQueue {
        name: decl.queue.to_string_lossy(),
    }
}

pub fn bind_queue_command(bind: &QueueBind) -> AmqpControlCommand {
    AmqpControlCommand::BindQueue {
        queue: bind.queue.to_string_lossy(),
        exchange: bind.exchange.to_string_lossy(),
        routing_key: bind.routing_key.to_string_lossy(),
    }
}
