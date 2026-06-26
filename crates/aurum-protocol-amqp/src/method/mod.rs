mod basic;
mod bits;
mod channel;
mod confirm;
mod connection;
mod exchange;
mod queue;

use bytes::BytesMut;

use crate::wire::{
    constants::{
        CLASS_BASIC, CLASS_CHANNEL, CLASS_CONFIRM, CLASS_CONNECTION, CLASS_EXCHANGE, CLASS_QUEUE,
    },
    WireError,
};

pub use basic::{
    BasicAck, BasicAckFlags, BasicCancel, BasicConsume, BasicConsumeOk, BasicDeliver, BasicMethod,
    BasicNack, BasicNackFlags, BasicPublish, BasicPublishFlags, BasicQos, BasicReject,
};
pub use channel::{ChannelClose, ChannelMethod, ChannelOpen};
pub use confirm::ConfirmMethod;
pub use connection::{
    ConnectionClose, ConnectionMethod, ConnectionOpen, ConnectionStart, ConnectionStartOk,
    ConnectionTune, ConnectionTuneOk,
};
pub use exchange::{ExchangeDeclare, ExchangeMethod};
pub use queue::{QueueBind, QueueDeclare, QueueDeclareOk, QueueMethod};

#[derive(Debug, Clone, PartialEq)]
pub enum AmqpMethod {
    Connection(ConnectionMethod),
    Channel(ChannelMethod),
    Exchange(ExchangeMethod),
    Queue(QueueMethod),
    Basic(BasicMethod),
    Confirm(ConfirmMethod),
}

pub fn decode_method(class_id: u16, method_id: u16, payload: &[u8]) -> Result<AmqpMethod, WireError> {
    let mut buf = payload;
    match class_id {
        CLASS_CONNECTION => decode_connection(method_id, &mut buf).map(AmqpMethod::Connection),
        CLASS_CHANNEL => decode_channel(method_id, &mut buf).map(AmqpMethod::Channel),
        CLASS_EXCHANGE => decode_exchange(method_id, &mut buf).map(AmqpMethod::Exchange),
        CLASS_QUEUE => decode_queue(method_id, &mut buf).map(AmqpMethod::Queue),
        CLASS_BASIC => decode_basic(method_id, &mut buf).map(AmqpMethod::Basic),
        CLASS_CONFIRM => decode_confirm(method_id, &mut buf).map(AmqpMethod::Confirm),
        _ => Err(WireError::UnknownFrameType),
    }
}

pub fn encode_method(method: &AmqpMethod, dst: &mut BytesMut) -> Result<(), WireError> {
    match method {
        AmqpMethod::Connection(m) => encode_connection(m, dst),
        AmqpMethod::Channel(m) => encode_channel(m, dst),
        AmqpMethod::Exchange(m) => encode_exchange(m, dst),
        AmqpMethod::Queue(m) => encode_queue(m, dst),
        AmqpMethod::Basic(m) => encode_basic(m, dst),
        AmqpMethod::Confirm(m) => encode_confirm(m, dst),
    }
}

pub fn method_class_id(method: &AmqpMethod) -> u16 {
    match method {
        AmqpMethod::Connection(_) => CLASS_CONNECTION,
        AmqpMethod::Channel(_) => CLASS_CHANNEL,
        AmqpMethod::Exchange(_) => CLASS_EXCHANGE,
        AmqpMethod::Queue(_) => CLASS_QUEUE,
        AmqpMethod::Basic(_) => CLASS_BASIC,
        AmqpMethod::Confirm(_) => CLASS_CONFIRM,
    }
}

pub fn method_id(method: &AmqpMethod) -> u16 {
    use crate::wire::constants::{basic, channel, confirm, connection, exchange, queue};
    match method {
        AmqpMethod::Connection(ConnectionMethod::Start(_)) => connection::START,
        AmqpMethod::Connection(ConnectionMethod::StartOk(_)) => connection::START_OK,
        AmqpMethod::Connection(ConnectionMethod::Tune(_)) => connection::TUNE,
        AmqpMethod::Connection(ConnectionMethod::TuneOk(_)) => connection::TUNE_OK,
        AmqpMethod::Connection(ConnectionMethod::Open(_)) => connection::OPEN,
        AmqpMethod::Connection(ConnectionMethod::OpenOk) => connection::OPEN_OK,
        AmqpMethod::Connection(ConnectionMethod::Close(_)) => connection::CLOSE,
        AmqpMethod::Connection(ConnectionMethod::CloseOk) => connection::CLOSE_OK,
        AmqpMethod::Channel(ChannelMethod::Open(_)) => channel::OPEN,
        AmqpMethod::Channel(ChannelMethod::OpenOk) => channel::OPEN_OK,
        AmqpMethod::Channel(ChannelMethod::Close(_)) => channel::CLOSE,
        AmqpMethod::Channel(ChannelMethod::CloseOk) => channel::CLOSE_OK,
        AmqpMethod::Exchange(ExchangeMethod::Declare(_)) => exchange::DECLARE,
        AmqpMethod::Exchange(ExchangeMethod::DeclareOk) => exchange::DECLARE_OK,
        AmqpMethod::Queue(QueueMethod::Declare(_)) => queue::DECLARE,
        AmqpMethod::Queue(QueueMethod::DeclareOk(_)) => queue::DECLARE_OK,
        AmqpMethod::Queue(QueueMethod::Bind(_)) => queue::BIND,
        AmqpMethod::Queue(QueueMethod::BindOk) => queue::BIND_OK,
        AmqpMethod::Basic(BasicMethod::Qos(_)) => basic::QOS,
        AmqpMethod::Basic(BasicMethod::QosOk) => basic::QOS_OK,
        AmqpMethod::Basic(BasicMethod::Consume(_)) => basic::CONSUME,
        AmqpMethod::Basic(BasicMethod::ConsumeOk(_)) => basic::CONSUME_OK,
        AmqpMethod::Basic(BasicMethod::Cancel(_)) => basic::CANCEL,
        AmqpMethod::Basic(BasicMethod::CancelOk) => basic::CANCEL_OK,
        AmqpMethod::Basic(BasicMethod::Publish(_)) => basic::PUBLISH,
        AmqpMethod::Basic(BasicMethod::Deliver(_)) => basic::DELIVER,
        AmqpMethod::Basic(BasicMethod::Ack(_)) => basic::ACK,
        AmqpMethod::Basic(BasicMethod::Nack(_)) => basic::NACK,
        AmqpMethod::Basic(BasicMethod::Reject(_)) => basic::REJECT,
        AmqpMethod::Confirm(ConfirmMethod::Select { .. }) => confirm::SELECT,
        AmqpMethod::Confirm(ConfirmMethod::SelectOk) => confirm::SELECT_OK,
    }
}

use basic::{decode_basic, encode_basic};
use channel::{decode_channel, encode_channel};
use confirm::{decode_confirm, encode_confirm};
use connection::{decode_connection, encode_connection};
use exchange::{decode_exchange, encode_exchange};
use queue::{decode_queue, encode_queue};
