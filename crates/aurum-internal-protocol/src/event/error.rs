use smallvec::SmallVec;
use aurum_types::{BatchId, ConsumerId, QueueId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandErrorKind {
    InvalidRoute               = 0,
    StaleRouteEpoch            = 1,
    QueueNotFound              = 2,
    ConsumerNotFound           = 3,
    InvalidDeliveryTag         = 4,
    DeliveryTagAlreadySettled  = 5,
    CreditExceeded             = 6,
    PermissionDenied           = 7,
    Backpressure               = 8,
    InternalInvariantViolation = 9,
    DuplicateQueue             = 10,
    DuplicateConsumer          = 11,
    ConsumerCancelled          = 12,
    ExchangeNotFound           = 13,
    Unroutable                 = 14,
    RouteIdInvalid             = 15,
    RouteGenerationMismatch    = 16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandError {
    pub kind: CommandErrorKind,
    pub queue_id: Option<QueueId>,
    pub consumer_id: Option<ConsumerId>,
    pub batch_id: Option<BatchId>,
}

impl CommandError {
    #[must_use]
    pub fn consumer(kind: CommandErrorKind, consumer_id: ConsumerId) -> Self {
        Self { kind, queue_id: None, consumer_id: Some(consumer_id), batch_id: None }
    }

    #[must_use]
    pub fn queue(kind: CommandErrorKind, queue_id: QueueId) -> Self {
        Self { kind, queue_id: Some(queue_id), consumer_id: None, batch_id: None }
    }

    #[must_use]
    pub fn global(kind: CommandErrorKind) -> Self {
        Self { kind, queue_id: None, consumer_id: None, batch_id: None }
    }
}

#[derive(Debug, Clone)]
pub struct CommandErrorBatch {
    pub errors: SmallVec<[CommandError; 4]>,
}

impl CommandErrorBatch {
    #[must_use]
    pub fn one(err: CommandError) -> Self {
        let mut errors = SmallVec::new();
        errors.push(err);
        Self { errors }
    }
}
