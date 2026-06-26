use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct PublishFlags: u16 {
        const PERSISTENT        = 0x0001;
        const MANDATORY         = 0x0002;
        const CONFIRM_REQUIRED  = 0x0004;
        const ROUTED_BY_ID      = 0x0008;
        const HAS_HEADERS       = 0x0010;
        const HAS_EXPIRATION    = 0x0020;
        const HAS_PRIORITY      = 0x0040;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct MessageFlags: u8 {
        const REDELIVERED    = 0x01;
        const COMPRESSED     = 0x02;
        const TRACED         = 0x04;
        const HAS_PAYLOAD_REF = 0x08;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct PayloadFlags: u8 {
        const PINNED    = 0x01;
        const SHARED    = 0x02;
        const EXTERNAL  = 0x04;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct AckBatchFlags: u8 {
        const MULTIPLE              = 0x01;
        const FROM_ADAPTER_COALESCER = 0x02;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct NackBatchFlags: u8 {
        const MULTIPLE   = 0x01;
        const REQUEUE    = 0x02;
        const DEAD_LETTER = 0x04;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct ConsumeFlags: u8 {
        const EXCLUSIVE = 0x01;
        const NO_LOCAL  = 0x02;
        const AUTO_ACK  = 0x04;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct DeliveryEventFlags: u8 {
        const REDELIVERED = 0x01;
        const COMPRESSED  = 0x02;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct CommandFlags: u8 {
        const LOW_LATENCY      = 0x01;
        const BEST_EFFORT      = 0x02;
        const REQUIRES_CONFIRM = 0x04;
    }
}
