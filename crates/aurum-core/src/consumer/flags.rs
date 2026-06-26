use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct DeliveryFlags: u8 {
        const REDELIVERED = 1 << 0;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct SegmentFlags: u8 {
        const REDELIVERED = 1 << 0;
        const HAS_HOLES   = 1 << 1;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct ConsumerFlags: u8 {
        const CANCELLED = 1 << 0;
        const BLOCKED   = 1 << 1;
    }
}
