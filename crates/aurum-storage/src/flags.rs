use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct RecordFlags: u16 {
        const NONE = 0;
        const COMPRESSED = 1 << 0;
        const HAS_CRC32C = 1 << 1;
        const BATCHED = 1 << 2;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct QueueIndexFlags: u16 {
        const NONE = 0;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct AckLedgerFlags: u16 {
        const NONE = 0;
    }
}
