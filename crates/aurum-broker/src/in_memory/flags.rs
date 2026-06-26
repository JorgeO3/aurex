use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct QueueRuntimeFlags: u16 {
        const ACTIVE   = 1 << 0;
        const DELETING = 1 << 1;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct ConsumerRuntimeFlags: u16 {
        const ACTIVE     = 1 << 0;
        const CANCELLED  = 1 << 1;
        const DRAINING   = 1 << 2;
    }
}
