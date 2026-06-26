use aurum_types::PayloadHandle;

use crate::flags::PayloadFlags;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadClass {
    Empty = 0,
    Tiny = 1,     // ≤ 64 B — can be inlined
    Small = 2,    // ≤ 4 KiB
    Medium = 3,   // ≤ 64 KiB
    Large = 4,    // > 64 KiB
    External = 5, // external gateway / sidecar buffer
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadDescriptor {
    pub handle: PayloadHandle,
    pub len: u32,
    pub class: PayloadClass,
    pub flags: PayloadFlags,
}

impl PayloadDescriptor {
    #[must_use]
    pub const fn new(handle: PayloadHandle, len: u32) -> Self {
        let class = match len {
            0 => PayloadClass::Empty,
            1..=64 => PayloadClass::Tiny,
            65..=4096 => PayloadClass::Small,
            4097..=65536 => PayloadClass::Medium,
            _ => PayloadClass::Large,
        };
        Self { handle, len, class, flags: PayloadFlags::empty() }
    }
}
