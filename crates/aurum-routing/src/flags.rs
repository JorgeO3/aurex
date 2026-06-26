use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct ExchangeFlags: u16 {
        const DURABLE      = 1 << 0;
        const AUTO_DELETE  = 1 << 1;
        const INTERNAL     = 1 << 2;
        const SYSTEM       = 1 << 3;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct BindingFlags: u16 {
        const ACTIVE = 1 << 0;
        const SYSTEM = 1 << 1;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct RouteFlags: u16 {
        const EMPTY                = 0;
        const FANOUT               = 1 << 0;
        const DIRECT               = 1 << 1;
        const HAS_MULTIPLE_TARGETS = 1 << 2;
        const UNROUTABLE           = 1 << 3;
    }
}
