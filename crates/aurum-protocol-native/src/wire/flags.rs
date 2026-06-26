use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct FrameFlags: u16 {
        const NONE        = 0;
        const RESPONSE    = 1 << 0;
        const EVENT       = 1 << 1;
        const ERROR       = 1 << 2;
        const COMPRESSED  = 1 << 3;
        const HAS_EXT     = 1 << 4;
        const MORE        = 1 << 5;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct NativeCapabilities: u64 {
        const ROUTE_ID        = 1 << 0;
        const PUBLISH_BATCH   = 1 << 1;
        const ACK_RANGE       = 1 << 2;
        const ACK_MASK        = 1 << 3;
        const NACK_RANGE      = 1 << 4;
        const NACK_MASK       = 1 << 5;
        const DELIVERY_BATCH  = 1 << 6;
        const COMPRESSION     = 1 << 7;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct NativeMessageFlags: u16 {
        const PERSISTENT   = 1 << 0;
        const MANDATORY    = 1 << 1;
        const COMPRESSED   = 1 << 2;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct NativeDeliveryFlags: u16 {
        const REDELIVERED = 1 << 0;
        const RANGE_START = 1 << 1;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct NativeConsumerFlags: u16 {
        const MANUAL_ACK = 1 << 0;
        const EXCLUSIVE  = 1 << 1;
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct CreditFlags: u16 {
        const ABSOLUTE = 1 << 0;
    }
}

pub fn validate_frame_flags(flags: FrameFlags) -> Result<(), crate::wire::error_code::NativeErrorCode> {
    if flags.intersects(FrameFlags::COMPRESSED | FrameFlags::HAS_EXT | FrameFlags::MORE) {
        return Err(crate::wire::error_code::NativeErrorCode::InvalidFlags);
    }
    Ok(())
}
