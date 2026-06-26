#![forbid(unsafe_code)]

use core::fmt;

use smallvec::SmallVec;

pub type Seq = u64;
pub type BlockIndex = u32;
pub type WordIndex = u8;

// ── Queue / routing IDs ───────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct QueueId(pub u32);

impl fmt::Debug for QueueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "q{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ExchangeId(pub u32);

impl fmt::Debug for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ex{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct RouteId {
    pub index: u32,
    pub generation: u32,
}

impl Default for RouteId {
    fn default() -> Self {
        Self { index: 0, generation: 0 }
    }
}

impl RouteId {
    #[must_use]
    pub const fn new(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    #[must_use]
    pub const fn index(self) -> u32 {
        self.index
    }

    #[must_use]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

impl fmt::Debug for RouteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "route#{}:{}", self.index, self.generation)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct RouteTableVersion(pub u64);

impl RouteTableVersion {
    pub const INITIAL: Self = Self(1);

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Debug for RouteTableVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rtv{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct RoutingKeyHash(pub u64);

impl fmt::Debug for RoutingKeyHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rk#{:016x}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct QueueSetId(pub u32);

impl fmt::Debug for QueueSetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "qset{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ShardId(pub u16);

impl fmt::Debug for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shard{}", self.0)
    }
}

// ── Connection / session / producer IDs ──────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ConnectionId(pub u64);

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "conn#{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct SourceId(pub u64);

impl fmt::Debug for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "src#{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ProducerId(pub u64);

impl fmt::Debug for ProducerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer#{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ConsumerId(pub u64);

impl fmt::Debug for ConsumerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "consumer#{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ChannelId(pub u32);

impl fmt::Debug for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ch#{}", self.0)
    }
}

// ── Command / batch correlation IDs ──────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct BatchId(pub u64);

impl fmt::Debug for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "batch#{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct CommandId(pub u64);

impl fmt::Debug for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cmd#{}", self.0)
    }
}

// ── Delivery tag ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct DeliveryTag(pub u64);

impl DeliveryTag {
    /// Sentinel for an unset or invalid tag (never emitted by a session).
    pub const INVALID: Self = Self(0);
    /// First tag ever emitted by a fresh session.
    pub const FIRST: Self = Self(1);
}

impl fmt::Debug for DeliveryTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tag#{}", self.0)
    }
}

// ── Epoch tokens ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct RouteEpoch(pub u32);

impl RouteEpoch {
    pub const ZERO: Self = Self(0);
}

impl fmt::Debug for RouteEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "re{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ShardEpoch(pub u32);

impl ShardEpoch {
    pub const ZERO: Self = Self(0);
}

impl fmt::Debug for ShardEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "se{}", self.0)
    }
}

// ── Payload handle ────────────────────────────────────────────────────────────

/// Opaque handle to a payload in a backend-specific storage (arena, io_uring
/// buffer, segment log, etc.).  PR4: treated as a u64 sequence number.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct PayloadHandle(pub u64);

impl fmt::Debug for PayloadHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ph#{}", self.0)
    }
}

// ── Delivery core types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryRange {
    pub start_seq: Seq,
    pub len: u32,
}

impl DeliveryRange {
    #[must_use]
    pub const fn new(start_seq: Seq, len: u32) -> Self {
        Self { start_seq, len }
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryMask {
    pub block: BlockIndex,
    pub word: WordIndex,
    pub mask: u64,
}

impl DeliveryMask {
    #[must_use]
    pub const fn new(block: BlockIndex, word: WordIndex, mask: u64) -> Self {
        Self { block, word, mask }
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.mask == 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeliveryWork {
    pub ranges: SmallVec<[DeliveryRange; 4]>,
    pub masks: SmallVec<[DeliveryMask; 4]>,
}

impl DeliveryWork {
    pub fn clear(&mut self) {
        self.ranges.clear();
        self.masks.clear();
    }

    #[must_use]
    pub fn delivered_messages(&self) -> u64 {
        let range_total: u64 = self.ranges.iter().map(|r| u64::from(r.len)).sum();
        let mask_total: u64 = self.masks.iter().map(|m| u64::from(m.mask.count_ones())).sum();
        range_total + mask_total
    }
}

// ── Misc neutral enums ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMode {
    Fifo,
    Work,
    Keyed,
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    PublishBatch,
    ConsumeStart,
    CreditUpdate,
    AckBatch,
    NackBatch,
    ResolveRoute,
    Admin,
}
