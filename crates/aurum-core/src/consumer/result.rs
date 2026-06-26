use smallvec::SmallVec;

use crate::queue::AckRange;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AckApplyResult {
    pub acked: u32,
    pub released_credit: u32,
    pub ranges: SmallVec<[AckRange; 4]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NackApplyResult {
    pub nacked: u32,
    pub requeued: u32,
    pub dropped: u32,
    pub dead_lettered: u32,
    pub released_credit: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelDisposition {
    RequeueUnacked,
    DropUnacked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CancelResult {
    pub requeued: u32,
    pub dropped: u32,
}

impl CancelResult {
    #[must_use]
    pub fn total(&self) -> u32 {
        self.requeued + self.dropped
    }
}
