#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(pub u64);

impl ConnectionId {
    pub const INVALID: Self = Self(0);

    #[must_use]
    pub fn is_valid(self) -> bool {
        self.0 != 0
    }
}
