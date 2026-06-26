use super::error::ConsumerError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchMode {
    Limited(u32),
    Unlimited,
}

#[derive(Debug, Clone, Copy)]
pub struct ConsumerCredit {
    mode: PrefetchMode,
    in_flight: u32,
}

impl ConsumerCredit {
    #[must_use]
    pub fn new(mode: PrefetchMode) -> Self {
        Self { mode, in_flight: 0 }
    }

    #[must_use]
    pub fn available(&self) -> u32 {
        match self.mode {
            PrefetchMode::Unlimited => u32::MAX,
            PrefetchMode::Limited(n) => n.saturating_sub(self.in_flight),
        }
    }

    pub fn reserve(&mut self, n: u32) -> Result<(), ConsumerError> {
        match self.mode {
            PrefetchMode::Unlimited => {
                self.in_flight = self.in_flight.saturating_add(n);
                Ok(())
            }
            PrefetchMode::Limited(limit) => {
                if self.in_flight.saturating_add(n) > limit {
                    Err(ConsumerError::InsufficientCredit)
                } else {
                    self.in_flight += n;
                    Ok(())
                }
            }
        }
    }

    pub fn release(&mut self, n: u32) {
        self.in_flight = self.in_flight.saturating_sub(n);
    }

    #[must_use]
    pub fn in_flight(&self) -> u32 {
        self.in_flight
    }

    #[must_use]
    pub fn prefetch_mode(&self) -> PrefetchMode {
        self.mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limited_credit_reserve_and_release() {
        let mut c = ConsumerCredit::new(PrefetchMode::Limited(10));
        assert_eq!(c.available(), 10);
        c.reserve(7).unwrap();
        assert_eq!(c.available(), 3);
        assert_eq!(c.in_flight(), 7);
        c.reserve(4).unwrap_err();
        c.release(3);
        assert_eq!(c.available(), 6);
        c.reserve(6).unwrap();
        assert_eq!(c.available(), 0);
    }

    #[test]
    fn unlimited_credit_never_fails() {
        let mut c = ConsumerCredit::new(PrefetchMode::Unlimited);
        c.reserve(u32::MAX).unwrap();
        c.release(u32::MAX);
    }
}
