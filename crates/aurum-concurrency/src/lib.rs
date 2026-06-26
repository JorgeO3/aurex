#![forbid(unsafe_code)]

use std::collections::VecDeque;

#[derive(Debug)]
pub struct PrototypeSpsc<T> {
    cap: usize,
    queue: VecDeque<T>,
}

impl<T> PrototypeSpsc<T> {
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self { cap, queue: VecDeque::with_capacity(cap) }
    }

    pub fn push(&mut self, value: T) -> Result<(), T> {
        if self.queue.len() == self.cap {
            return Err(value);
        }
        self.queue.push_back(value);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop_front()
    }
}
