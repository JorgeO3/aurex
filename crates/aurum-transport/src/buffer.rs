/// Simple growable read buffer with a hard cap for PR10 defensive limits.
#[derive(Debug)]
pub struct ReadBuffer {
    data: Vec<u8>,
    max_len: usize,
}

impl ReadBuffer {
    #[must_use]
    pub fn with_capacity(capacity: usize, max_len: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            max_len,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<(), usize> {
        if self.data.len() + bytes.len() > self.max_len {
            return Err(self.max_len);
        }
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    pub fn drain_prefix(&mut self, n: usize) {
        if n >= self.data.len() {
            self.data.clear();
        } else {
            self.data.drain(..n);
        }
    }
}

/// Write-side accumulator with a hard cap.
#[derive(Debug, Default)]
pub struct WriteBuffer {
    data: Vec<u8>,
    max_len: usize,
}

impl WriteBuffer {
    #[must_use]
    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            data: Vec::new(),
            max_len,
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn extend(&mut self, bytes: &[u8]) -> Result<(), usize> {
        if self.data.len() + bytes.len() > self.max_len {
            return Err(self.max_len);
        }
        self.data.extend_from_slice(bytes);
        Ok(())
    }
}
