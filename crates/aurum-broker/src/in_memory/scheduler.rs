/// Simple round-robin delivery scheduler for a single queue.
#[derive(Debug, Clone, Copy)]
pub struct SimpleDeliveryScheduler {
    pub max_delivery_passes: u32,
}

impl Default for SimpleDeliveryScheduler {
    fn default() -> Self {
        Self { max_delivery_passes: 16 }
    }
}
