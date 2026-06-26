use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct BrokerMetrics {
    pub accepted_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub frames_in: AtomicU64,
    pub frames_out: AtomicU64,
    pub commands_in: AtomicU64,
    pub commands_failed: AtomicU64,
    pub publish_confirmed: AtomicU64,
    pub deliveries_sent: AtomicU64,
    pub acks_applied: AtomicU64,
    pub nacks_applied: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct BrokerMetricsSnapshot {
    pub accepted_connections: u64,
    pub active_connections: u64,
    pub frames_in: u64,
    pub frames_out: u64,
    pub commands_in: u64,
    pub commands_failed: u64,
    pub publish_confirmed: u64,
    pub deliveries_sent: u64,
    pub acks_applied: u64,
    pub nacks_applied: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl BrokerMetrics {
    pub fn snapshot(&self) -> BrokerMetricsSnapshot {
        BrokerMetricsSnapshot {
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            frames_in: self.frames_in.load(Ordering::Relaxed),
            frames_out: self.frames_out.load(Ordering::Relaxed),
            commands_in: self.commands_in.load(Ordering::Relaxed),
            commands_failed: self.commands_failed.load(Ordering::Relaxed),
            publish_confirmed: self.publish_confirmed.load(Ordering::Relaxed),
            deliveries_sent: self.deliveries_sent.load(Ordering::Relaxed),
            acks_applied: self.acks_applied.load(Ordering::Relaxed),
            nacks_applied: self.nacks_applied.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        }
    }

    pub fn record_command_output(&self, output: &crate::in_memory::ShardOutputBatch) {
        self.publish_confirmed
            .fetch_add(u64::from(output.total_confirmed()), Ordering::Relaxed);
        self.deliveries_sent
            .fetch_add(output.total_delivered() as u64, Ordering::Relaxed);
        self.acks_applied.fetch_add(
            u64::from(output.settlements.iter().map(|s| s.settled).sum::<u32>()),
            Ordering::Relaxed,
        );
        if !output.errors.is_empty() {
            self.commands_failed
                .fetch_add(output.errors.len() as u64, Ordering::Relaxed);
        }
    }
}
