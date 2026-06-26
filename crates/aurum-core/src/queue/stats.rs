#[derive(Debug, Default, Clone, Copy)]
pub struct QueueStats {
    pub total_published: u64,
    pub total_delivered: u64,
    pub total_acked: u64,
    pub total_nacked: u64,
    pub total_retried: u64,
}
