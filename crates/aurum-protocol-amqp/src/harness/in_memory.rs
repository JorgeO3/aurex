//! Transport-neutral AMQP transcript harnesses live in `aurum-broker`.
//!
//! Use [`aurum_broker::in_memory::AmqpInMemoryHarness`] for in-process AMQP
//! bytes → broker → response frame testing. This crate owns the session and wire
//! codec; the broker crate wires them to an in-memory executor.

/// Minimal harness surface for transcript-style integration tests.
///
/// Concrete implementations (e.g. `AmqpInMemoryHarness` in `aurum-broker`) own
/// broker state and drive an [`crate::AmqpSession`].
pub trait AmqpTranscriptHarness {
    fn send_bytes(&mut self, bytes: &[u8]) -> Vec<u8>;
}
