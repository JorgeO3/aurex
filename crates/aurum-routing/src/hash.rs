/// Deterministic FNV-1a 64-bit hash for routing keys.
#[must_use]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::fnv1a64;

    #[test]
    fn fnv1a_is_deterministic() {
        assert_eq!(fnv1a64(b"orders"), fnv1a64(b"orders"));
        assert_ne!(fnv1a64(b"orders"), fnv1a64(b"payments"));
    }
}
