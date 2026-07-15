use std::sync::{
    LazyLock,
    atomic::{AtomicU64, Ordering},
};

static STATE: LazyLock<AtomicU64> = LazyLock::new(|| {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    AtomicU64::new(nanos ^ 0x9e37_79b9_7f4a_7c15)
});

pub(crate) fn rand_u64() -> u64 {
    let mut value = STATE
        .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    #[test]
    fn consecutive_values_differ() {
        assert_ne!(super::rand_u64(), super::rand_u64());
    }
}
