use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_is_monotonic_enough_to_be_a_clock() {
        let first = now_ms();
        assert!(first > 1_600_000_000_000, "expected a post-2020 timestamp");
        let second = now_ms();
        assert!(
            second >= first,
            "clock moved backwards: {first} -> {second}"
        );
    }
}
