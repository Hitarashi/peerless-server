//! Wall-clock helpers shared across the workspace.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch, or `0` if the clock predates it.
///
/// The single workspace millisecond clock. `u64` milliseconds is ~584 million
/// years, so the width is never the binding constraint and callers must not
/// widen it — a `u128` variant of this function previously existed and forced
/// needless `as` casts at every call site.
///
/// A system clock before 1970 yields `0` rather than panicking; every caller
/// here treats the value as an opaque elapsed-time baseline, never as an
/// absolute schedule, so the fallback is harmless.
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
