//! Shared retry configuration and exponential backoff.
//!
//! Every retry loop in the workspace draws its backoff from [`exponential_delay`] so a
//! single place owns the saturating doubling arithmetic, and the operator-facing budget
//! lives on [`RetryConfig`] so `ALAC_MAX_RETRIES` / `ALAC_RETRY_BASE_MS` are parsed,
//! capped, and defaulted in exactly one place.

use std::time::Duration;

use crate::limits::{MAX_RETRIES, MAX_RETRY_BASE_MS};

/// Environment variable overriding [`RetryConfig::retries`].
pub const ENV_MAX_RETRIES: &str = "ALAC_MAX_RETRIES";

/// Environment variable overriding [`RetryConfig::base_delay_ms`].
pub const ENV_RETRY_BASE_MS: &str = "ALAC_RETRY_BASE_MS";

/// Retries performed after the first attempt when nothing is configured.
pub const DEFAULT_RETRIES: u32 = 3;

/// Base delay doubled by [`exponential_delay`] when nothing is configured.
pub const DEFAULT_RETRY_BASE_MS: u64 = 2_000;

/// Highest exponent [`exponential_delay`] applies. `u64::MAX` scaling is reached long
/// before this, so the clamp only keeps the shift itself from overflowing.
const MAX_BACKOFF_EXPONENT: u32 = 31;

/// Delay before the retry that follows `retry_index` retries already performed:
/// `base_delay_ms * 2^retry_index`, saturating instead of overflowing.
pub fn exponential_delay(base_delay_ms: u64, retry_index: u32) -> Duration {
    let multiplier = 1_u64
        .checked_shl(retry_index.min(MAX_BACKOFF_EXPONENT))
        .unwrap_or(u64::MAX);
    Duration::from_millis(base_delay_ms.saturating_mul(multiplier))
}

/// Jitter multiplier in `[0.8, 1.2)` applied on top of an exponential delay, so retries
/// from concurrent workers do not stay in lockstep.
pub fn jitter_multiplier() -> f64 {
    0.8 + jitter_fraction() * 0.4
}

/// Uniform sample in `[0, 1)` from a process-wide xorshift seeded by the wall clock.
fn jitter_fraction() -> f64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut state = STATE.load(Ordering::Relaxed);
    if state == 0 {
        state = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15)
            | 1;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    STATE.store(state, Ordering::Relaxed);
    (state >> 11) as f64 / (1u64 << 53) as f64
}

/// A retry budget expressed as a total attempt count plus a base delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts performed in total, including the first one.
    pub total_attempts: u32,
    /// Base delay [`exponential_delay`] doubles per retry.
    pub base_delay_ms: u64,
}

impl RetryPolicy {
    pub const fn new(total_attempts: u32, base_delay_ms: u64) -> Self {
        Self {
            total_attempts,
            base_delay_ms,
        }
    }

    /// Three attempts with no delay, for tests.
    pub const fn test() -> Self {
        Self::new(3, 0)
    }

    pub fn delay_before_retry(&self, retry_index: u32) -> Duration {
        exponential_delay(self.base_delay_ms, retry_index)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3, 500)
    }
}

/// Operator-facing retry budget: retries performed after the first attempt, plus the base
/// delay [`exponential_delay`] doubles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    pub retries: u32,
    pub base_delay_ms: u64,
}

impl RetryConfig {
    /// The budget used when neither environment variable is set.
    pub const DEFAULT: Self = Self {
        retries: DEFAULT_RETRIES,
        base_delay_ms: DEFAULT_RETRY_BASE_MS,
    };

    pub const fn new(retries: u32, base_delay_ms: u64) -> Self {
        Self {
            retries,
            base_delay_ms,
        }
    }

    /// Reads [`ENV_MAX_RETRIES`] and [`ENV_RETRY_BASE_MS`], falling back to [`Self::DEFAULT`]
    /// for values that are absent, unparsable, or above their cap.
    pub fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] with the environment lookup injected, so the parsing and capping
    /// rules can be exercised without mutating process state.
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let retries = lookup(ENV_MAX_RETRIES)
            .and_then(|value| value.parse().ok())
            .filter(|value| *value <= MAX_RETRIES)
            .unwrap_or(DEFAULT_RETRIES);
        let base_delay_ms = lookup(ENV_RETRY_BASE_MS)
            .and_then(|value| value.parse().ok())
            .filter(|value| *value <= MAX_RETRY_BASE_MS)
            .unwrap_or(DEFAULT_RETRY_BASE_MS);
        Self::new(retries, base_delay_ms)
    }

    /// Attempts this budget performs, including the first one.
    pub const fn total_attempts(self) -> u32 {
        self.retries.saturating_add(1)
    }

    /// The same budget as a [`RetryPolicy`], for loops that count attempts.
    pub const fn policy(self) -> RetryPolicy {
        RetryPolicy::new(self.total_attempts(), self.base_delay_ms)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_delay_doubles_and_saturates() {
        assert_eq!(exponential_delay(500, 0), Duration::from_millis(500));
        assert_eq!(exponential_delay(500, 3), Duration::from_millis(4000));
        assert_eq!(exponential_delay(0, 9), Duration::ZERO);
        assert_eq!(
            exponential_delay(u64::MAX, 32),
            Duration::from_millis(u64::MAX)
        );
    }

    #[test]
    fn jitter_multiplier_stays_within_the_twenty_percent_band() {
        for _ in 0..1_000 {
            let jitter = jitter_multiplier();
            assert!(
                (0.8..1.2).contains(&jitter),
                "jitter out of range: {jitter}"
            );
        }
    }

    #[test]
    fn retry_config_defaults_when_environment_is_empty() {
        let config = RetryConfig::from_lookup(|_| None);
        assert_eq!(config, RetryConfig::DEFAULT);
        assert_eq!(config.total_attempts(), DEFAULT_RETRIES + 1);
        assert_eq!(config.policy(), RetryPolicy::new(4, DEFAULT_RETRY_BASE_MS));
    }

    #[test]
    fn retry_config_reads_environment_overrides() {
        let config = RetryConfig::from_lookup(|key| match key {
            ENV_MAX_RETRIES => Some("6".to_owned()),
            ENV_RETRY_BASE_MS => Some("250".to_owned()),
            _ => None,
        });
        assert_eq!(config, RetryConfig::new(6, 250));
    }

    #[test]
    fn retry_config_falls_back_on_unparsable_or_over_cap_values() {
        for (retries, base_delay_ms, expected) in [
            ("nope", "3000", RetryConfig::new(DEFAULT_RETRIES, 3000)),
            ("999", "3000", RetryConfig::new(DEFAULT_RETRIES, 3000)),
            ("4", "600000", RetryConfig::new(4, DEFAULT_RETRY_BASE_MS)),
            ("4", "soon", RetryConfig::new(4, DEFAULT_RETRY_BASE_MS)),
        ] {
            let config = RetryConfig::from_lookup(|key| match key {
                ENV_MAX_RETRIES => Some(retries.to_owned()),
                ENV_RETRY_BASE_MS => Some(base_delay_ms.to_owned()),
                _ => None,
            });
            assert_eq!(
                config, expected,
                "retries={retries:?} base_delay_ms={base_delay_ms:?}"
            );
        }
    }
}
