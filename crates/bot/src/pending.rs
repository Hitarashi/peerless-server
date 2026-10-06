//! TTL-bounded store for single-use callback confirmation payloads.
//!
//! Confirmation flows (`/delete`, `/import`) each keep a token-keyed map of pending
//! payloads that must disappear after a short window. The lifetime rules are identical,
//! so they live here once instead of being re-derived per handler.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

struct Entry<T> {
    value: T,
    expires_at: Instant,
}

/// Map from an opaque confirmation token to a payload, pruned on every access so an
/// unread entry cannot outlive its window.
pub(crate) struct PendingConfirmations<T> {
    entries: Mutex<HashMap<String, Entry<T>>>,
    ttl: Duration,
}

impl<T> PendingConfirmations<T> {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Stores `value` under a fresh token and returns that token.
    pub(crate) fn insert(&self, value: T) -> String {
        let token = next_token();
        let now = Instant::now();
        let mut entries = self.lock();
        entries.retain(|_, entry| entry.expires_at > now);
        entries.insert(
            token.clone(),
            Entry {
                value,
                expires_at: now + self.ttl,
            },
        );
        token
    }

    /// Removes and returns the payload behind `token`, or `None` when it is unknown or
    /// expired. A token is only ever handed out once.
    pub(crate) fn take(&self, token: &str) -> Option<T> {
        let mut entries = self.lock();
        entries.retain(|_, entry| entry.expires_at > Instant::now());
        entries.remove(token).map(|entry| entry.value)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry<T>>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Process-unique, monotonic confirmation token.
fn next_token() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_payload_is_returned_once_then_forgotten() {
        let pending = PendingConfirmations::new(Duration::from_secs(120));
        let token = pending.insert("payload");
        assert_eq!(pending.take(&token), Some("payload"));
        assert_eq!(pending.take(&token), None);
    }

    #[test]
    fn tokens_are_distinct_per_insert() {
        let pending = PendingConfirmations::new(Duration::from_secs(120));
        let first = pending.insert(1_u8);
        let second = pending.insert(2_u8);
        assert_ne!(first, second);
        assert_eq!(pending.take(&first), Some(1));
        assert_eq!(pending.take(&second), Some(2));
    }

    #[test]
    fn expired_payload_is_not_handed_out() {
        let pending = PendingConfirmations::new(Duration::ZERO);
        let token = pending.insert("payload");
        assert_eq!(pending.take(&token), None);
    }

    #[test]
    fn inserting_prunes_previously_expired_entries() {
        let pending = PendingConfirmations::new(Duration::ZERO);
        pending.insert("stale");
        pending.insert("fresh");
        assert_eq!(pending.lock().len(), 1, "expired entry pruned on insert");
    }
}
