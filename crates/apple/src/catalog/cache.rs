use std::time::{Duration, Instant};

use indexmap::IndexMap;

struct Entry<T> {
    value: T,
    expires_at: Instant,
}

pub(crate) struct Cache<T> {
    map: IndexMap<String, Entry<T>>,
    max_size: usize,
    default_ttl: Duration,
}

impl<T: Clone> Cache<T> {
    pub(crate) fn new(max_size: usize, default_ttl: Duration) -> Self {
        Self {
            map: IndexMap::new(),
            max_size,
            default_ttl,
        }
    }

    pub(crate) fn get(&mut self, key: &str, now: Instant) -> Option<T> {
        let entry = self.map.shift_remove(key)?;
        if now > entry.expires_at {
            return None;
        }
        let Entry { value, expires_at } = entry;
        self.map.insert(
            key.to_owned(),
            Entry {
                value: value.clone(),
                expires_at,
            },
        );
        Some(value)
    }

    pub(crate) fn set(&mut self, key: &str, value: T, now: Instant) {
        let ttl = self.default_ttl;
        self.set_with_ttl(key, value, ttl, now);
    }

    pub(crate) fn set_with_ttl(&mut self, key: &str, value: T, ttl: Duration, now: Instant) {
        if self.map.len() >= self.max_size {
            self.map.shift_remove_index(0);
        }
        self.map.insert(
            key.to_owned(),
            Entry {
                value,
                expires_at: now + ttl,
            },
        );
    }

    pub(crate) fn clear(&mut self) {
        self.map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_refreshes_recency_and_evicts_front() {
        let mut cache: Cache<u32> = Cache::new(2, Duration::from_secs(60));
        let now = Instant::now();
        cache.set("a", 1, now);
        cache.set("b", 2, now);
        assert_eq!(cache.get("a", now), Some(1));
        cache.set("c", 3, now);
        assert_eq!(cache.get("b", now), None, "b evicted as LRU");
        assert_eq!(cache.get("a", now), Some(1));
        assert_eq!(cache.get("c", now), Some(3));
    }

    #[test]
    fn expired_entries_are_gone() {
        let mut cache: Cache<u32> = Cache::new(10, Duration::from_millis(20));
        let now = Instant::now();
        cache.set("x", 7, now);
        assert_eq!(cache.get("x", now), Some(7));
        assert_eq!(cache.get("x", now + Duration::from_millis(30)), None);
    }

    #[test]
    fn update_at_capacity_still_evicts_front() {
        let mut cache: Cache<u32> = Cache::new(2, Duration::from_secs(60));
        let now = Instant::now();
        cache.set("a", 1, now);
        cache.set("b", 2, now);
        cache.set("a", 10, now);
        assert_eq!(cache.get("a", now), Some(10));
        assert_eq!(cache.get("b", now), Some(2));

        cache.set("c", 3, now);
        assert_eq!(cache.get("a", now), None, "a was front after b's refresh");
        assert_eq!(cache.get("b", now), Some(2));
        assert_eq!(cache.get("c", now), Some(3));
    }

    #[test]
    fn zero_capacity_degenerates_to_one() {
        let mut cache: Cache<u32> = Cache::new(0, Duration::from_secs(60));
        let now = Instant::now();
        cache.set("a", 1, now);
        assert_eq!(cache.get("a", now), Some(1));
        cache.set("b", 2, now);
        assert_eq!(cache.get("a", now), None, "a evicted");
        assert_eq!(cache.get("b", now), Some(2));
    }
}
