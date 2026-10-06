use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use moka::future::Cache;

pub const CHUNK_SIZE: usize = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkKey {
    pub document_id: i64,
    pub chunk_index: u64,
}

impl ChunkKey {
    pub const fn new(document_id: i64, chunk_index: u64) -> Self {
        Self {
            document_id,
            chunk_index,
        }
    }
}

#[derive(Clone)]
pub struct ChunkCache {
    cache: Cache<ChunkKey, Bytes>,
    metrics: Arc<ChunkCacheMetrics>,
}

#[derive(Default)]
struct ChunkCacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ChunkCacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
}

impl ChunkCache {
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .weigher(|_key, val: &Bytes| val.len() as u32)
            .max_capacity(max_capacity_bytes)
            .build();
        Self {
            cache,
            metrics: Arc::new(ChunkCacheMetrics::default()),
        }
    }

    pub async fn get(&self, document_id: i64, chunk_index: u64) -> Option<Bytes> {
        self.get_by_key(ChunkKey::new(document_id, chunk_index))
            .await
    }

    pub async fn insert(&self, document_id: i64, chunk_index: u64, data: Bytes) {
        self.insert_by_key(ChunkKey::new(document_id, chunk_index), data)
            .await;
    }

    pub async fn get_by_key(&self, key: ChunkKey) -> Option<Bytes> {
        let value = self.cache.get(&key).await;
        if value.is_some() {
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    pub async fn get_or_fetch<E>(
        &self,
        document_id: i64,
        chunk_index: u64,
        fetch: impl Future<Output = Result<Bytes, E>>,
    ) -> Result<Bytes, Arc<E>>
    where
        E: Send + Sync + 'static,
    {
        let key = ChunkKey::new(document_id, chunk_index);
        if let Some(value) = self.get_by_key(key).await {
            return Ok(value);
        }

        self.cache.try_get_with(key, fetch).await
    }

    pub async fn insert_by_key(&self, key: ChunkKey, data: Bytes) {
        self.cache.insert(key, data).await;
    }

    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub fn metrics_snapshot(&self) -> ChunkCacheMetricsSnapshot {
        ChunkCacheMetricsSnapshot {
            hits: self.metrics.hits.load(Ordering::Relaxed),
            misses: self.metrics.misses.load(Ordering::Relaxed),
        }
    }
}

impl Default for ChunkCache {
    fn default() -> Self {
        Self::new(256 * 1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_inserts_and_retrieves() {
        let cache = ChunkCache::new(10 * 1024 * 1024);
        let data = Bytes::from_static(b"lossless audio chunk 0");
        cache.insert(12345, 0, data.clone()).await;

        let retrieved = cache.get(12345, 0).await;
        assert_eq!(retrieved, Some(data.clone()));

        assert_eq!(cache.get_by_key(ChunkKey::new(12345, 0)).await, Some(data));
        assert_eq!(cache.get(12345, 1).await, None);
    }

    #[tokio::test]
    async fn cache_bounds_memory_weight() {
        let cache = ChunkCache::new(1024);
        let chunk = Bytes::from(vec![1u8; 600]);

        cache.insert(1, 0, chunk.clone()).await;
        cache.insert(1, 1, chunk.clone()).await;

        cache.cache.run_pending_tasks().await;

        assert!(cache.weighted_size() <= 1024);
    }
}
