use std::{future::Future, pin::Pin, time::Duration};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub mod bookkeeping;
pub mod cache;
pub mod delivery;
pub mod providers;

pub use bookkeeping::{
    RequestLog, TaskBookkeeping, TaskBookkeepingError, TaskBookkeepingOperation,
};
pub use cache::{
    AlbumCache, AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation,
    AlbumReplacementResult, AlbumUpload, CachedAlbum, CachedTrack, SaveTrackInput, TrackCache,
    TrackCacheError, TrackCacheOperation,
};
pub use delivery::{
    ChatDelivery, ChatMessageRef, ChatRef, Delivery, DeliveryError, DeliveryReceipt,
    DeliveryRejection, DumpMessageRef, DumpPublication, DumpPublish, UploadProgressCallback,
};
pub use providers::{
    ArtworkProvider, CollectionResolver, ProviderDeps, ProviderPresentation, Storefront,
    TrackAcquisition,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRetryPolicy {
    pub total_attempts: u32,
    pub base_delay_ms: u64,
}

impl StorageRetryPolicy {
    pub const fn new(total_attempts: u32, base_delay_ms: u64) -> Self {
        Self {
            total_attempts,
            base_delay_ms,
        }
    }

    pub const fn test() -> Self {
        Self::new(3, 0)
    }

    pub fn delay_before_retry(&self, retry_index: u32) -> Duration {
        let exponent = retry_index.min(31);
        let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        Duration::from_millis(self.base_delay_ms.saturating_mul(multiplier))
    }
}

impl Default for StorageRetryPolicy {
    fn default() -> Self {
        Self::new(3, 500)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    pub storage_retry: StorageRetryPolicy,
    pub upload_retry_base_ms: u64,
    pub upload_max_retries: u32,
}

impl OrchestratorConfig {
    pub const fn test() -> Self {
        Self {
            storage_retry: StorageRetryPolicy::test(),
            upload_retry_base_ms: 0,
            upload_max_retries: 0,
        }
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            storage_retry: StorageRetryPolicy::default(),
            upload_retry_base_ms: 2000,
            upload_max_retries: 3,
        }
    }
}

pub trait TaskDeps:
    TrackCache + AlbumCache + ProviderDeps + TaskBookkeeping + Delivery + Send + Sync + 'static
{
}

impl<T> TaskDeps for T where
    T: TrackCache + AlbumCache + ProviderDeps + TaskBookkeeping + Delivery + Send + Sync + 'static
{
}
