use std::{future::Future, pin::Pin};

pub use peerless_core::retry::{RetryConfig, RetryPolicy as StorageRetryPolicy};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub mod bookkeeping;
pub mod cache;
pub mod delivery;
pub mod providers;

pub use bookkeeping::TaskBookkeeping;
pub use cache::{
    AlbumCache, AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation,
    AlbumReplacementResult, AlbumUpload, CachedAlbum, CachedTrack, CachedTracksMap, SaveTrackInput,
    TrackCache, TrackCacheError, TrackCacheOperation,
};
pub use delivery::{
    AlbumDetailsCaption, ChatDelivery, ChatMessageRef, ChatRef, Delivery, DeliveryError,
    DeliveryReceipt, DeliveryRejection, DumpMessageRef, DumpPublication, DumpPublish, TrackCaption,
    UploadProgressCallback, ZipCaption,
};
pub use providers::{
    ArtworkProvider, CollectionResolver, ProviderDeps, ProviderPresentation, Storefront,
    TrackAcquisition,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestratorConfig {
    /// Retries for the storage cache reads/writes that block media work.
    pub storage_retry: StorageRetryPolicy,
    /// Retries for publishing to the dump channel.
    pub upload_retry: RetryConfig,
}

impl OrchestratorConfig {
    pub const fn test() -> Self {
        Self {
            storage_retry: StorageRetryPolicy::test(),
            upload_retry: RetryConfig::new(0, 0),
        }
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            storage_retry: StorageRetryPolicy::default(),
            upload_retry: RetryConfig::DEFAULT,
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
