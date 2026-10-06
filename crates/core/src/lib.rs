//! Storage-facing shared types.
//!
//! These types form the vocabulary that both the persistence layer (`db`) and
//! the rip orchestration engine (`engine`) speak. They live here so that `db`
//! does not need to depend on `engine`.

pub mod cache;
pub mod limits;
pub mod retry;
pub mod settings;

pub use cache::{
    AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation, AlbumReplacementResult,
    AlbumUpload, CachedAlbum, CachedTrack, CachedTracksMap, SaveTrackInput, TrackCacheError,
    TrackCacheOperation,
};
pub use limits::{
    MAX_COLLECTION_TRACKS, MAX_DOCUMENT_BYTES, MAX_RETRIES, MAX_RETRY_BASE_MS,
    validate_collection_limit,
};
pub use retry::{RetryConfig, RetryPolicy, exponential_delay, jitter_multiplier};
pub use settings::{
    BotSettings, FALLBACK_STOREFRONT, LyricspornApiEndpoint, RippingMode, default_settings,
    normalize_lyricsporn_api_url, resolve_default_storefront,
};
