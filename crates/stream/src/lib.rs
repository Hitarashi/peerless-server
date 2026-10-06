pub mod cache;
pub mod circuit_breaker;
pub mod engine;
pub mod metrics;
pub mod pipe;
pub mod worker_pool;

pub use cache::{CHUNK_SIZE, ChunkCache, ChunkCacheMetricsSnapshot, ChunkKey};
pub use circuit_breaker::CircuitBreaker;
pub use engine::{AudioStreamHeaders, AudioStreamResponse, StreamEngine, TrackMediaMetadata};
pub use metrics::{StreamMetrics, StreamMetricsSnapshot};
pub use pipe::{ByteRange, ChunkStream, LocationRefresher, StreamPipeParams, create_stream_pipe};
pub use worker_pool::{StreamWorkerPool, hash_bot_token};

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("File reference expired")]
    FileReferenceExpired,

    #[error("Flood wait: {0} seconds")]
    FloodWait(u64),

    #[error("All workers quarantined or unavailable")]
    AllWorkersUnavailable,

    #[error("Unsupported CDN redirect")]
    UnsupportedCdnRedirect,

    #[error("Chunk fetch retry budget exhausted: {0}")]
    ChunkFetchFailed(String),

    #[error("Telegram connect error: {0}")]
    Connect(#[from] ferogram::QuickConnectError),

    #[error("Telegram invocation error: {0}")]
    Telegram(#[from] ferogram::InvocationError),

    #[error("Database error: {0}")]
    Db(#[from] db::DbError),

    #[error("Track not found: {0}")]
    TrackNotFound(i32),

    #[error("No media document found for track {0}")]
    NoMediaDocument(i32),

    #[error("Invalid range: {0}")]
    InvalidRange(String),
}
