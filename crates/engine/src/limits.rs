use std::time::Duration;

pub use peerless_core::limits::{
    MAX_COLLECTION_TRACKS, MAX_DOCUMENT_BYTES, MAX_RETRIES, MAX_RETRY_BASE_MS,
    validate_collection_limit,
};

pub const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
pub const MAX_PROCESS_OUTPUT_BYTES: usize = 1024 * 1024;
pub const PROCESS_TIMEOUT: Duration = Duration::from_secs(15 * 60);
