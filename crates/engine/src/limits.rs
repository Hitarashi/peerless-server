//! Shared safety limits used at the boundaries of the bot and engine.

use std::time::Duration;

pub const MAX_DOCUMENT_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
pub const MAX_PROCESS_OUTPUT_BYTES: usize = 1024 * 1024;
pub const PROCESS_TIMEOUT: Duration = Duration::from_secs(15 * 60);
pub const MAX_COLLECTION_TRACKS: u32 = 10_000;
pub const MAX_RETRIES: u32 = 10;
pub const MAX_RETRY_BASE_MS: u64 = 60_000;

/// Validate values which can be supplied through the environment or settings
/// UI. Keeping this in the engine prevents the bot and persistence layers from
/// accepting different effective limits.
pub fn validate_collection_limit(value: u32) -> bool {
    value <= MAX_COLLECTION_TRACKS
}
