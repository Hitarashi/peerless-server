pub const MAX_DOCUMENT_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_COLLECTION_TRACKS: u32 = 10_000;

/// Upper bound accepted for an operator-configured retry count (`ALAC_MAX_RETRIES`).
pub const MAX_RETRIES: u32 = 10;

/// Upper bound accepted for an operator-configured base retry delay (`ALAC_RETRY_BASE_MS`).
pub const MAX_RETRY_BASE_MS: u64 = 60_000;

pub fn validate_collection_limit(value: u32) -> bool {
    value <= MAX_COLLECTION_TRACKS
}
