use std::collections::HashMap;

use music::{Codec, TrackRipResult};

#[derive(Debug, Clone, PartialEq)]
pub struct CachedTrack {
    pub track_id: String,
    pub codec: Codec,
    pub message_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SaveTrackInput {
    pub track_id: String,
    pub codec: Codec,
    pub message_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
}

impl SaveTrackInput {
    pub fn from_rip_result(
        track_id: &str,
        rip: &TrackRipResult,
        message_id: i64,
        file_id: &str,
        file_unique_id: &str,
    ) -> Self {
        let codec = rip.codec.parse::<Codec>().unwrap_or(Codec::Alac);
        Self {
            track_id: track_id.to_owned(),
            codec,
            message_id,
            file_id: file_id.to_owned(),
            file_unique_id: file_unique_id.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlbumUpload {
    pub album_id: String,
    pub codec: Codec,
    pub part_index: i32,
    pub total_parts: i32,
    pub message_id: i64,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: i64,
    pub generation_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlbumReplacementExpectation {
    Empty,
    Generation(String),
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlbumReplacementResult {
    Committed { displaced_message_ids: Vec<i64> },
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedAlbum {
    pub part_index: i32,
    pub total_parts: i32,
    pub message_id: i64,
    pub file_unique_id: String,
    pub generation_hash: String,
    pub file_size: i64,
    pub codec: Codec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackCacheOperation {
    Find,
    Save,
    Delete,
}

impl std::fmt::Display for TrackCacheOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Find => "find track",
            Self::Save => "save track",
            Self::Delete => "delete track",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrackCacheError {
    #[error("{operation} unavailable: {detail}")]
    Unavailable {
        operation: TrackCacheOperation,
        detail: String,
    },
    #[error("{operation} failed: {detail}")]
    Failed {
        operation: TrackCacheOperation,
        detail: String,
    },
}

impl TrackCacheError {
    pub fn unavailable(operation: TrackCacheOperation, detail: impl Into<String>) -> Self {
        Self::Unavailable {
            operation,
            detail: detail.into(),
        }
    }

    pub fn failed(operation: TrackCacheOperation, detail: impl Into<String>) -> Self {
        Self::Failed {
            operation,
            detail: detail.into(),
        }
    }

    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlbumCacheOperation {
    Find,
    Save,
    Replace,
    Delete,
}

impl std::fmt::Display for AlbumCacheOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Find => "find album",
            Self::Save => "save album",
            Self::Replace => "replace album",
            Self::Delete => "delete album",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AlbumCacheError {
    #[error("{operation} unavailable: {detail}")]
    Unavailable {
        operation: AlbumCacheOperation,
        detail: String,
    },
    #[error("{operation} conflicted: {detail}")]
    Conflict {
        operation: AlbumCacheOperation,
        detail: String,
    },
    #[error("{operation} failed: {detail}")]
    Failed {
        operation: AlbumCacheOperation,
        detail: String,
    },
}

impl AlbumCacheError {
    pub fn unavailable(operation: AlbumCacheOperation, detail: impl Into<String>) -> Self {
        Self::Unavailable {
            operation,
            detail: detail.into(),
        }
    }

    pub fn conflict(operation: AlbumCacheOperation, detail: impl Into<String>) -> Self {
        Self::Conflict {
            operation,
            detail: detail.into(),
        }
    }

    pub fn failed(operation: AlbumCacheOperation, detail: impl Into<String>) -> Self {
        Self::Failed {
            operation,
            detail: detail.into(),
        }
    }

    pub fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable { .. })
    }
}

pub type CachedTracksMap = HashMap<(String, Codec), CachedTrack>;
