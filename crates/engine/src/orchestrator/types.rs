use std::sync::Arc;

pub use music::{
    CodecPreference, Rendition, RenditionPolicy, RenditionWorkPlan, RenditionWorkUnit,
};
use tokio_util::sync::CancellationToken;

use super::deps::ChatMessageRef;
use crate::types::{ParsedTargetItem, Provider, TargetKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPhase {
    Resolving,
    CheckingCache,
    Queued,
    Processing,
    Delivering,
    WaitingDuplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalTaskState {
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ActiveRipTask {
    pub id: String,
    pub provider: Provider,
    pub source_track_ids: Vec<String>,
    pub chat_id: i64,

    pub delivery_chat_id: i64,
    pub user_id: i64,
    pub user_name: Option<String>,
    pub job_header: String,
    pub total_tracks: usize,

    pub controller: CancellationToken,
    pub is_cancelled: bool,
    pub cancelled_by: Option<String>,
    pub cached_count: usize,
    pub ripped_count: usize,
    pub failed_count: usize,
    pub completed: bool,
    pub start_time_ms: u64,

    pub queue_position: Option<u64>,
    pub phase: TaskPhase,
    pub terminal_state: Option<TerminalTaskState>,
    pub skipped_count: usize,
    pub is_cache_only: bool,
    pub is_group: bool,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct RipTaskOptions {
    pub provider: Provider,
    pub chat_id: i64,
    pub user_id: i64,
    pub user_name: Option<String>,

    pub delivery_chat_id: i64,
    pub is_group: bool,
    pub is_force: bool,
    pub is_cache_only: bool,
    pub single_storefront: Option<String>,
    pub parsed_items: Vec<ParsedTargetItem>,
    pub reply_to_message_id: Option<i64>,
    pub is_admin: bool,

    pub codec_preference: Option<CodecPreference>,

    pub rendition_policy: RenditionPolicy,
}

#[cfg(test)]
mod rendition_tests {
    use music::Codec;

    use super::*;

    #[test]
    fn optional_atmos_plan_is_track_major_and_constrained() {
        let plan = RenditionPolicy::PrimaryWithOptionalAtmos.work_plan(["one", "two"]);
        assert_eq!(
            plan.units()
                .iter()
                .map(|unit| (unit.track_id(), unit.rendition(), unit.required()))
                .collect::<Vec<_>>(),
            vec![
                ("one", Rendition::Primary, true),
                ("one", Rendition::Atmos, false),
                ("two", Rendition::Primary, true),
                ("two", Rendition::Atmos, false),
            ]
        );
        assert_eq!(
            plan.units()[0].accepted_cache_codecs(),
            &[Codec::Alac, Codec::Aac]
        );
        assert_eq!(plan.units()[1].accepted_cache_codecs(), &[Codec::Ec3]);
    }

    #[test]
    fn primary_only_plan_has_no_atmos_unit() {
        let plan = RenditionPolicy::PrimaryOnly.work_plan(["one"]);
        assert_eq!(plan.units().len(), 1);
        assert_eq!(
            plan.units()[0].codec_preference(),
            music::CodecPreference::HighestQuality
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteProgress {
    pub completed: u64,
    pub total: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackLabel {
    pub title: String,
    pub artist: String,
    pub artwork_url: Option<String>,
    pub track_index: Option<u32>,
    pub total_tracks: Option<u32>,
}

impl TrackLabel {
    pub fn new(title: impl Into<String>, artist: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            artist: artist.into(),
            artwork_url: None,
            track_index: None,
            total_tracks: None,
        }
    }

    pub fn with_position(mut self, track_index: Option<u32>, total_tracks: Option<u32>) -> Self {
        self.track_index = track_index;
        self.total_tracks = total_tracks;
        self
    }

    pub fn with_artwork_url(mut self, artwork_url: Option<String>) -> Self {
        self.artwork_url = artwork_url.filter(|url| !url.is_empty());
        self
    }

    pub fn from_meta(meta: &crate::types::TrackMeta) -> Self {
        Self {
            title: meta.title.clone(),
            artist: meta.artist.clone(),
            artwork_url: (!meta.artwork_url.is_empty()).then(|| meta.artwork_url.clone()),
            track_index: meta
                .track_number
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| *n > 0),
            total_tracks: meta
                .track_count
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| *n > 0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RipActivity {
    ResolvingMetadata,
    Connecting {
        track: TrackLabel,
    },
    Downloading {
        track: TrackLabel,
        progress: ByteProgress,
    },
    MaterializingCachedMedia {
        track: TrackLabel,
        progress: ByteProgress,
    },
    Decrypting {
        track: TrackLabel,
    },
    Tagging {
        track: TrackLabel,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadLane {
    Rip(RipActivity),
    CachedDelivery { track: TrackLabel },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadLane {
    Track {
        track: TrackLabel,
        progress: ByteProgress,
    },
    ArchiveBuild {
        archive: String,
        progress: ByteProgress,
    },
    ArchiveUpload {
        archive: String,
        progress: ByteProgress,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskActivity {
    Resolving,
    CheckingCache { item: String },
    Queued { position: u32 },
    SkippingUncached,
    CachedDelivered,
    ProcessingNext,
    WaitingDuplicate { inflight_job_id: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RipTaskProgress {
    pub job_id: String,
    pub total_tracks: usize,
    pub completed_tracks: usize,
    pub cached_count: usize,
    pub ripped_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub percent: u32,
    pub job_activity: Option<TaskActivity>,
    pub download: Option<DownloadLane>,
    pub upload: Option<UploadLane>,
    pub codec: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedTrack {
    pub id: String,
    pub error: String,

    pub kind: Option<FailedTrackKind>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub storefront: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailedTrackKind {
    TrackUnavailable,
    RenditionUnavailable,
    SourceOffline,
    Cancelled,
    Timeout,
    Authentication,
    LocalIo,
}

impl FailedTrackKind {
    pub fn of(error: &crate::ripper::RipError) -> Option<Self> {
        use crate::ripper::RipError;
        match error {
            RipError::TrackUnavailable { .. } => Some(Self::TrackUnavailable),
            RipError::RenditionUnavailable { .. } => Some(Self::RenditionUnavailable),
            RipError::SourceOffline { .. } => Some(Self::SourceOffline),
            RipError::Cancelled => Some(Self::Cancelled),
            RipError::Timeout { .. } => Some(Self::Timeout),
            RipError::Authentication { .. } => Some(Self::Authentication),
            RipError::LocalIo { .. } => Some(Self::LocalIo),
            _ => None,
        }
    }
}

impl FailedTrack {
    pub fn new(id: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            error: error.into(),
            kind: None,
            title: None,
            artist: None,
            storefront: None,
        }
    }

    pub fn with_meta(
        mut self,
        title: Option<String>,
        artist: Option<String>,
        storefront: Option<String>,
    ) -> Self {
        self.title = title;
        self.artist = artist;
        self.storefront = storefront;
        self
    }

    pub fn with_kind(mut self, kind: Option<FailedTrackKind>) -> Self {
        self.kind = kind;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RipTaskSummary {
    pub job_id: String,
    pub job_header: String,
    pub total_tracks: usize,
    pub cached_count: usize,
    pub ripped_count: usize,
    pub failed_count: usize,
    pub failed_tracks: Vec<FailedTrack>,
    pub skipped_uncached_tracks: Vec<String>,
    pub total_elapsed_sec: String,
    pub capped_count: usize,
    pub max_collection_limit: u32,
    pub is_cache_only: bool,
    pub is_group: bool,

    pub warnings: Vec<String>,

    pub zip_delivery: Option<ZipDeliveryInfo>,

    pub zip_deliveries: Vec<ZipDeliveryInfo>,

    pub first_delivered_msg_id: Option<ChatMessageRef>,

    pub codec: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZipDeliveryInfo {
    pub album: String,
    pub artist: String,

    pub release_year: String,
    pub total_tracks: usize,

    pub delivered_tracks: Option<usize>,
    pub total_parts: usize,

    pub size_bytes: i64,
    pub is_partial: bool,
    pub album_id: String,
    pub album_url: Option<String>,
    pub artwork_url: Option<String>,
    pub genre: Option<String>,
    pub record_label: Option<String>,
    pub copyright: Option<String>,
    pub photo_delivered: bool,

    pub codec: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionFailure {
    pub kind: TargetKind,
    pub id: String,
    pub error: String,
}

impl std::fmt::Display for ResolutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}: {}", kind_name(self.kind), self.id, self.error)
    }
}

fn kind_name(kind: TargetKind) -> &'static str {
    match kind {
        TargetKind::Track => "track",
        TargetKind::Album => "album",
        TargetKind::Artist => "artist",
        TargetKind::Playlist => "playlist",
    }
}

#[derive(Debug, Clone)]
pub enum OrchestratorEvent<'a> {
    Created(&'a ActiveRipTask),

    Started(&'a ActiveRipTask),

    Progress(&'a ActiveRipTask, &'a RipTaskProgress),

    Completed(&'a ActiveRipTask, &'a RipTaskSummary),

    Cancelled(&'a ActiveRipTask, &'a Option<String>),

    Failed(&'a ActiveRipTask, &'a str),
}

pub type EventCallback = Arc<dyn Fn(&OrchestratorEvent<'_>) + Send + Sync>;
