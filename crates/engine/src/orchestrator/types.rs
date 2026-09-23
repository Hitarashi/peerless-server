//! Orchestrator domain types.

use std::sync::Arc;

pub use music::{
    CodecPreference, Rendition, RenditionPolicy, RenditionWorkPlan, RenditionWorkUnit,
};
use tokio_util::sync::CancellationToken;

use super::deps::ChatMessageRef;
use crate::types::{ParsedTargetItem, Provider, TargetKind};

/// The non-terminal lifecycle phase of a job.  Terminality is represented by
/// `terminal_state` below so consumers can retain the last useful phase while
/// rendering a completed/cancelled job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPhase {
    Resolving,
    CheckingCache,
    Queued,
    Processing,
    Delivering,
    WaitingDuplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalJobState {
    Completed,
    Cancelled,
    Failed,
}

/// Descriptive alias for callers that prefer the `Job*` naming convention.
pub type JobTerminalState = TerminalJobState;

/// Live job bookkeeping. The orchestrator owns it, mutates it from several
/// tasks behind its mutex, and hands out read-only snapshots via events.
#[derive(Debug, Clone)]
pub struct ActiveRipJob {
    pub id: String,
    pub provider: Provider,
    pub source_track_ids: Vec<String>,
    pub chat_id: i64,
    /// Chat the delivered copies target (group jobs retarget to the user's
    /// DM); the bridge uses it to send ZIP details to the right chat.
    pub delivery_chat_id: i64,
    pub user_id: i64,
    pub user_name: Option<String>,
    pub job_header: String,
    pub total_tracks: usize,
    pub status_msg_id: i64,
    /// Shared cancellation token — cloned into every pipeline stage.
    pub controller: CancellationToken,
    pub is_cancelled: bool,
    pub cancelled_by: Option<String>,
    pub cached_count: usize,
    pub ripped_count: usize,
    pub failed_count: usize,
    pub completed: bool,
    pub start_time_ms: u64,
    /// Queue position, maintained by the queue rather than the job flow.
    pub queue_position: Option<u64>,
    pub phase: JobPhase,
    pub terminal_state: Option<TerminalJobState>,
    pub skipped_count: usize,
    pub is_cache_only: bool,
    pub is_group: bool,
    pub reply_to_message_id: Option<i64>,
}

/// Everything one rip request carries.
#[derive(Debug, Clone)]
pub struct RipJobOptions {
    pub provider: Provider,
    pub chat_id: i64,
    pub user_id: i64,
    pub user_name: Option<String>,
    /// Chat the file copy is delivered to (numbers only in practice; the
    /// bot resolves usernames/ids to i64 before enqueueing).
    pub delivery_chat_id: i64,
    pub is_group: bool,
    pub is_force: bool,
    pub is_cache_only: bool,
    pub single_storefront: Option<String>,
    pub parsed_items: Vec<ParsedTargetItem>,
    pub reply_to_message_id: Option<i64>,
    pub status_msg_id: i64,
    pub is_admin: bool,
    /// Preferred audio codec/quality preference (e.g. for Qobuz).
    pub codec_preference: Option<CodecPreference>,
    /// Renditions to acquire for this request. Primary is always required;
    /// Atmos, when selected, is optional.
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
            &[Codec::Alac, Codec::Aac, Codec::Flac]
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
}

impl TrackLabel {
    pub fn new(title: impl Into<String>, artist: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            artist: artist.into(),
        }
    }

    pub fn from_meta(meta: &crate::types::TrackMeta) -> Self {
        Self::new(meta.title.clone(), meta.artist.clone())
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
pub enum JobActivity {
    Resolving,
    CheckingCache { item: String },
    Queued { position: u32 },
    SkippingUncached,
    CachedDelivered,
    ProcessingNext,
    WaitingDuplicate { inflight_job_id: String },
}

/// A progress snapshot; every display slot is optional.
#[derive(Debug, Clone, PartialEq)]
pub struct RipJobProgress {
    pub job_id: String,
    pub total_tracks: usize,
    pub completed_tracks: usize,
    pub cached_count: usize,
    pub ripped_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub percent: u32,
    pub job_activity: Option<JobActivity>,
    pub download: Option<DownloadLane>,
    pub upload: Option<UploadLane>,
}

/// One failed track, as reported in the job summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedTrack {
    pub id: String,
    pub error: String,
    /// Disposition of the failure, from the typed rip error. `None` for
    /// upload-lane failures that never had a rip error.
    pub kind: Option<FailedTrackKind>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub storefront: Option<String>,
}

/// What a consumer does with a failed track: render text or a label.
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
    /// Disposition of a typed rip error; `None` for technical failures that
    /// carry no user-facing label.
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

/// The end-of-job report for the requesting chat.
#[derive(Debug, Clone, PartialEq)]
pub struct RipJobSummary {
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
    /// User-facing notes appended to the completion message (plain text;
    /// the bridge escapes them). Empty in the common case.
    pub warnings: Vec<String>,
    /// Metadata about a user-delivered album ZIP, rendered as the details
    /// message in the delivery chat. `None` for cache-only jobs and
    /// non-ZIP jobs.
    pub zip_delivery: Option<ZipDeliveryInfo>,
    /// Metadata for every delivered rendition archive. `zip_delivery` is
    /// retained as the primary/first entry compatibility view.
    pub zip_deliveries: Vec<ZipDeliveryInfo>,
    /// Telegram message ID of the first delivered track or ZIP in the delivery chat.
    pub first_delivered_msg_id: Option<ChatMessageRef>,
}

/// Album details for a delivered ZIP, powering the post-ZIP info message.
#[derive(Debug, Clone, PartialEq)]
pub struct ZipDeliveryInfo {
    pub album: String,
    pub artist: String,
    /// First four characters of the album release date, may be empty.
    pub release_year: String,
    pub total_tracks: usize,
    /// Tracks actually present in the delivered archive. `None` means the
    /// archive was reused but its sparse track count could not be derived from
    /// the per-track cache.
    pub delivered_tracks: Option<usize>,
    pub total_parts: usize,
    /// Total delivered archive bytes.
    pub size_bytes: i64,
    pub is_partial: bool,
    pub album_id: String,
    pub album_url: Option<String>,
    pub artwork_url: Option<String>,
    pub genre: Option<String>,
    pub record_label: Option<String>,
    pub copyright: Option<String>,
    pub photo_delivered: bool,
    /// Highest-quality codec in the archive (`alac`, `aac`, `mp4a.40.2`, `ec-3`).
    pub codec: Option<String>,
}

/// A target which could not be resolved.  The engine deliberately keeps this
/// structured; presentation (including HTML escaping) belongs to the bot.
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

/// Events emitted by the orchestrator.
#[derive(Debug, Clone)]
pub enum OrchestratorEvent<'a> {
    /// `job:created`
    Created(&'a ActiveRipJob),
    /// `job:started`
    Started(&'a ActiveRipJob),
    /// `job:progress`
    Progress(&'a ActiveRipJob, &'a RipJobProgress),
    /// `job:completed`
    Completed(&'a ActiveRipJob, &'a RipJobSummary),
    /// `job:cancelled`
    Cancelled(&'a ActiveRipJob, &'a Option<String>),
    /// `job:failed`
    Failed(&'a ActiveRipJob, &'a str),
}

/// Callback type subscribed to orchestrator events.
pub type EventCallback = Arc<dyn Fn(&OrchestratorEvent<'_>) + Send + Sync>;
