use std::sync::Arc;

pub use music::{
    CodecPreference, Rendition, RenditionPolicy, RenditionWorkPlan, RenditionWorkUnit,
};
use tokio_util::sync::CancellationToken;

use super::deps::ChatMessageRef;
use crate::types::{ParsedTargetItem, TargetKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPhase {
    Resolving,
    CheckingCache,
    Queued,
    Processing,
    Delivering,
    WaitingDuplicate,
}

impl TaskPhase {
    /// Phases where the task is actively driving the rip pipeline. Callers already
    /// have a finer-grained [`TaskActivity`] (or lane) to show for these, so they do
    /// not need a separate coarse status line.
    pub fn is_working(self) -> bool {
        matches!(
            self,
            Self::Resolving | Self::CheckingCache | Self::Processing
        )
    }

    /// Queued and duplicate-blocked tasks never have download/upload lanes.
    pub fn has_lane_activity(self) -> bool {
        !matches!(self, Self::Queued | Self::WaitingDuplicate)
    }

    /// Display ordering group: work in flight first, then queued, then blocked.
    pub fn display_rank(self) -> u8 {
        match self {
            Self::Queued => 1,
            Self::WaitingDuplicate => 2,
            _ => 0,
        }
    }

    /// Coarse [`TaskActivity`] to display for a task that has not produced a
    /// progress event yet.
    pub fn fallback_activity(
        self,
        header: &str,
        queue_position: Option<u64>,
    ) -> Option<TaskActivity> {
        match self {
            Self::Resolving => Some(TaskActivity::Resolving),
            Self::CheckingCache => Some(TaskActivity::CheckingCache {
                item: header.to_owned(),
            }),
            Self::Queued => Some(TaskActivity::Queued {
                position: queue_position
                    .and_then(|position| u32::try_from(position).ok())
                    .unwrap_or(1),
            }),
            Self::Delivering => Some(TaskActivity::CachedDelivered),
            Self::Processing | Self::WaitingDuplicate => None,
        }
    }
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

impl ByteProgress {
    pub fn new(completed: u64, total: Option<u64>) -> Self {
        Self { completed, total }
    }

    /// Completion percentage, or `None` while the total size is still unknown.
    pub fn percent(&self) -> Option<f32> {
        let total = self.total.filter(|total| *total > 0)?;
        Some((self.completed as f32 / total as f32) * 100.0)
    }
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

impl RipActivity {
    /// Wire stage name for this activity. This is the only place the externally
    /// visible download-stage vocabulary is defined.
    pub fn stage_name(&self) -> &'static str {
        match self {
            Self::ResolvingMetadata => "resolving_metadata",
            Self::Connecting { .. } => "connecting",
            Self::Downloading { .. } => "downloading",
            Self::MaterializingCachedMedia { .. } => "materializing_cached_media",
            Self::Decrypting { .. } => "decrypting",
            Self::Tagging { .. } => "tagging",
        }
    }

    /// Track this activity applies to, if it is track-scoped.
    pub fn track(&self) -> Option<&TrackLabel> {
        match self {
            Self::ResolvingMetadata => None,
            Self::Connecting { track }
            | Self::Downloading { track, .. }
            | Self::MaterializingCachedMedia { track, .. }
            | Self::Decrypting { track }
            | Self::Tagging { track } => Some(track),
        }
    }

    /// Byte progress, for the stages that stream bytes.
    pub fn byte_progress(&self) -> Option<&ByteProgress> {
        match self {
            Self::Downloading { progress, .. }
            | Self::MaterializingCachedMedia { progress, .. } => Some(progress),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadLane {
    Rip(RipActivity),
    CachedDelivery { track: TrackLabel },
}

impl DownloadLane {
    /// Wire stage name for this lane. This is the only place the externally
    /// visible download-stage vocabulary is defined.
    pub fn stage_name(&self) -> &'static str {
        match self {
            Self::Rip(activity) => activity.stage_name(),
            Self::CachedDelivery { .. } => "cached_delivery",
        }
    }

    /// Track this lane applies to, or `None` while it is still resolving metadata.
    pub fn track(&self) -> Option<&TrackLabel> {
        match self {
            Self::Rip(activity) => activity.track(),
            Self::CachedDelivery { track } => Some(track),
        }
    }

    pub fn byte_progress(&self) -> Option<&ByteProgress> {
        match self {
            Self::Rip(activity) => activity.byte_progress(),
            Self::CachedDelivery { .. } => None,
        }
    }
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

impl UploadLane {
    pub const ARCHIVE_BUILD_STAGE: &'static str = "building_archive";
    pub const ARCHIVE_UPLOAD_STAGE: &'static str = "uploading_archive";

    pub fn byte_progress(&self) -> &ByteProgress {
        match self {
            Self::Track { progress, .. }
            | Self::ArchiveBuild { progress, .. }
            | Self::ArchiveUpload { progress, .. } => progress,
        }
    }

    /// Album ZIP lanes move whole archives rather than single tracks.
    pub fn is_archive(&self) -> bool {
        matches!(self, Self::ArchiveBuild { .. } | Self::ArchiveUpload { .. })
    }

    /// Whether a serialized stage name denotes an album archive lane.
    pub fn is_archive_stage(stage: &str) -> bool {
        matches!(
            stage,
            Self::ARCHIVE_BUILD_STAGE | Self::ARCHIVE_UPLOAD_STAGE
        )
    }

    /// Wire stage name for this lane.
    pub fn stage_name(&self) -> &'static str {
        match self {
            Self::Track { .. } => "uploading_track",
            Self::ArchiveBuild { .. } => Self::ARCHIVE_BUILD_STAGE,
            Self::ArchiveUpload { .. } => Self::ARCHIVE_UPLOAD_STAGE,
        }
    }
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

impl TaskActivity {
    /// Wire stage name for this activity. This is the only place the externally
    /// visible job-stage vocabulary is defined.
    pub fn stage_name(&self) -> &'static str {
        match self {
            Self::Resolving => "resolving",
            Self::CheckingCache { .. } => "checking_cache",
            Self::Queued { .. } => "queued",
            Self::SkippingUncached => "skipping_uncached",
            Self::CachedDelivered => "cached_delivered",
            Self::ProcessingNext => "processing_next",
            Self::WaitingDuplicate { .. } => "waiting_duplicate",
        }
    }
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

#[cfg(test)]
mod task_state_tests {
    use super::*;

    #[test]
    fn phases_describe_working_lane_and_ordering_semantics() {
        for phase in [
            TaskPhase::Resolving,
            TaskPhase::CheckingCache,
            TaskPhase::Processing,
        ] {
            assert!(phase.is_working(), "{phase:?} hides the coarse status line");
            assert!(phase.has_lane_activity());
            assert_eq!(phase.display_rank(), 0);
        }

        assert!(!TaskPhase::Delivering.is_working());
        assert!(TaskPhase::Delivering.has_lane_activity());
        assert!(!TaskPhase::Queued.has_lane_activity());
        assert!(!TaskPhase::WaitingDuplicate.has_lane_activity());
        assert_eq!(TaskPhase::Queued.display_rank(), 1);
        assert_eq!(TaskPhase::WaitingDuplicate.display_rank(), 2);
    }

    #[test]
    fn phase_falls_back_to_the_activity_it_implies() {
        assert_eq!(
            TaskPhase::Resolving.fallback_activity("Album", None),
            Some(TaskActivity::Resolving)
        );
        assert_eq!(
            TaskPhase::CheckingCache.fallback_activity("Album", None),
            Some(TaskActivity::CheckingCache {
                item: "Album".to_owned()
            })
        );
        assert_eq!(
            TaskPhase::Queued.fallback_activity("Album", Some(3)),
            Some(TaskActivity::Queued { position: 3 })
        );
        assert_eq!(
            TaskPhase::Queued.fallback_activity("Album", None),
            Some(TaskActivity::Queued { position: 1 })
        );
        assert_eq!(
            TaskPhase::Delivering.fallback_activity("Album", None),
            Some(TaskActivity::CachedDelivered)
        );
        assert_eq!(TaskPhase::Processing.fallback_activity("Album", None), None);
        assert_eq!(
            TaskPhase::WaitingDuplicate.fallback_activity("Album", None),
            None
        );
    }

    #[test]
    fn byte_progress_percent_needs_a_known_total() {
        assert_eq!(ByteProgress::new(1, Some(2)).percent(), Some(50.0));
        assert_eq!(ByteProgress::new(512, None).percent(), None);
        assert_eq!(ByteProgress::new(10, Some(0)).percent(), None);
    }

    #[test]
    fn lanes_expose_their_track_bytes_and_archive_kind() {
        let metadata = DownloadLane::Rip(RipActivity::ResolvingMetadata);
        assert_eq!(metadata.stage_name(), "resolving_metadata");
        assert!(metadata.track().is_none());
        assert!(metadata.byte_progress().is_none());

        let downloading = DownloadLane::Rip(RipActivity::Downloading {
            track: TrackLabel::new("Song", "Artist"),
            progress: ByteProgress::new(1, Some(2)),
        });
        assert_eq!(downloading.stage_name(), "downloading");
        assert_eq!(
            downloading.track().map(|track| track.title.as_str()),
            Some("Song")
        );
        assert_eq!(
            downloading.byte_progress().and_then(ByteProgress::percent),
            Some(50.0)
        );

        let cached = DownloadLane::CachedDelivery {
            track: TrackLabel::new("Song", "Artist"),
        };
        assert_eq!(cached.stage_name(), "cached_delivery");
        assert!(cached.byte_progress().is_none());

        let track_upload = UploadLane::Track {
            track: TrackLabel::new("Song", "Artist"),
            progress: ByteProgress::new(1, Some(2)),
        };
        assert_eq!(track_upload.stage_name(), "uploading_track");
        assert!(!track_upload.is_archive());

        for (lane, stage) in [
            (
                UploadLane::ArchiveBuild {
                    archive: "Album.zip".to_owned(),
                    progress: ByteProgress::new(0, None),
                },
                UploadLane::ARCHIVE_BUILD_STAGE,
            ),
            (
                UploadLane::ArchiveUpload {
                    archive: "Album.zip".to_owned(),
                    progress: ByteProgress::new(1, Some(2)),
                },
                UploadLane::ARCHIVE_UPLOAD_STAGE,
            ),
        ] {
            assert!(lane.is_archive());
            assert_eq!(lane.stage_name(), stage);
            assert!(UploadLane::is_archive_stage(stage));
        }
        assert!(!UploadLane::is_archive_stage("uploading_track"));
    }

    #[test]
    fn activities_map_onto_their_wire_stage_names() {
        for (activity, stage) in [
            (TaskActivity::Resolving, "resolving"),
            (
                TaskActivity::CheckingCache {
                    item: "Album".to_owned(),
                },
                "checking_cache",
            ),
            (TaskActivity::Queued { position: 2 }, "queued"),
            (TaskActivity::SkippingUncached, "skipping_uncached"),
            (TaskActivity::CachedDelivered, "cached_delivered"),
            (TaskActivity::ProcessingNext, "processing_next"),
            (
                TaskActivity::WaitingDuplicate {
                    inflight_job_id: "job-1".to_owned(),
                },
                "waiting_duplicate",
            ),
        ] {
            assert_eq!(activity.stage_name(), stage);
        }
    }
}
