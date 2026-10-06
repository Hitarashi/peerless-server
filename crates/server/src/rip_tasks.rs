use engine::orchestrator::types::{DownloadLane, TaskActivity, UploadLane};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RipTaskRequest {
    #[schema(example = "1440857781")]
    pub track_id: Option<String>,

    #[schema(example = "1440857780")]
    pub album_id: Option<String>,
}

impl RipTaskRequest {
    pub fn target(&self) -> Option<(String, bool)> {
        if let Some(album_id) = self.album_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            return Some((album_id.to_string(), true));
        }
        if let Some(track_id) = self.track_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            return Some((track_id.to_string(), false));
        }
        None
    }
}

#[derive(Debug, Clone)]
pub struct ServerTaskMeta {
    pub task_id: String,

    pub rip_task_id: String,
    pub owner_id: i64,
    pub track_id: String,
    pub codec: Option<String>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
    pub artwork_url: Option<String>,
    pub controller: tokio_util::sync::CancellationToken,
    pub created_at: std::time::Instant,
    pub latest_progress: RipTaskProgress,
    pub is_album: bool,
    pub completed: bool,
    pub result_track_id: Option<i32>,
    pub error: Option<String>,
}

impl ServerTaskMeta {
    pub(crate) fn snapshot(&self, is_owner: bool) -> RipTaskSnapshot {
        let progress = &self.latest_progress;
        RipTaskSnapshot {
            task_id: self.task_id.clone(),
            source_track_id: self.track_id.clone(),
            title: self.title.clone(),
            artist: self.artist.clone(),
            album: self.album.clone(),
            duration: self.duration,
            artwork_url: self.artwork_url.clone(),
            job_stage: progress.job_stage.clone(),
            download: progress.download.clone(),
            upload: progress.upload.clone(),
            percent: progress.percent,
            result_track_id: self.result_track_id,
            is_cached: None,
            completed: self.completed,
            error: self.error.clone(),
            owner_id: Some(self.owner_id),
            is_owner,
            is_album: self.is_album,
            current_track_title: progress.current_track_title.clone(),
            current_track_artist: progress.current_track_artist.clone(),
            current_track_artwork_url: progress.current_track_artwork_url.clone(),
            current_track_index: progress.current_track_index,
            total_tracks: progress.total_tracks,
            completed_tracks: progress.completed_tracks,
            failed_tracks: progress.failed_tracks,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskSnapshot {
    pub task_id: String,
    pub source_track_id: String,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork_url: Option<String>,

    /// Optional orchestration activity; it may coexist with either active lane. The
    /// vocabulary is enumerated in the Playback WebSocket AsyncAPI document
    /// (`/api/v1/docs-ws.json`).
    #[schema(value_type = Option<String>, example = "queued")]
    pub job_stage: Option<RipTaskStage>,

    pub download: Option<RipTaskDownloadLane>,

    pub upload: Option<RipTaskUploadLane>,

    pub percent: Option<f32>,
    pub result_track_id: Option<i32>,
    pub is_cached: Option<bool>,
    pub completed: bool,
    pub error: Option<String>,
    #[schema(example = 123456789)]
    pub owner_id: Option<i64>,
    pub is_owner: bool,
    #[serde(default)]
    pub is_album: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_artist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_artwork_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tracks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_tracks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_tracks: Option<u32>,
}

/// A stage name on the wire.
///
/// The canonical task-state model lives in `engine::orchestrator::types`
/// (`TaskPhase`, `TaskActivity`, `DownloadLane`, `UploadLane`). This newtype is the
/// single serde adapter that projects that model onto the JSON contract: it carries
/// the stage names defined by [`TaskActivity::stage_name`],
/// [`DownloadLane::stage_name`] and [`UploadLane::stage_name`] and defines no
/// vocabulary of its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct RipTaskStage(String);

impl RipTaskStage {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this upload stage moves a whole album archive.
    pub fn is_archive(&self) -> bool {
        UploadLane::is_archive_stage(&self.0)
    }
}

impl From<TaskActivity> for RipTaskStage {
    fn from(activity: TaskActivity) -> Self {
        Self::new(activity.stage_name())
    }
}

impl From<&DownloadLane> for RipTaskStage {
    fn from(lane: &DownloadLane) -> Self {
        Self::new(lane.stage_name())
    }
}

impl From<&UploadLane> for RipTaskStage {
    fn from(lane: &UploadLane) -> Self {
        Self::new(lane.stage_name())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskDownloadLane {
    /// Download lane stage. The vocabulary is enumerated in the Playback WebSocket
    /// AsyncAPI document (`/api/v1/docs-ws.json`).
    #[schema(value_type = String, example = "downloading")]
    pub stage: RipTaskStage,

    pub title: Option<String>,

    pub artist: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork_url: Option<String>,

    pub bytes_done: Option<u64>,

    pub bytes_total: Option<u64>,

    pub percent: Option<f32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_index: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tracks: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskUploadLane {
    /// Upload lane stage. The vocabulary is enumerated in the Playback WebSocket
    /// AsyncAPI document (`/api/v1/docs-ws.json`).
    #[schema(value_type = String, example = "uploading_track")]
    pub stage: RipTaskStage,

    pub title: Option<String>,

    pub artist: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork_url: Option<String>,

    pub bytes_done: Option<u64>,

    pub bytes_total: Option<u64>,

    pub percent: Option<f32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub track_index: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tracks: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RipTaskProgress {
    pub job_stage: Option<RipTaskStage>,
    pub download: Option<RipTaskDownloadLane>,
    pub upload: Option<RipTaskUploadLane>,
    pub percent: Option<f32>,
    pub current_track_title: Option<String>,
    pub current_track_artist: Option<String>,
    pub current_track_artwork_url: Option<String>,
    pub current_track_index: Option<u32>,
    pub total_tracks: Option<u32>,
    pub completed_tracks: Option<u32>,
    pub failed_tracks: Option<u32>,
}

#[derive(Debug, Clone)]
pub enum TaskSyncEvent {
    Updated { task_id: String },
    Dismissed { task_id: String },
}

#[cfg(test)]
mod tests {
    use engine::orchestrator::types::{ByteProgress, RipActivity, TrackLabel};

    use super::*;

    fn downloading_lane() -> DownloadLane {
        DownloadLane::Rip(RipActivity::Downloading {
            track: TrackLabel::new("Track Four", "Artist Four"),
            progress: ByteProgress::new(40, Some(100)),
        })
    }

    fn uploading_track_lane() -> UploadLane {
        UploadLane::Track {
            track: TrackLabel::new("Track Three", "Artist Three"),
            progress: ByteProgress::new(75, Some(100)),
        }
    }

    #[test]
    fn snapshot_preserves_simultaneous_download_and_upload_lanes() {
        let download = RipTaskDownloadLane {
            stage: (&downloading_lane()).into(),
            title: Some("Track Four".to_owned()),
            artist: Some("Artist Four".to_owned()),
            artwork_url: None,
            bytes_done: Some(40),
            bytes_total: Some(100),
            percent: Some(40.0),
            codec: Some("alac".to_owned()),
            track_index: Some(4),
            total_tracks: Some(8),
        };
        let upload = RipTaskUploadLane {
            stage: (&uploading_track_lane()).into(),
            title: Some("Track Three".to_owned()),
            artist: Some("Artist Three".to_owned()),
            artwork_url: None,
            bytes_done: Some(75),
            bytes_total: Some(100),
            percent: Some(75.0),
            codec: Some("alac".to_owned()),
            track_index: Some(3),
            total_tracks: Some(8),
        };
        let task = ServerTaskMeta {
            task_id: "task-1".to_owned(),
            rip_task_id: "task-1".to_owned(),
            owner_id: 1,
            track_id: "source-1".to_owned(),
            codec: None,
            title: None,
            artist: None,
            album: None,
            duration: None,
            artwork_url: None,
            controller: tokio_util::sync::CancellationToken::new(),
            created_at: std::time::Instant::now(),
            latest_progress: RipTaskProgress {
                job_stage: None,
                download: Some(download.clone()),
                upload: Some(upload.clone()),
                percent: Some(43.75),
                current_track_title: Some("Track Three".to_owned()),
                current_track_artist: Some("Artist Three".to_owned()),
                current_track_artwork_url: None,
                current_track_index: Some(4),
                total_tracks: Some(8),
                completed_tracks: Some(3),
                failed_tracks: None,
            },
            is_album: true,
            completed: false,
            result_track_id: None,
            error: None,
        };

        let snapshot = task.snapshot(true);
        assert_eq!(snapshot.owner_id, Some(task.owner_id));
        assert_eq!(snapshot.download, Some(download));
        assert_eq!(snapshot.upload, Some(upload));
    }

    /// The canonical model's stage names are the JSON contract. This test pins
    /// every value the wire format has ever exposed.
    #[test]
    fn canonical_stage_names_round_trip_as_snake_case() {
        let download_stages = [
            (downloading_lane(), "downloading"),
            (
                DownloadLane::Rip(RipActivity::ResolvingMetadata),
                "resolving_metadata",
            ),
            (
                DownloadLane::Rip(RipActivity::Connecting {
                    track: TrackLabel::new("Song", "Artist"),
                }),
                "connecting",
            ),
            (
                DownloadLane::Rip(RipActivity::MaterializingCachedMedia {
                    track: TrackLabel::new("Song", "Artist"),
                    progress: ByteProgress::new(1, Some(2)),
                }),
                "materializing_cached_media",
            ),
            (
                DownloadLane::Rip(RipActivity::Decrypting {
                    track: TrackLabel::new("Song", "Artist"),
                }),
                "decrypting",
            ),
            (
                DownloadLane::Rip(RipActivity::Tagging {
                    track: TrackLabel::new("Song", "Artist"),
                }),
                "tagging",
            ),
            (
                DownloadLane::CachedDelivery {
                    track: TrackLabel::new("Song", "Artist"),
                },
                "cached_delivery",
            ),
        ];
        for (lane, expected) in download_stages {
            let stage = RipTaskStage::from(&lane);
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            assert_eq!(serde_json::from_str::<RipTaskStage>(&json).unwrap(), stage);
        }

        let upload_stages = [
            (uploading_track_lane(), "uploading_track"),
            (
                UploadLane::ArchiveBuild {
                    archive: "Album.zip".to_owned(),
                    progress: ByteProgress::new(0, None),
                },
                "building_archive",
            ),
            (
                UploadLane::ArchiveUpload {
                    archive: "Album.zip".to_owned(),
                    progress: ByteProgress::new(1, Some(2)),
                },
                "uploading_archive",
            ),
        ];
        for (lane, expected) in upload_stages {
            let stage = RipTaskStage::from(&lane);
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            assert_eq!(serde_json::from_str::<RipTaskStage>(&json).unwrap(), stage);
        }

        let job_stages = [
            (TaskActivity::Resolving, "resolving"),
            (
                TaskActivity::CheckingCache {
                    item: "Album".to_owned(),
                },
                "checking_cache",
            ),
            (TaskActivity::Queued { position: 1 }, "queued"),
            (TaskActivity::SkippingUncached, "skipping_uncached"),
            (TaskActivity::CachedDelivered, "cached_delivered"),
            (TaskActivity::ProcessingNext, "processing_next"),
            (
                TaskActivity::WaitingDuplicate {
                    inflight_job_id: "job-1".to_owned(),
                },
                "waiting_duplicate",
            ),
        ];
        for (activity, expected) in job_stages {
            let stage = RipTaskStage::from(activity.clone());
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            assert_eq!(serde_json::from_str::<RipTaskStage>(&json).unwrap(), stage);
        }
    }

    #[test]
    fn unknown_lane_total_keeps_done_bytes_but_has_no_percentage() {
        let progress = ByteProgress::new(512, None);
        let lane = RipTaskDownloadLane {
            stage: (&downloading_lane()).into(),
            title: None,
            artist: None,
            artwork_url: None,
            bytes_done: Some(progress.completed),
            bytes_total: progress.total,
            percent: progress.percent(),
            codec: None,
            track_index: None,
            total_tracks: None,
        };

        assert_eq!(lane.bytes_done, Some(512));
        assert_eq!(lane.bytes_total, None);
        assert_eq!(lane.percent, None);
        assert_eq!(ByteProgress::new(10, Some(0)).percent(), None);
    }

    #[test]
    fn archive_upload_stages_are_recognised_from_the_canonical_model() {
        for (lane, expected) in [
            (uploading_track_lane(), false),
            (
                UploadLane::ArchiveBuild {
                    archive: "Album.zip".to_owned(),
                    progress: ByteProgress::new(0, None),
                },
                true,
            ),
            (
                UploadLane::ArchiveUpload {
                    archive: "Album.zip".to_owned(),
                    progress: ByteProgress::new(0, None),
                },
                true,
            ),
        ] {
            let stage = RipTaskStage::from(&lane);
            assert_eq!(stage.is_archive(), expected);
            assert_eq!(UploadLane::is_archive_stage(stage.as_str()), expected);
        }
    }
}
