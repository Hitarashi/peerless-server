use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request payload to trigger an on-demand provider ripping job.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RipTaskRequest {
    /// Music provider name (`apple` or `qobuz`).
    #[schema(example = "apple")]
    pub provider: String,
    /// Provider-native track identifier.
    #[schema(example = "1440857781")]
    pub track_id: String,
    /// Desired codec (`alac`, `flac`, or `aac`); unsupported values fall back to `alac`.
    #[schema(example = "alac")]
    pub codec: Option<String>,
    /// Display metadata retained by the server for task recovery.
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
}

/// Thread-safe active task metadata stored in `ServerState::active_tasks`.
#[derive(Debug, Clone)]
pub struct ServerTaskMeta {
    pub task_id: String,
    pub job_id: Option<String>,
    pub owner_id: i64,
    pub provider: music::Provider,
    pub track_id: String,
    pub codec: Option<String>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
    pub controller: tokio_util::sync::CancellationToken,
    pub created_at: std::time::Instant,
    pub latest_progress: RipTaskProgress,
    pub is_album: bool,
}

impl ServerTaskMeta {
    pub(crate) fn snapshot(&self, is_owner: bool) -> RipTaskSnapshot {
        let progress = &self.latest_progress;
        RipTaskSnapshot {
            task_id: self.task_id.clone(),
            provider: self.provider.as_str().to_owned(),
            source_track_id: self.track_id.clone(),
            codec: self.codec.clone(),
            title: self.title.clone(),
            artist: self.artist.clone(),
            album: self.album.clone(),
            duration: self.duration,
            job_stage: progress.job_stage,
            download: progress.download.clone(),
            upload: progress.upload.clone(),
            percent: progress.percent,
            result_track_id: None,
            is_cached: None,
            completed: false,
            error: None,
            is_owner,
            is_album: self.is_album,
            current_track_title: progress.current_track_title.clone(),
            current_track_artist: progress.current_track_artist.clone(),
            current_track_index: progress.current_track_index,
            total_tracks: progress.total_tracks,
            completed_tracks: progress.completed_tracks,
        }
    }
}

/// Server-owned active task state returned to clients on reconnect and app startup.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskSnapshot {
    pub task_id: String,
    pub provider: String,
    pub source_track_id: String,
    pub codec: Option<String>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
    /// Optional orchestration activity that may coexist with either lane.
    pub job_stage: Option<RipTaskJobStage>,
    /// Download/materialization progress, independent of the upload lane.
    pub download: Option<RipTaskDownloadLane>,
    /// The single upload lane; uploads are serialized by the orchestrator.
    pub upload: Option<RipTaskUploadLane>,
    /// Overall album/task progress. Lane percentages are the phase-local progress values.
    pub percent: Option<f32>,
    pub result_track_id: Option<i32>,
    pub is_cached: Option<bool>,
    pub completed: bool,
    pub error: Option<String>,
    pub is_owner: bool,
    #[serde(default)]
    pub is_album: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_artist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tracks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_tracks: Option<u32>,
}

/// Orchestration activity that may coexist with either byte-oriented lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskJobStage {
    /// Resolving the requested job or its track metadata.
    Resolving,
    /// Looking up requested tracks in the local cache before media work begins.
    CheckingCache,
    /// Waiting for a position in the orchestrator's work queue.
    Queued,
    /// Skipping a requested track because it is unavailable in cache-only mode.
    SkippingUncached,
    /// Reusing and delivering a track that was already cached.
    CachedDelivered,
    /// Advancing to the next item in a multi-track job.
    ProcessingNext,
    /// Waiting for another in-flight job that owns the same requested work.
    WaitingDuplicate,
}

/// Stable download-lane stage vocabulary, serialized in snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskDownloadStage {
    /// Resolve provider metadata needed to identify the source track.
    ResolvingMetadata,
    /// Establish the provider connection before receiving source bytes.
    Connecting,
    /// Receive source audio bytes from the provider.
    Downloading,
    /// Decrypt the downloaded source audio after transfer completes.
    Decrypting,
    /// Write metadata/tags into the downloaded audio file.
    Tagging,
    /// Reuse a cached track for the current delivery or archive item.
    CachedDelivery,
    /// Download an already-cached Telegram media object into the local archive workspace.
    MaterializingCachedMedia,
}

/// Stable upload-lane stage vocabulary, serialized in snake_case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskUploadStage {
    /// Send one processed track to Telegram.
    UploadingTrack,
    /// Build the album ZIP archive from its processed track files.
    BuildingArchive,
    /// Send the completed album ZIP archive to Telegram.
    UploadingArchive,
}

/// Structured progress for the independent download lane.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskDownloadLane {
    /// Current phase in the download/cache-materialization pipeline.
    pub stage: RipTaskDownloadStage,
    /// Track title associated with this lane, when known.
    pub title: Option<String>,
    /// Track artist associated with this lane, when known.
    pub artist: Option<String>,
    /// Bytes processed so far. Null when the activity has no byte counter.
    pub bytes_done: Option<u64>,
    /// Expected byte total. Null when the total is unknown or the activity has no byte counter.
    pub bytes_total: Option<u64>,
    /// Percentage within this lane's current phase; null when the total is unknown.
    pub percent: Option<f32>,
}

/// Structured progress for the one serialized upload lane.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskUploadLane {
    /// Current phase in the Telegram upload/archive pipeline.
    pub stage: RipTaskUploadStage,
    /// Track or archive title associated with this lane, when known.
    pub title: Option<String>,
    /// Track artist associated with this lane, when known.
    pub artist: Option<String>,
    /// Bytes processed so far. Null when the activity has no byte counter.
    pub bytes_done: Option<u64>,
    /// Expected byte total. Null when the total is unknown or the activity has no byte counter.
    pub bytes_total: Option<u64>,
    /// Percentage within this lane's current phase; null when the total is unknown.
    pub percent: Option<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RipTaskProgress {
    pub job_stage: Option<RipTaskJobStage>,
    pub download: Option<RipTaskDownloadLane>,
    pub upload: Option<RipTaskUploadLane>,
    pub percent: Option<f32>,
    pub current_track_title: Option<String>,
    pub current_track_artist: Option<String>,
    pub current_track_index: Option<u32>,
    pub total_tracks: Option<u32>,
    pub completed_tracks: Option<u32>,
}

/// Derive a phase-local percentage without inventing a value for an unknown or zero total.
pub fn lane_percent(bytes_done: Option<u64>, bytes_total: Option<u64>) -> Option<f32> {
    let (Some(done), Some(total)) = (bytes_done, bytes_total.filter(|total| *total > 0)) else {
        return None;
    };
    Some((done as f32 / total as f32) * 100.0)
}

/// Internal notification forwarded to authenticated playback WebSocket clients.
#[derive(Debug, Clone)]
pub enum TaskSyncEvent {
    Updated { task_id: String },
    Dismissed { task_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_preserves_simultaneous_download_and_upload_lanes() {
        let download = RipTaskDownloadLane {
            stage: RipTaskDownloadStage::Downloading,
            title: Some("Track Four".to_owned()),
            artist: Some("Artist Four".to_owned()),
            bytes_done: Some(40),
            bytes_total: Some(100),
            percent: Some(40.0),
        };
        let upload = RipTaskUploadLane {
            stage: RipTaskUploadStage::UploadingTrack,
            title: Some("Track Three".to_owned()),
            artist: Some("Artist Three".to_owned()),
            bytes_done: Some(75),
            bytes_total: Some(100),
            percent: Some(75.0),
        };
        let task = ServerTaskMeta {
            task_id: "task-1".to_owned(),
            job_id: Some("job-1".to_owned()),
            owner_id: 1,
            provider: music::Provider::Apple,
            track_id: "source-1".to_owned(),
            codec: None,
            title: None,
            artist: None,
            album: None,
            duration: None,
            controller: tokio_util::sync::CancellationToken::new(),
            created_at: std::time::Instant::now(),
            latest_progress: RipTaskProgress {
                job_stage: None,
                download: Some(download.clone()),
                upload: Some(upload.clone()),
                percent: Some(43.75),
                current_track_title: Some("Track Three".to_owned()),
                current_track_artist: Some("Artist Three".to_owned()),
                current_track_index: Some(4),
                total_tracks: Some(8),
                completed_tracks: Some(3),
            },
            is_album: true,
        };

        let snapshot = task.snapshot(true);
        assert_eq!(snapshot.download, Some(download));
        assert_eq!(snapshot.upload, Some(upload));
    }

    #[test]
    fn all_stage_enums_round_trip_as_snake_case() {
        let download_stages = [
            (
                RipTaskDownloadStage::ResolvingMetadata,
                "resolving_metadata",
            ),
            (RipTaskDownloadStage::Connecting, "connecting"),
            (RipTaskDownloadStage::Downloading, "downloading"),
            (RipTaskDownloadStage::Decrypting, "decrypting"),
            (RipTaskDownloadStage::Tagging, "tagging"),
            (RipTaskDownloadStage::CachedDelivery, "cached_delivery"),
            (
                RipTaskDownloadStage::MaterializingCachedMedia,
                "materializing_cached_media",
            ),
        ];
        for (stage, expected) in download_stages {
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            assert_eq!(
                serde_json::from_str::<RipTaskDownloadStage>(&json).unwrap(),
                stage
            );
        }

        let upload_stages = [
            (RipTaskUploadStage::UploadingTrack, "uploading_track"),
            (RipTaskUploadStage::BuildingArchive, "building_archive"),
            (RipTaskUploadStage::UploadingArchive, "uploading_archive"),
        ];
        for (stage, expected) in upload_stages {
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            assert_eq!(
                serde_json::from_str::<RipTaskUploadStage>(&json).unwrap(),
                stage
            );
        }

        let job_stages = [
            (RipTaskJobStage::Resolving, "resolving"),
            (RipTaskJobStage::CheckingCache, "checking_cache"),
            (RipTaskJobStage::Queued, "queued"),
            (RipTaskJobStage::SkippingUncached, "skipping_uncached"),
            (RipTaskJobStage::CachedDelivered, "cached_delivered"),
            (RipTaskJobStage::ProcessingNext, "processing_next"),
            (RipTaskJobStage::WaitingDuplicate, "waiting_duplicate"),
        ];
        for (stage, expected) in job_stages {
            let json = serde_json::to_string(&stage).unwrap();
            assert_eq!(json, format!("\"{expected}\""));
            assert_eq!(
                serde_json::from_str::<RipTaskJobStage>(&json).unwrap(),
                stage
            );
        }
    }

    #[test]
    fn unknown_lane_total_keeps_done_bytes_but_has_no_percentage() {
        let bytes_done = Some(512);
        let bytes_total = None;
        let lane = RipTaskDownloadLane {
            stage: RipTaskDownloadStage::Downloading,
            title: None,
            artist: None,
            bytes_done,
            bytes_total,
            percent: lane_percent(bytes_done, bytes_total),
        };

        assert_eq!(lane.bytes_done, Some(512));
        assert_eq!(lane.bytes_total, None);
        assert_eq!(lane.percent, None);
        assert_eq!(lane_percent(Some(10), Some(0)), None);
    }
}
