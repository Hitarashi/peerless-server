use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RipTaskRequest {
    #[schema(example = "apple")]
    pub provider: String,

    #[schema(example = "1440857781")]
    pub track_id: String,

    #[schema(example = "alac")]
    pub codec: Option<String>,

    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,

    pub artwork_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServerTaskMeta {
    pub task_id: String,

    pub rip_task_id: String,
    pub owner_id: i64,
    pub provider: music::Provider,
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
}

impl ServerTaskMeta {
    pub(crate) fn snapshot(&self, is_owner: bool) -> RipTaskSnapshot {
        let progress = &self.latest_progress;
        RipTaskSnapshot {
            task_id: self.task_id.clone(),
            provider: self.provider.as_str().to_owned(),
            source_track_id: self.track_id.clone(),
            title: self.title.clone(),
            artist: self.artist.clone(),
            album: self.album.clone(),
            duration: self.duration,
            artwork_url: self.artwork_url.clone(),
            job_stage: progress.job_stage,
            download: progress.download.clone(),
            upload: progress.upload.clone(),
            percent: progress.percent,
            result_track_id: None,
            is_cached: None,
            completed: false,
            error: None,
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
    pub provider: String,
    pub source_track_id: String,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork_url: Option<String>,

    pub job_stage: Option<RipTaskJobStage>,

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskJobStage {
    Resolving,

    CheckingCache,

    Queued,

    SkippingUncached,

    CachedDelivered,

    ProcessingNext,

    WaitingDuplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskDownloadStage {
    ResolvingMetadata,

    Connecting,

    Downloading,

    Decrypting,

    Tagging,

    CachedDelivery,

    MaterializingCachedMedia,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskUploadStage {
    UploadingTrack,

    BuildingArchive,

    UploadingArchive,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskDownloadLane {
    pub stage: RipTaskDownloadStage,

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
    pub stage: RipTaskUploadStage,

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
    pub job_stage: Option<RipTaskJobStage>,
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

pub fn lane_percent(bytes_done: Option<u64>, bytes_total: Option<u64>) -> Option<f32> {
    let (Some(done), Some(total)) = (bytes_done, bytes_total.filter(|total| *total > 0)) else {
        return None;
    };
    Some((done as f32 / total as f32) * 100.0)
}

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
            artwork_url: None,
            bytes_done: Some(40),
            bytes_total: Some(100),
            percent: Some(40.0),
            codec: Some("alac".to_owned()),
            track_index: Some(4),
            total_tracks: Some(8),
        };
        let upload = RipTaskUploadLane {
            stage: RipTaskUploadStage::UploadingTrack,
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
            provider: music::Provider::Apple,
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
        };

        let snapshot = task.snapshot(true);
        assert_eq!(snapshot.owner_id, Some(task.owner_id));
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
            artwork_url: None,
            bytes_done,
            bytes_total,
            percent: lane_percent(bytes_done, bytes_total),
            codec: None,
            track_index: None,
            total_tracks: None,
        };

        assert_eq!(lane.bytes_done, Some(512));
        assert_eq!(lane.bytes_total, None);
        assert_eq!(lane.percent, None);
        assert_eq!(lane_percent(Some(10), Some(0)), None);
    }
}
