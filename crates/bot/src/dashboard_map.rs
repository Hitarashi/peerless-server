//! Engine job snapshot → dashboard view-model mapping.
//
//! Pure functions only: no Telegram I/O, no orchestrator state. The event
//! bridge stores the last engine snapshot per job and every dashboard render
//! (open, refresh callback, `refresh_all`) goes through [`snapshot_from`].

use std::collections::HashMap;

use engine::orchestrator::types::{
    ActiveRipJob, DownloadLane, JobActivity, JobPhase as EnginePhase, RipJobProgress, UploadLane,
};

use crate::dashboard::{DashboardJob, DashboardSnapshot, JobPhase};

/// Cached per-job rendering context. `ActiveRipJob` snapshots carry the live
/// counters, but `job_header`/`user_name` are finalized early; remembering
/// them keeps terminal rows (cancellation) renderable before removal.
#[derive(Debug, Default)]
pub struct JobContexts {
    jobs: HashMap<String, JobContext>,
}

#[derive(Debug, Clone)]
pub struct JobContext {
    /// Engine `job_header` post-resolution (rich HTML, already escaped).
    pub header: String,
    /// Requester display name (`user_name` from options).
    pub requester_name: String,
    pub job_activity: Option<JobActivity>,
    pub download: Option<DownloadLane>,
    pub upload: Option<UploadLane>,
}

impl JobContexts {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    /// Remember a job's rendering context from its latest engine snapshot.
    pub fn remember(&mut self, job: &ActiveRipJob) {
        let (job_activity, download, upload) = self
            .jobs
            .get(&job.id)
            .map(|context| {
                (
                    context.job_activity.clone(),
                    context.download.clone(),
                    context.upload.clone(),
                )
            })
            .unwrap_or((None, None, None));
        self.jobs.insert(
            job.id.clone(),
            JobContext {
                header: job.job_header.clone(),
                requester_name: job
                    .user_name
                    .clone()
                    .unwrap_or_else(|| format!("User {}", job.user_id)),
                job_activity,
                download,
                upload,
            },
        );
    }

    /// Remember the latest pipeline facts while retaining the job's
    /// presentation context across subsequent engine snapshots.
    pub fn remember_progress(&mut self, progress: &RipJobProgress) {
        if let Some(context) = self.jobs.get_mut(&progress.job_id) {
            context.job_activity = progress.job_activity.clone();
            context.download = progress.download.clone();
            context.upload = progress.upload.clone();
        }
    }

    /// Drop contexts for finished jobs so the registry cannot grow unbounded.
    pub fn forget(&mut self, job_id: &str) {
        self.jobs.remove(job_id);
    }

    pub fn get(&self, job_id: &str) -> Option<&JobContext> {
        self.jobs.get(job_id)
    }

    /// Iterate remembered contexts (used by whole-snapshot builds).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &JobContext)> {
        self.jobs.iter()
    }

    /// Insert/replace one context (used by registry snapshots).
    pub fn insert(&mut self, job_id: String, context: JobContext) {
        self.jobs.insert(job_id, context);
    }
}

/// Map an engine phase to the dashboard's view.
///
/// `Resolving`/`CheckingCache` render as `Processing` (an active-stage label):
/// the dashboard distinguishes waiting-in-queue, delivering from cache, and
/// waiting on an inflight duplicate from active work.
pub fn phase_from(engine_phase: EnginePhase) -> JobPhase {
    match engine_phase {
        EnginePhase::Queued => JobPhase::Queued,
        EnginePhase::Delivering => JobPhase::Delivering,
        EnginePhase::WaitingDuplicate => JobPhase::WaitingDuplicate,
        _ => JobPhase::Processing,
    }
}

fn fallback_job_activity(job: &ActiveRipJob) -> Option<JobActivity> {
    match job.phase {
        EnginePhase::Resolving => Some(JobActivity::Resolving),
        EnginePhase::CheckingCache => Some(JobActivity::CheckingCache {
            item: job.job_header.clone(),
        }),
        EnginePhase::Queued => Some(JobActivity::Queued {
            position: job
                .queue_position
                .and_then(|position| u32::try_from(position).ok())
                .unwrap_or(1),
        }),
        EnginePhase::Processing => None,
        EnginePhase::Delivering => Some(JobActivity::CachedDelivered),
        EnginePhase::WaitingDuplicate => None,
    }
}

/// Percent for the dashboard row, clamped to 100.
pub fn percent_from(progress: &RipJobProgress) -> u8 {
    progress.percent.min(100) as u8
}

/// Map an engine job snapshot into a dashboard row for a specific viewer.
///
/// Cancel permission is `viewer == requester || viewer_is_admin` .
pub fn job_to_dashboard(
    job: &ActiveRipJob,
    context: &JobContext,
    viewer_id: i64,
    viewer_is_admin: bool,
) -> DashboardJob {
    let total = job.total_tracks as u64;
    let completed = job.cached_count + job.ripped_count + job.failed_count + job.skipped_count;
    let percent = if total > 0 {
        ((completed as f64 / total as f64) * 100.0)
            .round()
            .clamp(0.0, 100.0) as u8
    } else {
        0
    };
    DashboardJob {
        id: job.id.clone(),
        requester_id: job.user_id,
        requester_name: context.requester_name.clone(),
        header: context.header.clone(),
        phase: phase_from(job.phase),
        queue_position: job.queue_position,
        cached: job.cached_count as u64,
        ripped: job.ripped_count as u64,
        failed: job.failed_count as u64,
        total,
        percent,
        is_cancel_allowed_for_viewer: viewer_is_admin || viewer_id == job.user_id,
        job_activity: context.job_activity.clone(),
        download: context.download.clone(),
        upload: context.upload.clone(),
    }
}

/// Build a whole dashboard snapshot from the engine's active jobs.
///
/// Jobs missing a remembered context (e.g. an engine restart missed by the
/// bridge) still render with a fallback header rather than disappearing.
pub fn snapshot_from(
    active: &[ActiveRipJob],
    contexts: &JobContexts,
    viewer_id: i64,
    viewer_is_admin: bool,
    ripping_mode: &str,
    mirror_health: Option<String>,
) -> DashboardSnapshot {
    let mut ordered = active.to_vec();
    ordered.sort_by(|left, right| {
        fn key(job: &ActiveRipJob) -> (u8, u64, u64) {
            match job.phase {
                // The currently running/delivering job is always listed before work
                // waiting in the queue. Queue positions then order pending
                // jobs deterministically; waiting duplicate jobs come after queued jobs.
                EnginePhase::Delivering | EnginePhase::Processing => (0, 0, job.start_time_ms),
                EnginePhase::Queued => {
                    (1, job.queue_position.unwrap_or(u64::MAX), job.start_time_ms)
                }
                EnginePhase::WaitingDuplicate => (2, 0, job.start_time_ms),
                _ => (0, 0, job.start_time_ms),
            }
        }
        key(left)
            .cmp(&key(right))
            .then_with(|| left.id.cmp(&right.id))
    });
    let current_job_activity = ordered.iter().find_map(|job| {
        let context = contexts.get(&job.id);
        context
            .and_then(|value| value.job_activity.clone())
            .or_else(|| {
                context
                    .is_none()
                    .then(|| fallback_job_activity(job))
                    .flatten()
            })
    });
    let current_download = ordered.iter().find_map(|job| {
        (job.phase != EnginePhase::Queued && job.phase != EnginePhase::WaitingDuplicate)
            .then(|| {
                contexts
                    .get(&job.id)
                    .and_then(|context| context.download.clone())
            })
            .flatten()
    });
    let current_upload = ordered.iter().find_map(|job| {
        (job.phase != EnginePhase::Queued && job.phase != EnginePhase::WaitingDuplicate)
            .then(|| {
                contexts
                    .get(&job.id)
                    .and_then(|context| context.upload.clone())
            })
            .flatten()
    });
    let jobs = ordered
        .iter()
        .map(|job| {
            let context = contexts
                .get(&job.id)
                .cloned()
                .unwrap_or_else(|| JobContext {
                    header: job.job_header.clone(),
                    requester_name: job
                        .user_name
                        .clone()
                        .unwrap_or_else(|| format!("User {}", job.user_id)),
                    job_activity: None,
                    download: None,
                    upload: None,
                });
            job_to_dashboard(job, &context, viewer_id, viewer_is_admin)
        })
        .collect();
    DashboardSnapshot {
        ripping_mode: ripping_mode.to_owned(),
        mirror_health,
        current_job_activity,
        current_download,
        current_upload,
        jobs,
    }
}

#[cfg(test)]
mod tests {
    use engine::orchestrator::types::{ByteProgress, RipActivity, TrackLabel};

    use super::*;

    fn engine_job(
        phase: EnginePhase,
        queue_position: Option<u64>,
        user: i64,
        user_name: Option<&str>,
    ) -> ActiveRipJob {
        use tokio_util::sync::CancellationToken;
        ActiveRipJob {
            id: "job_1".into(),
            provider: music::Provider::Apple,
            source_track_ids: vec!["album_1".into()],
            chat_id: 100,
            delivery_chat_id: 100,
            user_id: user,
            user_name: user_name.map(str::to_owned),
            job_header: "Album: <b>X</b> by <b>Y</b>".into(),
            total_tracks: 10,
            status_msg_id: 55,
            controller: CancellationToken::new(),
            is_cancelled: false,
            cancelled_by: None,
            cached_count: 2,
            ripped_count: 3,
            failed_count: 1,
            completed: false,
            start_time_ms: 0,
            queue_position,
            phase,
            terminal_state: None,
            skipped_count: 0,
            is_cache_only: false,
            is_group: false,
            reply_to_message_id: None,
        }
    }

    #[test]
    fn queued_jobs_map_to_waiting_rows_with_position() {
        let job = engine_job(EnginePhase::Queued, Some(3), 7, Some("Alice"));
        let ctx = JobContext {
            header: job.job_header.clone(),
            requester_name: "Alice".into(),
            job_activity: None,
            download: None,
            upload: None,
        };
        let row = job_to_dashboard(&job, &ctx, 7, false);
        assert_eq!(row.phase, JobPhase::Queued);
        assert_eq!(row.queue_position, Some(3));
        assert_eq!(row.requester_name, "Alice");
        assert_eq!(row.cached, 2);
        assert_eq!(row.ripped, 3);
        assert_eq!(row.failed, 1);
        assert_eq!(row.total, 10);
        assert_eq!(row.percent, 60); // (2+3+1+0)/10
    }

    #[test]
    fn resolving_and_processing_map_to_processing() {
        for phase in [
            EnginePhase::Resolving,
            EnginePhase::CheckingCache,
            EnginePhase::Processing,
        ] {
            let job = engine_job(phase, Some(0), 1, None);
            assert_eq!(phase_from(job.phase), JobPhase::Processing);
        }
        assert_eq!(phase_from(EnginePhase::Queued), JobPhase::Queued);
        assert_eq!(phase_from(EnginePhase::Delivering), JobPhase::Delivering);
        assert_eq!(
            phase_from(EnginePhase::WaitingDuplicate),
            JobPhase::WaitingDuplicate
        );
    }

    #[test]
    fn cancel_permission_is_requester_or_admin() {
        let ctx = JobContext {
            header: "h".into(),
            requester_name: "u".into(),
            job_activity: None,
            download: None,
            upload: None,
        };
        let job = engine_job(EnginePhase::Processing, None, 42, Some("Bob"));
        assert!(job_to_dashboard(&job, &ctx, 42, false).is_cancel_allowed_for_viewer);
        assert!(job_to_dashboard(&job, &ctx, 99, true).is_cancel_allowed_for_viewer);
        assert!(!job_to_dashboard(&job, &ctx, 99, false).is_cancel_allowed_for_viewer);
    }

    #[test]
    fn snapshot_includes_all_active_jobs_with_mode_and_health() {
        let mut contexts = JobContexts::new();
        contexts.remember(&engine_job(EnginePhase::Processing, None, 1, Some("A")));
        let active = vec![engine_job(EnginePhase::Processing, None, 1, Some("A"))];
        let snapshot = snapshot_from(&active, &contexts, 1, false, "live", Some("healthy".into()));
        assert_eq!(snapshot.jobs.len(), 1);
        assert_eq!(snapshot.ripping_mode, "live");
        assert_eq!(snapshot.mirror_health.as_deref(), Some("healthy"));
        assert_eq!(snapshot.jobs[0].requester_name, "A");
    }

    #[test]
    fn snapshot_lists_processing_jobs_before_queued_jobs() {
        let mut processing = engine_job(EnginePhase::Processing, Some(0), 1, Some("Drake"));
        processing.id = "processing".into();
        processing.start_time_ms = 20;
        let mut queued = engine_job(EnginePhase::Queued, Some(1), 2, Some("Hitarashi"));
        queued.id = "queued".into();
        queued.start_time_ms = 30;

        let mut contexts = JobContexts::new();
        contexts.remember(&processing);
        contexts.remember(&queued);
        let snapshot = snapshot_from(
            &[queued, processing],
            &contexts,
            1,
            false,
            "live",
            Some("healthy".into()),
        );

        assert_eq!(snapshot.jobs[0].requester_name, "Drake");
        assert_eq!(snapshot.jobs[1].requester_name, "Hitarashi");
    }

    #[test]
    fn progress_activity_is_preserved_for_dashboard_rows() {
        let job = engine_job(EnginePhase::Processing, Some(0), 7, Some("Alice"));
        let download = DownloadLane::Rip(RipActivity::Downloading {
            track: TrackLabel::new("Song", "Artist"),
            progress: ByteProgress {
                completed: 1_048_576,
                total: Some(2_097_152),
            },
        });
        let upload = UploadLane::Track {
            track: TrackLabel::new("Song", "Artist"),
            progress: ByteProgress {
                completed: 1_048_576,
                total: Some(2_097_152),
            },
        };
        let mut contexts = JobContexts::new();
        contexts.remember(&job);
        contexts.remember_progress(&RipJobProgress {
            job_id: job.id.clone(),
            total_tracks: 10,
            completed_tracks: 2,
            cached_count: 0,
            ripped_count: 2,
            failed_count: 0,
            skipped_count: 0,
            percent: 20,
            job_activity: Some(JobActivity::ProcessingNext),
            download: Some(download.clone()),
            upload: Some(upload.clone()),
        });

        let snapshot = snapshot_from(&[job], &contexts, 7, false, "live", None);
        assert_eq!(
            snapshot.jobs[0].job_activity,
            Some(JobActivity::ProcessingNext)
        );
        assert_eq!(snapshot.jobs[0].download, Some(download.clone()));
        assert_eq!(snapshot.jobs[0].upload, Some(upload.clone()));
        assert_eq!(snapshot.current_download, Some(download));
        assert_eq!(snapshot.current_upload, Some(upload));
    }

    #[test]
    fn upload_only_progress_stays_in_the_upload_lane() {
        let job = engine_job(EnginePhase::Processing, Some(0), 7, Some("Alice"));
        let mut contexts = JobContexts::new();
        contexts.remember(&job);
        let upload = UploadLane::Track {
            track: TrackLabel::new("Song", "Artist"),
            progress: ByteProgress {
                completed: 4 * 1_048_576,
                total: None,
            },
        };
        contexts.remember_progress(&RipJobProgress {
            job_id: job.id.clone(),
            total_tracks: 1,
            completed_tracks: 0,
            cached_count: 0,
            ripped_count: 0,
            failed_count: 0,
            skipped_count: 0,
            percent: 0,
            job_activity: None,
            download: None,
            upload: Some(upload.clone()),
        });

        let snapshot = snapshot_from(&[job], &contexts, 7, false, "live", None);
        assert_eq!(snapshot.current_download, None);
        assert_eq!(snapshot.current_upload, Some(upload.clone()));
        assert_eq!(snapshot.jobs[0].download, None);
        assert_eq!(snapshot.jobs[0].upload, Some(upload));
    }

    #[test]
    fn zip_upload_progress_stays_in_the_upload_lane() {
        let job = engine_job(EnginePhase::Processing, Some(0), 7, Some("Alice"));
        let mut contexts = JobContexts::new();
        contexts.remember(&job);
        let upload = UploadLane::ArchiveUpload {
            archive: "Bharat".into(),
            progress: ByteProgress {
                completed: 1_048_576,
                total: Some(2_097_152),
            },
        };
        contexts.remember_progress(&RipJobProgress {
            job_id: job.id.clone(),
            total_tracks: 1,
            completed_tracks: 0,
            cached_count: 0,
            ripped_count: 0,
            failed_count: 0,
            skipped_count: 0,
            percent: 0,
            job_activity: None,
            download: None,
            upload: Some(upload.clone()),
        });

        let snapshot = snapshot_from(&[job], &contexts, 7, false, "live", None);
        assert_eq!(snapshot.current_download, None);
        assert_eq!(snapshot.current_upload, Some(upload));
        let (rendered, _) = crate::dashboard::render(&snapshot, 1);
        assert!(rendered.contains(
            "<b>⬆️ Uploading ZIP:</b> <b>Bharat</b> <code>[■■■■■■□□□□□□] 50% (1.0/2.0 MB)</code>"
        ));
    }

    #[test]
    fn cache_checking_and_resolving_fallback_to_semantic_status() {
        let job_cache = engine_job(EnginePhase::CheckingCache, None, 1, Some("Alice"));
        let contexts = JobContexts::new();
        let s_cache = snapshot_from(&[job_cache], &contexts, 1, false, "live", None);
        assert_eq!(
            s_cache.current_job_activity,
            Some(JobActivity::CheckingCache {
                item: "Album: <b>X</b> by <b>Y</b>".into()
            })
        );
        assert_eq!(s_cache.current_download, None);

        let job_resolve = engine_job(EnginePhase::Resolving, None, 1, Some("Alice"));
        let s_resolve = snapshot_from(&[job_resolve], &contexts, 1, false, "live", None);
        assert_eq!(s_resolve.current_job_activity, Some(JobActivity::Resolving));
        assert_eq!(s_resolve.current_download, None);
    }

    #[test]
    fn percent_is_clamped_and_zero_total_renders_zero() {
        let progress = RipJobProgress {
            job_id: "j".into(),
            total_tracks: 0,
            completed_tracks: 0,
            cached_count: 0,
            ripped_count: 0,
            failed_count: 0,
            skipped_count: 0,
            percent: 0,
            job_activity: None,
            download: None,
            upload: None,
        };
        assert_eq!(percent_from(&progress), 0);
    }
}
