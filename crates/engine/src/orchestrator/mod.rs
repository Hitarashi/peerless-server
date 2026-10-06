pub mod deps;
pub mod types;

mod admissions;
mod cached_lane;
mod dispatch;
mod events;
mod finalize;
mod helpers;
mod inflight;
mod job_run;
mod rip_lane;
mod upload_lane;
mod zip_state;

use std::{
    any::Any,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use admissions::*;
use cached_lane::*;
use dispatch::*;
use events::*;
use finalize::*;
use helpers::*;
use inflight::*;
pub use inflight::{InflightEntry, InflightTargetKey};
use job_run::*;
use music::{Rendition, time::now_ms};
use peerless_core::retry::{exponential_delay, jitter_multiplier};
use rip_lane::*;
use tokio_util::sync::CancellationToken;
use upload_lane::*;
use zip_state::*;

use crate::{
    filename::StandardFilename,
    orchestrator::{
        deps::{
            AlbumCache, AlbumCacheError, AlbumDetailsCaption, AlbumReplacementExpectation,
            AlbumReplacementResult, AlbumUpload, CachedAlbum, CachedTrack, ChatDelivery,
            ChatMessageRef, ChatRef, Delivery, DeliveryError, DeliveryReceipt, DeliveryRejection,
            DumpMessageRef, DumpPublication, DumpPublish, OrchestratorConfig, ProviderDeps,
            SaveTrackInput, StorageRetryPolicy, TaskBookkeeping, TaskDeps, TrackCache,
            TrackCaption, UploadProgressCallback, ZipCaption,
        },
        types::{
            ActiveRipTask, ByteProgress, DownloadLane, EventCallback, FailedTrack, FailedTrackKind,
            OrchestratorEvent, ResolutionFailure, RipActivity, RipTaskOptions, RipTaskProgress,
            RipTaskSummary, TaskActivity, TaskPhase, TerminalTaskState, TrackLabel, UploadLane,
            ZipDeliveryInfo,
        },
    },
    queue::{EnqueueOptions, SequentialRipQueue},
    ripper::{RipError, RipOptions, RipProgressCallback},
    settings::{BotSettings, resolve_default_storefront},
    types::{AlbumTracks, ArtistTracks, Codec, Provider, TargetKind, TrackRipResult},
    zip::{
        TELEGRAM_SPLIT_THRESHOLD_BYTES, ZipTrackEntry, album_generation_hash,
        build_zip_entry_filename_with_codec, create_zip_archive, plan_zip_parts_with_codec,
    },
};

#[derive(Debug)]
pub enum OrchestratorError {
    DependenciesNotSet,
    Cancelled,
    Message(String),
    ResolutionFailed { failures: Vec<ResolutionFailure> },
    AdmissionLimit,
    UserAdmissionLimit,
}

impl std::fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DependenciesNotSet => write!(
                f,
                "RipOrchestrator dependencies not configured. Call setDependencies() first."
            ),
            Self::Cancelled => f.write_str("Download was cancelled"),
            Self::Message(message) => f.write_str(message),
            Self::ResolutionFailed { failures } => {
                write!(f, "Failed to resolve any tracks")?;
                if !failures.is_empty() {
                    write!(f, ": ")?;
                    for (index, failure) in failures.iter().enumerate() {
                        if index > 0 {
                            write!(f, "; ")?;
                        }
                        write!(f, "{failure}")?;
                    }
                }
                Ok(())
            }
            Self::AdmissionLimit => write!(f, "job admission limit reached"),
            Self::UserAdmissionLimit => {
                write!(f, "user already has the maximum number of active jobs")
            }
        }
    }
}

impl std::error::Error for OrchestratorError {}

impl From<crate::queue::QueueError> for OrchestratorError {
    fn from(e: crate::queue::QueueError) -> Self {
        OrchestratorError::Message(e.to_string())
    }
}

struct TaskContext {
    config: OrchestratorConfig,
    options: RipTaskOptions,
    zip_build: bool,
    zip_deliver: bool,
    zip_states: Vec<Arc<ZipState>>,
    zip_reuse: HashMap<Rendition, Vec<CachedAlbum>>,
    zip_expectations: HashMap<Rendition, AlbumReplacementExpectation>,

    zip_reuse_atmos_track_count: Option<usize>,
    zip_album: String,
    zip_artist: String,
    zip_album_id: String,
    zip_album_url: Option<String>,
    zip_genre: Option<String>,
    zip_record_label: Option<String>,
    zip_copyright: Option<String>,
    zip_artwork_url: Option<String>,
    zip_release_date: String,
    warnings: Vec<String>,

    atmos_warning: Arc<std::sync::Mutex<Option<String>>>,

    finalizing_rendition: Arc<Mutex<Option<Rendition>>>,

    fatal_error: Arc<Mutex<Option<String>>>,
    primary_zip_error: Arc<Mutex<Option<String>>>,
    zip_new_dump_messages: Arc<Mutex<Vec<DumpMessageRef>>>,
    is_multi_track: bool,
    max_collection_limit: u32,
    capped_count: usize,
    queue_start_time_ms: u64,
    ripped_count: Arc<std::sync::atomic::AtomicUsize>,
    failed_tracks: Arc<Mutex<Vec<FailedTrack>>>,
    first_delivered_msg_id: Arc<Mutex<Option<ChatMessageRef>>>,
    zip_delivery_infos: Arc<Mutex<Vec<ZipDeliveryInfo>>>,
}

impl TaskContext {
    fn zip_state(&self, rendition: Rendition) -> Option<&Arc<ZipState>> {
        self.zip_states
            .iter()
            .find(|state| state.rendition == rendition)
    }
}

fn set_fatal_error(ctx: &TaskContext, error: impl Into<String>) {
    let mut fatal_error = ctx.fatal_error.lock().expect("fatal error poisoned");
    if fatal_error.is_none() {
        *fatal_error = Some(error.into());
    }
}

fn fatal_error(ctx: &TaskContext) -> Option<String> {
    ctx.fatal_error
        .lock()
        .expect("fatal error poisoned")
        .clone()
}

struct TaskShared {
    job: ActiveRipTask,
    progress: PipelineState,
}

#[derive(Default)]
struct PipelineState {
    job_activity: Mutex<Option<TaskActivity>>,
    download: Mutex<Option<DownloadLane>>,
    upload: Mutex<Option<UploadLane>>,
    codec: Mutex<Option<String>>,
}

pub struct RipOrchestrator {
    config: OrchestratorConfig,
    bus: EventBus,
    jobs: Arc<Mutex<HashMap<String, Arc<Mutex<TaskShared>>>>>,
    queue: SequentialRipQueue,

    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    admissions: Arc<Mutex<Admissions>>,
    cache_delivery_semaphore: Arc<tokio::sync::Semaphore>,
    inflight_items: Arc<Mutex<HashMap<InflightTargetKey, Arc<InflightEntry>>>>,
}

impl Default for RipOrchestrator {
    fn default() -> Self {
        Self::new(OrchestratorConfig::default())
    }
}

impl RipOrchestrator {
    pub fn new(config: OrchestratorConfig) -> Self {
        Self {
            config,
            bus: EventBus::new(),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            queue: SequentialRipQueue::new(),
            upload_lane: Arc::new(Mutex::new(None)),
            admissions: Arc::new(Mutex::new(Admissions::default())),
            cache_delivery_semaphore: Arc::new(tokio::sync::Semaphore::new(2)),
            inflight_items: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn subscribe(&self, callback: EventCallback) {
        self.bus
            .subscribers
            .lock()
            .expect("subscribers poisoned")
            .push(callback);
    }

    pub fn get_active_tasks(&self) -> Vec<ActiveRipTask> {
        self.jobs
            .lock()
            .expect("jobs poisoned")
            .values()
            .filter_map(|shared| {
                let job = &shared.lock().expect("job poisoned").job;
                (!job.completed).then(|| job.clone())
            })
            .collect()
    }

    pub fn get_task(&self, id: &str) -> Option<ActiveRipTask> {
        self.jobs
            .lock()
            .expect("jobs poisoned")
            .get(id)
            .map(|shared| shared.lock().expect("job poisoned").job.clone())
    }
    pub fn cancel_task(&self, id: &str, cancelled_by: Option<&str>) -> bool {
        let Some(shared) = self.jobs.lock().expect("jobs poisoned").get(id).cloned() else {
            return false;
        };

        let cancelled_by = cancelled_by.map(str::to_string);
        let mut guard = shared.lock().expect("job poisoned");
        if guard.job.is_cancelled || guard.job.terminal_state.is_some() {
            return false;
        }
        guard.job.is_cancelled = true;
        guard.job.cancelled_by = cancelled_by;
        guard.job.controller.cancel();
        drop(guard);
        true
    }

    pub async fn start_task<D: TaskDeps>(
        &self,
        deps: Arc<D>,
        options: &RipTaskOptions,
    ) -> Result<RipTaskSummary, OrchestratorError> {
        self.start_task_with_group(deps, options, None).await
    }

    pub async fn start_task_in_group<D: TaskDeps>(
        &self,
        deps: Arc<D>,
        options: &RipTaskOptions,
        group_id: String,
    ) -> Result<RipTaskSummary, OrchestratorError> {
        self.start_task_with_group(deps, options, Some(group_id))
            .await
    }

    async fn start_task_with_group<D: TaskDeps>(
        &self,
        deps: Arc<D>,
        options: &RipTaskOptions,
        admission_group_id: Option<String>,
    ) -> Result<RipTaskSummary, OrchestratorError> {
        if !deps.supports_provider(options.provider.clone()) {
            return Err(OrchestratorError::Message(format!(
                "provider {} is not available",
                options.provider
            )));
        }
        let job_id = cuid2::create_id();
        self.admit_in_group(&job_id, options, admission_group_id.as_deref())?;
        let mut admission_guard = AdmissionGuard::new(Arc::clone(&self.admissions), job_id.clone());

        let settings = deps.settings_snapshot();

        let job_controller = CancellationToken::new();
        let mut job_header = deps.default_job_header().to_owned();
        if options.parsed_items.len() == 1 {
            let it = &options.parsed_items[0];
            job_header = match it.kind {
                TargetKind::Album => format!("Album {}", it.id),
                TargetKind::Playlist => format!("Playlist {}", it.id),
                TargetKind::Artist => format!("Artist {}", it.id),
                TargetKind::Track => format!("Track {}", it.id),
            };
        } else if options.parsed_items.len() > 1 {
            job_header = format!("Batch ({} links)", options.parsed_items.len());
        }

        let shared: Arc<Mutex<TaskShared>> = Arc::new(Mutex::new(TaskShared {
            job: ActiveRipTask {
                id: job_id.clone(),
                provider: options.provider.clone(),
                source_track_ids: options
                    .parsed_items
                    .iter()
                    .map(|item| item.id.clone())
                    .collect(),
                chat_id: options.chat_id,
                delivery_chat_id: options.delivery_chat_id,
                user_id: options.user_id,
                user_name: options.user_name.clone(),
                job_header,
                total_tracks: 0,
                controller: job_controller.clone(),
                is_cancelled: false,
                cancelled_by: None,
                cached_count: 0,
                ripped_count: 0,
                failed_count: 0,
                completed: false,
                start_time_ms: now_ms(),
                queue_position: None,
                phase: TaskPhase::Resolving,
                terminal_state: None,
                skipped_count: 0,
                is_cache_only: options.is_cache_only,
                is_group: options.is_group,
                reply_to_message_id: options.reply_to_message_id,
            },
            progress: PipelineState::default(),
        }));
        self.jobs
            .lock()
            .expect("jobs poisoned")
            .insert(job_id.clone(), Arc::clone(&shared));
        let mut job_table_guard = JobTableGuard::new(Arc::clone(&self.jobs), job_id.clone());
        {
            let guard = shared.lock().expect("job poisoned");
            self.bus.emit(&OrchestratorEvent::Created(&guard.job));
        }

        let target_keys: Vec<InflightTargetKey> = options
            .parsed_items
            .iter()
            .map(|item| InflightTargetKey {
                provider: options.provider.clone(),
                kind: item.kind,
                id: item.id.clone(),
                storefront: item
                    .storefront
                    .clone()
                    .or_else(|| options.single_storefront.clone()),
            })
            .collect();

        let inflight_entry = loop {
            let maybe_inflight = if !target_keys.is_empty() {
                let guard = self.inflight_items.lock().expect("inflight poisoned");
                target_keys.iter().find_map(|k| guard.get(k).cloned())
            } else {
                None
            };

            let Some(inflight) = maybe_inflight else {
                let entry = Arc::new(InflightEntry {
                    job_id: job_id.clone(),
                    notify: Arc::new(tokio::sync::Notify::new()),
                    success: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                });
                let mut guard = self.inflight_items.lock().expect("inflight poisoned");
                for key in &target_keys {
                    guard.insert(key.clone(), Arc::clone(&entry));
                }
                break entry;
            };

            self.set_phase(&shared, TaskPhase::WaitingDuplicate);
            self.bus.set_job_activity(
                &shared,
                Some(TaskActivity::WaitingDuplicate {
                    inflight_job_id: inflight.job_id.clone(),
                }),
            );
            self.bus.emit_progress(&shared);

            let notify = Arc::clone(&inflight.notify);
            tokio::select! {
                _ = notify.notified() => {}
                _ = job_controller.cancelled() => {
                    self.terminalize(&shared, TerminalTaskState::Cancelled, None, None);
                    job_table_guard.remove();
                    admission_guard.release();
                    return Err(OrchestratorError::Cancelled);
                }
            }
        };

        let inflight_guard = InflightGuard::new(
            Arc::clone(&self.inflight_items),
            target_keys,
            inflight_entry,
        );

        let result =
            match futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(self.run_job(
                Arc::clone(&deps),
                options,
                Arc::clone(&shared),
                job_controller,
                settings,
            )))
            .await
            {
                Ok(result) => result,
                Err(panic) => Err(OrchestratorError::Message(panic_message(panic))),
            };

        inflight_guard.finish(result.is_ok());

        let cancelled = shared.lock().expect("job poisoned").job.is_cancelled;
        let result = if cancelled {
            self.terminalize(&shared, TerminalTaskState::Cancelled, None, None);

            result
        } else {
            match &result {
                Ok(summary) => {
                    self.terminalize(&shared, TerminalTaskState::Completed, Some(summary), None);
                }
                Err(err) => {
                    let message = err.to_string();
                    self.terminalize(&shared, TerminalTaskState::Failed, None, Some(&message));
                }
            }
            result
        };
        job_table_guard.remove();
        admission_guard.release();
        result
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;

    fn options(user_id: i64, is_admin: bool) -> RipTaskOptions {
        RipTaskOptions {
            provider: Provider::Apple,
            chat_id: user_id,
            user_id,
            user_name: None,
            delivery_chat_id: user_id,
            is_group: false,
            is_force: false,
            is_cache_only: false,
            single_storefront: None,
            parsed_items: Vec::new(),
            reply_to_message_id: None,
            is_admin,
            codec_preference: None,
            rendition_policy: music::RenditionPolicy::PrimaryOnly,
        }
    }

    #[test]
    fn job_ids_are_cuid2_and_fit_callbacks() {
        let id = cuid2::create_id();
        let next_id = cuid2::create_id();
        assert!(cuid2::is_cuid2(&id));
        assert_ne!(id, next_id);
        assert_eq!(id.len(), 24);
        let callback_data = format!("cancel:{id}");
        assert!(callback_data.len() <= 64);
        assert_eq!(
            callback_data.strip_prefix("cancel:").map(str::trim),
            Some(id.as_str())
        );
    }

    #[test]
    fn admission_limits_users_and_global_jobs() {
        let orchestrator = RipOrchestrator::new(OrchestratorConfig::test());

        let user = options(1, false);
        for job in ["u1", "u2", "u3", "u4"] {
            orchestrator.admit(job, &user).expect("user job");
        }
        assert!(matches!(
            orchestrator.admit("u5", &user),
            Err(OrchestratorError::UserAdmissionLimit)
        ));

        let admin = options(2, true);
        for job in ["a1", "a2", "a3", "a4", "a5"] {
            orchestrator.admit(job, &admin).expect("admin job");
        }

        for user_id in 3..=14 {
            orchestrator
                .admit(&format!("j{user_id}"), &options(user_id, false))
                .expect("global capacity");
        }
        assert!(matches!(
            orchestrator.admit("overflow", &options(100, false)),
            Err(OrchestratorError::AdmissionLimit)
        ));

        orchestrator.release_admission("j3");
        orchestrator
            .admit("after-release", &options(100, false))
            .expect("released slot");
    }
}
