pub mod caption;
pub mod deps;
pub mod types;

use std::{
    any::Any,
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use music::{Rendition, time::now_ms};
use tokio_util::sync::CancellationToken;

use crate::{
    filename::StandardFilename,
    orchestrator::{
        caption::{
            AlbumDetailsCaptionMetadata, DumpCaptionMetadata, DumpZipCaptionMetadata,
            clamp_str_utf16, format_album_details_caption, format_dump_caption,
            format_zip_dump_caption, html_escape,
        },
        deps::{
            AlbumCache, AlbumCacheError, AlbumReplacementExpectation, AlbumReplacementResult,
            AlbumUpload, CachedAlbum, CachedTrack, ChatDelivery, ChatMessageRef, ChatRef, Delivery,
            DeliveryError, DeliveryReceipt, DeliveryRejection, DumpMessageRef, DumpPublication,
            DumpPublish, OrchestratorConfig, ProviderDeps, SaveTrackInput, StorageRetryPolicy,
            TaskBookkeeping, TaskDeps, TrackCache, UploadProgressCallback,
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

async fn find_cached_tracks_with_retry<D: TrackCache>(
    cache: &D,
    track_ids: &[String],
    policy: &StorageRetryPolicy,
) -> Result<HashMap<(String, Codec), CachedTrack>, crate::orchestrator::deps::TrackCacheError> {
    let attempts = policy.total_attempts.max(1);
    let mut attempt = 0;
    loop {
        match cache.find_cached_tracks(track_ids).await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_unavailable() && attempt + 1 < attempts => {
                tokio::time::sleep(policy.delay_before_retry(attempt)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn save_track_with_retry<D: TrackCache>(
    cache: &D,
    input: SaveTrackInput,
    policy: &StorageRetryPolicy,
) -> Result<(), crate::orchestrator::deps::TrackCacheError> {
    let attempts = policy.total_attempts.max(1);
    let mut attempt = 0;
    loop {
        match cache.save_track(input.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_unavailable() && attempt + 1 < attempts => {
                tokio::time::sleep(policy.delay_before_retry(attempt)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedTrackItem {
    id: String,
    title: Option<String>,
    artist: Option<String>,
    artwork_url: Option<String>,
    storefront: Option<String>,
    is_streamable: Option<bool>,
}

#[derive(Clone)]
struct PipelineItem {
    track_id: String,
    storefront: Option<String>,
    meta_title: Option<String>,
    meta_artist: Option<String>,
    artwork_url: Option<String>,
    is_streamable: Option<bool>,
    rendition: Rendition,
    cached: Option<CachedTrack>,
    track_index: Option<u32>,
    total_tracks: Option<u32>,
}

struct PipelineRipResult {
    track_id: String,
    rip_result: TrackRipResult,
    start_time_ms: u64,
    rendition: Rendition,
    track_index: Option<u32>,
    total_tracks: Option<u32>,
}

struct ZipState {
    rendition: Rendition,
    dir: PathBuf,
    sources: Arc<Mutex<Vec<ZipTrackEntry>>>,
    codec: Arc<Mutex<Option<String>>>,
    generation_hash: Option<String>,
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

fn set_primary_zip_error(ctx: &TaskContext, error: impl Into<String>) {
    let mut primary_zip_error = ctx
        .primary_zip_error
        .lock()
        .expect("primary ZIP error poisoned");
    if primary_zip_error.is_none() {
        *primary_zip_error = Some(error.into());
    }
}

fn primary_zip_error(ctx: &TaskContext) -> Option<String> {
    ctx.primary_zip_error
        .lock()
        .expect("primary ZIP error poisoned")
        .clone()
}

fn remember_zip_dump_message(ctx: &TaskContext, message_id: DumpMessageRef) {
    ctx.zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned")
        .push(message_id);
}

fn transfer_zip_dump_messages(ctx: &TaskContext, committed: &[DumpMessageRef]) {
    let mut pending = ctx
        .zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned");
    for message_id in committed {
        if let Some(index) = pending
            .iter()
            .position(|pending_id| pending_id == message_id)
        {
            pending.remove(index);
        }
    }
}

fn take_uncommitted_zip_dump_messages(ctx: &TaskContext) -> Vec<DumpMessageRef> {
    let mut pending = ctx
        .zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned");
    std::mem::take(&mut *pending)
}

fn archive_codec_replaced(replacement: Codec, existing: Codec) -> bool {
    match replacement {
        Codec::Alac | Codec::Aac => matches!(existing, Codec::Alac | Codec::Aac),
        other => existing == other,
    }
}

fn record_lane_task_panic(
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &TaskContext,
    item_id: &str,
    message: String,
) {
    let error = format!("lane-2 task panicked for {item_id}: {message}");
    set_fatal_error(ctx, error.clone());
    let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
    if !failures.iter().any(|failure| failure.id == item_id) {
        failures.push(FailedTrack {
            id: item_id.to_owned(),
            error,
            kind: None,
            title: None,
            artist: None,
            storefront: None,
        });
        shared.lock().expect("job poisoned").job.failed_count = failures.len();
    }
}

fn seed_zip_codec(state: &ZipState, codec: Codec) {
    let rank = |codec: Codec| match codec {
        Codec::Alac => 3,
        Codec::Ec3 => 2,
        Codec::Aac => 1,
    };
    let mut current = state.codec.lock().expect("zip codec poisoned");
    if current
        .as_deref()
        .and_then(|value| value.parse::<Codec>().ok())
        .is_none_or(|existing| rank(codec) > rank(existing))
    {
        *current = Some(codec.as_str().to_owned());
    }
}

fn codec_allowed_for_rendition(rendition: Rendition, codec: Codec) -> bool {
    match rendition {
        Rendition::Primary => matches!(codec, Codec::Alac | Codec::Aac),
        Rendition::Atmos => codec == Codec::Ec3,
    }
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

#[derive(Clone)]
struct EventBus {
    subscribers: Arc<Mutex<Vec<EventCallback>>>,
}

struct Admission {
    user_id: i64,
    is_admin: bool,
    group_id: Option<String>,
}

#[derive(Default)]
struct Admissions {
    jobs: HashMap<String, Admission>,
}

impl EventBus {
    fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn emit(&self, event: &OrchestratorEvent<'_>) {
        for cb in self
            .subscribers
            .lock()
            .expect("subscribers poisoned")
            .iter()
        {
            let callback_result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(event)));
            if callback_result.is_err() {
                tracing::error!("orchestrator event subscriber panicked");
            }
        }
    }

    fn set_job_activity(&self, shared: &Arc<Mutex<TaskShared>>, activity: Option<TaskActivity>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .job_activity
            .lock()
            .expect("job activity poisoned") = activity;
    }

    fn set_download(&self, shared: &Arc<Mutex<TaskShared>>, lane: Option<DownloadLane>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .download
            .lock()
            .expect("download progress poisoned") = lane;
    }

    fn set_upload(&self, shared: &Arc<Mutex<TaskShared>>, lane: Option<UploadLane>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .upload
            .lock()
            .expect("upload progress poisoned") = lane;
    }

    fn set_codec(&self, shared: &Arc<Mutex<TaskShared>>, codec: Option<String>) {
        if codec.is_none() {
            return;
        }
        let guard = shared.lock().expect("job poisoned");
        *guard.progress.codec.lock().expect("codec poisoned") = codec;
    }

    fn emit_progress(&self, shared: &Arc<Mutex<TaskShared>>) {
        let (job, progress) = {
            let guard = shared.lock().expect("job poisoned");
            let completed_tracks = guard.job.cached_count
                + guard.job.ripped_count
                + guard.job.failed_count
                + guard.job.skipped_count;
            let percent = if guard.job.total_tracks > 0 {
                ((completed_tracks as f64 / guard.job.total_tracks as f64) * 100.0).round() as u32
            } else {
                0
            };
            let progress = RipTaskProgress {
                job_id: guard.job.id.clone(),
                total_tracks: guard.job.total_tracks,
                completed_tracks,
                cached_count: guard.job.cached_count,
                ripped_count: guard.job.ripped_count,
                failed_count: guard.job.failed_count,
                skipped_count: guard.job.skipped_count,
                percent,
                job_activity: guard
                    .progress
                    .job_activity
                    .lock()
                    .expect("job activity poisoned")
                    .clone(),
                download: guard
                    .progress
                    .download
                    .lock()
                    .expect("download progress poisoned")
                    .clone(),
                upload: guard
                    .progress
                    .upload
                    .lock()
                    .expect("upload progress poisoned")
                    .clone(),
                codec: guard.progress.codec.lock().expect("codec poisoned").clone(),
            };
            (guard.job.clone(), progress)
        };
        self.emit(&OrchestratorEvent::Progress(&job, &progress));
    }
}

struct DownloadProgressGuard {
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
}

impl Drop for DownloadProgressGuard {
    fn drop(&mut self) {
        self.bus.set_download(&self.shared, None);
        self.bus.emit_progress(&self.shared);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InflightTargetKey {
    pub provider: Provider,
    pub kind: TargetKind,
    pub id: String,
    pub storefront: Option<String>,
}

pub struct InflightEntry {
    pub job_id: String,
    pub notify: Arc<tokio::sync::Notify>,
    pub success: Arc<std::sync::atomic::AtomicBool>,
}

struct InflightGuard {
    table: Arc<Mutex<HashMap<InflightTargetKey, Arc<InflightEntry>>>>,
    keys: Vec<InflightTargetKey>,
    entry: Arc<InflightEntry>,
    armed: bool,
}

impl InflightGuard {
    fn new(
        table: Arc<Mutex<HashMap<InflightTargetKey, Arc<InflightEntry>>>>,
        keys: Vec<InflightTargetKey>,
        entry: Arc<InflightEntry>,
    ) -> Self {
        Self {
            table,
            keys,
            entry,
            armed: true,
        }
    }

    fn finish(mut self, success: bool) {
        if success {
            self.entry
                .success
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.cleanup();
    }

    fn cleanup(&mut self) {
        if self.armed {
            {
                let mut guard = self.table.lock().expect("inflight poisoned");
                for key in &self.keys {
                    guard.remove(key);
                }
            }
            self.entry.notify.notify_waiters();
            self.armed = false;
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.cleanup();
    }
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

struct LaneTask {
    run: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,

    label: &'static str,

    on_panic: Option<Box<dyn FnOnce(String) + Send>>,
}

type FinalizeResult = Result<RipTaskSummary, String>;
type FinalizeSender = tokio::sync::oneshot::Sender<FinalizeResult>;

struct FinalizationGuard {
    summary_tx: Arc<Mutex<Option<FinalizeSender>>>,
    workspace_paths: Vec<PathBuf>,
}

impl FinalizationGuard {
    fn new(
        summary_tx: Arc<Mutex<Option<FinalizeSender>>>,
        rip_job_dir: PathBuf,
        zip_states: &[Arc<ZipState>],
    ) -> Self {
        let mut workspace_paths = Vec::with_capacity(zip_states.len() + 1);
        workspace_paths.push(rip_job_dir);
        workspace_paths.extend(zip_states.iter().map(|state| state.dir.clone()));
        Self {
            summary_tx,
            workspace_paths,
        }
    }

    fn send(&mut self, result: FinalizeResult) {
        if let Some(tx) = self
            .summary_tx
            .lock()
            .expect("summary sender poisoned")
            .take()
        {
            let _ = tx.send(result);
        }
    }

    async fn finish(&mut self, result: FinalizeResult) {
        for path in &self.workspace_paths {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
        self.send(result);
    }
}

struct AdmissionGuard {
    admissions: Arc<Mutex<Admissions>>,
    job_id: String,
    armed: bool,
}

impl AdmissionGuard {
    fn new(admissions: Arc<Mutex<Admissions>>, job_id: String) -> Self {
        Self {
            admissions,
            job_id,
            armed: true,
        }
    }

    fn release(&mut self) {
        if self.armed {
            self.admissions
                .lock()
                .expect("admissions poisoned")
                .jobs
                .remove(&self.job_id);
            self.armed = false;
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct JobTableGuard {
    jobs: Arc<Mutex<HashMap<String, Arc<Mutex<TaskShared>>>>>,
    job_id: String,
    armed: bool,
}

impl JobTableGuard {
    fn new(jobs: Arc<Mutex<HashMap<String, Arc<Mutex<TaskShared>>>>>, job_id: String) -> Self {
        Self {
            jobs,
            job_id,
            armed: true,
        }
    }

    fn remove(&mut self) {
        if self.armed {
            self.jobs
                .lock()
                .expect("jobs poisoned")
                .remove(&self.job_id);
            self.armed = false;
        }
    }
}

impl Drop for JobTableGuard {
    fn drop(&mut self) {
        self.remove();
    }
}

struct WorkspaceGuard {
    paths: Vec<PathBuf>,
    armed: bool,
}

impl WorkspaceGuard {
    fn new() -> Self {
        Self {
            paths: Vec::new(),
            armed: true,
        }
    }

    fn add(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WorkspaceGuard {
    fn drop(&mut self) {
        if self.armed {
            for path in &self.paths {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }
}

impl Drop for FinalizationGuard {
    fn drop(&mut self) {
        for path in &self.workspace_paths {
            let _ = std::fs::remove_dir_all(path);
        }
        self.send(Err("finalize marker stopped unexpectedly".to_owned()));
    }
}

async fn push_lane_task(
    lane: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    label: &'static str,
    on_panic: Option<Box<dyn FnOnce(String) + Send>>,
    job_cancellation: Option<&CancellationToken>,
    queue_cancellation: Option<&CancellationToken>,
    run: impl FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
) -> bool {
    let tx = {
        let guard = lane.lock().expect("upload lane poisoned");
        match guard.as_ref() {
            Some(tx) => tx.clone(),
            None => return false,
        }
    };
    let task = LaneTask {
        run: Box::new(run),
        label,
        on_panic,
    };
    let send_result = match (job_cancellation, queue_cancellation) {
        (Some(job_cancellation), Some(queue_cancellation)) => {
            tokio::select! {
                result = tx.send(task) => result,
                _ = job_cancellation.cancelled() => return false,
                _ = queue_cancellation.cancelled() => return false,
            }
        }
        (Some(job_cancellation), None) => {
            tokio::select! {
                result = tx.send(task) => result,
                _ = job_cancellation.cancelled() => return false,
            }
        }
        (None, Some(queue_cancellation)) => {
            tokio::select! {
                result = tx.send(task) => result,
                _ = queue_cancellation.cancelled() => return false,
            }
        }
        (None, None) => tx.send(task).await,
    };
    match send_result {
        Ok(()) => true,
        Err(_) => {
            tracing::error!(
                lane = "upload",
                item = label,
                "upload lane closed; task dropped"
            );
            false
        }
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

    fn ensure_upload_lane(&self) -> tokio::sync::mpsc::Sender<LaneTask> {
        let mut guard = self.upload_lane.lock().expect("upload lane poisoned");
        if let Some(tx) = guard.as_ref() {
            return tx.clone();
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LaneTask>(16);
        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                let LaneTask {
                    run,
                    label,
                    on_panic,
                } = task;
                let result = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    async move { run().await },
                ))
                .await;
                if let Err(panic) = result {
                    let message = panic_message(panic);
                    if let Some(on_panic) = on_panic {
                        on_panic(message.clone());
                    }
                    tracing::error!(
                        lane = "upload",
                        item = label,
                        error = %message,
                        "upload lane task panicked; lane continues"
                    );
                }
            }
        });
        *guard = Some(tx.clone());
        tx
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

    fn set_phase(&self, shared: &Arc<Mutex<TaskShared>>, phase: TaskPhase) {
        shared.lock().expect("job poisoned").job.phase = phase;
    }

    fn terminalize(
        &self,
        shared: &Arc<Mutex<TaskShared>>,
        state: TerminalTaskState,
        summary: Option<&RipTaskSummary>,
        error: Option<&str>,
    ) -> bool {
        let (job, cancelled_by, state) = {
            let mut guard = shared.lock().expect("job poisoned");
            if guard.job.terminal_state.is_some() {
                return false;
            }

            let state = if state != TerminalTaskState::Cancelled
                && (guard.job.is_cancelled || guard.job.controller.is_cancelled())
            {
                TerminalTaskState::Cancelled
            } else {
                state
            };
            guard.job.terminal_state = Some(state);
            guard.job.completed = true;
            (guard.job.clone(), guard.job.cancelled_by.clone(), state)
        };
        match state {
            TerminalTaskState::Completed => {
                if let Some(summary) = summary {
                    self.bus.emit(&OrchestratorEvent::Completed(&job, summary));
                }
            }
            TerminalTaskState::Cancelled => {
                self.bus
                    .emit(&OrchestratorEvent::Cancelled(&job, &cancelled_by));
            }
            TerminalTaskState::Failed => {
                if let Some(error) = error {
                    self.bus.emit(&OrchestratorEvent::Failed(&job, error));
                }
            }
        }
        true
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

    #[cfg(test)]
    fn admit(&self, job_id: &str, options: &RipTaskOptions) -> Result<(), OrchestratorError> {
        self.admit_in_group(job_id, options, None)
    }

    fn admit_in_group(
        &self,
        job_id: &str,
        options: &RipTaskOptions,
        group_id: Option<&str>,
    ) -> Result<(), OrchestratorError> {
        let mut admissions = self.admissions.lock().expect("admissions poisoned");

        if !options.is_admin {
            let group_already_admitted = group_id.is_some_and(|group_id| {
                admissions.jobs.values().any(|admission| {
                    !admission.is_admin
                        && admission.user_id == options.user_id
                        && admission.group_id.as_deref() == Some(group_id)
                })
            });
            let mut global_groups = HashSet::new();
            let mut user_groups = HashSet::new();
            for (existing_job_id, admission) in &admissions.jobs {
                if admission.is_admin {
                    continue;
                }
                let group_key = admission
                    .group_id
                    .clone()
                    .unwrap_or_else(|| existing_job_id.clone());
                global_groups.insert((admission.user_id, group_key.clone()));
                if admission.user_id == options.user_id {
                    user_groups.insert(group_key);
                }
            }

            if !group_already_admitted && global_groups.len() >= 16 {
                return Err(OrchestratorError::AdmissionLimit);
            }
            if !group_already_admitted && user_groups.len() >= 4 {
                return Err(OrchestratorError::UserAdmissionLimit);
            }
        }
        admissions.jobs.insert(
            job_id.to_owned(),
            Admission {
                user_id: options.user_id,
                is_admin: options.is_admin,
                group_id: group_id.map(str::to_owned),
            },
        );
        Ok(())
    }

    pub fn release_admission(&self, job_id: &str) {
        self.admissions
            .lock()
            .expect("admissions poisoned")
            .jobs
            .remove(job_id);
    }

    async fn run_job<D: TaskDeps>(
        &self,
        deps: Arc<D>,
        options: &RipTaskOptions,
        shared: Arc<Mutex<TaskShared>>,
        job_controller: CancellationToken,
        settings: BotSettings,
    ) -> Result<RipTaskSummary, OrchestratorError> {
        self.set_phase(&shared, TaskPhase::Resolving);
        self.bus
            .set_job_activity(&shared, Some(TaskActivity::Resolving));
        self.bus.emit_progress(&shared);

        let mut resolved_tracks: Vec<ResolvedTrackItem> = Vec::new();
        let mut album_name: Option<String> = None;
        let mut album_artist: Option<String> = None;
        let mut album_artwork_url: Option<String> = None;
        let mut album_release_date: Option<String> = None;
        let mut album_genre: Option<String> = None;
        let mut album_record_label: Option<String> = None;
        let mut album_copyright: Option<String> = None;
        let mut album_id: Option<String> = None;
        let mut album_sf: Option<String> = None;
        let mut resolution_failures = Vec::new();

        for item in &options.parsed_items {
            if job_controller.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }

            let effective_sf = item
                .storefront
                .clone()
                .or_else(|| options.single_storefront.clone())
                .unwrap_or_else(|| resolve_default_storefront(&settings).to_owned());

            let resolution: Result<(), String> = match item.kind {
                TargetKind::Track => {
                    resolved_tracks.push(ResolvedTrackItem {
                        id: item.id.clone(),
                        title: None,
                        artist: None,
                        artwork_url: None,
                        storefront: Some(effective_sf.clone()),
                        is_streamable: None,
                    });
                    Ok(())
                }
                TargetKind::Album => match deps
                    .fetch_album_tracks(
                        options.provider.clone(),
                        &item.id,
                        effective_sf.as_str().into(),
                    )
                    .await
                {
                    Ok(AlbumTracks { album, tracks }) => {
                        album_name = Some(album.album.clone());
                        album_artist = Some(album.artist.clone());
                        album_artwork_url = Some(album.artwork_url.clone());
                        album_release_date = Some(album.release_date.clone());
                        album_genre = album.genre.clone();
                        album_record_label = album.record_label.clone();
                        album_copyright = album.copyright.clone();
                        album_id = Some(item.id.clone());
                        album_sf = Some(effective_sf.clone());
                        for t in tracks {
                            resolved_tracks.push(ResolvedTrackItem {
                                id: t.id.clone(),
                                title: Some(t.title.clone()),
                                artist: Some(t.artist.clone()),
                                artwork_url: if t.artwork_url.is_empty() {
                                    (!album.artwork_url.is_empty())
                                        .then(|| album.artwork_url.clone())
                                } else {
                                    Some(t.artwork_url.clone())
                                },
                                storefront: Some(effective_sf.clone()),
                                is_streamable: t.is_streamable,
                            });
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                TargetKind::Artist => {
                    match deps
                        .fetch_artist_tracks(
                            options.provider.clone(),
                            &item.id,
                            effective_sf.as_str().into(),
                        )
                        .await
                    {
                        Ok(ArtistTracks {
                            artist_name,
                            tracks,
                            ..
                        }) => {
                            album_artist = Some(artist_name.clone());
                            for t in tracks {
                                resolved_tracks.push(ResolvedTrackItem {
                                    id: t.id.clone(),
                                    title: Some(t.title.clone()),
                                    artist: Some(t.artist.clone()),
                                    artwork_url: (!t.artwork_url.is_empty())
                                        .then(|| t.artwork_url.clone()),
                                    storefront: Some(effective_sf.clone()),
                                    is_streamable: None,
                                });
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                TargetKind::Playlist => {
                    match deps
                        .fetch_playlist_tracks(
                            options.provider.clone(),
                            &item.id,
                            effective_sf.as_str().into(),
                        )
                        .await
                    {
                        Ok(data) => {
                            for t in data.tracks {
                                resolved_tracks.push(ResolvedTrackItem {
                                    id: t.id.clone(),
                                    title: Some(t.title.clone()),
                                    artist: Some(t.artist.clone()),
                                    artwork_url: None,
                                    storefront: Some(effective_sf.clone()),
                                    is_streamable: None,
                                });
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            };

            if let Err(err_msg) = resolution {
                tracing::error!(
                    kind = kind_str(item.kind),
                    id = %item.id,
                    error = %err_msg,
                    "Failed to resolve target item"
                );
                resolution_failures.push(ResolutionFailure {
                    kind: item.kind,
                    id: item.id.clone(),
                    error: err_msg,
                });
            }
        }

        if resolved_tracks.is_empty() {
            if resolution_failures.is_empty() {
                resolution_failures.push(ResolutionFailure {
                    kind: TargetKind::Track,
                    id: String::new(),
                    error: "No valid tracks found to process.".to_string(),
                });
            }
            return Err(OrchestratorError::ResolutionFailed {
                failures: resolution_failures,
            });
        }

        let mut seen_ids: HashSet<String> = HashSet::new();
        let unique_tracks: Vec<ResolvedTrackItem> = resolved_tracks
            .into_iter()
            .filter(|t| seen_ids.insert(t.id.clone()))
            .collect();

        let mut capped_count = 0usize;
        let max_collection_limit = settings.max_collection_tracks;
        let mut tracks_to_process = unique_tracks;
        if !options.is_admin
            && max_collection_limit > 0
            && tracks_to_process.len() > max_collection_limit as usize
        {
            capped_count = tracks_to_process.len() - max_collection_limit as usize;
            let original = tracks_to_process.len();
            tracks_to_process.truncate(max_collection_limit as usize);
            tracing::warn!(
                user_id = options.user_id,
                original,
                capped = max_collection_limit,
                "Collection capped for non-admin user"
            );
        }

        fn non_empty(s: &Option<String>) -> Option<&str> {
            s.as_deref().filter(|s| !s.is_empty())
        }
        let header = match (non_empty(&album_name), non_empty(&album_artist)) {
            (Some(name), Some(artist)) => {
                if let (Some(id), Some(sf)) = (&album_id, &album_sf) {
                    let album_url =
                        deps.album_url(options.provider.clone(), id, sf.as_str().into());
                    if let Some(album_url) = album_url {
                        format!(
                            "Album: <a href=\"{album_url}\"><b>{}</b></a> by <b>{}</b>",
                            html_escape(name),
                            html_escape(artist)
                        )
                    } else {
                        format!(
                            "Album: <b>{}</b> by <b>{}</b>",
                            html_escape(name),
                            html_escape(artist)
                        )
                    }
                } else {
                    format!(
                        "Album: <b>{}</b> by <b>{}</b>",
                        html_escape(name),
                        html_escape(artist)
                    )
                }
            }
            _ if tracks_to_process.len() == 1 => {
                let first = &tracks_to_process[0];
                match (non_empty(&first.title), non_empty(&first.artist)) {
                    (Some(title), Some(artist)) => {
                        format!(
                            "<b>{}</b> - <b>{}</b>",
                            html_escape(title),
                            html_escape(artist)
                        )
                    }
                    _ if options.is_cache_only => {
                        format!("Track Cache: <code>{}</code>", html_escape(&first.id))
                    }
                    _ => format!("Track ID: <code>{}</code>", html_escape(&first.id)),
                }
            }
            _ if options.is_cache_only => {
                format!("Batch Cache: <b>{} tracks</b>", tracks_to_process.len())
            }
            _ => format!("Batch: <b>{} tracks</b>", tracks_to_process.len()),
        };
        {
            let mut guard = shared.lock().expect("job poisoned");
            guard.job.job_header = header;
            guard.job.total_tracks = tracks_to_process.len();
        }

        let is_album_job = options.parsed_items.len() == 1
            && options.parsed_items[0].kind == TargetKind::Album
            && tracks_to_process.len() > 1;
        let zip_build = is_album_job;

        let zip_deliver = is_album_job && !options.is_cache_only;
        let mut warnings = Vec::new();

        let zip_generation_hash = zip_build.then(|| {
            let ids: Vec<&str> = tracks_to_process.iter().map(|t| t.id.as_str()).collect();
            album_generation_hash(options.provider.as_str(), &options.parsed_items[0].id, &ids)
        });
        let mut workspace_guard = WorkspaceGuard::new();
        let mut zip_states: Vec<Arc<ZipState>> = Vec::new();
        if zip_build {
            for rendition in options.rendition_policy.renditions() {
                let dir = std::env::temp_dir().join(format!(
                    "zip_job_{}_{}",
                    cuid2::create_id(),
                    match rendition {
                        Rendition::Primary => "primary",
                        Rendition::Atmos => "atmos",
                    }
                ));
                workspace_guard.add(dir.clone());
                if let Err(error) = tokio::fs::create_dir_all(&dir).await {
                    return Err(OrchestratorError::Message(format!(
                        "create ZIP workspace: {error}"
                    )));
                }
                zip_states.push(Arc::new(ZipState {
                    rendition: *rendition,
                    dir,
                    sources: Arc::new(Mutex::new(Vec::new())),
                    codec: Arc::new(Mutex::new(None)),
                    generation_hash: zip_generation_hash.clone(),
                }));
            }
        }

        self.set_phase(&shared, TaskPhase::CheckingCache);
        let check_item = { shared.lock().expect("job poisoned").job.job_header.clone() };
        self.bus.set_job_activity(
            &shared,
            Some(TaskActivity::CheckingCache { item: check_item }),
        );
        self.bus.emit_progress(&shared);
        let requested_ids: Vec<String> = tracks_to_process
            .iter()
            .map(|track| track.id.clone())
            .collect();
        let mut existing_tracks_map = match find_cached_tracks_with_retry(
            deps.as_ref(),
            &requested_ids,
            &self.config.storage_retry,
        )
        .await
        {
            Ok(existing) => existing,
            Err(error) => {
                tracing::error!(%error, "track cache lookup failed; refusing to start media work");
                return Err(OrchestratorError::Message(error.to_string()));
            }
        };

        self.bus.set_job_activity(&shared, None);

        if options.is_force && options.is_admin {
            let mut old_message_ids: Vec<DumpMessageRef> = Vec::new();
            for item in &tracks_to_process {
                for rendition in options.rendition_policy.renditions() {
                    for codec in rendition.accepted_cache_codecs() {
                        let lookup_key = (item.id.clone(), *codec);
                        if let Some(cached) = existing_tracks_map.remove(&lookup_key) {
                            old_message_ids.push(DumpMessageRef::new(cached.message_id));
                            let _ = deps.delete_track(&item.id, Some(*codec)).await;
                        }
                    }
                }
            }
            if !old_message_ids.is_empty() {
                tracing::debug!(
                    count = old_message_ids.len(),
                    "Deleting old dump messages on force re-rip prior to queue"
                );
                let _ = deps.retract_dump(&old_message_ids).await;
            }
        }

        let mut pipeline_items: Vec<PipelineItem> = Vec::new();
        let cached_count = 0usize;
        let is_multi_track = tracks_to_process.len() > 1;
        let first_delivered_msg_id: Option<ChatMessageRef> = None;

        let existing_album_rows = if zip_build {
            match deps
                .find_albums(options.provider.clone(), &options.parsed_items[0].id, None)
                .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    return Err(OrchestratorError::Message(format!(
                        "album ZIP cache lookup failed: {error}"
                    )));
                }
            }
        } else {
            Vec::new()
        };

        let zip_expectations: HashMap<Rendition, AlbumReplacementExpectation> = if zip_build {
            options
                .rendition_policy
                .renditions()
                .iter()
                .map(|rendition| {
                    let replacement_codec = match rendition {
                        Rendition::Primary => Codec::Alac,
                        Rendition::Atmos => Codec::Ec3,
                    };
                    let rows = existing_album_rows
                        .iter()
                        .filter(|row| archive_codec_replaced(replacement_codec, row.codec))
                        .collect::<Vec<_>>();
                    let expectation = match rows.first() {
                        None => AlbumReplacementExpectation::Empty,
                        Some(first)
                            if rows
                                .iter()
                                .all(|row| row.generation_hash == first.generation_hash) =>
                        {
                            AlbumReplacementExpectation::Generation(first.generation_hash.clone())
                        }
                        Some(_) => AlbumReplacementExpectation::Mixed,
                    };
                    (*rendition, expectation)
                })
                .collect()
        } else {
            HashMap::new()
        };

        let mut zip_reuse: HashMap<Rendition, Vec<CachedAlbum>> = HashMap::new();
        if let Some(hash) = &zip_generation_hash
            && !options.is_force
        {
            for rendition in options.rendition_policy.renditions() {
                let codecs: &[Codec] = match rendition {
                    Rendition::Primary => &[Codec::Alac, Codec::Aac],
                    Rendition::Atmos => &[Codec::Ec3],
                };
                for codec in codecs {
                    let rows = existing_album_rows
                        .iter()
                        .filter(|row| row.codec == *codec)
                        .cloned()
                        .collect::<Vec<_>>();
                    if !rows.is_empty()
                        && rows.iter().all(|row| row.generation_hash == *hash)
                        && rows.len() == rows[0].total_parts.max(1) as usize
                        && (1..=rows.len())
                            .zip(&rows)
                            .all(|(n, row)| row.part_index as usize == n)
                    {
                        zip_reuse.insert(*rendition, rows);
                        break;
                    }
                }
            }
        }

        let is_multi = tracks_to_process.len() > 1;
        let total_count = is_multi.then_some(tracks_to_process.len() as u32);
        for (index, item) in tracks_to_process.iter().enumerate() {
            if job_controller.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }
            let track_idx = is_multi.then_some((index + 1) as u32);
            for rendition in options.rendition_policy.renditions() {
                let cached = rendition
                    .accepted_cache_codecs()
                    .iter()
                    .find_map(|codec| existing_tracks_map.get(&(item.id.clone(), *codec)).cloned());
                pipeline_items.push(PipelineItem {
                    track_id: item.id.clone(),
                    storefront: item.storefront.clone(),
                    meta_title: item.title.clone(),
                    meta_artist: item.artist.clone(),
                    artwork_url: item.artwork_url.clone(),
                    is_streamable: item.is_streamable,
                    rendition: *rendition,
                    cached,
                    track_index: track_idx,
                    total_tracks: total_count,
                });
            }
        }
        for rendition in options.rendition_policy.renditions() {
            let reuse_valid = zip_reuse.contains_key(rendition)
                && (*rendition == Rendition::Atmos
                    || pipeline_items
                        .iter()
                        .filter(|item| item.rendition == *rendition)
                        .all(|item| item.cached.is_some()));
            if !reuse_valid {
                zip_reuse.remove(rendition);
            }
        }

        let zip_reuse_atmos_track_count = if zip_reuse.contains_key(&Rendition::Atmos) {
            let count = tracks_to_process
                .iter()
                .filter(|item| {
                    existing_tracks_map
                        .get(&(item.id.clone(), Codec::Ec3))
                        .is_some_and(|cached| cached.codec == Codec::Ec3)
                })
                .count();
            (count > 0).then_some(count)
        } else {
            None
        };
        if zip_reuse.contains_key(&Rendition::Atmos) {
            pipeline_items.retain(|item| item.rendition != Rendition::Atmos);
        }
        let mut uncached_items = pipeline_items;
        let has_fresh = uncached_items.iter().any(|item| item.cached.is_none());
        let cache_hits_present = uncached_items.iter().any(|item| item.cached.is_some());
        let can_rip_live = settings.can_rip_live(options.is_admin)
            && settings.can_rip_provider(&options.provider, options.is_admin);

        if !can_rip_live
            && !settings.can_rip_provider(&options.provider, options.is_admin)
            && (has_fresh || !cache_hits_present)
        {
            let provider_name = if options.provider.is_apple() {
                "Apple Music"
            } else {
                options.provider.as_str()
            };
            warnings.push(format!(
                "{} live ripping is currently disabled by administrator.",
                provider_name
            ));
        }

        let zip_delivery: Option<ZipDeliveryInfo> = None;

        let summary = |cached_count: usize,
                       ripped_count: usize,
                       failed: Vec<FailedTrack>,
                       skipped: Vec<String>,
                       elapsed: &str,
                       zip_delivery: Option<ZipDeliveryInfo>,
                       first_msg_id: Option<ChatMessageRef>| {
            let guard = shared.lock().expect("job poisoned");
            RipTaskSummary {
                job_id: guard.job.id.clone(),
                job_header: guard.job.job_header.clone(),
                total_tracks: guard.job.total_tracks,
                cached_count,
                ripped_count,
                failed_count: failed.len(),
                failed_tracks: failed,
                skipped_uncached_tracks: skipped,
                total_elapsed_sec: elapsed.to_string(),
                capped_count,
                max_collection_limit,
                is_cache_only: options.is_cache_only,
                is_group: options.is_group,
                warnings: warnings.clone(),
                zip_delivery: zip_delivery.clone(),
                zip_deliveries: zip_delivery.clone().into_iter().collect(),
                first_delivered_msg_id: first_msg_id,
                codec: zip_delivery.as_ref().and_then(|z| z.codec.clone()),
            }
        };

        if !can_rip_live && (!zip_build || has_fresh) && !cache_hits_present {
            let skipped: Vec<String> = uncached_items
                .iter()
                .filter(|item| item.cached.is_none() && item.rendition == Rendition::Primary)
                .map(|i| i.track_id.clone())
                .collect();
            {
                let mut guard = shared.lock().expect("job poisoned");
                guard.job.skipped_count = skipped.len();
            }
            self.bus
                .set_job_activity(&shared, Some(TaskActivity::SkippingUncached));
            self.bus.emit_progress(&shared);
            let elapsed = format!(
                "{:.1}",
                (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                    as f64)
                    / 1000.0
            );

            return Ok(summary(
                cached_count,
                0,
                Vec::new(),
                skipped,
                &elapsed,
                zip_delivery,
                first_delivered_msg_id,
            ));
        }
        if !can_rip_live && has_fresh && cache_hits_present {
            let skipped: Vec<String> = uncached_items
                .iter()
                .filter(|item| item.cached.is_none() && item.rendition == Rendition::Primary)
                .map(|item| item.track_id.clone())
                .collect();
            shared.lock().expect("job poisoned").job.skipped_count = skipped.len();
            uncached_items.retain(|item| item.cached.is_some());
        }

        let all_tracks_cached = uncached_items.iter().all(|item| item.cached.is_some());
        let album_zip_reusable = !zip_build || zip_reuse.contains_key(&Rendition::Primary);

        if all_tracks_cached && album_zip_reusable {
            let current_job_id = shared.lock().expect("job poisoned").job.id.clone();
            tracing::info!(
                job_id = %current_job_id,
                tracks_count = uncached_items.len(),
                is_album = is_album_job,
                "Job is 100% cached; executing priority cache delivery bypassing rip queue"
            );
            self.set_phase(&shared, TaskPhase::Delivering);
            self.bus
                .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
            self.bus.emit_progress(&shared);

            let _permit = self
                .cache_delivery_semaphore
                .acquire()
                .await
                .map_err(|_| OrchestratorError::Cancelled)?;

            if job_controller.is_cancelled() {
                let elapsed = format!(
                    "{:.1}",
                    (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                        as f64)
                        / 1000.0
                );
                return Ok(summary(0, 0, Vec::new(), Vec::new(), &elapsed, None, None));
            }

            {
                let guard = shared.lock().expect("job poisoned");
                self.bus.emit(&OrchestratorEvent::Started(&guard.job));
            }

            let mut cached_count = 0usize;
            let mut first_delivered_msg_id: Option<ChatMessageRef> = None;
            let should_pace = uncached_items.len() > 1;
            let mut delivery_failed = false;

            if !options.is_cache_only && !zip_deliver {
                for item in uncached_items.iter_mut() {
                    if job_controller.is_cancelled() {
                        let elapsed = format!(
                            "{:.1}",
                            (now_ms().saturating_sub(
                                shared.lock().expect("job poisoned").job.start_time_ms
                            ) as f64)
                                / 1000.0
                        );
                        return Ok(summary(
                            cached_count,
                            0,
                            Vec::new(),
                            Vec::new(),
                            &elapsed,
                            None,
                            first_delivered_msg_id,
                        ));
                    }
                    if let Some(cached) = &item.cached {
                        let cache_codec = cached.codec;
                        self.bus.set_download(
                            &shared,
                            Some(DownloadLane::CachedDelivery {
                                track: TrackLabel::new(
                                    item.meta_title.clone().unwrap_or_default(),
                                    item.meta_artist.clone().unwrap_or_default(),
                                )
                                .with_artwork_url(item.artwork_url.clone())
                                .with_position(item.track_index, item.total_tracks),
                            }),
                        );
                        self.bus.emit_progress(&shared);

                        let reply_to = (options.delivery_chat_id == options.chat_id)
                            .then_some(options.reply_to_message_id)
                            .flatten();

                        let copy_result = tokio::select! {
                            res = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                                destination: ChatRef::new(options.delivery_chat_id),
                                source: DumpMessageRef::new(cached.message_id),
                                reply_to: reply_to.map(ChatMessageRef::new),
                                silent: is_multi_track,
                            }) => res,
                            _ = job_controller.cancelled() => {
                                let elapsed = format!(
                                    "{:.1}",
                                    (now_ms().saturating_sub(
                                        shared.lock().expect("job poisoned").job.start_time_ms
                                    ) as f64)
                                        / 1000.0
                                );
                                return Ok(summary(
                                    cached_count,
                                    0,
                                    Vec::new(),
                                    Vec::new(),
                                    &elapsed,
                                    None,
                                    first_delivered_msg_id,
                                ));
                            }
                        };

                        match copy_result {
                            Ok(DeliveryReceipt::Message(sent_id)) => {
                                if first_delivered_msg_id.is_none() {
                                    first_delivered_msg_id = Some(sent_id);
                                }

                                cached_count += 1;
                                {
                                    let mut guard = shared.lock().expect("job poisoned");
                                    guard.job.cached_count = cached_count;
                                }
                                self.bus.set_download(&shared, None);
                                self.bus
                                    .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
                                self.bus.emit_progress(&shared);

                                if should_pace {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(120))
                                        .await;
                                }
                            }
                            Ok(DeliveryReceipt::PreviewDelivered) | Err(_) => {
                                tracing::warn!(
                                    track_id = %item.track_id,
                                    "cached track delivery failed; deleting cache and falling back to rip queue"
                                );
                                let _ = deps.delete_track(&item.track_id, Some(cache_codec)).await;
                                item.cached = None;
                                self.bus.set_download(&shared, None);
                                delivery_failed = true;
                                break;
                            }
                        }
                    }
                }
            } else if zip_deliver && !options.is_cache_only {
                let mut zip_deliveries = Vec::new();
                for state in &zip_states {
                    if let Some(rows) = zip_reuse.get(&state.rendition) {
                        let direct_res = deliver_cached_zip_rows_direct(
                            &deps,
                            &shared,
                            options,
                            state.rendition,
                            rows,
                            &job_controller,
                            &mut first_delivered_msg_id,
                        )
                        .await;

                        match direct_res {
                            Ok((delivered, size)) => {
                                if delivered > 0 {
                                    let codec = rows[0].codec.as_str().to_owned();
                                    let info = ZipDeliveryInfo {
                                        album: album_name
                                            .clone()
                                            .unwrap_or_else(|| "Album".to_owned()),
                                        artist: album_artist
                                            .clone()
                                            .unwrap_or_else(|| "Unknown Artist".to_owned()),
                                        release_year: album_release_date
                                            .clone()
                                            .unwrap_or_default()
                                            .chars()
                                            .take(4)
                                            .collect(),
                                        total_tracks: tracks_to_process.len(),
                                        delivered_tracks: if state.rendition == Rendition::Primary {
                                            Some(tracks_to_process.len())
                                        } else {
                                            zip_reuse_atmos_track_count
                                        },
                                        total_parts: delivered,
                                        size_bytes: size,
                                        is_partial: false,
                                        album_id: options
                                            .parsed_items
                                            .first()
                                            .map(|i| i.id.clone())
                                            .unwrap_or_default(),
                                        album_url: match (&album_id, &album_sf) {
                                            (Some(id), Some(storefront)) => deps.album_url(
                                                options.provider.clone(),
                                                id,
                                                storefront.as_str().into(),
                                            ),
                                            _ => None,
                                        },
                                        artwork_url: album_artwork_url
                                            .clone()
                                            .filter(|url| !url.is_empty()),
                                        genre: album_genre.clone(),
                                        record_label: album_record_label.clone(),
                                        copyright: album_copyright.clone(),
                                        photo_delivered: false,
                                        codec: Some(codec),
                                    };
                                    zip_deliveries.push(info);
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "cached ZIP delivery failed; falling back to rip queue"
                                );
                                delivery_failed = true;
                                break;
                            }
                        }
                    }
                }
                if !delivery_failed {
                    cached_count = tracks_to_process.len();
                    {
                        let mut guard = shared.lock().expect("job poisoned");
                        guard.job.cached_count = cached_count;
                    }
                    self.bus
                        .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
                    self.bus.emit_progress(&shared);
                    let first_zip = zip_deliveries.first().cloned();
                    let elapsed = format!(
                        "{:.1}",
                        (now_ms()
                            .saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                            as f64)
                            / 1000.0
                    );
                    let mut res = summary(
                        cached_count,
                        0,
                        Vec::new(),
                        Vec::new(),
                        &elapsed,
                        first_zip,
                        first_delivered_msg_id,
                    );
                    res.zip_deliveries = zip_deliveries;
                    return Ok(res);
                }
            } else {
                cached_count = tracks_to_process.len();
                {
                    let mut guard = shared.lock().expect("job poisoned");
                    guard.job.cached_count = cached_count;
                }
                self.bus
                    .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
                self.bus.emit_progress(&shared);
            }

            if !delivery_failed {
                let elapsed = format!(
                    "{:.1}",
                    (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                        as f64)
                        / 1000.0
                );

                return Ok(summary(
                    cached_count,
                    0,
                    Vec::new(),
                    Vec::new(),
                    &elapsed,
                    None,
                    first_delivered_msg_id,
                ));
            }
        }

        self.set_phase(&shared, TaskPhase::Queued);
        self.bus
            .set_job_activity(&shared, Some(TaskActivity::Queued { position: 1 }));
        self.bus.emit_progress(&shared);

        let job_id = shared.lock().expect("job poisoned").job.id.clone();
        tracing::info!(
            job_id = %job_id,
            tracks_count = uncached_items.len(),
            force = options.is_force,
            is_group = options.is_group,
            is_cache_only = options.is_cache_only,
            delivery_chat_id = options.delivery_chat_id,
            "Rip job queued"
        );

        let queue_start_time = shared.lock().expect("job poisoned").job.start_time_ms;

        let _ = self.ensure_upload_lane();

        let job_ctx = Arc::new(TaskContext {
            config: self.config.clone(),
            options: options.clone(),
            zip_build,
            zip_deliver,
            zip_states: zip_states.clone(),
            zip_reuse: zip_reuse.clone(),
            zip_expectations: zip_expectations.clone(),
            zip_reuse_atmos_track_count,
            zip_album: album_name.clone().unwrap_or_else(|| "Album".to_owned()),
            zip_artist: album_artist
                .clone()
                .unwrap_or_else(|| "Unknown Artist".to_owned()),
            zip_album_id: options
                .parsed_items
                .first()
                .map(|item| item.id.clone())
                .unwrap_or_default(),
            zip_album_url: match (&album_id, &album_sf) {
                (Some(id), Some(storefront)) => {
                    deps.album_url(options.provider.clone(), id, storefront.as_str().into())
                }
                _ => None,
            },
            zip_genre: album_genre.clone(),
            zip_record_label: album_record_label.clone(),
            zip_copyright: album_copyright.clone(),
            zip_artwork_url: album_artwork_url.clone().filter(|url| !url.is_empty()),
            zip_release_date: album_release_date.clone().unwrap_or_default(),
            warnings: warnings.clone(),
            atmos_warning: Arc::new(std::sync::Mutex::new(None)),
            finalizing_rendition: Arc::new(Mutex::new(None)),
            fatal_error: Arc::new(Mutex::new(None)),
            primary_zip_error: Arc::new(Mutex::new(None)),
            zip_new_dump_messages: Arc::new(Mutex::new(Vec::new())),
            is_multi_track,
            max_collection_limit,
            capped_count,
            queue_start_time_ms: queue_start_time,
            ripped_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            failed_tracks: Arc::new(Mutex::new(Vec::new())),
            first_delivered_msg_id: Arc::new(Mutex::new(None)),
            zip_delivery_infos: Arc::new(Mutex::new(Vec::new())),
        });

        let rip_job_dir =
            std::env::temp_dir().join(format!("rip_job_{id}", id = cuid2::create_id()));
        workspace_guard.add(rip_job_dir.clone());
        if let Err(error) = tokio::fs::create_dir_all(&rip_job_dir).await {
            return Err(OrchestratorError::Message(format!(
                "create rip workspace: {error}"
            )));
        }

        let (summary_tx, summary_rx) = tokio::sync::oneshot::channel::<FinalizeResult>();
        let summary_tx = Arc::new(Mutex::new(Some(summary_tx)));

        let task_deps = Arc::clone(&deps);
        let task_shared = Arc::clone(&shared);
        let task_items = uncached_items;
        let task_controller = job_controller.clone();
        let task_bus = self.bus.clone();
        let task_job_ctx = Arc::clone(&job_ctx);
        let task_upload_lane = self.upload_lane.clone();
        let task_queue = self.queue.clone();
        let task_workspace_guard = workspace_guard;
        let task_summary_tx = Arc::clone(&summary_tx);
        let task_rip_job_dir = rip_job_dir.clone();

        let callback_shared = Arc::clone(&shared);
        let callback_bus = self.bus.clone();
        let on_position_change = Arc::new(move |position: u64| {
            callback_shared
                .lock()
                .expect("job poisoned")
                .job
                .queue_position = Some(position);
            callback_bus.set_job_activity(
                &callback_shared,
                Some(TaskActivity::Queued {
                    position: u32::try_from(position).unwrap_or(u32::MAX),
                }),
            );
            callback_bus.emit_progress(&callback_shared);
        });
        let callback_shared = Arc::clone(&shared);
        let callback_bus = self.bus.clone();
        let on_start = Arc::new(move || {
            {
                let mut guard = callback_shared.lock().expect("job poisoned");
                guard.job.phase = TaskPhase::Processing;
                guard.job.queue_position = Some(0);
            }
            let guard = callback_shared.lock().expect("job poisoned");
            callback_bus.emit(&OrchestratorEvent::Started(&guard.job));
            drop(guard);
            callback_bus.set_job_activity(&callback_shared, None);
            callback_bus.emit_progress(&callback_shared);
        });

        let task = move |queue_signal: CancellationToken| {
            Box::pin(async move {
                run_lane_one(LaneOneContext {
                    deps: task_deps,
                    bus: task_bus,
                    shared: task_shared,
                    uncached_items: &task_items,
                    job_controller: task_controller,
                    queue_signal,
                    ctx: task_job_ctx,
                    upload_lane: task_upload_lane,
                    queue: task_queue,
                    workspace_guard: task_workspace_guard,
                    summary_tx: task_summary_tx,
                    rip_job_dir: task_rip_job_dir,
                })
                .await
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        };

        match self
            .queue
            .enqueue(
                task,
                Some(EnqueueOptions {
                    signal: Some(job_controller.clone()),
                    on_position_change: Some(on_position_change),
                    on_start: Some(on_start),
                }),
            )
            .await
        {
            Ok(_) => {}
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&rip_job_dir).await;
                for state in &zip_states {
                    let _ = tokio::fs::remove_dir_all(&state.dir).await;
                }
                return Err(error.into());
            }
        }

        match summary_rx.await {
            Ok(Ok(summary)) => Ok(summary),
            Ok(Err(error)) => Err(OrchestratorError::Message(error)),
            Err(_) => {
                let _ = tokio::fs::remove_dir_all(&rip_job_dir).await;
                for state in &zip_states {
                    let _ = tokio::fs::remove_dir_all(&state.dir).await;
                }
                Err(OrchestratorError::Message(
                    "finalize marker stopped unexpectedly".to_owned(),
                ))
            }
        }
    }
}

struct LaneOneContext<'a, D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    uncached_items: &'a [PipelineItem],
    job_controller: CancellationToken,
    queue_signal: CancellationToken,
    ctx: Arc<TaskContext>,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    queue: SequentialRipQueue,
    workspace_guard: WorkspaceGuard,
    summary_tx: Arc<Mutex<Option<FinalizeSender>>>,
    rip_job_dir: PathBuf,
}

enum RipLaneOutcome {
    Ripped(Box<PipelineRipResult>),
    Finished,
    Cancelled,
    Stop,
}

enum CacheResolution {
    Hit,
    Rerip,
    Cancelled,
    Failed(String),
}

struct CacheResolutionGuard {
    sender: Option<tokio::sync::oneshot::Sender<CacheResolution>>,
}

impl CacheResolutionGuard {
    fn new(sender: tokio::sync::oneshot::Sender<CacheResolution>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    fn send(&mut self, result: CacheResolution) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}

impl Drop for CacheResolutionGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(CacheResolution::Failed(
                "cached lane task stopped unexpectedly".to_owned(),
            ));
        }
    }
}

struct FinalizingRenditionGuard {
    rendition: Arc<Mutex<Option<Rendition>>>,
}

impl FinalizingRenditionGuard {
    fn new(rendition: Arc<Mutex<Option<Rendition>>>) -> Self {
        Self { rendition }
    }
}

impl Drop for FinalizingRenditionGuard {
    fn drop(&mut self) {
        if !std::thread::panicking()
            && let Ok(mut rendition) = self.rendition.lock()
        {
            *rendition = None;
        }
    }
}

fn is_lane_cancelled(
    shared: &Arc<Mutex<TaskShared>>,
    job_controller: &CancellationToken,
    queue_signal: &CancellationToken,
) -> bool {
    shared.lock().expect("job poisoned").job.is_cancelled
        || job_controller.is_cancelled()
        || queue_signal.is_cancelled()
}

struct RipFreshInput<'a, D> {
    deps: &'a Arc<D>,
    bus: &'a EventBus,
    shared: &'a Arc<Mutex<TaskShared>>,
    ctx: &'a Arc<TaskContext>,
    job_controller: &'a CancellationToken,
    queue_signal: &'a CancellationToken,
    item: PipelineItem,
    rip_job_dir: &'a Path,
}

async fn rip_fresh_item<D>(input: RipFreshInput<'_, D>) -> RipLaneOutcome
where
    D: ProviderDeps + TaskBookkeeping + 'static,
{
    let RipFreshInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_signal,
        item,
        rip_job_dir,
    } = input;
    if is_lane_cancelled(shared, job_controller, queue_signal) {
        return RipLaneOutcome::Cancelled;
    }
    bus.set_job_activity(shared, None);
    bus.set_download(shared, None);
    bus.emit_progress(shared);
    if item.is_streamable == Some(false) {
        if item.rendition == Rendition::Atmos {
            return RipLaneOutcome::Finished;
        }
        let err_msg = deps.unavailable_track_message().to_owned();
        {
            let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
            failures.push(FailedTrack {
                id: item.track_id.clone(),
                error: err_msg.clone(),
                kind: Some(FailedTrackKind::TrackUnavailable),
                title: item.meta_title.clone(),
                artist: item.meta_artist.clone(),
                storefront: item.storefront.clone(),
            });
            shared.lock().expect("job poisoned").job.failed_count = failures.len();
        }
        bus.set_job_activity(shared, Some(TaskActivity::ProcessingNext));
        bus.emit_progress(shared);
        return RipLaneOutcome::Finished;
    }

    let track_start_time = now_ms();
    let update_single_track_header =
        !ctx.is_multi_track && item.meta_title.is_none() && item.meta_artist.is_none();
    let item_track_index = item.track_index;
    let item_total_tracks = item.total_tracks;

    let on_progress: RipProgressCallback = {
        let shared = Arc::clone(shared);
        let bus = bus.clone();
        Arc::new(move |mut activity| {
            match &mut activity {
                RipActivity::Connecting { track }
                | RipActivity::Downloading { track, .. }
                | RipActivity::MaterializingCachedMedia { track, .. }
                | RipActivity::Decrypting { track }
                | RipActivity::Tagging { track } => {
                    if track.track_index.is_none() {
                        track.track_index = item_track_index;
                    }
                    if track.total_tracks.is_none() {
                        track.total_tracks = item_total_tracks;
                    }
                }
                RipActivity::ResolvingMetadata => {}
            }
            if update_single_track_header {
                let track = match &activity {
                    RipActivity::Connecting { track }
                    | RipActivity::Downloading { track, .. }
                    | RipActivity::MaterializingCachedMedia { track, .. }
                    | RipActivity::Decrypting { track }
                    | RipActivity::Tagging { track } => Some(track),
                    RipActivity::ResolvingMetadata => None,
                };
                if let Some(track) = track {
                    let label = if track.artist.is_empty() {
                        track.title.clone()
                    } else if track.title.is_empty() {
                        track.artist.clone()
                    } else {
                        format!("{} - {}", track.title, track.artist)
                    };
                    if !label.is_empty() {
                        shared.lock().expect("job poisoned").job.job_header =
                            format!("<b>{}</b>", html_escape(&label));
                    }
                }
            }
            bus.set_download(&shared, Some(DownloadLane::Rip(activity)));
            bus.emit_progress(&shared);
        })
    };

    let default_sf = resolve_default_storefront(&deps.settings_snapshot()).to_owned();
    let storefront = item.storefront.clone().unwrap_or(default_sf);
    let codec_preference = if item.rendition == Rendition::Atmos {
        music::CodecPreference::Atmos
    } else {
        ctx.options
            .codec_preference
            .unwrap_or_else(|| item.rendition.codec_preference())
    };
    let rip_options = RipOptions {
        provider: ctx.options.provider.clone(),
        storefront: &storefront,
        on_progress: Some(&on_progress),
        signal: Some(queue_signal.clone()),
        output_dir: Some(rip_job_dir),
        codec_preference,
    };
    let rip_result = match deps.rip(&item.track_id, rip_options).await {
        Ok(rip_result) => {
            bus.set_codec(shared, Some(rip_result.codec.clone()));
            rip_result
        }
        Err(error) => {
            bus.set_download(shared, None);
            if is_lane_cancelled(shared, job_controller, queue_signal) {
                bus.emit_progress(shared);
                return RipLaneOutcome::Cancelled;
            }
            if item.rendition == Rendition::Atmos
                && matches!(&error, RipError::RenditionUnavailable { .. })
            {
                tracing::debug!(track_id = %item.track_id, "Atmos rendition unavailable");
                bus.emit_progress(shared);
                return RipLaneOutcome::Finished;
            }

            let err_msg = error.to_string();
            let duration_ms = (now_ms() - track_start_time) as i64;
            if item.rendition == Rendition::Primary {
                let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
                failures.push(FailedTrack {
                    id: item.track_id.clone(),
                    error: err_msg.clone(),
                    kind: FailedTrackKind::of(&error),
                    title: item.meta_title.clone(),
                    artist: item.meta_artist.clone(),
                    storefront: item.storefront.clone(),
                });
                shared.lock().expect("job poisoned").job.failed_count = failures.len();
            }
            tracing::error!(
                track_id = %item.track_id,
                duration_ms,
                error = %err_msg,
                "Rip job failed"
            );

            bus.set_job_activity(shared, Some(TaskActivity::ProcessingNext));
            bus.emit_progress(shared);

            if matches!(error, RipError::SourceOffline { .. })
                && item.rendition == Rendition::Primary
            {
                tracing::error!(
                    track_id = %item.track_id,
                    error = %err_msg,
                    "Mirror source offline; skipping track and continuing batch"
                );
            }
            return RipLaneOutcome::Finished;
        }
    };

    bus.set_download(shared, None);
    bus.emit_progress(shared);
    if is_lane_cancelled(shared, job_controller, queue_signal) {
        return RipLaneOutcome::Cancelled;
    }
    RipLaneOutcome::Ripped(Box::new(PipelineRipResult {
        track_id: item.track_id,
        rip_result,
        start_time_ms: track_start_time,
        rendition: item.rendition,
        track_index: item.track_index,
        total_tracks: item.total_tracks,
    }))
}

struct UploadLaneInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    queue_cancellation: Option<CancellationToken>,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    upload_item: PipelineRipResult,
}

async fn enqueue_upload_task<D>(input: UploadLaneInput<D>) -> bool
where
    D: TrackCache + Delivery + TaskBookkeeping + 'static,
{
    let UploadLaneInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_cancellation,
        upload_lane,
        upload_item,
    } = input;
    let panic_shared = Arc::clone(&shared);
    let panic_ctx = Arc::clone(&ctx);
    let panic_track_id = upload_item.track_id.clone();
    let panic_rendition = upload_item.rendition;
    let panic_title = upload_item.rip_result.title.clone();
    let panic_artist = upload_item.rip_result.artist.clone();
    let item_ctx = Arc::clone(&ctx);
    let task_controller = job_controller.clone();
    push_lane_task(
        &upload_lane,
        "upload_track",
        Some(Box::new(move |message| {
            record_lane_task_panic(&panic_shared, &panic_ctx, &panic_track_id, message);
            if panic_rendition == Rendition::Primary {
                let mut failures = panic_ctx.failed_tracks.lock().expect("failures poisoned");
                if let Some(failure) = failures
                    .iter_mut()
                    .find(|failure| failure.id == panic_track_id)
                {
                    failure.title = Some(panic_title.clone());
                    failure.artist = Some(panic_artist.clone());
                }
            }
        })),
        Some(&job_controller),
        queue_cancellation.as_ref(),
        move || {
            Box::pin(async move {
                run_upload_item(deps, bus, shared, item_ctx, task_controller, upload_item).await;
            })
        },
    )
    .await
}

struct CachedResolutionInput<'a, D> {
    deps: &'a Arc<D>,
    bus: &'a EventBus,
    shared: &'a Arc<Mutex<TaskShared>>,
    ctx: &'a Arc<TaskContext>,
    job_controller: &'a CancellationToken,
    queue_signal: &'a CancellationToken,
    item: &'a PipelineItem,
    cached: &'a CachedTrack,
}

async fn resolve_cached_item<D>(input: CachedResolutionInput<'_, D>) -> CacheResolution
where
    D: TrackCache + Delivery + TaskBookkeeping,
{
    let CachedResolutionInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_signal,
        item,
        cached,
    } = input;
    let is_cancelled = || {
        shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
            || queue_signal.is_cancelled()
    };
    let track_id = cached.track_id.clone();
    let codec = cached.codec;
    if is_cancelled() {
        return CacheResolution::Cancelled;
    }
    if !codec_allowed_for_rendition(item.rendition, cached.codec) {
        tracing::warn!(
            track_id = %item.track_id,
            codec = %cached.codec.as_str(),
            "cached track codec does not match rendition; reripping"
        );
        let _ = deps.delete_track(&track_id, Some(codec)).await;
        return CacheResolution::Rerip;
    }

    bus.set_job_activity(shared, None);
    bus.set_download(
        shared,
        Some(DownloadLane::CachedDelivery {
            track: TrackLabel::new(
                item.meta_title.clone().unwrap_or_default(),
                item.meta_artist.clone().unwrap_or_default(),
            )
            .with_artwork_url(item.artwork_url.clone())
            .with_position(item.track_index, item.total_tracks),
        }),
    );
    bus.emit_progress(shared);
    let progress_guard = DownloadProgressGuard {
        bus: bus.clone(),
        shared: Arc::clone(shared),
    };
    let _ = &progress_guard;

    if !ctx.options.is_cache_only && !ctx.zip_deliver {
        let reply_to = (ctx.options.delivery_chat_id == ctx.options.chat_id)
            .then_some(ctx.options.reply_to_message_id)
            .flatten();
        let copy_result = tokio::select! {
            result = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(ctx.options.delivery_chat_id),
                source: DumpMessageRef::new(cached.message_id),
                reply_to: reply_to.map(ChatMessageRef::new),
                silent: ctx.is_multi_track,
            }) => result,
            _ = job_controller.cancelled() => return CacheResolution::Cancelled,
            _ = queue_signal.cancelled() => return CacheResolution::Cancelled,
        };
        let sent_id = match copy_result {
            Ok(DeliveryReceipt::Message(sent_id)) => sent_id,
            Ok(DeliveryReceipt::PreviewDelivered) => {
                tracing::warn!(track_id = %item.track_id, "cached track delivery returned a preview receipt");
                let _ = deps.delete_track(&track_id, Some(codec)).await;
                return CacheResolution::Rerip;
            }
            Err(_) => {
                tracing::warn!(track_id = %item.track_id, "cached track delivery failed; reripping");
                let _ = deps.delete_track(&track_id, Some(codec)).await;
                return CacheResolution::Rerip;
            }
        };
        if is_cancelled() {
            return CacheResolution::Cancelled;
        }
        if ctx
            .first_delivered_msg_id
            .lock()
            .expect("first message poisoned")
            .is_none()
        {
            *ctx.first_delivered_msg_id
                .lock()
                .expect("first message poisoned") = Some(sent_id);
        }
    }

    if ctx.zip_build
        && !ctx.zip_reuse.contains_key(&item.rendition)
        && let Some(state) = ctx.zip_state(item.rendition)
    {
        let title = item.meta_title.as_deref().unwrap_or("");
        let artist = item.meta_artist.as_deref().unwrap_or("");
        let filename = build_zip_entry_filename_with_codec(
            None,
            title,
            artist,
            &item.track_id,
            cached.codec.as_str(),
        );
        let destination = state.dir.join(&filename);
        let track = TrackLabel::new(
            item.meta_title.clone().unwrap_or_default(),
            item.meta_artist.clone().unwrap_or_default(),
        )
        .with_artwork_url(item.artwork_url.clone())
        .with_position(item.track_index, item.total_tracks);
        bus.set_download(
            shared,
            Some(DownloadLane::Rip(RipActivity::MaterializingCachedMedia {
                track: track.clone(),
                progress: ByteProgress {
                    completed: 0,
                    total: None,
                },
            })),
        );
        bus.emit_progress(shared);
        let progress_bus = bus.clone();
        let progress_shared = Arc::clone(shared);
        let materialization_progress: UploadProgressCallback = Arc::new(move |completed, total| {
            progress_bus.set_download(
                &progress_shared,
                Some(DownloadLane::Rip(RipActivity::MaterializingCachedMedia {
                    track: track.clone(),
                    progress: ByteProgress {
                        completed,
                        total: (total > 0).then_some(total),
                    },
                })),
            );
            progress_bus.emit_progress(&progress_shared);
        });
        let download_result = tokio::select! {
            result = deps.materialize_cached(
                DumpMessageRef::new(cached.message_id),
                &destination,
                Some(&materialization_progress),
            ) => result,
            _ = job_controller.cancelled() => {
                let _ = tokio::fs::remove_file(&destination).await;
                return CacheResolution::Cancelled;
            }
            _ = queue_signal.cancelled() => {
                let _ = tokio::fs::remove_file(&destination).await;
                return CacheResolution::Cancelled;
            }
        };
        if let Err(error) = download_result {
            tracing::warn!(
                track_id = %item.track_id,
                %error,
                "cached ZIP source unavailable; reripping"
            );
            let _ = tokio::fs::remove_file(&destination).await;
            let _ = deps.delete_track(&track_id, Some(codec)).await;
            return CacheResolution::Rerip;
        }
        if is_cancelled() {
            let _ = tokio::fs::remove_file(&destination).await;
            return CacheResolution::Cancelled;
        }
        match tokio::fs::metadata(&destination).await {
            Ok(metadata) => {
                seed_zip_codec(state, cached.codec);
                state
                    .sources
                    .lock()
                    .expect("zip sources poisoned")
                    .push(ZipTrackEntry {
                        file_path: destination,
                        archive_filename: filename,
                        file_size: metadata.len(),
                    });
            }
            Err(error) => {
                tracing::warn!(
                    track_id = %item.track_id,
                    %error,
                    "cached ZIP source disappeared; reripping"
                );
                let _ = tokio::fs::remove_file(&destination).await;
                let _ = deps.delete_track(&track_id, Some(codec)).await;
                return CacheResolution::Rerip;
            }
        }
    }
    if is_cancelled() {
        return CacheResolution::Cancelled;
    }
    if item.rendition == Rendition::Primary {
        shared.lock().expect("job poisoned").job.cached_count += 1;
    }
    bus.set_job_activity(shared, Some(TaskActivity::CachedDelivered));
    CacheResolution::Hit
}

struct CachedLaneInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    queue_signal: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    item: PipelineItem,
    cached: CachedTrack,
}

enum OrderedSlot {
    Cached {
        item: PipelineItem,
        cached: CachedTrack,
    },
    Fresh(PipelineRipResult),
    Finished,
}

const ORDERED_SLOT_CAPACITY: usize = 16;

fn ordered_slot_capacity(item_count: usize) -> usize {
    item_count.saturating_add(1).max(ORDERED_SLOT_CAPACITY)
}

struct PendingCachedItem {
    resolution: tokio::sync::oneshot::Receiver<CacheResolution>,
}

enum CacheEnqueueResult {
    Pending(Box<PendingCachedItem>),
    Cancelled,
    Failed(String),
}

struct OrderedDispatchInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    queue: SequentialRipQueue,
    slots: tokio::sync::mpsc::Receiver<OrderedSlot>,
    ordered_capacity: usize,
    rip_job_dir: PathBuf,
    finalization_guard: FinalizationGuard,
}

async fn enqueue_cached_lane_task<D>(input: CachedLaneInput<D>) -> CacheEnqueueResult
where
    D: TrackCache + Delivery + TaskBookkeeping + 'static,
{
    let CachedLaneInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_signal,
        upload_lane,
        item,
        cached,
    } = input;
    let (resolution_tx, resolution_rx) = tokio::sync::oneshot::channel();
    let resolution_guard = CacheResolutionGuard::new(resolution_tx);
    let cache_deps = Arc::clone(&deps);
    let cache_bus = bus.clone();
    let cache_shared = Arc::clone(&shared);
    let cache_ctx = Arc::clone(&ctx);
    let cache_controller = job_controller.clone();
    let cache_queue_signal = queue_signal.clone();
    let cache_item = item.clone();
    let cache_value = cached.clone();
    let panic_shared = Arc::clone(&shared);
    let panic_ctx = Arc::clone(&ctx);
    let panic_track_id = item.track_id.clone();
    let pushed = push_lane_task(
        &upload_lane,
        "cached_item",
        Some(Box::new(move |message| {
            record_lane_task_panic(&panic_shared, &panic_ctx, &panic_track_id, message);
        })),
        Some(&job_controller),
        Some(&queue_signal),
        move || {
            Box::pin(async move {
                let mut resolution = resolution_guard;
                let result = resolve_cached_item(CachedResolutionInput {
                    deps: &cache_deps,
                    bus: &cache_bus,
                    shared: &cache_shared,
                    ctx: &cache_ctx,
                    job_controller: &cache_controller,
                    queue_signal: &cache_queue_signal,
                    item: &cache_item,
                    cached: &cache_value,
                })
                .await;
                resolution.send(result);
            })
        },
    )
    .await;
    if !pushed {
        return if shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
            || queue_signal.is_cancelled()
        {
            CacheEnqueueResult::Cancelled
        } else {
            CacheEnqueueResult::Failed("cached lane task could not be queued".to_owned())
        };
    }
    CacheEnqueueResult::Pending(Box::new(PendingCachedItem {
        resolution: resolution_rx,
    }))
}

async fn run_lane_one<D>(input: LaneOneContext<'_, D>)
where
    D: TrackCache + AlbumCache + ProviderDeps + Delivery + TaskBookkeeping + 'static,
{
    let LaneOneContext {
        deps,
        bus,
        shared,
        uncached_items,
        job_controller,
        queue_signal,
        ctx,
        upload_lane,
        queue,
        mut workspace_guard,
        summary_tx,
        rip_job_dir,
    } = input;
    tracing::debug!("Rip job started from queue");

    let ordered_capacity = ordered_slot_capacity(uncached_items.len());
    let (slot_tx, slot_rx) = tokio::sync::mpsc::channel(ordered_capacity);
    let finalization_guard = FinalizationGuard::new(
        Arc::clone(&summary_tx),
        rip_job_dir.clone(),
        &ctx.zip_states,
    );

    workspace_guard.disarm();
    tokio::spawn(run_ordered_dispatch(OrderedDispatchInput {
        deps: Arc::clone(&deps),
        bus: bus.clone(),
        shared: Arc::clone(&shared),
        ctx: Arc::clone(&ctx),
        job_controller: job_controller.clone(),
        upload_lane: Arc::clone(&upload_lane),
        queue,
        slots: slot_rx,
        ordered_capacity,
        rip_job_dir: rip_job_dir.clone(),
        finalization_guard,
    }));

    let is_cancelled = || {
        shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
            || queue_signal.is_cancelled()
    };

    for item in uncached_items {
        if is_cancelled() {
            break;
        }
        let slot = if let Some(cached) = item.cached.clone() {
            bus.set_codec(&shared, Some(cached.codec.as_str().to_string()));
            OrderedSlot::Cached {
                item: item.clone(),
                cached,
            }
        } else {
            match rip_fresh_item(RipFreshInput {
                deps: &deps,
                bus: &bus,
                shared: &shared,
                ctx: &ctx,
                job_controller: &job_controller,
                queue_signal: &queue_signal,
                item: item.clone(),
                rip_job_dir: &rip_job_dir,
            })
            .await
            {
                RipLaneOutcome::Ripped(upload_item) => OrderedSlot::Fresh(*upload_item),
                RipLaneOutcome::Finished => continue,
                RipLaneOutcome::Cancelled | RipLaneOutcome::Stop => break,
            }
        };

        let sent = tokio::select! {
            result = slot_tx.send(slot) => result.is_ok(),
            _ = job_controller.cancelled() => false,
            _ = queue_signal.cancelled() => false,
        };
        if !sent {
            break;
        }
    }

    let _ = slot_tx.send(OrderedSlot::Finished).await;
}

async fn enqueue_finalize_marker<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    finalization_guard: FinalizationGuard,
) where
    D: AlbumCache + Delivery + ProviderDeps + 'static,
{
    let marker_panic_shared = Arc::clone(&shared);
    let marker_panic_ctx = Arc::clone(&ctx);
    let marker_controller = job_controller.clone();
    let pushed = push_lane_task(&upload_lane, "finalize_job", None, None, None, move || {
        Box::pin(async move {
            let mut finalization_guard = finalization_guard;
            let result = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                finalize_job(deps, bus, shared, ctx, marker_controller),
            ))
            .await
            .unwrap_or_else(|_| {
                let optional_atmos = marker_panic_ctx
                    .finalizing_rendition
                    .lock()
                    .ok()
                    .and_then(|rendition| *rendition)
                    == Some(Rendition::Atmos);
                if optional_atmos {
                    tracing::warn!(
                        "optional Atmos ZIP finalization panicked; keeping primary result"
                    );
                    Ok(build_job_summary(
                        &marker_panic_shared,
                        &marker_panic_ctx,
                        None,
                    ))
                } else {
                    Err("finalize marker panicked".to_owned())
                }
            });
            finalization_guard.finish(result).await;
        })
    })
    .await;
    if !pushed {
        tracing::error!(lane = "upload", "failed to enqueue finalize marker");
    }
}

struct OrderedReripInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    queue: SequentialRipQueue,
    item: PipelineItem,
    rip_job_dir: PathBuf,
}

fn submit_ordered_rerip_item<D>(input: OrderedReripInput<D>) -> crate::queue::TaskReceiver
where
    D: ProviderDeps + TaskBookkeeping + 'static,
{
    let OrderedReripInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue,
        item,
        rip_job_dir,
    } = input;
    let task_deps = Arc::clone(&deps);
    let task_bus = bus.clone();
    let task_shared = Arc::clone(&shared);
    let task_ctx = Arc::clone(&ctx);
    let task_controller = job_controller.clone();
    let task_rip_job_dir = rip_job_dir;
    queue.submit(
        move |queue_signal| {
            Box::pin(async move {
                rip_fresh_item(RipFreshInput {
                    deps: &task_deps,
                    bus: &task_bus,
                    shared: &task_shared,
                    ctx: &task_ctx,
                    job_controller: &task_controller,
                    queue_signal: &queue_signal,
                    item,
                    rip_job_dir: &task_rip_job_dir,
                })
                .await
            })
        },
        Some(EnqueueOptions {
            on_position_change: None,
            on_start: None,
            signal: Some(job_controller),
        }),
    )
}

fn ordered_rerip_result(
    result: Result<crate::queue::TaskResult, tokio::sync::oneshot::error::RecvError>,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
) -> RipLaneOutcome {
    let cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    match result {
        Ok(Ok(value)) => match value.downcast::<RipLaneOutcome>() {
            Ok(outcome) => *outcome,
            Err(_) => {
                set_fatal_error(
                    ctx,
                    "ordered fallback rip returned an invalid result".to_owned(),
                );
                RipLaneOutcome::Stop
            }
        },
        Ok(Err(error)) => {
            if cancelled() {
                RipLaneOutcome::Cancelled
            } else {
                set_fatal_error(ctx, format!("ordered fallback rip failed: {error}"));
                RipLaneOutcome::Stop
            }
        }
        Err(_) => {
            if cancelled() {
                RipLaneOutcome::Cancelled
            } else {
                set_fatal_error(
                    ctx,
                    "ordered fallback rip queue stopped unexpectedly".to_owned(),
                );
                RipLaneOutcome::Stop
            }
        }
    }
}

struct OrderedSlotDrain<'a> {
    slots: &'a mut tokio::sync::mpsc::Receiver<OrderedSlot>,
    buffered: &'a mut VecDeque<OrderedSlot>,
    received_finished: &'a mut bool,
    ordered_capacity: usize,
}

async fn wait_for_cache_resolution(
    mut resolution: tokio::sync::oneshot::Receiver<CacheResolution>,
    drain: &mut OrderedSlotDrain<'_>,
    job_controller: &CancellationToken,
    ctx: &Arc<TaskContext>,
) -> CacheResolution {
    loop {
        tokio::select! {
            result = &mut resolution => {
                return result.unwrap_or_else(|_| CacheResolution::Failed(
                    "cached result channel closed unexpectedly".to_owned(),
                ));
            }
            slot = drain.slots.recv(), if !*drain.received_finished => {
                match slot {
                    Some(OrderedSlot::Finished) => *drain.received_finished = true,
                    Some(slot) => {
                        if drain.buffered.len() < drain.ordered_capacity {
                            drain.buffered.push_back(slot);
                        } else {
                            set_fatal_error(
                                ctx,
                                "ordered dispatcher buffer exhausted while resolving cache".to_owned(),
                            );
                        }
                    }
                    None => {
                        return CacheResolution::Failed(
                            "ordered dispatcher stopped before its finish marker".to_owned(),
                        );
                    }
                }
            }
            _ = job_controller.cancelled() => return CacheResolution::Cancelled,
        }
    }
}

async fn wait_for_ordered_rerip(
    mut completion: crate::queue::TaskReceiver,
    drain: &mut OrderedSlotDrain<'_>,
    job_controller: &CancellationToken,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
) -> RipLaneOutcome {
    let mut cancelled = false;
    loop {
        tokio::select! {
            result = &mut completion => {
                let outcome = ordered_rerip_result(result, shared, ctx, job_controller);
                if cancelled {
                    return RipLaneOutcome::Cancelled;
                }
                return outcome;
            }
            slot = drain.slots.recv(), if !*drain.received_finished => {
                match slot {
                    Some(OrderedSlot::Finished) => *drain.received_finished = true,
                    Some(slot) => {
                        if drain.buffered.len() < drain.ordered_capacity {
                            drain.buffered.push_back(slot);
                        } else {
                            set_fatal_error(
                                ctx,
                                "ordered dispatcher buffer exhausted while reripping".to_owned(),
                            );
                        }
                    }
                    None => {
                        return if cancelled {
                            RipLaneOutcome::Cancelled
                        } else {
                            set_fatal_error(
                                ctx,
                                "ordered dispatcher stopped before its finish marker".to_owned(),
                            );
                            RipLaneOutcome::Stop
                        };
                    }
                }
            }
            _ = job_controller.cancelled(), if !cancelled => {

                cancelled = true;
            }
        }
    }
}

async fn run_ordered_dispatch<D>(input: OrderedDispatchInput<D>)
where
    D: TrackCache + AlbumCache + ProviderDeps + Delivery + TaskBookkeeping + 'static,
{
    let OrderedDispatchInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        upload_lane,
        queue,
        mut slots,
        ordered_capacity,
        rip_job_dir,
        mut finalization_guard,
    } = input;
    let mut received_finished = false;
    let mut stop_dispatch = false;
    let mut buffered = VecDeque::with_capacity(ordered_capacity);

    loop {
        let slot = if let Some(slot) = buffered.pop_front() {
            slot
        } else if received_finished {
            break;
        } else {
            match slots.recv().await {
                Some(slot) => slot,
                None => break,
            }
        };
        if matches!(&slot, OrderedSlot::Finished) {
            received_finished = true;
            continue;
        }
        if stop_dispatch
            || shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
        {
            stop_dispatch = true;
            continue;
        }

        match slot {
            OrderedSlot::Fresh(upload_item) => {
                let pushed = enqueue_upload_task(UploadLaneInput {
                    deps: Arc::clone(&deps),
                    bus: bus.clone(),
                    shared: Arc::clone(&shared),
                    ctx: Arc::clone(&ctx),
                    job_controller: job_controller.clone(),
                    queue_cancellation: None,
                    upload_lane: Arc::clone(&upload_lane),
                    upload_item,
                })
                .await;
                if !pushed {
                    if !shared.lock().expect("job poisoned").job.is_cancelled
                        && !job_controller.is_cancelled()
                    {
                        set_fatal_error(&ctx, "ordered upload could not be queued".to_owned());
                    }
                    stop_dispatch = true;
                }
            }
            OrderedSlot::Cached { item, cached } => {
                let cache_result = enqueue_cached_lane_task(CachedLaneInput {
                    deps: Arc::clone(&deps),
                    bus: bus.clone(),
                    shared: Arc::clone(&shared),
                    ctx: Arc::clone(&ctx),
                    job_controller: job_controller.clone(),

                    queue_signal: job_controller.clone(),
                    upload_lane: Arc::clone(&upload_lane),
                    item: item.clone(),
                    cached,
                })
                .await;
                let cache_result = match cache_result {
                    CacheEnqueueResult::Pending(pending) => {
                        let mut drain = OrderedSlotDrain {
                            slots: &mut slots,
                            buffered: &mut buffered,
                            received_finished: &mut received_finished,
                            ordered_capacity,
                        };
                        wait_for_cache_resolution(
                            pending.resolution,
                            &mut drain,
                            &job_controller,
                            &ctx,
                        )
                        .await
                    }
                    CacheEnqueueResult::Cancelled => CacheResolution::Cancelled,
                    CacheEnqueueResult::Failed(error) => CacheResolution::Failed(error),
                };

                match cache_result {
                    CacheResolution::Hit => {}
                    CacheResolution::Failed(error) => set_fatal_error(&ctx, error),
                    CacheResolution::Cancelled => stop_dispatch = true,
                    CacheResolution::Rerip => {
                        let completion = submit_ordered_rerip_item(OrderedReripInput {
                            deps: Arc::clone(&deps),
                            bus: bus.clone(),
                            shared: Arc::clone(&shared),
                            ctx: Arc::clone(&ctx),
                            job_controller: job_controller.clone(),
                            queue: queue.clone(),
                            item: PipelineItem {
                                cached: None,
                                ..item
                            },
                            rip_job_dir: rip_job_dir.clone(),
                        });
                        let mut drain = OrderedSlotDrain {
                            slots: &mut slots,
                            buffered: &mut buffered,
                            received_finished: &mut received_finished,
                            ordered_capacity,
                        };
                        match wait_for_ordered_rerip(
                            completion,
                            &mut drain,
                            &job_controller,
                            &shared,
                            &ctx,
                        )
                        .await
                        {
                            RipLaneOutcome::Ripped(upload_item) => {
                                if !enqueue_upload_task(UploadLaneInput {
                                    deps: Arc::clone(&deps),
                                    bus: bus.clone(),
                                    shared: Arc::clone(&shared),
                                    ctx: Arc::clone(&ctx),
                                    job_controller: job_controller.clone(),
                                    queue_cancellation: None,
                                    upload_lane: Arc::clone(&upload_lane),
                                    upload_item: *upload_item,
                                })
                                .await
                                {
                                    if !shared.lock().expect("job poisoned").job.is_cancelled
                                        && !job_controller.is_cancelled()
                                    {
                                        set_fatal_error(
                                            &ctx,
                                            "ordered fallback upload could not be queued"
                                                .to_owned(),
                                        );
                                    }
                                    stop_dispatch = true;
                                }
                            }
                            RipLaneOutcome::Finished => {}
                            RipLaneOutcome::Cancelled | RipLaneOutcome::Stop => {
                                stop_dispatch = true;
                            }
                        }
                    }
                }
            }
            OrderedSlot::Finished => unreachable!("finished slot handled above"),
        }
    }

    if !received_finished
        && !shared.lock().expect("job poisoned").job.is_cancelled
        && !job_controller.is_cancelled()
    {
        finalization_guard
            .finish(Err("ordered dispatcher stopped unexpectedly".to_owned()))
            .await;
        return;
    }

    enqueue_finalize_marker(
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        upload_lane,
        finalization_guard,
    )
    .await;
}

fn build_job_summary(
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &TaskContext,
    zip_delivery: Option<ZipDeliveryInfo>,
) -> RipTaskSummary {
    let total_elapsed_sec = format!(
        "{:.1}",
        (now_ms().saturating_sub(ctx.queue_start_time_ms)) as f64 / 1000.0
    );
    let failed = ctx.failed_tracks.lock().expect("failures poisoned").clone();
    let first_msg_id = *ctx.first_delivered_msg_id.lock().unwrap();
    let zip_deliveries = ctx.zip_delivery_infos.lock().unwrap().clone();
    let guard = shared.lock().expect("job poisoned");
    let codec = guard
        .progress
        .codec
        .lock()
        .expect("codec poisoned")
        .clone()
        .or_else(|| zip_deliveries.first().and_then(|z| z.codec.clone()));
    RipTaskSummary {
        job_id: guard.job.id.clone(),
        job_header: guard.job.job_header.clone(),
        total_tracks: guard.job.total_tracks,
        cached_count: guard.job.cached_count,
        ripped_count: ctx.ripped_count.load(std::sync::atomic::Ordering::SeqCst),
        failed_count: failed.len(),
        failed_tracks: failed,
        skipped_uncached_tracks: Vec::new(),
        total_elapsed_sec,
        capped_count: ctx.capped_count,
        max_collection_limit: ctx.max_collection_limit,
        is_cache_only: ctx.options.is_cache_only,
        is_group: ctx.options.is_group,
        warnings: ctx
            .atmos_warning
            .lock()
            .expect("atmos poisoned")
            .clone()
            .into_iter()
            .chain(ctx.warnings.clone())
            .collect(),
        zip_delivery: zip_deliveries.first().cloned().or(zip_delivery),
        zip_deliveries,
        first_delivered_msg_id: first_msg_id,
        codec,
    }
}

async fn run_upload_item<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    upload_item: PipelineRipResult,
) where
    D: TrackCache + Delivery + TaskBookkeeping,
{
    let uploaded_ok = upload_one(&deps, &bus, &shared, &ctx, &job_controller, &upload_item).await;

    let cancelled =
        shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    if uploaded_ok
        && ctx.zip_build
        && !cancelled
        && let Some(state) = ctx.zip_state(upload_item.rendition)
    {
        let filename = build_zip_entry_filename_with_codec(
            Some(upload_item.rip_result.track_number),
            &upload_item.rip_result.title,
            &upload_item.rip_result.artist,
            &upload_item.track_id,
            &upload_item.rip_result.codec,
        );
        let destination = state.dir.join(&filename);
        if let Err(error) = tokio::fs::copy(&upload_item.rip_result.file_path, &destination).await {
            tracing::warn!(
                %error,
                track_id = %upload_item.track_id,
                rendition = ?upload_item.rendition,
                "failed to stage track for ZIP"
            );
            if upload_item.rendition == Rendition::Primary {
                set_primary_zip_error(
                    &ctx,
                    format!(
                        "primary ZIP staging failed for {}: {error}",
                        upload_item.track_id
                    ),
                );
            } else {
                *ctx.atmos_warning.lock().expect("atmos poisoned") = Some(format!(
                    "optional Atmos ZIP staging failed for {}: {error}",
                    upload_item.track_id
                ));
            }
        } else if shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
        {
            let _ = tokio::fs::remove_file(&destination).await;
        } else {
            match tokio::fs::metadata(&destination).await {
                Ok(metadata) => {
                    if let Ok(codec) = upload_item.rip_result.codec.parse::<Codec>()
                        && codec_allowed_for_rendition(upload_item.rendition, codec)
                    {
                        seed_zip_codec(state, codec);
                    }
                    state
                        .sources
                        .lock()
                        .expect("zip sources poisoned")
                        .push(ZipTrackEntry {
                            file_path: destination,
                            archive_filename: filename,
                            file_size: metadata.len(),
                        });
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        track_id = %upload_item.track_id,
                        rendition = ?upload_item.rendition,
                        "staged ZIP source disappeared"
                    );
                    let _ = tokio::fs::remove_file(&destination).await;
                    if upload_item.rendition == Rendition::Primary {
                        set_primary_zip_error(
                            &ctx,
                            format!(
                                "primary ZIP source metadata failed for {}: {error}",
                                upload_item.track_id
                            ),
                        );
                    } else {
                        *ctx.atmos_warning.lock().expect("atmos poisoned") = Some(format!(
                            "optional Atmos ZIP source metadata failed for {}: {error}",
                            upload_item.track_id
                        ));
                    }
                }
            }
        }
    }

    delete_file_if_exists(&upload_item.rip_result.file_path).await;
}

async fn finalize_job<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
) -> FinalizeResult
where
    D: AlbumCache + Delivery + ProviderDeps,
{
    if let Some(error) = fatal_error(&ctx) {
        return Err(error);
    }
    let zip_delivery = finalize_zip(&deps, &bus, &shared, &ctx, &job_controller).await?;
    if let Some(error) = fatal_error(&ctx) {
        return Err(error);
    }
    Ok(build_job_summary(&shared, &ctx, zip_delivery))
}

async fn finalize_zip<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
) -> Result<Option<ZipDeliveryInfo>, String>
where
    D: AlbumCache + Delivery + ProviderDeps,
{
    let result = finalize_zip_inner(deps, bus, shared, ctx, job_controller).await;
    let cancelled =
        shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    if result.is_err() || cancelled {
        let message_ids = take_uncommitted_zip_dump_messages(ctx);
        if !message_ids.is_empty()
            && let Err(error) = deps.retract_dump(&message_ids).await
        {
            tracing::error!(%error, "failed to retract ZIP dump publication");
        }
    }
    result
}

async fn deliver_cached_zip_rows_direct<D>(
    deps: &Arc<D>,
    shared: &Arc<Mutex<TaskShared>>,
    options: &RipTaskOptions,
    rendition: Rendition,
    rows: &[CachedAlbum],
    job_controller: &CancellationToken,
    first_delivered_msg_id: &mut Option<ChatMessageRef>,
) -> Result<(usize, i64), String>
where
    D: Delivery,
{
    let reply_to = (options.delivery_chat_id == options.chat_id)
        .then_some(options.reply_to_message_id)
        .flatten();
    let mut delivered = 0usize;
    let mut size = 0i64;
    for row in rows {
        if shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled() {
            return Ok((delivered, size));
        }
        let result = tokio::select! {
            res = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(options.delivery_chat_id),
                source: DumpMessageRef::new(row.message_id),
                reply_to: reply_to.map(ChatMessageRef::new),
                silent: rows.len() > 1,
            }) => res,
            _ = job_controller.cancelled() => return Ok((delivered, size)),
        };
        match result {
            Ok(DeliveryReceipt::Message(sent_id)) => {
                if shared.lock().expect("job poisoned").job.is_cancelled
                    || job_controller.is_cancelled()
                {
                    return Ok((delivered, size));
                }
                if first_delivered_msg_id.is_none() {
                    *first_delivered_msg_id = Some(sent_id);
                }
                delivered += 1;
                size += row.file_size;
                if rows.len() > 1 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
                }
            }
            Ok(DeliveryReceipt::PreviewDelivered) if rendition == Rendition::Primary => {
                return Err("primary ZIP delivery returned a preview receipt".to_owned());
            }
            Ok(DeliveryReceipt::PreviewDelivered) => {
                tracing::warn!(
                    rendition = ?rendition,
                    "optional Atmos ZIP delivery returned a preview receipt"
                );
            }
            Err(error) if rendition == Rendition::Primary => {
                return Err(format!("primary ZIP delivery failed: {error}"));
            }
            Err(error) => {
                tracing::warn!(%error, rendition = ?rendition, "optional Atmos ZIP delivery failed");
            }
        }
    }
    Ok((delivered, size))
}

async fn deliver_cached_zip_rows<D>(
    deps: &Arc<D>,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    rendition: Rendition,
    rows: &[CachedAlbum],
    job_controller: &CancellationToken,
) -> Result<(usize, i64), String>
where
    D: Delivery,
{
    let options = &ctx.options;
    let reply_to = (options.delivery_chat_id == options.chat_id)
        .then_some(options.reply_to_message_id)
        .flatten();
    let mut delivered = 0usize;
    let mut size = 0i64;
    for row in rows {
        if shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled() {
            return Ok((delivered, size));
        }
        let result = deps
            .deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(options.delivery_chat_id),
                source: DumpMessageRef::new(row.message_id),
                reply_to: reply_to.map(ChatMessageRef::new),
                silent: rows.len() > 1,
            })
            .await;
        match result {
            Ok(DeliveryReceipt::Message(sent_id)) => {
                if shared.lock().expect("job poisoned").job.is_cancelled
                    || job_controller.is_cancelled()
                {
                    return Ok((delivered, size));
                }
                let mut first = ctx.first_delivered_msg_id.lock().unwrap();
                if first.is_none() {
                    *first = Some(sent_id);
                }
                delivered += 1;
                size += row.file_size;
            }
            Ok(DeliveryReceipt::PreviewDelivered) if rendition == Rendition::Primary => {
                return Err("primary ZIP delivery returned a preview receipt".to_owned());
            }
            Ok(DeliveryReceipt::PreviewDelivered) => {
                tracing::warn!(
                    rendition = ?rendition,
                    "optional Atmos ZIP delivery returned a preview receipt"
                );
            }
            Err(error) if rendition == Rendition::Primary => {
                return Err(format!("primary ZIP delivery failed: {error}"));
            }
            Err(error) => {
                tracing::warn!(%error, rendition = ?rendition, "optional Atmos ZIP delivery failed");
            }
        }
    }
    Ok((delivered, size))
}

async fn finalize_zip_inner<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
) -> Result<Option<ZipDeliveryInfo>, String>
where
    D: AlbumCache + Delivery + ProviderDeps,
{
    let options = &ctx.options;
    if !ctx.zip_build {
        return Ok(None);
    }
    if let Some(error) = primary_zip_error(ctx) {
        return Err(error);
    }
    let finalizing_guard = FinalizingRenditionGuard::new(Arc::clone(&ctx.finalizing_rendition));
    let is_cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    if is_cancelled() {
        return Ok(None);
    }
    let expected_tracks = shared.lock().expect("job poisoned").job.total_tracks;
    let failures = ctx.failed_tracks.lock().expect("failures poisoned").clone();
    let mut first_delivery = None;

    for state in &ctx.zip_states {
        *ctx.finalizing_rendition
            .lock()
            .expect("finalizing rendition poisoned") = Some(state.rendition);
        if is_cancelled() {
            return Ok(first_delivery);
        }
        if let Some(error) = primary_zip_error(ctx) {
            return Err(error);
        }
        if let Some(rows) = ctx.zip_reuse.get(&state.rendition) {
            if ctx.zip_deliver && !options.is_cache_only {
                let (delivered, size) = deliver_cached_zip_rows(
                    deps,
                    shared,
                    ctx,
                    state.rendition,
                    rows,
                    job_controller,
                )
                .await?;
                if delivered > 0 {
                    let codec = rows[0].codec.as_str().to_owned();
                    let info = ZipDeliveryInfo {
                        album: ctx.zip_album.clone(),
                        artist: ctx.zip_artist.clone(),
                        release_year: ctx.zip_release_date.chars().take(4).collect(),
                        total_tracks: expected_tracks,
                        delivered_tracks: if state.rendition == Rendition::Primary {
                            Some(expected_tracks)
                        } else {
                            ctx.zip_reuse_atmos_track_count
                        },
                        total_parts: delivered,
                        size_bytes: size,
                        is_partial: false,
                        album_id: ctx.zip_album_id.clone(),
                        album_url: ctx.zip_album_url.clone(),
                        artwork_url: ctx.zip_artwork_url.clone(),
                        genre: ctx.zip_genre.clone(),
                        record_label: ctx.zip_record_label.clone(),
                        copyright: ctx.zip_copyright.clone(),
                        photo_delivered: false,
                        codec: Some(codec),
                    };
                    if first_delivery.is_none() {
                        first_delivery = Some(info.clone());
                    }
                    ctx.zip_delivery_infos.lock().unwrap().push(info);
                }
            }
            continue;
        }
        let mut replacement_uploads = Vec::new();
        let mut rendition_dump_messages = Vec::new();
        let mut entries = state.sources.lock().expect("zip sources poisoned").clone();
        if entries.is_empty() {
            continue;
        }
        entries.sort_by(|a, b| a.archive_filename.cmp(&b.archive_filename));
        let complete = match state.rendition {
            Rendition::Primary => failures.is_empty() && entries.len() == expected_tracks,
            Rendition::Atmos => true,
        };
        let should_publish =
            complete || (ctx.zip_deliver && !options.is_cache_only && !entries.is_empty());
        if !should_publish {
            continue;
        }

        let cover_bytes = match &ctx.zip_artwork_url {
            Some(url) => deps.fetch_artwork(url).await,
            None => None,
        };
        if is_cancelled() {
            return Ok(first_delivery);
        }
        let cover_path = match &cover_bytes {
            Some(bytes) => {
                let path = state.dir.join("cover.jpg");
                tokio::fs::write(&path, bytes).await.ok().map(|_| path)
            }
            None => None,
        };
        let thumb_path = match &ctx.zip_artwork_url {
            Some(url) if !url.is_empty() => {
                let thumb_url = deps.artwork_url_at_size(options.provider.clone(), url, 320);
                match deps.fetch_artwork(&thumb_url).await {
                    Some(bytes) if !bytes.is_empty() => {
                        let path = state.dir.join("cover_thumb.jpg");
                        tokio::fs::write(&path, bytes).await.ok().map(|_| path)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        if is_cancelled() {
            return Ok(first_delivery);
        }
        let thumb_path_str = thumb_path
            .as_deref()
            .map(|path| path.to_string_lossy().into_owned());
        let default_codec = "alac";
        let default_enum_codec = Codec::Alac;
        let codec = state
            .codec
            .lock()
            .expect("zip codec poisoned")
            .clone()
            .or_else(|| (state.rendition == Rendition::Atmos).then(|| "ec-3".to_owned()))
            .unwrap_or_else(|| default_codec.to_owned());
        let album_codec = codec.parse::<Codec>().unwrap_or(default_enum_codec);
        let plans = match plan_zip_parts_with_codec(
            &ctx.zip_artist,
            &ctx.zip_album,
            &ctx.zip_release_date,
            &entries,
            cover_path,
            TELEGRAM_SPLIT_THRESHOLD_BYTES,
            &codec,
        ) {
            Ok(plans) => plans,
            Err(error) => {
                if state.rendition == Rendition::Primary {
                    return Err(format!("primary ZIP planning failed: {error}"));
                }
                tracing::warn!(%error, "optional Atmos ZIP planning failed");
                continue;
            }
        };
        let mut delivered_parts = 0usize;
        let mut delivered_size = 0i64;
        let expected_parts = plans.len();
        let mut all_parts_uploaded = true;
        enum ZipSendOutcome {
            Published(DumpPublication),
            Delivered,
        }
        for plan in plans {
            let archive_name: StandardFilename = if complete {
                plan.archive_filename.clone().into_standard()
            } else {
                plan.archive_filename.clone().into_partial()
            };
            let output = state.dir.join(&archive_name);
            let bus_clone = bus.clone();
            let shared_clone = Arc::clone(shared);
            let cancel = job_controller.clone();
            let output_clone = output.clone();
            let plan_clone = plan.clone();
            let title_clone = archive_name.as_str().to_string();
            if is_cancelled() {
                return Ok(first_delivery);
            }
            let build = tokio::task::spawn_blocking(move || {
                bus_clone.set_upload(
                    &shared_clone,
                    Some(UploadLane::ArchiveBuild {
                        archive: title_clone.clone(),
                        progress: ByteProgress {
                            completed: 0,
                            total: None,
                        },
                    }),
                );
                bus_clone.emit_progress(&shared_clone);
                let progress_bus = bus_clone.clone();
                let progress_shared = Arc::clone(&shared_clone);
                let progress_title = title_clone.clone();
                let on_progress = move |uploaded: u64, total: u64| {
                    progress_bus.set_upload(
                        &progress_shared,
                        Some(UploadLane::ArchiveBuild {
                            archive: progress_title.clone(),
                            progress: ByteProgress {
                                completed: uploaded,
                                total: Some(total),
                            },
                        }),
                    );
                    progress_bus.emit_progress(&progress_shared);
                };
                let result = create_zip_archive(
                    &output_clone,
                    &plan_clone,
                    Some(&on_progress),
                    Some(&cancel),
                );
                bus_clone.set_upload(&shared_clone, None);
                bus_clone.emit_progress(&shared_clone);
                result
            })
            .await;
            if is_cancelled() {
                let _ = tokio::fs::remove_file(&output).await;
                return Ok(first_delivery);
            }
            let size = match build {
                Ok(Ok(size)) => size,
                Ok(Err(error)) => {
                    let _ = tokio::fs::remove_file(&output).await;
                    if state.rendition == Rendition::Primary {
                        return Err(format!("primary ZIP build failed: {error}"));
                    }
                    all_parts_uploaded = false;
                    tracing::warn!(%error, "optional Atmos ZIP build failed");
                    continue;
                }
                Err(error) => {
                    let _ = tokio::fs::remove_file(&output).await;
                    if state.rendition == Rendition::Primary {
                        return Err(format!("primary ZIP build task failed: {error}"));
                    }
                    all_parts_uploaded = false;
                    tracing::warn!(%error, "optional Atmos ZIP build task failed");
                    continue;
                }
            };
            if size > TELEGRAM_SPLIT_THRESHOLD_BYTES {
                let _ = tokio::fs::remove_file(&output).await;
                if state.rendition == Rendition::Primary {
                    return Err("primary ZIP exceeds the upload size limit".to_owned());
                }
                all_parts_uploaded = false;
                tracing::warn!("optional Atmos ZIP exceeds the upload size limit");
                continue;
            }
            let caption = format_zip_dump_caption(
                &DumpZipCaptionMetadata {
                    album_id: &ctx.zip_album_id,
                    codec: Some(album_codec.as_str()),
                    part_index: plan.part_index as i32,
                    total_parts: plan.total_parts as i32,
                    generation_hash: state.generation_hash.as_deref().unwrap_or(""),
                },
                complete,
                failures.len(),
            );
            let path = output.to_string_lossy().into_owned();
            if is_cancelled() {
                let _ = tokio::fs::remove_file(&output).await;
                return Ok(first_delivery);
            }
            let on_upload: UploadProgressCallback = {
                let shared = Arc::clone(shared);
                let bus = bus.clone();
                let title = archive_name.as_str().to_string();
                Arc::new(move |uploaded, total| {
                    bus.set_upload(
                        &shared,
                        Some(UploadLane::ArchiveUpload {
                            archive: title.clone(),
                            progress: ByteProgress {
                                completed: uploaded,
                                total: Some(total),
                            },
                        }),
                    );
                    bus.emit_progress(&shared);
                })
            };
            bus.set_upload(
                shared,
                Some(UploadLane::ArchiveUpload {
                    archive: archive_name.as_str().to_string(),
                    progress: ByteProgress {
                        completed: 0,
                        total: None,
                    },
                }),
            );
            bus.emit_progress(shared);
            let upload: Result<ZipSendOutcome, DeliveryError> = if complete {
                tokio::select! {
                    result = deps.publish_to_dump(DumpPublish::ZipDocument {
                        file_path: path.clone(),
                        thumb_path: thumb_path_str.clone(),
                        caption_html: caption.clone(),
                        on_upload_progress: Some(Arc::clone(&on_upload)),
                    }) => result.map(ZipSendOutcome::Published),
                    _ = job_controller.cancelled() => {
                        bus.set_upload(shared, None);
                        bus.emit_progress(shared);
                        return Ok(first_delivery);
                    }
                }
            } else {
                let chat_upload = tokio::select! {
                    result = deps.deliver_to_chat(ChatDelivery::ZipDocument {
                        destination: ChatRef::new(options.delivery_chat_id),
                        file_path: path.clone(),
                        thumb_path: thumb_path_str.clone(),
                        caption_html: caption.clone(),
                        on_upload_progress: Some(Arc::clone(&on_upload)),
                    }) => result,
                    _ = job_controller.cancelled() => {
                        bus.set_upload(shared, None);
                        bus.emit_progress(shared);
                        return Ok(first_delivery);
                    }
                };
                match chat_upload {
                    Ok(DeliveryReceipt::Message(sent_id)) => {
                        let mut first = ctx.first_delivered_msg_id.lock().unwrap();
                        if first.is_none() {
                            *first = Some(sent_id);
                        }
                        Ok(ZipSendOutcome::Delivered)
                    }
                    Ok(DeliveryReceipt::PreviewDelivered) => Err(DeliveryError::UnexpectedMedia),
                    Err(error) => Err(error),
                }
            };
            bus.set_upload(shared, None);
            bus.emit_progress(shared);
            match upload {
                Ok(ZipSendOutcome::Published(upload)) if complete => {
                    rendition_dump_messages.push(upload.message);
                    remember_zip_dump_message(ctx, upload.message);
                    if is_cancelled() {
                        return Ok(first_delivery);
                    }
                    if ctx.zip_deliver && !options.is_cache_only {
                        if is_cancelled() {
                            return Ok(first_delivery);
                        }
                        let reply_to = (options.delivery_chat_id == options.chat_id)
                            .then_some(options.reply_to_message_id)
                            .flatten();
                        let copy_result = tokio::select! {
                            result = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                                destination: ChatRef::new(options.delivery_chat_id),
                                source: upload.message,
                                reply_to: reply_to.map(ChatMessageRef::new),
                                silent: plan.total_parts > 1,
                            }) => result,
                            _ = job_controller.cancelled() => return Ok(first_delivery),
                        };
                        match copy_result {
                            Ok(DeliveryReceipt::Message(sent_id)) => {
                                if is_cancelled() {
                                    return Ok(first_delivery);
                                }
                                let mut first = ctx.first_delivered_msg_id.lock().unwrap();
                                if first.is_none() {
                                    *first = Some(sent_id);
                                }
                                delivered_parts += 1;
                                delivered_size += size as i64;
                            }
                            Ok(DeliveryReceipt::PreviewDelivered) => {
                                if state.rendition == Rendition::Primary {
                                    return Err("primary ZIP delivery returned a preview receipt"
                                        .to_owned());
                                }
                                tracing::warn!(
                                    "optional Atmos ZIP delivery returned a preview receipt"
                                );
                            }
                            Err(error) if state.rendition == Rendition::Primary => {
                                return Err(format!("primary ZIP delivery failed: {error}"));
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "optional Atmos ZIP delivery failed"
                                );
                            }
                        }
                    }
                    replacement_uploads.push(AlbumUpload {
                        provider: options.provider.clone(),
                        album_id: ctx.zip_album_id.clone(),
                        codec: album_codec,
                        part_index: plan.part_index as i32,
                        total_parts: plan.total_parts as i32,
                        message_id: upload.message.id(),
                        file_id: upload.file_id,
                        file_unique_id: upload.file_unique_id,
                        file_size: size as i64,
                        generation_hash: state.generation_hash.clone().unwrap_or_default(),
                    });
                }
                Ok(ZipSendOutcome::Delivered) if !complete && !options.is_cache_only => {
                    delivered_parts += 1;
                    delivered_size += size as i64;
                }
                Err(error) if state.rendition == Rendition::Primary => {
                    return Err(format!("primary ZIP upload failed: {error}"));
                }
                Err(error) => {
                    all_parts_uploaded = false;
                    tracing::warn!(%error, "optional Atmos ZIP upload failed");
                }
                Ok(ZipSendOutcome::Delivered) => {}
                Ok(ZipSendOutcome::Published(_)) => {
                    all_parts_uploaded = false;
                    tracing::warn!("ZIP publication was returned for a direct delivery");
                }
            }
        }
        if complete && all_parts_uploaded && replacement_uploads.len() == expected_parts {
            let new_message_ids = replacement_uploads
                .iter()
                .map(|upload| DumpMessageRef::new(upload.message_id))
                .collect::<Vec<_>>();
            let expected = ctx
                .zip_expectations
                .get(&state.rendition)
                .cloned()
                .unwrap_or(AlbumReplacementExpectation::Mixed);
            let replacement = deps
                .replace_albums(
                    options.provider.clone(),
                    &ctx.zip_album_id,
                    album_codec,
                    expected,
                    replacement_uploads,
                )
                .await;
            match replacement {
                Err(AlbumCacheError::Conflict { .. }) => {
                    if let Err(error) = deps.retract_dump(&rendition_dump_messages).await {
                        tracing::error!(
                            %error,
                            rendition = ?state.rendition,
                            "failed to retract ZIP uploads after cache conflict"
                        );
                    }
                    transfer_zip_dump_messages(ctx, &rendition_dump_messages);
                    let winner_rows = match deps
                        .find_albums(
                            options.provider.clone(),
                            &ctx.zip_album_id,
                            Some(album_codec),
                        )
                        .await
                    {
                        Ok(rows) => rows,
                        Err(error) if state.rendition == Rendition::Primary => {
                            return Err(format!(
                                "primary ZIP cache conflict winner lookup failed: {error}"
                            ));
                        }
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                rendition = ?state.rendition,
                                "optional Atmos cache conflict winner lookup failed"
                            );
                            Vec::new()
                        }
                    };
                    if winner_rows.is_empty() {
                        if state.rendition == Rendition::Primary {
                            return Err(
                                "primary ZIP cache conflict had no committed winner".to_owned()
                            );
                        }
                        tracing::warn!(
                            rendition = ?state.rendition,
                            "optional Atmos cache conflict had no committed winner"
                        );
                    } else if ctx.zip_deliver && !options.is_cache_only {
                        let (winner_parts, winner_size) = deliver_cached_zip_rows(
                            deps,
                            shared,
                            ctx,
                            state.rendition,
                            &winner_rows,
                            job_controller,
                        )
                        .await?;
                        delivered_parts = winner_parts;
                        delivered_size = winner_size;
                    }
                    tracing::info!(
                        rendition = ?state.rendition,
                        "discarded ZIP uploads and reused committed cache winner"
                    );
                }
                Err(error) => {
                    if state.rendition == Rendition::Primary {
                        return Err(format!("primary ZIP cache replacement failed: {error}"));
                    }
                    tracing::warn!(%error, "optional Atmos ZIP cache replacement failed");
                    let _ = deps.retract_dump(&rendition_dump_messages).await;
                    transfer_zip_dump_messages(ctx, &rendition_dump_messages);
                }
                Ok(AlbumReplacementResult::Stale) => {
                    let _ = deps.retract_dump(&rendition_dump_messages).await;
                    transfer_zip_dump_messages(ctx, &rendition_dump_messages);
                    tracing::info!(
                        rendition = ?state.rendition,
                        "discarded stale ZIP replacement"
                    );
                }
                Ok(AlbumReplacementResult::Committed {
                    displaced_message_ids,
                }) => {
                    transfer_zip_dump_messages(ctx, &new_message_ids);
                    let old_message_ids = displaced_message_ids
                        .into_iter()
                        .map(DumpMessageRef::new)
                        .filter(|message_id| !new_message_ids.contains(message_id))
                        .collect::<Vec<_>>();
                    if !old_message_ids.is_empty() {
                        let _ = deps.retract_dump(&old_message_ids).await;
                    }
                }
            }
        } else if complete && state.rendition == Rendition::Atmos {
            let _ = deps.retract_dump(&rendition_dump_messages).await;
            transfer_zip_dump_messages(ctx, &rendition_dump_messages);
            tracing::warn!("optional Atmos ZIP publication was incomplete");
        } else if state.rendition == Rendition::Primary && complete {
            return Err("primary ZIP publication was incomplete".to_owned());
        }
        if ctx.zip_deliver && !options.is_cache_only && delivered_parts > 0 {
            let release_year = ctx.zip_release_date.chars().take(4).collect::<String>();
            let caption_meta = AlbumDetailsCaptionMetadata {
                album: &ctx.zip_album,
                artist: &ctx.zip_artist,
                album_url: ctx.zip_album_url.as_deref(),
                total_tracks: expected_tracks,
                delivered_tracks: Some(entries.len()),
                size_bytes: delivered_size,
                total_parts: delivered_parts,
                release_year: &release_year,
                genre: ctx.zip_genre.as_deref(),
                record_label: ctx.zip_record_label.as_deref(),
                is_partial: !complete,
                user_name: options.user_name.as_deref(),
                user_id: options.user_id,
                codec: Some(codec.as_str()),
            };
            let details = format_album_details_caption(&caption_meta);
            let photo_delivered = if let Some(bytes) = &cover_bytes {
                if is_cancelled() {
                    return Ok(first_delivery);
                }
                let delivered = tokio::select! {
                    result = deps.deliver_to_chat(ChatDelivery::Photo {
                        destination: ChatRef::new(options.delivery_chat_id),
                        image_bytes: bytes.clone(),
                        caption_html: details.clone(),
                    }) => matches!(result, Ok(DeliveryReceipt::PreviewDelivered)),
                    _ = job_controller.cancelled() => return Ok(first_delivery),
                };
                if is_cancelled() {
                    return Ok(first_delivery);
                }
                delivered
            } else {
                false
            };
            let info = ZipDeliveryInfo {
                album: ctx.zip_album.clone(),
                artist: ctx.zip_artist.clone(),
                release_year,
                total_tracks: expected_tracks,
                delivered_tracks: Some(entries.len()),
                total_parts: delivered_parts,
                size_bytes: delivered_size,
                is_partial: !complete,
                album_id: ctx.zip_album_id.clone(),
                album_url: ctx.zip_album_url.clone(),
                artwork_url: ctx.zip_artwork_url.clone(),
                genre: ctx.zip_genre.clone(),
                record_label: ctx.zip_record_label.clone(),
                copyright: ctx.zip_copyright.clone(),
                photo_delivered,
                codec: Some(codec),
            };
            if first_delivery.is_none() {
                first_delivery = Some(info.clone());
            }
            ctx.zip_delivery_infos.lock().unwrap().push(info);
        }
    }
    drop(finalizing_guard);
    Ok(first_delivery)
}

async fn rollback_cancelled<D>(
    deps: &Arc<D>,
    track_id: &str,
    dump_message_id: DumpMessageRef,
    delete_record: bool,
    codec: Option<Codec>,
) where
    D: TrackCache + Delivery,
{
    let dump_removed = deps.retract_dump(&[dump_message_id]).await.is_ok();
    if dump_removed && delete_record {
        let _ = deps.delete_track(track_id, codec).await;
    }
}

async fn upload_one<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
    upload_item: &PipelineRipResult,
) -> bool
where
    D: TrackCache + Delivery + TaskBookkeeping,
{
    let options = &ctx.options;
    let track_id = upload_item.track_id.clone();
    let rip_result = &upload_item.rip_result;
    bus.set_codec(shared, Some(rip_result.codec.clone()));
    let track_label = TrackLabel::new(rip_result.title.clone(), rip_result.artist.clone())
        .with_artwork_url(
            (!rip_result.artwork_url.is_empty()).then(|| rip_result.artwork_url.clone()),
        )
        .with_position(
            upload_item.track_index.or_else(|| {
                u32::try_from(rip_result.track_number)
                    .ok()
                    .filter(|n| *n > 0)
            }),
            upload_item.total_tracks.or_else(|| {
                u32::try_from(rip_result.track_count)
                    .ok()
                    .filter(|n| *n > 0)
            }),
        );
    let is_cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();

    let caption = format_dump_caption(&DumpCaptionMetadata {
        track_id: &track_id,
        codec: Some(&rip_result.codec),
    });
    let plain_caption = format!(
        "{} - {}\n{}",
        rip_result.title, rip_result.artist, rip_result.album
    );
    let mut current_caption = caption.clone();
    let mut used_plain_caption = false;

    bus.set_upload(
        shared,
        Some(UploadLane::Track {
            track: track_label.clone(),
            progress: ByteProgress {
                completed: 0,
                total: None,
            },
        }),
    );
    bus.emit_progress(shared);

    let max_retries = ctx.config.upload_max_retries;
    enum SendOutcome {
        Audio(crate::orchestrator::deps::DumpPublication),
    }
    let mut outcome: Option<SendOutcome> = None;
    'upload: for attempt in 0..=max_retries {
        if is_cancelled() {
            break 'upload;
        }
        let on_upload: UploadProgressCallback = {
            let shared = Arc::clone(shared);
            let bus = bus.clone();
            let label = track_label.clone();
            Arc::new(move |uploaded, total| {
                bus.set_upload(
                    &shared,
                    Some(UploadLane::Track {
                        track: label.clone(),
                        progress: ByteProgress {
                            completed: uploaded,
                            total: Some(total),
                        },
                    }),
                );
                bus.emit_progress(&shared);
            })
        };

        let upload_result = tokio::select! {
            result = deps.publish_to_dump(DumpPublish::TrackAudio {
                file_path: rip_result.file_path.clone(),
                title: rip_result.title.clone(),
                performer: rip_result.artist.clone(),
                duration: rip_result.duration,
                caption_html: current_caption.clone(),
                on_upload_progress: Some(Arc::clone(&on_upload)),
            }) => result,
            _ = job_controller.cancelled() => break 'upload,
        };
        match upload_result {
            Ok(upload) => {
                outcome = Some(SendOutcome::Audio(upload));

                break 'upload;
            }
            Err(upload_err) => {
                if is_cancelled() {
                    break 'upload;
                }
                if matches!(
                    upload_err,
                    DeliveryError::Rejected(
                        DeliveryRejection::EntityBoundsInvalid | DeliveryRejection::CaptionTooLong
                    )
                ) && !used_plain_caption
                {
                    used_plain_caption = true;
                    current_caption = clamp_str_utf16(&plain_caption, 1024);
                    continue 'upload;
                }
                if upload_err.is_transient() && attempt < max_retries {
                    let jitter = 0.8 + (now_ms() % 400) as f64 / 1000.0;
                    let delay =
                        ctx.config.upload_retry_base_ms as f64 * 2f64.powi(attempt as i32) * jitter;
                    tracing::warn!(
                        track_id = %track_id,
                        attempt,
                        max_retries,
                        delay_ms = delay.round() as u64,
                        error = %upload_err,
                        "Track upload to dump failed, retrying"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(delay as u64)) => {}
                        _ = job_controller.cancelled() => {}
                    }
                } else {
                    tracing::error!(
                        track_id = %track_id,
                        attempts = attempt + 1,
                        error = %upload_err,
                        "All upload retries exhausted for track"
                    );

                    if upload_item.rendition == Rendition::Primary {
                        let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
                        failures.push(FailedTrack {
                            id: track_id.clone(),
                            error: upload_err.to_string(),
                            kind: None,
                            title: Some(rip_result.title.clone()),
                            artist: Some(rip_result.artist.clone()),
                            storefront: None,
                        });
                        shared.lock().expect("job poisoned").job.failed_count = failures.len();
                    }
                    bus.set_upload(shared, None);
                    bus.emit_progress(shared);
                    return false;
                }
            }
        }
    }

    let Some(outcome) = outcome else {
        bus.set_upload(shared, None);
        bus.emit_progress(shared);
        return false;
    };

    let SendOutcome::Audio(dump_upload) = outcome;

    let post_upload: Result<i64, String> = async {
        if is_cancelled() {
            if let Err(error) = deps.retract_dump(&[dump_upload.message]).await {
                tracing::error!(%error, "failed to retract cancelled track publication");
            }
            return Err("cancelled".to_owned());
        }
        let cache_input = SaveTrackInput::from_rip_result(
            &track_id,
            rip_result,
            dump_upload.message.id(),
            &dump_upload.file_id,
            &dump_upload.file_unique_id,
        );
        if let Err(error) =
            save_track_with_retry(deps.as_ref(), cache_input, &ctx.config.storage_retry).await
        {
            if let Err(retract_error) = deps.retract_dump(&[dump_upload.message]).await {
                tracing::error!(
                    %retract_error,
                    track_id = %track_id,
                    "failed to retract track publication after cache persistence failure"
                );
            }
            return Err(format!("track cache persistence failed: {error}"));
        }

        if is_cancelled() {
            let rip_codec = rip_result.codec.parse::<Codec>().ok();
            rollback_cancelled(deps, &track_id, dump_upload.message_id(), true, rip_codec).await;
            return Err("cancelled".to_owned());
        }

        if !options.is_cache_only && !ctx.zip_deliver {
            let reply_to = (options.delivery_chat_id == options.chat_id)
                .then_some(options.reply_to_message_id)
                .flatten();
            let sent_id = deps
                .deliver_to_chat(ChatDelivery::DumpCopy {
                    destination: ChatRef::new(options.delivery_chat_id),
                    source: dump_upload.message,
                    reply_to: reply_to.map(ChatMessageRef::new),
                    silent: ctx.is_multi_track,
                })
                .await
                .map_err(|e| e.to_string())?;
            let DeliveryReceipt::Message(sent_id) = sent_id else {
                return Err("track delivery returned a preview receipt".to_owned());
            };
            let mut guard = ctx.first_delivered_msg_id.lock().unwrap();
            if guard.is_none() {
                *guard = Some(sent_id);
            }
        }

        if is_cancelled() {
            rollback_cancelled(
                deps,
                &track_id,
                dump_upload.message_id(),
                true,
                rip_result.codec.parse::<Codec>().ok(),
            )
            .await;
            return Err("cancelled".to_owned());
        }

        let total_duration_ms = (now_ms() - upload_item.start_time_ms) as i64;
        Ok(total_duration_ms)
    }
    .await;

    match post_upload {
        Ok(total_duration_ms) => {
            bus.set_upload(shared, None);
            if upload_item.rendition == Rendition::Primary {
                let new_count = ctx
                    .ripped_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                shared.lock().expect("job poisoned").job.ripped_count = new_count;
            }

            bus.emit_progress(shared);

            tracing::info!(
                track = format!("{} - {}", rip_result.title, rip_result.artist),
                time = format!("{:.1}s", total_duration_ms as f64 / 1000.0),
                event = if options.is_cache_only {
                    "Track cached to dump"
                } else {
                    "Track completed"
                },
                "Track completed"
            );
            true
        }
        Err(err_msg) => {
            bus.set_upload(shared, None);
            if is_cancelled() {
                return false;
            }
            if upload_item.rendition == Rendition::Primary {
                record_failure(
                    shared,
                    ctx,
                    TrackFailureDetails {
                        track_id: &track_id,
                        err_msg,
                        title: Some(upload_item.rip_result.title.clone()),
                        artist: Some(upload_item.rip_result.artist.clone()),
                    },
                );
            }
            bus.emit_progress(shared);
            false
        }
    }
}

struct TrackFailureDetails<'a> {
    track_id: &'a str,
    err_msg: String,
    title: Option<String>,
    artist: Option<String>,
}

fn record_failure(
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &TaskContext,
    details: TrackFailureDetails<'_>,
) {
    let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
    failures.push(FailedTrack {
        id: details.track_id.to_string(),
        error: details.err_msg,
        kind: None,
        title: details.title,
        artist: details.artist,
        storefront: None,
    });
    shared.lock().expect("job poisoned").job.failed_count = failures.len();
}

fn kind_str(kind: TargetKind) -> &'static str {
    match kind {
        TargetKind::Track => "track",
        TargetKind::Album => "album",
        TargetKind::Artist => "artist",
        TargetKind::Playlist => "playlist",
    }
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "task panicked".to_owned()
    }
}

async fn delete_file_if_exists(path: &str) {
    let _ = tokio::fs::remove_file(std::path::Path::new(path)).await;
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
