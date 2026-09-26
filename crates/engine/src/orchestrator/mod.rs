//! Rip orchestrator for the live `/get` command contract.
//!
//! Owns job bookkeeping, resolves parsed items to tracks, feeds cache hits and
//! misses through one ordered pipeline, and runs two concurrent lanes: lane 1
//! rips and tags (one job at a time, through the sequential rip queue) while
//! lane 2 performs fresh-track Telegram uploads and ZIP work from all jobs on
//! a single global dispatcher — so downloads never wait on uploads and vice
//! versa. Cache hits become ordered lane-2 tasks; a failed cache delivery or
//! archive-source materialization schedules a lane-1 rerip continuation without
//! losing FIFO ordering.
//!
//! Upload retry exhaustion records a failed track and continues later tasks
//! instead of rejecting the whole job: one bad upload never strands the
//! remaining work, and the failure still surfaces in the summary.

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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use music::Rendition;
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
            AlbumUpload, ArtworkProvider, CachedAlbum, CachedTrack, ChatDelivery, ChatMessageRef,
            ChatRef, CollectionResolver, Delivery, DeliveryError, DeliveryReceipt,
            DeliveryRejection, DumpMessageRef, DumpPublication, DumpPublish, JobBookkeeping,
            JobDeps, OrchestratorConfig, ProviderAccess, ProviderComposition, ProviderPresentation,
            RequestLog, SaveTrackInput, StorageRetryPolicy, TrackAcquisition, TrackCache,
            UploadProgressCallback,
        },
        types::{
            ActiveRipJob, ByteProgress, DownloadLane, EventCallback, FailedTrack, FailedTrackKind,
            JobActivity, JobPhase, OrchestratorEvent, ResolutionFailure, RipActivity,
            RipJobOptions, RipJobProgress, RipJobSummary, TerminalJobState, TrackLabel, UploadLane,
            ZipDeliveryInfo,
        },
    },
    queue::{EnqueueOptions, SequentialRipQueue},
    ripper::{RipError, RipOptions, RipProgressCallback},
    settings::BotSettings,
    types::{AlbumTracks, ArtistTracks, Codec, Provider, TargetKind, TrackKey, TrackRipResult},
    zip::{
        TELEGRAM_SPLIT_THRESHOLD_BYTES, ZipTrackEntry, album_generation_hash,
        build_zip_entry_filename_with_codec, create_zip_archive, plan_zip_parts_with_codec,
    },
};

/// All orchestrator failures surface as plain messages, while resolution
/// failures retain every failed target for the bot to render.
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn find_cached_tracks_with_retry<D: TrackCache>(
    cache: &D,
    keys: &[TrackKey],
    policy: &StorageRetryPolicy,
) -> Result<HashMap<TrackKey, CachedTrack>, crate::orchestrator::deps::TrackCacheError> {
    let attempts = policy.total_attempts.max(1);
    let mut attempt = 0;
    loop {
        match cache.find_cached_tracks(keys).await {
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

/// A resolved track to rip.
#[derive(Debug, Clone)]
struct ResolvedTrackItem {
    id: String,
    title: Option<String>,
    artist: Option<String>,
    storefront: Option<String>,
    is_streamable: Option<bool>,
}

/// One item moving through the rip work feed.
#[derive(Clone)]
struct PipelineItem {
    track_id: String,
    storefront: Option<String>,
    meta_title: Option<String>,
    meta_artist: Option<String>,
    is_streamable: Option<bool>,
    rendition: Rendition,
    cached: Option<CachedTrack>,
}

/// One finished rip awaiting its upload.
struct PipelineRipResult {
    track_id: String,
    rip_result: TrackRipResult,
    start_time_ms: u64,
    rendition: Rendition,
}

/// Independent archive state for one requested rendition. Keeping the
/// directories and source lists separate is important: a sparse Atmos
/// archive must never contaminate the primary archive.
struct ZipState {
    rendition: Rendition,
    dir: PathBuf,
    sources: Arc<Mutex<Vec<ZipTrackEntry>>>,
    codec: Arc<Mutex<Option<String>>>,
    generation_hash: Option<String>,
}

/// Everything the two lanes need about one job, shared by `Arc` into the
/// lane-2 upload items and the finalize marker.
struct JobContext {
    config: OrchestratorConfig,
    options: RipJobOptions,
    zip_build: bool,
    zip_deliver: bool,
    zip_states: Vec<Arc<ZipState>>,
    zip_reuse: HashMap<Rendition, Vec<CachedAlbum>>,
    zip_expectations: HashMap<Rendition, AlbumReplacementExpectation>,
    /// Number of tracks in a reused sparse Atmos archive, derived from the
    /// persisted EC-3 track cache. `None` is retained when the archive has no
    /// matching per-track rows (for example, a legacy/indexed ZIP).
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
    /// Set once when an Atmos-requested job delivers a non-Atmos rip; folded
    /// into the summary warnings.
    atmos_warning: Arc<std::sync::Mutex<Option<String>>>,
    /// Rendition currently being finalized. The marker uses this only to
    /// classify an unexpected panic: Atmos remains best-effort, while a
    /// primary panic fails the job instead of reporting a false completion.
    finalizing_rendition: Arc<Mutex<Option<Rendition>>>,
    /// A lane-2 panic or a required ZIP staging failure makes the whole job
    /// fail. Keeping this separate from per-track failures prevents the
    /// finalize marker from mistaking an unsettled lane task for a successful
    /// partial job.
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

impl JobContext {
    fn zip_state(&self, rendition: Rendition) -> Option<&Arc<ZipState>> {
        self.zip_states
            .iter()
            .find(|state| state.rendition == rendition)
    }
}

fn set_fatal_error(ctx: &JobContext, error: impl Into<String>) {
    let mut fatal_error = ctx.fatal_error.lock().expect("fatal error poisoned");
    if fatal_error.is_none() {
        *fatal_error = Some(error.into());
    }
}

fn fatal_error(ctx: &JobContext) -> Option<String> {
    ctx.fatal_error
        .lock()
        .expect("fatal error poisoned")
        .clone()
}

fn set_primary_zip_error(ctx: &JobContext, error: impl Into<String>) {
    let mut primary_zip_error = ctx
        .primary_zip_error
        .lock()
        .expect("primary ZIP error poisoned");
    if primary_zip_error.is_none() {
        *primary_zip_error = Some(error.into());
    }
}

fn primary_zip_error(ctx: &JobContext) -> Option<String> {
    ctx.primary_zip_error
        .lock()
        .expect("primary ZIP error poisoned")
        .clone()
}

fn remember_zip_dump_message(ctx: &JobContext, message_id: DumpMessageRef) {
    ctx.zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned")
        .push(message_id);
}

/// Transfer exactly one ownership record for every committed message. A
/// vector is intentionally used instead of a set because test adapters (and
/// a retried fake transport) may expose the same numeric id more than once;
/// one committed upload must not accidentally transfer a different pending
/// upload with the same id.
fn transfer_zip_dump_messages(ctx: &JobContext, committed: &[DumpMessageRef]) {
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

fn take_uncommitted_zip_dump_messages(ctx: &JobContext) -> Vec<DumpMessageRef> {
    let mut pending = ctx
        .zip_new_dump_messages
        .lock()
        .expect("ZIP messages poisoned");
    std::mem::take(&mut *pending)
}

fn archive_codec_replaced(replacement: Codec, existing: Codec) -> bool {
    match replacement {
        Codec::Alac | Codec::Aac => matches!(existing, Codec::Alac | Codec::Aac),
        Codec::Flac => existing == Codec::Flac,
        other => existing == other,
    }
}

fn record_lane_task_panic(
    shared: &Arc<Mutex<JobShared>>,
    ctx: &JobContext,
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

/// Record the strongest codec that actually contributed a source to an
/// archive.  In particular, an AAC cache hit must never leave the state at
/// its historical ALAC default: the resulting filename, caption, and album
/// row all derive from this value.
fn seed_zip_codec(state: &ZipState, codec: Codec) {
    let rank = |codec: Codec| match codec {
        Codec::Alac | Codec::Flac => 3,
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
        Rendition::Primary => matches!(codec, Codec::Alac | Codec::Aac | Codec::Flac),
        Rendition::Atmos => codec == Codec::Ec3,
    }
}

/// Job state shared by the orchestrator and both lanes (mutated by
/// reference from several concurrent stages).
struct JobShared {
    job: ActiveRipJob,
    progress: PipelineState,
}

#[derive(Default)]
struct PipelineState {
    job_activity: Mutex<Option<JobActivity>>,
    download: Mutex<Option<DownloadLane>>,
    upload: Mutex<Option<UploadLane>>,
}

/// Event registry shared by the orchestrator and its pipelines.
#[derive(Clone)]
struct EventBus {
    subscribers: Arc<Mutex<Vec<EventCallback>>>,
}

#[derive(Default)]
struct Admissions {
    jobs: HashMap<String, (i64, bool)>,
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

    fn set_job_activity(&self, shared: &Arc<Mutex<JobShared>>, activity: Option<JobActivity>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .job_activity
            .lock()
            .expect("job activity poisoned") = activity;
    }

    fn set_download(&self, shared: &Arc<Mutex<JobShared>>, lane: Option<DownloadLane>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .download
            .lock()
            .expect("download progress poisoned") = lane;
    }

    fn set_upload(&self, shared: &Arc<Mutex<JobShared>>, lane: Option<UploadLane>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .upload
            .lock()
            .expect("upload progress poisoned") = lane;
    }

    fn emit_progress(&self, shared: &Arc<Mutex<JobShared>>) {
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
            let progress = RipJobProgress {
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
            };
            (guard.job.clone(), progress)
        };
        self.emit(&OrchestratorEvent::Progress(&job, &progress));
    }
}

struct DownloadProgressGuard {
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
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

/// The orchestrator: subscriber registry + job table + the rip queue.
pub struct RipOrchestrator {
    config: OrchestratorConfig,
    bus: EventBus,
    jobs: Arc<Mutex<HashMap<String, Arc<Mutex<JobShared>>>>>,
    queue: SequentialRipQueue,
    /// Lane 2: a single global dispatcher serializing every Telegram-I/O
    /// task (track uploads, zip packaging, deliveries). Items from ALL
    /// jobs interleave here while lane 1 keeps ripping — download never
    /// waits for upload and vice versa. Bounded at 16 pending items so a
    /// stalled upload lane back-pressures lane 1 instead of eating disk.
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

/// A lane-2 task: a self-contained boxed closure capturing everything it
/// needs (the job's `Arc<D>` deps, shared job context, files to act on).
/// The dispatcher runs items FIFO; the job's cancellation token is captured
/// inside each closure, which must check it and clean up after itself.
struct LaneTask {
    run: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
    /// Label for panic diagnostics only.
    label: &'static str,
    /// Settles the item/job when the task itself panics. A swallowed panic is
    /// otherwise indistinguishable from a successful no-op to the marker.
    on_panic: Option<Box<dyn FnOnce(String) + Send>>,
}

type FinalizeResult = Result<RipJobSummary, String>;
type FinalizeSender = tokio::sync::oneshot::Sender<FinalizeResult>;

/// Owns the one result that closes a job's two-lane pipeline.
///
/// The marker is deliberately the only normal owner that can settle this
/// sender. Keeping the workspace paths in the same guard closes the two
/// failure holes around a bounded queue: a marker can be dropped before it
/// starts, or its future can panic after it starts. In either case `Drop`
/// both wakes `start_job` with an error and removes the files synchronously as
/// a last-resort cleanup. The normal path uses the async cleanup below.
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

/// Releases a global admission slot if a job unwinds before the ordinary
/// `start_job` epilogue gets to do so.
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

/// Removes a job table entry if setup or a dependency panics after admission.
struct JobTableGuard {
    jobs: Arc<Mutex<HashMap<String, Arc<Mutex<JobShared>>>>>,
    job_id: String,
    armed: bool,
}

impl JobTableGuard {
    fn new(jobs: Arc<Mutex<HashMap<String, Arc<Mutex<JobShared>>>>>, job_id: String) -> Self {
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

/// Synchronous last-resort cleanup for workspaces created before the marker
/// takes ownership. The normal marker path uses async removal.
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
        // This path is used only when a marker is dropped or panics. The
        // directories are temporary workspaces, and synchronous removal is
        // preferable to leaving a live job and its files stranded.
        for path in &self.workspace_paths {
            let _ = std::fs::remove_dir_all(path);
        }
        self.send(Err("finalize marker stopped unexpectedly".to_owned()));
    }
}

/// Push one item into the global upload lane (FIFO across all jobs, so a
/// job's items run in order and its finalize marker runs last).
/// Back-pressures the caller when the 16-item buffer is full, which pauses
/// lane 1 instead of eating disk.
///
/// Returns false when the lane was never initialized — the item is dropped
/// and the caller must finish without it (the job is going away anyway).
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

    /// Lazily spawn the global lane-2 dispatcher (idempotent).
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

    /// Subscribe to every orchestrator event.
    pub fn subscribe(&self, callback: EventCallback) {
        self.bus
            .subscribers
            .lock()
            .expect("subscribers poisoned")
            .push(callback);
    }

    /// All jobs that have not reached a terminal state.
    pub fn get_active_jobs(&self) -> Vec<ActiveRipJob> {
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

    /// Look up one job's live snapshot.
    pub fn get_job(&self, id: &str) -> Option<ActiveRipJob> {
        self.jobs
            .lock()
            .expect("jobs poisoned")
            .get(id)
            .map(|shared| shared.lock().expect("job poisoned").job.clone())
    }

    fn set_phase(&self, shared: &Arc<Mutex<JobShared>>, phase: JobPhase) {
        shared.lock().expect("job poisoned").job.phase = phase;
    }

    /// Emit one and only one terminal event.  Cancellation removes the job
    /// from the public table immediately, so late queue/pipeline completion
    /// cannot manufacture a second terminal event.
    fn terminalize(
        &self,
        shared: &Arc<Mutex<JobShared>>,
        state: TerminalJobState,
        summary: Option<&RipJobSummary>,
        error: Option<&str>,
    ) -> bool {
        let (job, cancelled_by, state) = {
            let mut guard = shared.lock().expect("job poisoned");
            if guard.job.terminal_state.is_some() {
                return false;
            }
            // The shared job mutex is the linearization point for both
            // cancellation and terminalization. A cancellation accepted
            // before this lock is acquired wins; one accepted afterwards
            // observes the terminal state and is rejected.
            let state = if state != TerminalJobState::Cancelled
                && (guard.job.is_cancelled || guard.job.controller.is_cancelled())
            {
                TerminalJobState::Cancelled
            } else {
                state
            };
            guard.job.terminal_state = Some(state);
            guard.job.completed = true;
            (guard.job.clone(), guard.job.cancelled_by.clone(), state)
        };
        match state {
            TerminalJobState::Completed => {
                if let Some(summary) = summary {
                    self.bus.emit(&OrchestratorEvent::Completed(&job, summary));
                }
            }
            TerminalJobState::Cancelled => {
                self.bus
                    .emit(&OrchestratorEvent::Cancelled(&job, &cancelled_by));
            }
            TerminalJobState::Failed => {
                if let Some(error) = error {
                    self.bus.emit(&OrchestratorEvent::Failed(&job, error));
                }
            }
        }
        true
    }

    /// Request cancellation; false when missing, already cancelled, or
    /// completed. The terminal event and admission slot are retained until
    /// the lane-2 finalization marker has cleaned up the job's workspaces.
    pub fn cancel_job(&self, id: &str, cancelled_by: Option<&str>) -> bool {
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

    /// Run the whole rip flow for one request. Deps arrive per call.
    pub async fn start_job<D: JobDeps>(
        &self,
        deps: Arc<D>,
        options: &RipJobOptions,
    ) -> Result<RipJobSummary, OrchestratorError> {
        if !deps.providers().supports_provider(options.provider) {
            return Err(OrchestratorError::Message(format!(
                "provider {} is not available",
                options.provider
            )));
        }
        let job_id = cuid2::create_id();
        self.admit(&job_id, options)?;
        let mut admission_guard = AdmissionGuard::new(Arc::clone(&self.admissions), job_id.clone());

        // Settings are a snapshot.  The live-availability decision is made
        // after resolution and cache delivery, not as an early gate.
        let settings = deps.settings_snapshot();

        let job_controller = CancellationToken::new();
        let mut job_header = deps
            .providers()
            .presentation()
            .default_job_header()
            .to_owned();
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

        let shared: Arc<Mutex<JobShared>> = Arc::new(Mutex::new(JobShared {
            job: ActiveRipJob {
                id: job_id.clone(),
                provider: options.provider,
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
                status_msg_id: options.status_msg_id,
                controller: job_controller.clone(),
                is_cancelled: false,
                cancelled_by: None,
                cached_count: 0,
                ripped_count: 0,
                failed_count: 0,
                completed: false,
                start_time_ms: now_ms(),
                queue_position: None,
                phase: JobPhase::Resolving,
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
                provider: options.provider,
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

            self.set_phase(&shared, JobPhase::WaitingDuplicate);
            self.bus.set_job_activity(
                &shared,
                Some(JobActivity::WaitingDuplicate {
                    inflight_job_id: inflight.job_id.clone(),
                }),
            );
            self.bus.emit_progress(&shared);

            let notify = Arc::clone(&inflight.notify);
            tokio::select! {
                _ = notify.notified() => {}
                _ = job_controller.cancelled() => {
                    self.terminalize(&shared, TerminalJobState::Cancelled, None, None);
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
            self.terminalize(&shared, TerminalJobState::Cancelled, None, None);
            // Preserve the existing caller contract: a cancellation that
            // reached the marker still resolves the pipeline's summary, but
            // its sole terminal event is Cancelled. Queue/admission failures
            // before a marker remain errors and are returned unchanged.
            result
        } else {
            match &result {
                Ok(summary) => {
                    self.terminalize(&shared, TerminalJobState::Completed, Some(summary), None);
                }
                Err(err) => {
                    let message = err.to_string();
                    self.terminalize(&shared, TerminalJobState::Failed, None, Some(&message));
                }
            }
            result
        };
        job_table_guard.remove();
        admission_guard.release();
        result
    }

    fn admit(&self, job_id: &str, options: &RipJobOptions) -> Result<(), OrchestratorError> {
        let mut admissions = self.admissions.lock().expect("admissions poisoned");
        // Admins bypass every admission cap (user + global). Their jobs still
        // occupy a slot so `/cancel_<id>` bookkeeping and the dashboard can find
        // them, but they never crowd anyone out nor get crowded out.
        if !options.is_admin {
            let non_admin_jobs = admissions
                .jobs
                .values()
                .filter(|(_, is_admin)| !*is_admin)
                .count();
            if non_admin_jobs >= 16 {
                return Err(OrchestratorError::AdmissionLimit);
            }
            let user_jobs = admissions
                .jobs
                .values()
                .filter(|(user_id, _)| *user_id == options.user_id)
                .count();
            if user_jobs >= 4 {
                return Err(OrchestratorError::UserAdmissionLimit);
            }
        }
        admissions
            .jobs
            .insert(job_id.to_owned(), (options.user_id, options.is_admin));
        Ok(())
    }

    pub fn release_admission(&self, job_id: &str) {
        self.admissions
            .lock()
            .expect("admissions poisoned")
            .jobs
            .remove(job_id);
    }

    /// The queue phase of the job flow: resolve → cap → cache lookup →
    /// admission of the two-lane pipeline.
    async fn run_job<D: JobDeps>(
        &self,
        deps: Arc<D>,
        options: &RipJobOptions,
        shared: Arc<Mutex<JobShared>>,
        job_controller: CancellationToken,
        settings: BotSettings,
    ) -> Result<RipJobSummary, OrchestratorError> {
        self.set_phase(&shared, JobPhase::Resolving);
        self.bus
            .set_job_activity(&shared, Some(JobActivity::Resolving));
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
                .unwrap_or_else(|| "us".to_string());

            let resolution: Result<(), String> = match item.kind {
                TargetKind::Track => {
                    resolved_tracks.push(ResolvedTrackItem {
                        id: item.id.clone(),
                        title: None,
                        artist: None,
                        storefront: Some(effective_sf.clone()),
                        is_streamable: None,
                    });
                    Ok(())
                }
                TargetKind::Album => match deps
                    .providers()
                    .collections()
                    .fetch_album_tracks(&item.id, &effective_sf)
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
                        .providers()
                        .collections()
                        .fetch_artist_tracks(&item.id, &effective_sf)
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
                        .providers()
                        .collections()
                        .fetch_playlist_tracks(&item.id, &effective_sf)
                        .await
                    {
                        Ok(data) => {
                            for t in data.tracks {
                                resolved_tracks.push(ResolvedTrackItem {
                                    id: t.id.clone(),
                                    title: Some(t.title.clone()),
                                    artist: Some(t.artist.clone()),
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
                    let album_url = deps.providers().presentation().album_url(id, sf);
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

        // Two-lane ZIP semantics:
        // - `zip_build`: every single-album job with >1 track packages and
        //   caches the archive in the dump channel.
        // - `zip_deliver`: a multi-track album is a ZIP delivery job, so
        //   individual track delivery is replaced by the archive.
        let is_album_job = options.parsed_items.len() == 1
            && options.parsed_items[0].kind == TargetKind::Album
            && tracks_to_process.len() > 1;
        let zip_build = is_album_job;
        // A single-track album remains an ordinary track delivery; it does
        // not create an empty or one-track archive.
        let zip_deliver = is_album_job && !options.is_cache_only;
        let mut warnings = Vec::new();
        // Generation identity of the resolved track set. Cached ZIP
        // parts recorded with this hash can be reused instead of rebuilt.
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

        self.set_phase(&shared, JobPhase::CheckingCache);
        let check_item = { shared.lock().expect("job poisoned").job.job_header.clone() };
        self.bus.set_job_activity(
            &shared,
            Some(JobActivity::CheckingCache { item: check_item }),
        );
        self.bus.emit_progress(&shared);
        let requested_ids: Vec<TrackKey> = tracks_to_process
            .iter()
            .flat_map(|track| {
                options
                    .rendition_policy
                    .renditions()
                    .iter()
                    .flat_map(move |rendition| {
                        rendition.accepted_cache_codecs().iter().map(move |codec| {
                            TrackKey::new(options.provider, track.id.clone()).with_codec(*codec)
                        })
                    })
            })
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
                        let lookup_key =
                            TrackKey::new(options.provider, item.id.clone()).with_codec(*codec);
                        if let Some(cached) = existing_tracks_map.remove(&lookup_key) {
                            old_message_ids.push(DumpMessageRef::new(cached.message_id));
                            let _ = deps.delete_track(&lookup_key).await;
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

        // Expand the request into track-major units. Cached units stay in the
        // same ordered feed as fresh units; this prevents a cached Atmos copy
        // from overtaking an uncached primary rendition.
        let mut pipeline_items: Vec<PipelineItem> = Vec::new();
        let cached_count = 0usize;
        let is_multi_track = tracks_to_process.len() > 1;
        let first_delivered_msg_id: Option<ChatMessageRef> = None;

        // Take one snapshot of all archive rows for the replacement groups.
        // Besides powering reuse, this is the compare-and-swap generation
        // observed before the potentially long rebuild.  The repository
        // rechecks it while holding the replacement key lock.
        let existing_album_rows = if zip_build {
            match deps
                .find_albums(options.provider, &options.parsed_items[0].id, None)
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
                        Rendition::Primary => match options.provider {
                            Provider::Qobuz => Codec::Flac,
                            _ => Codec::Alac,
                        },
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

        // A complete archive can be reused independently for each codec. The
        // Atmos archive is allowed to be sparse, so a non-empty valid archive
        // is sufficient to reuse it.
        let mut zip_reuse: HashMap<Rendition, Vec<CachedAlbum>> = HashMap::new();
        if let Some(hash) = &zip_generation_hash
            && !options.is_force
        {
            for rendition in options.rendition_policy.renditions() {
                let codecs: &[Codec] = match rendition {
                    Rendition::Primary => &[Codec::Alac, Codec::Flac, Codec::Aac],
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

        for item in &tracks_to_process {
            if job_controller.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }
            for rendition in options.rendition_policy.renditions() {
                let cached = rendition.accepted_cache_codecs().iter().find_map(|codec| {
                    existing_tracks_map
                        .get(&TrackKey::new(options.provider, item.id.clone()).with_codec(*codec))
                        .cloned()
                });
                pipeline_items.push(PipelineItem {
                    track_id: item.id.clone(),
                    storefront: item.storefront.clone(),
                    meta_title: item.title.clone(),
                    meta_artist: item.artist.clone(),
                    is_streamable: item.is_streamable,
                    rendition: *rendition,
                    cached,
                });
            }
        }
        for rendition in options.rendition_policy.renditions() {
            // Atmos archives are intentionally sparse: unavailable Atmos
            // tracks have no individual cache row, but a complete cached
            // sparse archive is still independently reusable.
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
        // Album rows only describe archive parts. For a reused sparse Atmos
        // archive, derive the actual file count from the persisted EC-3 track
        // rows instead of treating the number of parts as the number of
        // tracks. If those rows are unavailable, keep the value unknown rather
        // than fabricating a count.
        let zip_reuse_atmos_track_count = if zip_reuse.contains_key(&Rendition::Atmos) {
            let count = tracks_to_process
                .iter()
                .filter(|item| {
                    existing_tracks_map
                        .get(
                            &TrackKey::new(options.provider, item.id.clone())
                                .with_codec(Codec::Ec3),
                        )
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
            && settings.can_rip_provider(options.provider, options.is_admin);

        if !can_rip_live
            && !settings.can_rip_provider(options.provider, options.is_admin)
            && (has_fresh || !cache_hits_present)
        {
            warnings.push(format!(
                "{} live ripping is currently disabled by administrator.",
                match options.provider {
                    music::Provider::Apple => "Apple Music",
                    music::Provider::Qobuz => "Qobuz",
                }
            ));
        }

        // Delivery metadata for the ZIP details message. Populated by
        // the reuse and rebuild finalization paths; None on cache-only.
        let zip_delivery: Option<ZipDeliveryInfo> = None;

        let summary = |cached_count: usize,
                       ripped_count: usize,
                       failed: Vec<FailedTrack>,
                       skipped: Vec<String>,
                       elapsed: &str,
                       zip_delivery: Option<ZipDeliveryInfo>,
                       first_msg_id: Option<ChatMessageRef>| {
            let guard = shared.lock().expect("job poisoned");
            RipJobSummary {
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
            }
        };

        // Reusable archive rows are consumed by the same finalization path as
        // newly-built rows. Keeping the decision in the per-rendition state
        // avoids mixing primary and Atmos archive identities.
        // Maintenance mode skips misses. Cache-only jobs still rip
        // uncached tracks, but keep the resulting audio in the dump channel
        // instead of delivering a copy to the requester.
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
                .set_job_activity(&shared, Some(JobActivity::SkippingUncached));
            self.bus.emit_progress(&shared);
            let elapsed = format!(
                "{:.1}",
                (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                    as f64)
                    / 1000.0
            );
            // No staging or packaging will happen for the skipped tracks;
            // drop the zip workspace too.
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
            self.set_phase(&shared, JobPhase::Delivering);
            self.bus
                .set_job_activity(&shared, Some(JobActivity::CachedDelivered));
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
                        let cache_key = TrackKey::new(options.provider, item.track_id.clone())
                            .with_codec(cached.codec);
                        self.bus.set_download(
                            &shared,
                            Some(DownloadLane::CachedDelivery {
                                track: TrackLabel::new(cached.title.clone(), cached.artist.clone()),
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
                                let _ = deps
                                    .log_request(RequestLog {
                                        telegram_id: options.user_id,
                                        chat_id: options.chat_id,
                                        track_key: cache_key,
                                        is_cache_hit: true,
                                        duration_ms: Some(0),
                                        status: "completed".to_owned(),
                                        error_reason: None,
                                    })
                                    .await;

                                cached_count += 1;
                                {
                                    let mut guard = shared.lock().expect("job poisoned");
                                    guard.job.cached_count = cached_count;
                                }
                                self.bus.set_download(&shared, None);
                                self.bus
                                    .set_job_activity(&shared, Some(JobActivity::CachedDelivered));
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
                                let _ = deps.delete_track(&cache_key).await;
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
                                            (Some(id), Some(storefront)) => deps
                                                .providers()
                                                .presentation()
                                                .album_url(id, storefront),
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
                        .set_job_activity(&shared, Some(JobActivity::CachedDelivered));
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
                    .set_job_activity(&shared, Some(JobActivity::CachedDelivered));
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

        self.set_phase(&shared, JobPhase::Queued);
        self.bus
            .set_job_activity(&shared, Some(JobActivity::Queued { position: 1 }));
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

        // Include resolution/cache time in the elapsed value, as the former
        // cache-only path did; this also avoids reporting a completed cached
        // job as `0.0s` after a slow cache lookup.
        let queue_start_time = shared.lock().expect("job poisoned").job.start_time_ms;

        // Lane 2 must exist before lane 1 can push items into it. The
        // dispatcher is global and lazily spawned once per orchestrator.
        let _ = self.ensure_upload_lane();

        // The job context moves into both lanes: every lane-2 item holds a
        // clone, and the finalize marker holds the last one.
        let job_ctx = Arc::new(JobContext {
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
                    deps.providers().presentation().album_url(id, storefront)
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

        // Create lane-1's workspace before submitting the queue item so an
        // admission/queue failure has a path it can clean. The finalization
        // guard receives the same path after the marker is enqueued.
        let rip_job_dir =
            std::env::temp_dir().join(format!("rip_job_{id}", id = cuid2::create_id()));
        workspace_guard.add(rip_job_dir.clone());
        if let Err(error) = tokio::fs::create_dir_all(&rip_job_dir).await {
            return Err(OrchestratorError::Message(format!(
                "create rip workspace: {error}"
            )));
        }
        // The finalize marker (last lane-2 item for this job) resolves the
        // job summary; `start_job` awaits it after the enqueue returns.
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
                Some(JobActivity::Queued {
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
                guard.job.phase = JobPhase::Processing;
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

        // Lane 1 returns as soon as its ordered work feed has been handed to
        // lane 2. The marker is queued after that feed and is the only source
        // of the final summary.
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
                // The task never ran (aborted while pending, queue cleared,
                // or it panicked). No marker owns the workspaces in this
                // case, so remove both the pre-created rip workspace and any
                // ZIP workspaces here.
                let _ = tokio::fs::remove_dir_all(&rip_job_dir).await;
                for state in &zip_states {
                    let _ = tokio::fs::remove_dir_all(&state.dir).await;
                }
                return Err(error.into());
            }
        }

        // Wait for the finalize marker (the last lane-2 item for this job)
        // so the job only goes terminal once every upload and the ZIP are
        // done. A missing marker is an error, never a successful lane-1
        // fallback: reporting completion here would strand uploads and hide
        // a broken dispatcher.
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

/// Lane 1: the rip loop and bounded handoff to lane 2.
/// Runs as the sequential rip queue's task (one job at a time), so the
/// queue slot is held only while rips happen: each finished rip hands its
/// upload off to lane 2 (the global upload dispatcher), and the job's
/// finalize marker — pushed after the last rip — completes the archive,
/// cleans both workspaces, and resolves the summary `start_job` awaits.
///
/// A cache task that cannot deliver/materialize its source hands the fallback
/// to a detached continuation. Lane 2 never submits or waits for a rip
/// continuation behind the active queue item, and the sequential queue is
/// free while cache I/O is pending.
///
struct LaneOneContext<'a, D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    uncached_items: &'a [PipelineItem],
    job_controller: CancellationToken,
    queue_signal: CancellationToken,
    ctx: Arc<JobContext>,
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
    shared: &Arc<Mutex<JobShared>>,
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
    shared: &'a Arc<Mutex<JobShared>>,
    ctx: &'a Arc<JobContext>,
    job_controller: &'a CancellationToken,
    queue_signal: &'a CancellationToken,
    item: PipelineItem,
    rip_job_dir: &'a Path,
}

/// Rip one item on lane 1. There is deliberately no cache, Telegram, ZIP, or
/// filesystem operation here; the result is handed to lane 2 by the caller.
async fn rip_fresh_item<D>(input: RipFreshInput<'_, D>) -> RipLaneOutcome
where
    D: ProviderAccess + JobBookkeeping + 'static,
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
        let presentation = deps.providers().presentation();
        let err_msg = presentation.unavailable_track_message().to_owned();
        let log_message = presentation.unavailable_track_log_message();
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
        tracing::warn!(track_id = %item.track_id, "{log_message}");
        let log_result = deps
            .log_request(RequestLog {
                telegram_id: ctx.options.user_id,
                chat_id: ctx.options.chat_id,
                track_key: TrackKey::new(ctx.options.provider, item.track_id.clone()),
                is_cache_hit: false,
                duration_ms: Some(0),
                status: "failed".to_owned(),
                error_reason: Some(err_msg),
            })
            .await;
        if let Err(error) = log_result {
            tracing::warn!(%error, track_id = %item.track_id, "request log failed for unavailable track");
        }
        bus.set_job_activity(shared, Some(JobActivity::ProcessingNext));
        bus.emit_progress(shared);
        return RipLaneOutcome::Finished;
    }

    let track_start_time = now_ms();
    let update_single_track_header =
        !ctx.is_multi_track && item.meta_title.is_none() && item.meta_artist.is_none();

    let on_progress: RipProgressCallback = {
        let shared = Arc::clone(shared);
        let bus = bus.clone();
        Arc::new(move |activity| {
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

    let storefront = item.storefront.clone().unwrap_or_else(|| "us".to_owned());
    let codec_preference = if item.rendition == Rendition::Atmos {
        music::CodecPreference::Atmos
    } else {
        ctx.options
            .codec_preference
            .unwrap_or_else(|| item.rendition.codec_preference())
    };
    let rip_options = RipOptions {
        provider: ctx.options.provider,
        storefront: &storefront,
        on_progress: Some(&on_progress),
        signal: Some(queue_signal.clone()),
        output_dir: Some(rip_job_dir),
        codec_preference,
    };
    let rip_result = match deps
        .providers()
        .acquisition()
        .rip(&item.track_id, rip_options)
        .await
    {
        Ok(rip_result) => rip_result,
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
            let log_result = deps
                .log_request(RequestLog {
                    telegram_id: ctx.options.user_id,
                    chat_id: ctx.options.chat_id,
                    track_key: if item.rendition == Rendition::Primary {
                        TrackKey::new(ctx.options.provider, item.track_id.clone())
                    } else {
                        TrackKey::new(ctx.options.provider, item.track_id.clone())
                            .with_codec(item.rendition.accepted_cache_codecs()[0])
                    },
                    is_cache_hit: false,
                    duration_ms: Some(duration_ms),
                    status: "failed".to_owned(),
                    error_reason: Some(err_msg.clone()),
                })
                .await;
            if let Err(log_error) = log_result {
                tracing::warn!(
                    %log_error,
                    track_id = %item.track_id,
                    "request log failed after rip failure"
                );
            }

            bus.set_job_activity(shared, Some(JobActivity::ProcessingNext));
            bus.emit_progress(shared);
            // Source-offline failures skip this track but no longer stop the
            // batch: the wrapper fallback covers the remaining tracks.
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
    }))
}

struct UploadLaneInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
    queue_cancellation: Option<CancellationToken>,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    upload_item: PipelineRipResult,
}

async fn enqueue_upload_task<D>(input: UploadLaneInput<D>) -> bool
where
    D: TrackCache + Delivery + JobBookkeeping + 'static,
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
    shared: &'a Arc<Mutex<JobShared>>,
    ctx: &'a Arc<JobContext>,
    job_controller: &'a CancellationToken,
    queue_signal: &'a CancellationToken,
    item: &'a PipelineItem,
    cached: &'a CachedTrack,
}

async fn resolve_cached_item<D>(input: CachedResolutionInput<'_, D>) -> CacheResolution
where
    D: TrackCache + Delivery + JobBookkeeping,
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
    let cache_key = cached.track_key.clone();
    if is_cancelled() {
        return CacheResolution::Cancelled;
    }
    if !codec_allowed_for_rendition(item.rendition, cached.codec) {
        tracing::warn!(
            track_id = %item.track_id,
            codec = %cached.codec.as_str(),
            "cached track codec does not match rendition; reripping"
        );
        let _ = deps.delete_track(&cache_key).await;
        return CacheResolution::Rerip;
    }

    bus.set_job_activity(shared, None);
    bus.set_download(
        shared,
        Some(DownloadLane::CachedDelivery {
            track: TrackLabel::new(cached.title.clone(), cached.artist.clone()),
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
                let _ = deps.delete_track(&cache_key).await;
                return CacheResolution::Rerip;
            }
            Err(_) => {
                tracing::warn!(track_id = %item.track_id, "cached track delivery failed; reripping");
                let _ = deps.delete_track(&cache_key).await;
                return CacheResolution::Rerip;
            }
        };
        if is_cancelled() {
            return CacheResolution::Cancelled;
        }
        let log_result = tokio::select! {
            result = deps.log_request(RequestLog {
                telegram_id: ctx.options.user_id,
                chat_id: ctx.options.chat_id,
                track_key: cache_key.clone(),
                is_cache_hit: true,
                duration_ms: Some(0),
                status: "completed".to_owned(),
                error_reason: None,
            }) => result,
            _ = job_controller.cancelled() => return CacheResolution::Cancelled,
            _ = queue_signal.cancelled() => return CacheResolution::Cancelled,
        };
        if let Err(error) = log_result {
            tracing::warn!(%error, track_id = %item.track_id, "request log failed for cached track");
        }
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
    } else if !ctx.options.is_cache_only && ctx.zip_deliver {
        let log_result = tokio::select! {
            result = deps.log_request(RequestLog {
                telegram_id: ctx.options.user_id,
                chat_id: ctx.options.chat_id,
                track_key: cache_key.clone(),
                is_cache_hit: true,
                duration_ms: Some(0),
                status: "completed".to_owned(),
                error_reason: None,
            }) => result,
            _ = job_controller.cancelled() => return CacheResolution::Cancelled,
            _ = queue_signal.cancelled() => return CacheResolution::Cancelled,
        };
        if let Err(error) = log_result {
            tracing::warn!(%error, track_id = %item.track_id, "request log failed for cached ZIP track");
        }
        if is_cancelled() {
            return CacheResolution::Cancelled;
        }
    }

    if ctx.zip_build
        && !ctx.zip_reuse.contains_key(&item.rendition)
        && let Some(state) = ctx.zip_state(item.rendition)
    {
        let filename = build_zip_entry_filename_with_codec(
            None,
            &cached.title,
            &cached.artist,
            &item.track_id,
            cached.codec.as_str(),
        );
        let destination = state.dir.join(&filename);
        let track = TrackLabel::new(cached.title.clone(), cached.artist.clone());
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
            let _ = deps.delete_track(&cache_key).await;
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
                let _ = deps.delete_track(&cache_key).await;
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
    bus.set_job_activity(shared, Some(JobActivity::CachedDelivered));
    CacheResolution::Hit
}

struct CachedLaneInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
    queue_signal: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    item: PipelineItem,
    cached: CachedTrack,
}

/// One item in a job's ordered output stream.  Cache work is described here,
/// rather than started immediately by lane 1, so a later cache task cannot
/// perform a copy or ZIP staging operation ahead of a fallback rerip.
enum OrderedSlot {
    Cached {
        item: PipelineItem,
        cached: CachedTrack,
    },
    Fresh(PipelineRipResult),
    Finished,
}

const ORDERED_SLOT_CAPACITY: usize = 16;

/// The ordered handoff must be able to drain a whole resolved collection when
/// its head is waiting for a cache fallback.  Keeping this tied to the number
/// of items in the already-capped collection makes the per-job buffer bounded
/// without imposing a second, smaller limit on output ordering.
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

/// Per-job ordered dispatcher.  It is deliberately separate from both the
/// sequential rip queue and the global Telegram lane: it may wait for this
/// job's cache result or fallback rerip while the global upload lane remains
/// free to process other jobs.
struct OrderedDispatchInput<D> {
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    queue: SequentialRipQueue,
    slots: tokio::sync::mpsc::Receiver<OrderedSlot>,
    ordered_capacity: usize,
    rip_job_dir: PathBuf,
    finalization_guard: FinalizationGuard,
}

/// Hand a cache hit to lane 2 and return an explicit result handoff.
///
/// A failed cache operation is a request for the *active* lane-1 task to
/// rerip the item.  It must not submit another task to `SequentialRipQueue`:
/// that queue is already running this job, and lane 2 would otherwise wait on
/// a continuation that cannot start until this function returns.  The
/// one-shot is the handoff: lane 2 reports the cache outcome and immediately
/// becomes available for other jobs; a detached continuation later submits
/// the fallback to lane 1.
async fn enqueue_cached_lane_task<D>(input: CachedLaneInput<D>) -> CacheEnqueueResult
where
    D: TrackCache + Delivery + JobBookkeeping + 'static,
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

/// Upload retry exhaustion is recorded as a track failure and the job drains
/// the remaining results.  Results are handed to a bounded, per-job ordered
/// dispatcher; lane 1 never waits for a Telegram operation or a fallback
/// rerip.
async fn run_lane_one<D>(input: LaneOneContext<'_, D>)
where
    D: TrackCache + AlbumCache + ProviderAccess + Delivery + JobBookkeeping + 'static,
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
    // The detached dispatcher now owns cleanup and the summary sender.  The
    // lane-1 guard remains armed only until that ownership transfer.
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

    // Ripping remains sequential, but handoff is bounded.  If the ordered
    // dispatcher is waiting on an earlier cache slot, at most sixteen later
    // artifacts are retained before lane 1 applies backpressure and stops
    // producing more files.
    for item in uncached_items {
        if is_cancelled() {
            break;
        }
        let slot = if let Some(cached) = item.cached.clone() {
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

    // Do not select cancellation here: the dispatcher must receive this
    // marker even after cancellation so it can enqueue the terminal marker,
    // clean staged files, and settle start_job's summary receiver.
    let _ = slot_tx.send(OrderedSlot::Finished).await;
}

async fn enqueue_finalize_marker<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    finalization_guard: FinalizationGuard,
) where
    D: AlbumCache + Delivery + ProviderAccess + 'static,
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
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
    queue: SequentialRipQueue,
    item: PipelineItem,
    rip_job_dir: PathBuf,
}

fn submit_ordered_rerip_item<D>(input: OrderedReripInput<D>) -> crate::queue::TaskReceiver
where
    D: ProviderAccess + JobBookkeeping + 'static,
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
    shared: &Arc<Mutex<JobShared>>,
    ctx: &Arc<JobContext>,
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

/// Keep consuming the lane-1 handoff while an earlier cache operation is
/// unresolved.  The cache task itself runs on the global lane, so waiting here
/// must not let the bounded handoff fill and pin the sequential rip queue.
async fn wait_for_cache_resolution(
    mut resolution: tokio::sync::oneshot::Receiver<CacheResolution>,
    drain: &mut OrderedSlotDrain<'_>,
    job_controller: &CancellationToken,
    ctx: &Arc<JobContext>,
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

/// Wait for a fallback submitted to the sequential rip queue while draining
/// later lane-1 results.  Later results remain buffered and are not handed to
/// Telegram/ZIP work until this missing ordinal has settled.
async fn wait_for_ordered_rerip(
    mut completion: crate::queue::TaskReceiver,
    drain: &mut OrderedSlotDrain<'_>,
    job_controller: &CancellationToken,
    shared: &Arc<Mutex<JobShared>>,
    ctx: &Arc<JobContext>,
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
                // Keep the queue receiver alive until the queue item settles;
                // otherwise a marker could run while a cancelled fallback is
                // still writing into the shared workspace.
                cancelled = true;
            }
        }
    }
}

/// Drive one job's ordered output stream.  This task may wait for a cache
/// operation or for its own fallback to obtain the sequential rip queue, but
/// it never runs in the global lane-2 worker.  Consequently another job's
/// uploads remain dispatchable while this job's missing slot is settling.
async fn run_ordered_dispatch<D>(input: OrderedDispatchInput<D>)
where
    D: TrackCache + AlbumCache + ProviderAccess + Delivery + JobBookkeeping + 'static,
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
            // Keep receiving until the terminal marker so lane 1 can apply
            // bounded backpressure without getting stranded on cancellation.
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
                    // The original queue child is no longer the owner of the
                    // detached dispatcher.  The job token is its lifetime
                    // signal instead.
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
                        // Submit the fallback behind the currently active
                        // lane-1 task, then keep draining that task's bounded
                        // handoff while it finishes.  Waiting on `queue` here
                        // without draining would deadlock once the handoff is
                        // full: lane 1 owns the queue and cannot return, while
                        // the fallback waits behind it.
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

    // The marker is ordered after every task submitted by this coordinator.
    // On cancellation it also performs the normal ZIP rollback and workspace
    // cleanup before resolving the summary receiver.
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

/// Snapshot the shared job state into a `RipJobSummary` for the given
/// lane results. Elapsed time is measured from queue admission.
fn build_job_summary(
    shared: &Arc<Mutex<JobShared>>,
    ctx: &JobContext,
    zip_delivery: Option<ZipDeliveryInfo>,
) -> RipJobSummary {
    let total_elapsed_sec = format!(
        "{:.1}",
        (now_ms().saturating_sub(ctx.queue_start_time_ms)) as f64 / 1000.0
    );
    let failed = ctx.failed_tracks.lock().expect("failures poisoned").clone();
    let first_msg_id = *ctx.first_delivered_msg_id.lock().unwrap();
    let zip_deliveries = ctx.zip_delivery_infos.lock().unwrap().clone();
    let guard = shared.lock().expect("job poisoned");
    RipJobSummary {
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
    }
}

/// One lane-2 track item: dump upload (retries/backoff), cache row, user
/// copy (unless the archive replaces individual delivery), request log,
/// file cleanup, and — for zip jobs — staging the audio into the zip
/// workspace for the finalize marker to package.
async fn run_upload_item<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
    upload_item: PipelineRipResult,
) where
    D: TrackCache + Delivery + JobBookkeeping,
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

/// The lane-2 finalize marker: the last item for a job. Packages the
/// staged sources into a (possibly split) archive, publishes it, cleans
/// both workspaces, and resolves the job summary on the oneshot
/// `start_job` awaits.
///
/// Publication gating (the always-zip rule):
/// - Complete archive → always uploaded to the dump and `save_album`'d
///   (the cache), regardless of who asked; user copies/details are sent for
///   multi-track album requests (`zip_deliver`).
/// - Incomplete archive → never cached; delivered as `[Partial].zip`
///   straight to the delivery chat only for user-facing `zip_deliver` jobs;
///   otherwise skipped entirely.
async fn finalize_job<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<JobShared>>,
    ctx: Arc<JobContext>,
    job_controller: CancellationToken,
) -> FinalizeResult
where
    D: AlbumCache + Delivery + ProviderAccess,
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

/// The archive half of the finalize marker. A failed publication removes only
/// messages uploaded by this attempt; old cached rows/messages are never
/// deleted before the replacement transaction commits.
async fn finalize_zip<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<JobShared>>,
    ctx: &Arc<JobContext>,
    job_controller: &CancellationToken,
) -> Result<Option<ZipDeliveryInfo>, String>
where
    D: AlbumCache + Delivery + ProviderAccess,
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
    shared: &Arc<Mutex<JobShared>>,
    options: &RipJobOptions,
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
    shared: &Arc<Mutex<JobShared>>,
    ctx: &Arc<JobContext>,
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

/// Builds/delivers one archive rendition. Cache publication is staged in
/// memory and replaced only after every required primary part succeeded.
async fn finalize_zip_inner<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<JobShared>>,
    ctx: &Arc<JobContext>,
    job_controller: &CancellationToken,
) -> Result<Option<ZipDeliveryInfo>, String>
where
    D: AlbumCache + Delivery + ProviderAccess,
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
            // In particular, never publish an empty Atmos archive.
            continue;
        }
        entries.sort_by(|a, b| a.archive_filename.cmp(&b.archive_filename));
        let complete = match state.rendition {
            Rendition::Primary => failures.is_empty() && entries.len() == expected_tracks,
            Rendition::Atmos => true, // Atmos is intentionally sparse/optional.
        };
        let should_publish =
            complete || (ctx.zip_deliver && !options.is_cache_only && !entries.is_empty());
        if !should_publish {
            continue;
        }

        let cover_bytes = match &ctx.zip_artwork_url {
            Some(url) => deps.providers().artwork().fetch_artwork(url).await,
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
                let thumb_url = deps.providers().artwork().artwork_url_at_size(url, 320);
                match deps.providers().artwork().fetch_artwork(&thumb_url).await {
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
        let default_codec = match ctx.options.provider {
            Provider::Qobuz => "flac",
            _ => "alac",
        };
        let default_enum_codec = match ctx.options.provider {
            Provider::Qobuz => Codec::Flac,
            _ => Codec::Alac,
        };
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
            let zip_title = if plan.total_parts > 1 {
                format!(
                    "{} (Part {}/{})",
                    ctx.zip_album, plan.part_index, plan.total_parts
                )
            } else {
                ctx.zip_album.clone()
            };
            let bus_clone = bus.clone();
            let shared_clone = Arc::clone(shared);
            let cancel = job_controller.clone();
            let output_clone = output.clone();
            let plan_clone = plan.clone();
            let title_clone = zip_title.clone();
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
                    provider: options.provider,
                    album_id: &ctx.zip_album_id,
                    codec: Some(album_codec.as_str()),
                    album: &ctx.zip_album,
                    artist: &ctx.zip_artist,
                    filename: archive_name.as_str(),
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
                let title = zip_title.clone();
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
                    archive: zip_title.clone(),
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
                        provider: options.provider,
                        album_id: ctx.zip_album_id.clone(),
                        codec: album_codec,
                        part_index: plan.part_index as i32,
                        total_parts: plan.total_parts as i32,
                        message_id: upload.message.id(),
                        file_id: upload.file_id,
                        file_unique_id: upload.file_unique_id,
                        file_size: size as i64,
                        file_name: archive_name.to_string(),
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
                    options.provider,
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
                        .find_albums(options.provider, &ctx.zip_album_id, Some(album_codec))
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
                    // Another rebuild won after this job took its snapshot.
                    // Its rows/messages are not ours to remove; only the
                    // documents uploaded by this attempt are cleaned.
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
                    // The transaction has committed. These messages are now
                    // the cache's owners, so cancellation or a later
                    // rendition failure must never include them in rollback
                    // cleanup.  Displaced IDs were captured by that same
                    // transaction, not by a racy preflight query.
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

/// Best-effort cleanup for a cancellation after an upload has completed.
async fn rollback_cancelled<D>(
    deps: &Arc<D>,
    provider: Provider,
    track_id: &str,
    dump_message_id: DumpMessageRef,
    delete_record: bool,
    codec: Option<Codec>,
) where
    D: TrackCache + Delivery,
{
    let dump_removed = deps.retract_dump(&[dump_message_id]).await.is_ok();
    if dump_removed && delete_record {
        let mut key = TrackKey::new(provider, track_id);
        if let Some(c) = codec {
            key = key.with_codec(c);
        }
        let _ = deps.delete_track(&key).await;
    }
}

/// One upload iteration: caption → send (retries + backoff) → save → copy →
/// log. Upload-retries-exhausted is recorded as a track failure here, which
/// keeps the job alive to drain remaining results while preserving the
/// failure in the summary.
///
/// Runs on lane 2, so it only checks the job's own cancellation token:
/// rip-queue lifecycle tokens (position, queue abort) do not apply.
async fn upload_one<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<JobShared>>,
    ctx: &Arc<JobContext>,
    job_controller: &CancellationToken,
    upload_item: &PipelineRipResult,
) -> bool
where
    D: TrackCache + Delivery + JobBookkeeping,
{
    let options = &ctx.options;
    let track_id = upload_item.track_id.clone();
    let rip_result = &upload_item.rip_result;
    let track_label = TrackLabel::new(rip_result.title.clone(), rip_result.artist.clone());
    let is_cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();

    let caption = format_dump_caption(&DumpCaptionMetadata {
        track_key: TrackKey::new(options.provider, track_id.clone()),
        title: &rip_result.title,
        artist: &rip_result.artist,
        album: &rip_result.album,
        duration: rip_result.duration,
        bit_depth: rip_result.bit_depth,
        sample_rate: rip_result.sample_rate,
        codec: Some(&rip_result.codec),
        genre: Some(&rip_result.genre),
        release_date: Some(&rip_result.release_date),
        track_number: Some(rip_result.track_number),
        track_count: Some(rip_result.track_count),
        isrc: rip_result.isrc.as_deref(),
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

    // Send with configured retries.  The initial call is attempt zero, so
    // `max_retries + 1` calls are made in the ordinary case.
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
                // The post-upload transaction performs cancellation cleanup
                // before saving or delivering anything derived from this
                // message.  Do not start another awaited operation here.
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
                    // Record the track failure and keep the job alive so
                    // later tracks still upload.
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
        // Cancelled mid-retries: stop without recording a failure.
        bus.set_upload(shared, None);
        bus.emit_progress(shared);
        return false;
    };

    let SendOutcome::Audio(dump_upload) = outcome;

    // Post-upload block: save + copy + log share one try/catch — any
    // failure records the track failure and continues.
    let post_upload: Result<i64, String> = async {
        if is_cancelled() {
            if let Err(error) = deps.retract_dump(&[dump_upload.message]).await {
                tracing::error!(%error, "failed to retract cancelled track publication");
            }
            return Err("cancelled".to_owned());
        }
        let cache_input = SaveTrackInput::from_rip_result(
            options.provider,
            &track_id,
            rip_result,
            dump_upload.message.id(),
            &dump_upload.file_id,
            &dump_upload.file_unique_id,
        );
        if let Err(error) = save_track_with_retry(deps.as_ref(), cache_input, &ctx.config.storage_retry).await {
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
            rollback_cancelled(
                deps,
                options.provider,
                &track_id,
                dump_upload.message_id(),
                true,
                rip_codec,
            )
            .await;
            return Err("cancelled".to_owned());
        }

        // The user copy is skipped when the archive replaces individual
        // delivery (`zip_deliver`) or on cache-only jobs.
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
                options.provider,
                &track_id,
                dump_upload.message_id(),
                true,
                rip_result.codec.parse::<Codec>().ok(),
            )
            .await;
            return Err("cancelled".to_owned());
        }

        let total_duration_ms = (now_ms() - upload_item.start_time_ms) as i64;
        if let Err(error) = deps.log_request(RequestLog {
            telegram_id: options.user_id,
            chat_id: options.chat_id,
            track_key: TrackKey::new(options.provider, track_id.clone()),
            is_cache_hit: false,
            duration_ms: Some(total_duration_ms),
            status: "completed".to_string(),
            error_reason: None,
        }).await {
            tracing::warn!(%error, track_id = %track_id, "request log failed after track completion");
        }

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

            // Publish the completed upload immediately. Without this event a
            // single-track job could leave the dashboard showing
            // `Uploading` until the terminal refresh arrived.
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
                    deps,
                    TrackFailureDetails {
                        track_id: &track_id,
                        err_msg,
                        start_time_ms: upload_item.start_time_ms,
                        title: Some(upload_item.rip_result.title.clone()),
                        artist: Some(upload_item.rip_result.artist.clone()),
                    },
                )
                .await;
            }
            bus.emit_progress(shared);
            false
        }
    }
}

struct TrackFailureDetails<'a> {
    track_id: &'a str,
    err_msg: String,
    start_time_ms: u64,
    title: Option<String>,
    artist: Option<String>,
}

/// Record a track failure: push the row, update the counter, and log the
/// request.
async fn record_failure<D>(
    shared: &Arc<Mutex<JobShared>>,
    ctx: &JobContext,
    deps: &Arc<D>,
    details: TrackFailureDetails<'_>,
) where
    D: JobBookkeeping,
{
    {
        let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
        failures.push(FailedTrack {
            id: details.track_id.to_string(),
            error: details.err_msg.clone(),
            kind: None,
            title: details.title,
            artist: details.artist,
            storefront: None,
        });
        shared.lock().expect("job poisoned").job.failed_count = failures.len();
    }
    tracing::error!(track_id = %details.track_id, error = %details.err_msg, "Track upload failed");
    let log_result = deps
        .log_request(RequestLog {
            telegram_id: ctx.options.user_id,
            chat_id: ctx.options.chat_id,
            track_key: TrackKey::new(ctx.options.provider, details.track_id),
            is_cache_hit: false,
            duration_ms: Some((now_ms() - details.start_time_ms) as i64),
            status: "failed".to_string(),
            error_reason: Some(details.err_msg),
        })
        .await;
    if let Err(error) = log_result {
        tracing::warn!(%error, track_id = %details.track_id, "request log failed after track failure");
    }
}

// helpers
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

    fn options(user_id: i64, is_admin: bool) -> RipJobOptions {
        RipJobOptions {
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
            status_msg_id: 0,
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
        // Per-user cap: 4 concurrent jobs for normal users.
        let user = options(1, false);
        for job in ["u1", "u2", "u3", "u4"] {
            orchestrator.admit(job, &user).expect("user job");
        }
        assert!(matches!(
            orchestrator.admit("u5", &user),
            Err(OrchestratorError::UserAdmissionLimit)
        ));

        // Admins bypass both caps (per-user and global) but still occupy a
        // slot for `/cancel_<id>` bookkeeping.
        let admin = options(2, true);
        for job in ["a1", "a2", "a3", "a4", "a5"] {
            orchestrator.admit(job, &admin).expect("admin job");
        }

        // Global cap: 16 non-admin jobs. The admin jobs above do not count.
        for user_id in 3..=14 {
            orchestrator
                .admit(&format!("j{user_id}"), &options(user_id, false))
                .expect("global capacity");
        }
        assert!(matches!(
            orchestrator.admit("overflow", &options(100, false)),
            Err(OrchestratorError::AdmissionLimit)
        ));
        // The global cap frees a slot when a non-admin job is released.
        orchestrator.release_admission("j3");
        orchestrator
            .admit("after-release", &options(100, false))
            .expect("released slot");
    }
}
