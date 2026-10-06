use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use bot::{BotState, handlers, html};
use ferogram::{Client, InputMessage, PeerRef, filters::Dispatcher};
use tokio::{signal, sync::Semaphore};
use tracing::info;
use tracing_subscriber::EnvFilter;

const PROGRESS_UPDATE_INTERVAL: Duration = Duration::from_millis(250);

const MIN_APP_KEY_LEN: usize = 32;

#[derive(Default)]
struct ProgressThrottle {
    last_emitted_at: Option<Instant>,
    last_progress: Option<server::rip_tasks::RipTaskProgress>,
}

impl ProgressThrottle {
    fn should_emit(&mut self, now: Instant, progress: &server::rip_tasks::RipTaskProgress) -> bool {
        let Some(previous) = &self.last_progress else {
            self.last_emitted_at = Some(now);
            self.last_progress = Some(progress.clone());
            return true;
        };

        let phase_changed = !same_progress_phase(previous, progress);
        let progress_changed = previous != progress;
        let interval_elapsed = self
            .last_emitted_at
            .is_some_and(|last| now.saturating_duration_since(last) >= PROGRESS_UPDATE_INTERVAL);
        if phase_changed || (progress_changed && interval_elapsed) {
            self.last_emitted_at = Some(now);
            self.last_progress = Some(progress.clone());
            true
        } else {
            false
        }
    }
}

fn same_progress_phase(
    previous: &server::rip_tasks::RipTaskProgress,
    current: &server::rip_tasks::RipTaskProgress,
) -> bool {
    fn same_download_phase(
        previous: &Option<server::rip_tasks::RipTaskDownloadLane>,
        current: &Option<server::rip_tasks::RipTaskDownloadLane>,
    ) -> bool {
        match (previous, current) {
            (Some(previous), Some(current)) => {
                previous.stage == current.stage
                    && previous.title == current.title
                    && previous.artist == current.artist
            }
            (None, None) => true,
            _ => false,
        }
    }

    fn same_upload_phase(
        previous: &Option<server::rip_tasks::RipTaskUploadLane>,
        current: &Option<server::rip_tasks::RipTaskUploadLane>,
    ) -> bool {
        match (previous, current) {
            (Some(previous), Some(current)) => {
                previous.stage == current.stage
                    && previous.title == current.title
                    && previous.artist == current.artist
            }
            (None, None) => true,
            _ => false,
        }
    }

    same_download_phase(&previous.download, &current.download)
        && same_upload_phase(&previous.upload, &current.upload)
        && previous.job_stage == current.job_stage
        && previous.current_track_title == current.current_track_title
        && previous.current_track_artist == current.current_track_artist
        && previous.current_track_index == current.current_track_index
        && previous.total_tracks == current.total_tracks
        && previous.completed_tracks == current.completed_tracks
}

fn lane_byte_values(
    progress: &engine::orchestrator::types::ByteProgress,
) -> (Option<u64>, Option<u64>, Option<f32>) {
    let bytes_done = Some(progress.completed);
    let bytes_total = progress.total;
    let percent = server::rip_tasks::lane_percent(bytes_done, bytes_total);
    (bytes_done, bytes_total, percent)
}

fn extract_archive_codec(archive_name: &str) -> Option<String> {
    let name = archive_name.strip_suffix(".zip").unwrap_or(archive_name);
    let mut parts = Vec::new();
    let mut current = name;
    while let Some(start) = current.find('[') {
        let remainder = &current[start + 1..];
        if let Some(end) = remainder.find(']') {
            let tag = remainder[..end].trim();
            if tag != "Partial" && !tag.is_empty() {
                parts.push(tag);
            }
            current = &remainder[end + 1..];
        } else {
            break;
        }
    }
    parts.pop().map(|s| s.to_string())
}

fn map_job_stage(
    activity: Option<&engine::orchestrator::types::TaskActivity>,
) -> Option<server::rip_tasks::RipTaskJobStage> {
    use engine::orchestrator::types::TaskActivity;
    use server::rip_tasks::RipTaskJobStage as Stage;

    match activity? {
        TaskActivity::Resolving => Some(Stage::Resolving),
        TaskActivity::CheckingCache { .. } => Some(Stage::CheckingCache),
        TaskActivity::Queued { .. } => Some(Stage::Queued),
        TaskActivity::SkippingUncached => Some(Stage::SkippingUncached),
        TaskActivity::CachedDelivered => Some(Stage::CachedDelivered),
        TaskActivity::ProcessingNext => Some(Stage::ProcessingNext),
        TaskActivity::WaitingDuplicate { .. } => Some(Stage::WaitingDuplicate),
    }
}

#[derive(Debug)]
struct Env {
    api_id: i32,
    api_hash: String,
    bot_token: String,
    admin_id: i64,
    dump_channel_id: i64,
    database_url: String,
    app_key: String,
    log_level: String,
    stream_worker_bot_tokens: Vec<String>,
}

fn required(name: &str, invalid: &mut Vec<String>) -> String {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            invalid.push(name.to_owned());
            String::new()
        }
    }
}

fn parse<T: std::str::FromStr>(name: &str, value: String, invalid: &mut Vec<String>) -> Option<T> {
    if value.trim().is_empty() {
        if !invalid.iter().any(|item| item == name) {
            invalid.push(name.to_owned());
        }
        return None;
    }
    match value.parse() {
        Ok(parsed) => Some(parsed),
        Err(_) => {
            if !invalid.iter().any(|item| item == name) {
                invalid.push(name.to_owned());
            }
            None
        }
    }
}

fn load_env() -> Result<Env> {
    let _ = dotenvy::from_filename(".env");
    let mut invalid = Vec::new();
    let api_id =
        parse("API_ID", required("API_ID", &mut invalid), &mut invalid).unwrap_or_default();
    let api_hash = required("API_HASH", &mut invalid);
    let bot_token = required("BOT_TOKEN", &mut invalid);
    let admin_id =
        parse("ADMIN_ID", required("ADMIN_ID", &mut invalid), &mut invalid).unwrap_or_default();
    let dump_channel_id = parse(
        "DUMP_CHANNEL_ID",
        required("DUMP_CHANNEL_ID", &mut invalid),
        &mut invalid,
    )
    .unwrap_or_default();
    let database_url = required("DATABASE_URL", &mut invalid);

    let app_key = required("APP_KEY", &mut invalid);

    let log_level = std::env::var("LOG_LEVEL")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".to_owned());
    let stream_worker_bot_tokens = std::env::var("STREAM_WORKER_BOT_TOKENS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if !invalid.is_empty() {
        return Err(anyhow!(
            "missing or invalid environment variables: {}",
            invalid.join(", ")
        ));
    }

    if app_key.trim().len() < MIN_APP_KEY_LEN {
        return Err(anyhow!(
            "invalid environment variable: APP_KEY must be at least {MIN_APP_KEY_LEN} characters; \
             set it to a private, unique secret (for example: openssl rand -hex {MIN_APP_KEY_LEN})"
        ));
    }
    Ok(Env {
        api_id,
        api_hash,
        bot_token,
        admin_id,
        dump_channel_id,
        database_url,
        app_key,
        log_level,
        stream_worker_bot_tokens,
    })
}

fn init_tracing(log_level: &str) {
    let level = if log_level.eq_ignore_ascii_case("critical") {
        "error"
    } else {
        log_level
    };
    let filter =
        format!("warn,bot={level},db={level},engine={level},stream={level},server={level}");
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(filter))
        .add_directive(
            "symphonia_core::formats::probe=error"
                .parse()
                .expect("static Symphonia log directive is valid"),
        );
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let env = load_env().context("invalid startup configuration")?;
    init_tracing(&env.log_level);

    info!("Running database migrations...");
    let database = db::connect(&env.database_url)
        .await
        .context("connect to PostgreSQL")?;
    db::migrate(&database)
        .await
        .context("run database migrations")?;
    info!("Migrations completed successfully!");
    let auth = db::Auth::new(database.clone(), env.admin_id);

    let (client, shutdown) = Client::builder()
        .api_id(env.api_id)
        .api_hash(env.api_hash.clone())
        .session("bot-data/session")
        .catch_up(true)
        .experimental_features(ferogram::ExperimentalFeatures {
            allow_zero_hash: true,
            ..Default::default()
        })
        .retry_policy(Arc::new(
            bot::telegram_retry::BoundedTelegramRetry::default(),
        ))
        .connect()
        .await
        .context("connect to Telegram")?;
    let is_authorized = client.is_authorized().await.unwrap_or(false);
    if !is_authorized {
        client
            .bot_sign_in(&env.bot_token)
            .await
            .context("sign in bot")?;
        client
            .save_session()
            .await
            .context("save Telegram session")?;
    }
    let me = client.get_me().await.context("get bot identity")?;
    info!(username = ?me.username, bot_id = me.id, dump_channel = env.dump_channel_id, "Bot started successfully");

    let settings_store = Arc::new(db::SettingsStore::new(database.clone()));
    let rip_deps = Arc::new(
        bot::rip_deps::RipDeps::new(
            Arc::new(client.clone()),
            PeerRef::from(env.dump_channel_id),
            db::TracksRepository::new(database.clone()),
            Arc::clone(&settings_store),
            database.clone(),
        )
        .await
        .map_err(|error| anyhow!(error))
        .context("initialize rip dependencies")?,
    );

    let orchestrator = Arc::new(engine::orchestrator::RipOrchestrator::new(
        bot::rip_deps::orchestrator_config(),
    ));

    let session_store = db::WorkerSessionStore::from_env(database.clone());
    let worker_pool = stream::StreamWorkerPool::new(
        Some(client.clone()),
        &env.stream_worker_bot_tokens,
        env.api_id,
        &env.api_hash,
        Some(session_store),
    )
    .await
    .context("initialize stream worker pool")?;

    let stream_engine = Arc::new(stream::StreamEngine::new(
        worker_pool,
        Arc::new(stream::ChunkCache::default()),
        db::TracksRepository::new(database.clone()),
        Some(client.clone()),
        PeerRef::from(env.dump_channel_id),
    ));

    let session_manager = Arc::new(db::SessionManager::new(database.clone(), env.admin_id));
    let tracks_repo = Arc::new(db::TracksRepository::new(database.clone()));
    let app_key = env.app_key.clone();

    let initial_settings = settings_store.get_settings();
    let port = std::env::var("STREAM_SERVER_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(initial_settings.stream_server_port);

    let server_config = server::ServerConfig {
        host: [0, 0, 0, 0].into(),
        port,
        app_key: app_key.clone(),
    };

    let orchestrator_for_tasks = orchestrator.clone();
    let rip_deps_for_tasks = rip_deps.clone();
    let settings_store_for_tasks = Arc::clone(&settings_store);
    let admin_id = env.admin_id;
    let rip_task_runner: server::RipTaskRunner = Arc::new(
        move |state, task_id, provider, track_id, codec, user_id, controller| {
            let orchestrator = orchestrator_for_tasks.clone();
            let rip_deps = rip_deps_for_tasks.clone();
            let settings_store = Arc::clone(&settings_store_for_tasks);
            tokio::spawn(async move {
                if controller.is_cancelled() {
                    return;
                }
                let settings = settings_store.get_settings();
                let default_storefront =
                    engine::settings::resolve_default_storefront(&settings).to_owned();
                let item = engine::types::ParsedTargetItem {
                    id: track_id.clone(),
                    kind: music::TargetKind::Track,
                    storefront: Some(default_storefront.clone()),
                };
                let codec_preference = codec.map(|c| match c {
                    music::Codec::Alac => music::CodecPreference::HighestQuality,
                    music::Codec::Aac => music::CodecPreference::LosslessCd,
                    _ => music::CodecPreference::HighestQuality,
                });
                let is_admin = user_id == admin_id;
                let options = engine::orchestrator::types::RipTaskOptions {
                    provider,
                    chat_id: 0,
                    user_id,
                    user_name: Some(task_id.clone()),
                    delivery_chat_id: 0,
                    is_group: false,
                    is_force: false,
                    is_cache_only: true,
                    single_storefront: Some(default_storefront),
                    parsed_items: vec![item],
                    reply_to_message_id: None,
                    is_admin,
                    codec_preference,
                    rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
                };

                tracing::info!(task_id = %task_id, "Executing background rip task via RipOrchestrator");
                if let Err(e) = orchestrator.start_task(rip_deps, &options).await {
                    tracing::warn!(task_id = %task_id, error = %e, "Background rip task failed");
                    state.fail_task(&task_id, &e.to_string());
                }
            })
        },
    );

    let server_state = Arc::new(
        server::ServerState::new(
            stream_engine.clone(),
            session_manager.clone(),
            tracks_repo.clone(),
            settings_store,
            orchestrator.clone(),
            app_key.clone(),
        )
        .with_admin_id(env.admin_id)
        .with_telegram_client(client.clone())
        .with_rip_task_runner(rip_task_runner),
    );

    let server_state_for_events = Arc::clone(&server_state);
    let progress_throttles = Mutex::new(HashMap::<String, ProgressThrottle>::new());
    orchestrator.subscribe(Arc::new(move |event| {
        use engine::orchestrator::types::{
            DownloadLane, OrchestratorEvent, RipActivity, UploadLane,
        };

        let state = &server_state_for_events;

        fn plain_job_title(value: &str) -> String {
            let mut in_tag = false;
            let mut plain = String::with_capacity(value.len());
            for character in value.chars() {
                match character {
                    '<' => in_tag = true,
                    '>' => in_tag = false,
                    _ if !in_tag => plain.push(character),
                    _ => {}
                }
            }
            let normalized = plain.split_whitespace().collect::<Vec<_>>().join(" ");
            html::unescape(&normalized)
        }

        fn parse_job_title_and_artist(value: &str, is_album: bool) -> (String, Option<String>) {
            let plain = plain_job_title(value);
            let trimmed = plain.strip_prefix("Album: ").unwrap_or(&plain);
            if is_album || plain.starts_with("Album: ") {
                if let Some((album, artist)) = trimmed.split_once(" by ") {
                    return (album.trim().to_string(), Some(artist.trim().to_string()));
                }
                return (trimmed.trim().to_string(), None);
            }
            (plain, None)
        }

        let find_task = |job: &engine::orchestrator::types::ActiveRipTask,
                         register_if_missing: bool|
         -> Option<server::rip_tasks::ServerTaskMeta> {
            let mut tasks = state.active_tasks.write();
            let is_album = job.total_tracks > 1;
            let (parsed_title, parsed_artist) =
                parse_job_title_and_artist(&job.job_header, is_album);

            if let Some(task) = job
                .user_name
                .as_ref()
                .and_then(|uname| tasks.get_mut(uname))
            {
                if task.rip_task_id.is_empty() {
                    task.rip_task_id = job.id.clone();
                }
                if is_album {
                    task.is_album = true;
                }
                return Some(task.clone());
            }

            for task in tasks.values_mut() {
                if task.rip_task_id == job.id {
                    if task.task_id.starts_with("bot_") {
                        task.is_album = is_album;
                        task.title = Some(parsed_title.clone());
                        task.artist = parsed_artist
                            .clone()
                            .or_else(|| is_album.then(|| format!("{} tracks", job.total_tracks)));
                    }
                    return Some(task.clone());
                }
            }

            for task in tasks.values_mut() {
                if task.rip_task_id.is_empty()
                    && (task.owner_id == job.user_id || job.user_id == 0)
                    && task.provider == job.provider
                    && job.source_track_ids.iter().any(|id| id == &task.track_id)
                {
                    task.rip_task_id = job.id.clone();
                    if is_album {
                        task.is_album = true;
                    }
                    return Some(task.clone());
                }
            }

            if !register_if_missing {
                return None;
            }

            let task_id = format!("bot_{}", job.id);
            let meta = server::rip_tasks::ServerTaskMeta {
                task_id: task_id.clone(),
                rip_task_id: job.id.clone(),
                owner_id: job.user_id,
                provider: job.provider.clone(),
                track_id: job
                    .source_track_ids
                    .first()
                    .cloned()
                    .unwrap_or_else(|| job.id.clone()),
                codec: None,
                title: Some(parsed_title),
                artist: parsed_artist
                    .or_else(|| is_album.then(|| format!("{} tracks", job.total_tracks))),
                album: None,
                duration: None,
                artwork_url: None,
                controller: tokio_util::sync::CancellationToken::new(),
                created_at: std::time::Instant::now(),
                latest_progress: server::rip_tasks::RipTaskProgress {
                    job_stage: Some(server::rip_tasks::RipTaskJobStage::Queued),
                    download: None,
                    upload: None,
                    percent: Some(0.0),
                    current_track_title: None,
                    current_track_artist: None,
                    current_track_artwork_url: None,
                    current_track_index: None,
                    total_tracks: is_album.then_some(job.total_tracks as u32),
                    completed_tracks: is_album.then_some(0),
                    failed_tracks: is_album.then_some(0),
                },
                is_album,
            };
            tasks.insert(task_id.clone(), meta.clone());
            drop(tasks);
            let _ = state
                .task_sync_tx
                .send(server::rip_tasks::TaskSyncEvent::Updated { task_id });
            Some(meta)
        };

        match event {
            OrchestratorEvent::Created(job) => {
                if let Some(task) = find_task(job, true) {
                    let task_controller = task.controller.clone();
                    let job_controller = job.controller.clone();
                    if task_controller.is_cancelled() {
                        job_controller.cancel();
                    } else {
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = task_controller.cancelled() => {
                                    job_controller.cancel();
                                }
                                _ = job_controller.cancelled() => {
                                    task_controller.cancel();
                                }
                            }
                        });
                    }
                }
            }
            OrchestratorEvent::Progress(job, progress) => {
                if let Some(task) = find_task(job, true) {
                    let download_codec = progress.codec.clone().or_else(|| task.codec.clone());
                    let download = progress.download.as_ref().map(|download_lane| {
                        use server::rip_tasks::RipTaskDownloadStage as Stage;

                        match download_lane {
                            DownloadLane::Rip(activity) => match activity {
                                RipActivity::ResolvingMetadata => {
                                    server::rip_tasks::RipTaskDownloadLane {
                                        stage: Stage::ResolvingMetadata,
                                        title: None,
                                        artist: None,
                                        artwork_url: None,
                                        bytes_done: None,
                                        bytes_total: None,
                                        percent: None,
                                        codec: download_codec.clone(),
                                        track_index: None,
                                        total_tracks: None,
                                    }
                                }
                                RipActivity::Connecting { track } => {
                                    server::rip_tasks::RipTaskDownloadLane {
                                        stage: Stage::Connecting,
                                        title: Some(track.title.clone()),
                                        artist: Some(track.artist.clone()),
                                        artwork_url: track.artwork_url.clone(),
                                        bytes_done: None,
                                        bytes_total: None,
                                        percent: None,
                                        codec: download_codec.clone(),
                                        track_index: track.track_index,
                                        total_tracks: track.total_tracks,
                                    }
                                }
                                RipActivity::Downloading { track, progress } => {
                                    let (bytes_done, bytes_total, percent) =
                                        lane_byte_values(progress);
                                    server::rip_tasks::RipTaskDownloadLane {
                                        stage: Stage::Downloading,
                                        title: Some(track.title.clone()),
                                        artist: Some(track.artist.clone()),
                                        artwork_url: track.artwork_url.clone(),
                                        bytes_done,
                                        bytes_total,
                                        percent,
                                        codec: download_codec.clone(),
                                        track_index: track.track_index,
                                        total_tracks: track.total_tracks,
                                    }
                                }
                                RipActivity::MaterializingCachedMedia { track, progress } => {
                                    let (bytes_done, bytes_total, percent) =
                                        lane_byte_values(progress);
                                    server::rip_tasks::RipTaskDownloadLane {
                                        stage: Stage::MaterializingCachedMedia,
                                        title: Some(track.title.clone()),
                                        artist: Some(track.artist.clone()),
                                        artwork_url: track.artwork_url.clone(),
                                        bytes_done,
                                        bytes_total,
                                        percent,
                                        codec: download_codec.clone(),
                                        track_index: track.track_index,
                                        total_tracks: track.total_tracks,
                                    }
                                }
                                RipActivity::Decrypting { track } => {
                                    server::rip_tasks::RipTaskDownloadLane {
                                        stage: Stage::Decrypting,
                                        title: Some(track.title.clone()),
                                        artist: Some(track.artist.clone()),
                                        artwork_url: track.artwork_url.clone(),
                                        bytes_done: None,
                                        bytes_total: None,
                                        percent: None,
                                        codec: download_codec.clone(),
                                        track_index: track.track_index,
                                        total_tracks: track.total_tracks,
                                    }
                                }
                                RipActivity::Tagging { track } => {
                                    server::rip_tasks::RipTaskDownloadLane {
                                        stage: Stage::Tagging,
                                        title: Some(track.title.clone()),
                                        artist: Some(track.artist.clone()),
                                        artwork_url: track.artwork_url.clone(),
                                        bytes_done: None,
                                        bytes_total: None,
                                        percent: None,
                                        codec: download_codec.clone(),
                                        track_index: track.track_index,
                                        total_tracks: track.total_tracks,
                                    }
                                }
                            },
                            DownloadLane::CachedDelivery { track } => {
                                server::rip_tasks::RipTaskDownloadLane {
                                    stage: Stage::CachedDelivery,
                                    title: Some(track.title.clone()),
                                    artist: Some(track.artist.clone()),
                                    artwork_url: track.artwork_url.clone(),
                                    bytes_done: None,
                                    bytes_total: None,
                                    percent: None,
                                    codec: download_codec.clone(),
                                    track_index: track.track_index,
                                    total_tracks: track.total_tracks,
                                }
                            }
                        }
                    });

                    let upload = progress.upload.as_ref().map(|upload_lane| {
                        use server::rip_tasks::RipTaskUploadStage as Stage;

                        match upload_lane {
                            UploadLane::Track {
                                track,
                                progress: byte_progress,
                                ..
                            } => {
                                let (bytes_done, bytes_total, percent) =
                                    lane_byte_values(byte_progress);
                                server::rip_tasks::RipTaskUploadLane {
                                    stage: Stage::UploadingTrack,
                                    title: Some(track.title.clone()),
                                    artist: Some(track.artist.clone()),
                                    artwork_url: track.artwork_url.clone(),
                                    bytes_done,
                                    bytes_total,
                                    percent,
                                    codec: progress.codec.clone().or_else(|| task.codec.clone()),
                                    track_index: track.track_index,
                                    total_tracks: track.total_tracks,
                                }
                            }
                            UploadLane::ArchiveBuild {
                                archive,
                                progress: byte_progress,
                            } => {
                                let (bytes_done, bytes_total, percent) =
                                    lane_byte_values(byte_progress);
                                let archive_codec = extract_archive_codec(archive)
                                    .or_else(|| progress.codec.clone())
                                    .or_else(|| task.codec.clone());
                                server::rip_tasks::RipTaskUploadLane {
                                    stage: Stage::BuildingArchive,
                                    title: Some(archive.clone()),
                                    artist: None,
                                    artwork_url: task.artwork_url.clone(),
                                    bytes_done,
                                    bytes_total,
                                    percent,
                                    codec: archive_codec,
                                    track_index: None,
                                    total_tracks: None,
                                }
                            }
                            UploadLane::ArchiveUpload {
                                archive,
                                progress: byte_progress,
                            } => {
                                let (bytes_done, bytes_total, percent) =
                                    lane_byte_values(byte_progress);
                                let archive_codec = extract_archive_codec(archive)
                                    .or_else(|| progress.codec.clone())
                                    .or_else(|| task.codec.clone());
                                server::rip_tasks::RipTaskUploadLane {
                                    stage: Stage::UploadingArchive,
                                    title: Some(archive.clone()),
                                    artist: None,
                                    artwork_url: task.artwork_url.clone(),
                                    bytes_done,
                                    bytes_total,
                                    percent,
                                    codec: archive_codec,
                                    track_index: None,
                                    total_tracks: None,
                                }
                            }
                        }
                    });

                    let is_archive = upload.as_ref().is_some_and(|lane| {
                        matches!(
                            lane.stage,
                            server::rip_tasks::RipTaskUploadStage::BuildingArchive
                                | server::rip_tasks::RipTaskUploadStage::UploadingArchive
                        )
                    });
                    let lane_percent = upload
                        .as_ref()
                        .and_then(|lane| lane.percent)
                        .or_else(|| download.as_ref().and_then(|lane| lane.percent));
                    let overall_percent = if is_archive {
                        lane_percent
                    } else {
                        match (lane_percent, job.total_tracks) {
                            (Some(track_percent), total) if total > 0 => {
                                let finished =
                                    job.cached_count + job.ripped_count + job.failed_count;
                                Some(
                                    ((finished as f32 + track_percent / 100.0) / total as f32)
                                        * 100.0,
                                )
                            }
                            _ => lane_percent,
                        }
                    };

                    let (current_track_title, current_track_artist, current_track_artwork_url) =
                        if let Some(upload) = &upload {
                            (
                                upload.title.clone(),
                                upload.artist.clone(),
                                upload.artwork_url.clone(),
                            )
                        } else if let Some(download) = &download {
                            (
                                download.title.clone(),
                                download.artist.clone(),
                                download.artwork_url.clone(),
                            )
                        } else {
                            (None, None, None)
                        };

                    let is_album = job.total_tracks > 1;
                    let (current_track_index, total_tracks, completed_tracks, failed_tracks) =
                        if is_album {
                            let total = job.total_tracks as u32;
                            let failed = if job.failed_count > 0 {
                                Some(job.failed_count as u32)
                            } else {
                                None
                            };
                            if is_archive {
                                (None, Some(total), Some(total), failed)
                            } else {
                                let finished = (job.cached_count + job.ripped_count) as u32;
                                let current_idx = (finished + 1).min(total);
                                (Some(current_idx), Some(total), Some(finished), failed)
                            }
                        } else {
                            (None, None, None, None)
                        };

                    let next_progress = server::rip_tasks::RipTaskProgress {
                        job_stage: map_job_stage(progress.job_activity.as_ref()),
                        download,
                        upload,
                        percent: overall_percent,
                        current_track_title,
                        current_track_artist,
                        current_track_artwork_url,
                        current_track_index,
                        total_tracks,
                        completed_tracks,
                        failed_tracks,
                    };
                    let should_emit = progress_throttles
                        .lock()
                        .expect("progress throttle map poisoned")
                        .entry(task.task_id.clone())
                        .or_default()
                        .should_emit(Instant::now(), &next_progress);
                    if should_emit {
                        state.update_task_progress_extended(&task.task_id, next_progress);
                    }
                }
            }
            OrchestratorEvent::Completed(job, summary) => {
                if let Some(task) = find_task(job, false) {
                    if let Some(summary_codec) = &summary.codec {
                        let mut tasks = state.active_tasks.write();
                        if let Some(t) = tasks.get_mut(&task.task_id) {
                            t.codec = Some(summary_codec.clone());
                        }
                    }
                    progress_throttles
                        .lock()
                        .expect("progress throttle map poisoned")
                        .remove(&task.task_id);
                    if summary.failed_count > 0 || !summary.failed_tracks.is_empty() {
                        let error = summary
                            .failed_tracks
                            .first()
                            .map(|failed| failed.error.as_str())
                            .unwrap_or("One or more tracks failed");
                        state.fail_task(&task.task_id, error);
                    } else {
                        state.complete_task(&task.task_id);
                    }
                }
            }
            OrchestratorEvent::Cancelled(job, _by) => {
                if let Some(task) = find_task(job, false) {
                    progress_throttles
                        .lock()
                        .expect("progress throttle map poisoned")
                        .remove(&task.task_id);
                    state.cancel_task(&task.task_id, "Cancelled by user");
                }
            }
            OrchestratorEvent::Failed(job, err) => {
                if let Some(task) = find_task(job, false) {
                    progress_throttles
                        .lock()
                        .expect("progress throttle map poisoned")
                        .remove(&task.task_id);
                    state.fail_task(&task.task_id, err);
                }
            }
            _ => {}
        }
    }));

    let server_shutdown = tokio_util::sync::CancellationToken::new();
    let server_task = tokio::spawn({
        let server_shutdown = server_shutdown.clone();
        async move {
            if let Err(e) = server::run_server(server_config, server_state, server_shutdown).await {
                tracing::error!(error = %e, "Axum streaming server failed");
            }
        }
    });

    let state = Arc::new(BotState {
        client: client.clone(),
        auth,
        rip_deps,
        rip_orchestrator: orchestrator,
        admin_id: env.admin_id,
        bot_id: me.id,
        bot_username: me.username.clone(),
        dump_channel_id: env.dump_channel_id,
        dump_peer: PeerRef::from(env.dump_channel_id),
        stats: Some(db::StatsRepository::new(database.clone())),
        db_client: database.clone(),
        started_at: std::time::Instant::now(),
        stream_engine: Some(stream_engine),
        session_manager,
        app_key: app_key.clone(),
        tracks_repo: tracks_repo.clone(),
    });

    bot::event_bridge::start(Arc::clone(&state));
    let mut dispatcher = Dispatcher::new();
    handlers::register(&mut dispatcher, state);

    if let Err(error) = client
        .send_message(
            PeerRef::from(env.admin_id),
            InputMessage::html("<b>Peerless is up and alive.</b>\nStartup completed successfully."),
        )
        .await
    {
        tracing::warn!(error = %error, "startup notification to administrator failed");
    }

    let dispatcher = Arc::new(dispatcher);
    let update_slots = Arc::new(Semaphore::new(32));
    let mut updates = client.stream_updates();
    #[cfg(unix)]
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    #[cfg(unix)]
    let sigterm_signal = async { sigterm.recv().await };
    #[cfg(not(unix))]
    let sigterm_signal = std::future::pending::<Option<()>>();
    tokio::select! {
        _ = async {
            while let Some(update) = updates.next().await {
                let dispatcher = Arc::clone(&dispatcher);
                let Ok(slot) = Arc::clone(&update_slots).acquire_owned().await else {
                    break;
                };
                tokio::spawn(async move {
                    dispatcher.dispatch(update).await;
                    drop(slot);
                });
            }
        } => {},
        _ = signal::ctrl_c() => {},
        _ = sigterm_signal => {},
        _ = shutdown.cancelled() => {},
    }
    info!("Shutting down bot...");
    shutdown.cancel();
    server_shutdown.cancel();
    let _ = server_task.await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use server::rip_tasks::{RipTaskDownloadLane, RipTaskDownloadStage, RipTaskProgress};

    use super::*;

    fn progress(stage: RipTaskDownloadStage, bytes_done: u64) -> RipTaskProgress {
        RipTaskProgress {
            job_stage: None,
            download: Some(RipTaskDownloadLane {
                stage,
                title: Some("Track".to_owned()),
                artist: Some("Artist".to_owned()),
                artwork_url: None,
                bytes_done: Some(bytes_done),
                bytes_total: Some(100),
                percent: Some(bytes_done as f32),
                codec: None,
                track_index: None,
                total_tracks: None,
            }),
            upload: None,
            percent: Some(bytes_done as f32),
            current_track_title: Some("Track".to_owned()),
            current_track_artist: Some("Artist".to_owned()),
            current_track_artwork_url: None,
            current_track_index: Some(1),
            total_tracks: Some(1),
            completed_tracks: Some(0),
            failed_tracks: None,
        }
    }

    #[test]
    fn unknown_byte_total_keeps_completed_bytes_and_null_percent() {
        let (bytes_done, bytes_total, percent) =
            lane_byte_values(&engine::orchestrator::types::ByteProgress {
                completed: 512,
                total: None,
            });

        assert_eq!(bytes_done, Some(512));
        assert_eq!(bytes_total, None);
        assert_eq!(percent, None);
    }

    #[test]
    fn progress_throttle_emits_at_the_250ms_boundary_and_on_phase_changes() {
        let started = Instant::now();
        let mut throttle = ProgressThrottle::default();
        assert!(throttle.should_emit(started, &progress(RipTaskDownloadStage::Downloading, 1)));
        assert!(!throttle.should_emit(
            started + Duration::from_millis(249),
            &progress(RipTaskDownloadStage::Downloading, 2)
        ));
        assert!(throttle.should_emit(
            started + Duration::from_millis(250),
            &progress(RipTaskDownloadStage::Downloading, 2)
        ));
        assert!(!throttle.should_emit(
            started + Duration::from_millis(500),
            &progress(RipTaskDownloadStage::Downloading, 2)
        ));
        assert!(throttle.should_emit(
            started + Duration::from_millis(501),
            &progress(RipTaskDownloadStage::Decrypting, 0)
        ));
    }
}
