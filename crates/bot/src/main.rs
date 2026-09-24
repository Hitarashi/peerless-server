use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use bot::{BotState, handlers};
use ferogram::{Client, InputMessage, PeerRef, filters::Dispatcher};
use tokio::{signal, sync::Semaphore};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
struct Env {
    api_id: i32,
    api_hash: String,
    bot_token: String,
    admin_id: i64,
    dump_channel_id: i64,
    database_url: String,
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
    // Precedence: LOG_LEVEL || RUST_LOG || 'info'.
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
    Ok(Env {
        api_id,
        api_hash,
        bot_token,
        admin_id,
        dump_channel_id,
        database_url,
        log_level,
        stream_worker_bot_tokens,
    })
}

fn init_tracing(log_level: &str) {
    // Our crates honor LOG_LEVEL (default info); external crates are pinned
    // to warn so their internal chatter (ferogram session/connection logs,
    // etc.) stays quiet unless something is actually wrong.
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
        // The builder takes ownership of the hash (the source API does not
        // implement Into<String> for &String), so clone this small value.
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
    client
        .bot_sign_in(&env.bot_token)
        .await
        .context("sign in bot")?;
    client
        .save_session()
        .await
        .context("save Telegram session")?;
    let me = client.get_me().await.context("get bot identity")?;
    info!(username = ?me.username, bot_id = me.id, dump_channel = env.dump_channel_id, "Bot started successfully");

    let rip_deps = Arc::new(
        bot::rip_deps::RipDeps::new(
            Arc::new(client.clone()),
            PeerRef::from(env.dump_channel_id),
            db::TracksRepository::new(database.clone()),
            db::RequestLogRepository::new(database.clone()),
            db::SettingsStore::new(database.clone()),
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
    let library_manager = Arc::new(db::LibraryManager::new(database.clone()));
    let tracks_repo = Arc::new(db::TracksRepository::new(database.clone()));
    let settings_store = Arc::new(db::SettingsStore::new(database.clone()));
    let app_key =
        std::env::var("APP_KEY").unwrap_or_else(|_| "IQfVm8yrIR83zlWvEZ5Fr9fpN6lGgWhV".to_string());

    let initial_settings = settings_store.get_settings();
    let port = std::env::var("STREAM_SERVER_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(initial_settings.stream_server_port);

    let server_config = server::ServerConfig {
        host: [0, 0, 0, 0].into(),
        port,
        app_key: app_key.clone(),
        cors_origins: vec!["*".to_string()],
    };

    let apple_catalog = Arc::new(apple::Catalog::new(apple::ReqwestTransport::new()));
    let orchestrator_for_tasks = orchestrator.clone();
    let rip_deps_for_tasks = rip_deps.clone();
    let admin_id = env.admin_id;
    let rip_task_runner: server::RipTaskRunner = Arc::new(
        move |state, task_id, provider, track_id, codec, user_id, controller| {
            let orchestrator = orchestrator_for_tasks.clone();
            let rip_deps = rip_deps_for_tasks.clone();
            tokio::spawn(async move {
                if controller.is_cancelled() {
                    return;
                }
                let item = engine::types::ParsedTargetItem {
                    id: track_id.clone(),
                    kind: music::TargetKind::Track,
                    storefront: Some("us".to_string()),
                };
                let codec_preference = codec.map(|c| match c {
                    music::Codec::Alac | music::Codec::Flac => {
                        music::CodecPreference::HighestQuality
                    }
                    music::Codec::Aac => music::CodecPreference::LosslessCd,
                    _ => music::CodecPreference::HighestQuality,
                });
                let is_admin = user_id == admin_id;
                let options = engine::orchestrator::types::RipJobOptions {
                    provider,
                    chat_id: 0,
                    user_id,
                    user_name: Some(task_id.clone()),
                    delivery_chat_id: 0,
                    is_group: false,
                    is_force: false,
                    is_cache_only: true,
                    single_storefront: Some("us".to_string()),
                    parsed_items: vec![item],
                    reply_to_message_id: None,
                    status_msg_id: 0,
                    is_admin,
                    codec_preference,
                    rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
                };

                tracing::info!(task_id = %task_id, "Executing background rip task via RipOrchestrator");
                if let Err(e) = orchestrator.start_job(rip_deps, &options).await {
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
            library_manager,
            tracks_repo.clone(),
            settings_store,
            orchestrator.clone(),
            app_key.clone(),
        )
        .with_admin_id(env.admin_id)
        .with_catalog_service(apple_catalog)
        .with_telegram_client(client.clone())
        .with_rip_task_runner(rip_task_runner),
    );

    // Mirror orchestrator activity into the server-owned live task feed.
    let server_state_for_events = Arc::clone(&server_state);
    orchestrator.subscribe(Arc::new(move |event| {
        use engine::orchestrator::types::{
            DownloadLane, JobActivity, OrchestratorEvent, RipActivity, UploadLane,
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
            plain.split_whitespace().collect::<Vec<_>>().join(" ")
        }

        fn parse_job_title_and_artist(value: &str, is_album: bool) -> (String, Option<String>) {
            let plain = plain_job_title(value);
            if is_album {
                let trimmed = plain.strip_prefix("Album: ").unwrap_or(&plain);
                if let Some((album, artist)) = trimmed.split_once(" by ") {
                    return (album.trim().to_string(), Some(artist.trim().to_string()));
                }
                return (trimmed.trim().to_string(), None);
            }
            (plain, None)
        }

        // Match app-created tasks first. Ordinary bot jobs get a server-owned
        // task record too, so every client sees the same active job feed.
        let find_task = |job: &engine::orchestrator::types::ActiveRipJob,
                         register_if_missing: bool|
         -> Option<server::tasks::ServerTaskMeta> {
            let mut tasks = state.active_tasks.write();
            let is_album = job.total_tracks > 1;
            let (parsed_title, parsed_artist) =
                parse_job_title_and_artist(&job.job_header, is_album);

            // App rip tasks put their server task ID in user_name.
            if let Some(task) = job
                .user_name
                .as_ref()
                .and_then(|uname| tasks.get_mut(uname))
            {
                if task.job_id.is_none() {
                    task.job_id = Some(job.id.clone());
                }
                if is_album {
                    task.is_album = true;
                }
                return Some(task.clone());
            }
            // Repeated orchestrator events resolve through the assigned job ID.
            for task in tasks.values_mut() {
                if task.job_id.as_deref() == Some(&job.id) {
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
            // Recover a matching app task if its Created event raced the
            // orchestrator event bridge.
            for task in tasks.values_mut() {
                if task.job_id.is_none()
                    && (task.owner_id == job.user_id || job.user_id == 0)
                    && task.provider == job.provider
                    && job.source_track_ids.iter().any(|id| id == &task.track_id)
                {
                    task.job_id = Some(job.id.clone());
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
            let meta = server::tasks::ServerTaskMeta {
                task_id: task_id.clone(),
                job_id: Some(job.id.clone()),
                owner_id: job.user_id,
                provider: job.provider,
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
                controller: tokio_util::sync::CancellationToken::new(),
                created_at: std::time::Instant::now(),
                latest_progress: server::tasks::RipTaskProgress {
                    stage: "queued".to_string(),
                    percent: Some(0.0),
                    speed: None,
                    current_track_title: None,
                    current_track_artist: None,
                    current_track_index: None,
                    total_tracks: is_album.then_some(job.total_tracks as u32),
                    completed_tracks: is_album.then_some(0),
                },
                is_album,
            };
            tasks.insert(task_id.clone(), meta.clone());
            drop(tasks);
            let _ = state
                .task_sync_tx
                .send(server::tasks::TaskSyncEvent::Updated { task_id });
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
                    let mut stage = None;
                    let mut percent = None;
                    let mut current_track_title = None;
                    let mut current_track_artist = None;

                    let mut speed_info = None;
                    let mut is_archive = false;

                    if let Some(upload_lane) = &progress.upload {
                        match upload_lane {
                            UploadLane::Track {
                                track,
                                progress: byte_p,
                                ..
                            } => {
                                stage = Some("uploading_telegram");
                                percent = byte_p
                                    .total
                                    .map(|tot| (byte_p.completed as f32 / tot as f32) * 100.0);
                                current_track_title = Some(track.title.clone());
                                current_track_artist = Some(track.artist.clone());
                                if let Some(tot) = byte_p.total {
                                    if tot > 0 {
                                        speed_info = Some(format!(
                                            "{:.1}/{:.1} MB",
                                            byte_p.completed as f64 / 1_048_576.0,
                                            tot as f64 / 1_048_576.0
                                        ));
                                    }
                                }
                            }
                            UploadLane::ArchiveBuild { progress: byte_p, .. } => {
                                stage = Some("packaging_zip");
                                percent = byte_p
                                    .total
                                    .map(|tot| (byte_p.completed as f32 / tot as f32) * 100.0);
                                current_track_title = Some("Album ZIP archive".to_string());
                                current_track_artist = None;
                                is_archive = true;
                                if let Some(tot) = byte_p.total {
                                    if tot > 0 {
                                        speed_info = Some(format!(
                                            "{:.1}/{:.1} MB",
                                            byte_p.completed as f64 / 1_048_576.0,
                                            tot as f64 / 1_048_576.0
                                        ));
                                    }
                                }
                            }
                            UploadLane::ArchiveUpload { progress: byte_p, .. } => {
                                stage = Some("uploading_zip");
                                percent = byte_p
                                    .total
                                    .map(|tot| (byte_p.completed as f32 / tot as f32) * 100.0);
                                current_track_title = Some("Album ZIP archive".to_string());
                                current_track_artist = None;
                                is_archive = true;
                                if let Some(tot) = byte_p.total {
                                    if tot > 0 {
                                        speed_info = Some(format!(
                                            "{:.1}/{:.1} MB",
                                            byte_p.completed as f64 / 1_048_576.0,
                                            tot as f64 / 1_048_576.0
                                        ));
                                    }
                                }
                            }
                        }
                    } else if let Some(DownloadLane::Rip(rip_activity)) = &progress.download {
                        match rip_activity {
                            RipActivity::Downloading {
                                track,
                                progress: byte_p,
                            } => {
                                stage = Some("downloading");
                                percent = byte_p
                                    .total
                                    .map(|tot| (byte_p.completed as f32 / tot as f32) * 100.0);
                                current_track_title = Some(track.title.clone());
                                current_track_artist = Some(track.artist.clone());
                            }
                            RipActivity::Decrypting { track } => {
                                stage = Some("decrypting");
                                current_track_title = Some(track.title.clone());
                                current_track_artist = Some(track.artist.clone());
                            }
                            RipActivity::Tagging { track } => {
                                stage = Some("tagging");
                                current_track_title = Some(track.title.clone());
                                current_track_artist = Some(track.artist.clone());
                            }
                            RipActivity::Connecting { track } => {
                                stage = Some("connecting");
                                current_track_title = Some(track.title.clone());
                                current_track_artist = Some(track.artist.clone());
                            }
                            RipActivity::ResolvingMetadata => {
                                stage = Some("resolving");
                            }
                        }
                    } else if let Some(DownloadLane::CachedDelivery { track }) = &progress.download
                    {
                        stage = Some("cached_delivery");
                        current_track_title = Some(track.title.clone());
                        current_track_artist = Some(track.artist.clone());
                    }

                    if stage.is_none()
                        && let Some(job_act) = &progress.job_activity
                    {
                        match job_act {
                            JobActivity::Resolving => stage = Some("resolving"),
                            JobActivity::CheckingCache { .. } => stage = Some("checking_cache"),
                            JobActivity::Queued { .. } => stage = Some("queued"),
                            _ => {}
                        }
                    }

                    if let Some(stg) = stage {
                        let is_album = job.total_tracks > 1;

                        let overall_percent = if is_archive {
                            percent
                        } else {
                            match (percent, job.total_tracks) {
                                (Some(track_percent), total) if total > 0 => {
                                    let finished =
                                        job.cached_count + job.ripped_count + job.failed_count;
                                    Some(
                                        ((finished as f32 + track_percent / 100.0) / total as f32)
                                            * 100.0,
                                    )
                                }
                                _ => percent,
                            }
                        };

                        let (current_track_index, total_tracks, completed_tracks) = if is_album {
                            let total = job.total_tracks as u32;
                            if is_archive {
                                (None, Some(total), Some(total))
                            } else {
                                let finished = (job.cached_count + job.ripped_count) as u32;
                                let current_idx = (finished + 1).min(total);
                                (Some(current_idx), Some(total), Some(finished))
                            }
                        } else {
                            (None, None, None)
                        };

                        state.update_task_progress_extended(
                            &task.task_id,
                            stg,
                            overall_percent,
                            speed_info,
                            current_track_title,
                            current_track_artist,
                            current_track_index,
                            total_tracks,
                            completed_tracks,
                        );
                    }
                }
            }
            OrchestratorEvent::Completed(job, summary) => {
                if let Some(task) = find_task(job, false) {
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
                    state.cancel_task(&task.task_id, "Cancelled by user");
                }
            }
            OrchestratorEvent::Failed(job, err) => {
                if let Some(task) = find_task(job, false) {
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

    // Bridge subscribes once; its consumer renders status messages + dashboard.
    bot::event_bridge::start(Arc::clone(&state));
    // 24h auto-dump scheduler .
    tokio::spawn(bot::handlers::dump::scheduler_loop(Arc::clone(&state)));
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
