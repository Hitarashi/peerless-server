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
    let rip_task_runner: server::RipTaskRunner = Arc::new(
        move |task_id, provider, track_id, codec, user_id| {
            let orchestrator = orchestrator_for_tasks.clone();
            let rip_deps = rip_deps_for_tasks.clone();
            tokio::spawn(async move {
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
                let options = engine::orchestrator::types::RipJobOptions {
                    provider,
                    chat_id: 0,
                    user_id,
                    user_name: None,
                    delivery_chat_id: 0,
                    is_group: false,
                    is_force: false,
                    is_cache_only: false,
                    single_storefront: Some("us".to_string()),
                    parsed_items: vec![item],
                    reply_to_message_id: None,
                    status_msg_id: 0,
                    is_admin: false,
                    codec_preference,
                    rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
                };

                tracing::info!(task_id = %task_id, "Executing background rip task via RipOrchestrator");
                if let Err(e) = orchestrator.start_job(rip_deps, &options).await {
                    tracing::warn!(task_id = %task_id, error = %e, "Background rip task failed");
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
        .with_catalog_service(apple_catalog)
        .with_rip_task_runner(rip_task_runner),
    );

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
