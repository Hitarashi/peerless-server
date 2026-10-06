pub mod auth;
pub mod docs;
pub mod error;
pub mod gateway;
pub mod health;
pub mod integrations;
pub mod lookup;
pub mod playback_sync;
pub mod probe;
pub mod rip_task_rpc;
pub mod rip_tasks;
pub mod streaming;

use std::{collections::HashMap, net::IpAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    routing::{get, post},
};
pub use error::ServerError;
use moka::future::Cache;
use tokio::{net::TcpListener, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: IpAddr,
    pub port: u16,

    pub app_key: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: [0, 0, 0, 0].into(),
            port: 4444,
            app_key: String::new(),
        }
    }
}

pub type RipTaskRunner = Arc<
    dyn Fn(
            Arc<ServerState>,
            String,
            music::Provider,
            String,
            Option<music::Codec>,
            i64,
            tokio_util::sync::CancellationToken,
        ) -> tokio::task::JoinHandle<()>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct ServerState {
    pub db: db::DbPool,
    pub stream_engine: Arc<stream::StreamEngine>,
    pub session_mgr: Arc<db::SessionManager>,
    pub tracks_repo: Arc<db::TracksRepository>,
    pub settings_store: Arc<db::SettingsStore>,
    pub rip_orchestrator: Arc<engine::orchestrator::RipOrchestrator>,
    pub token_cache: Arc<Cache<String, auth::AuthedUser>>,
    pub telegram_client: Option<ferogram::Client>,
    pub avatar_cache: Arc<Cache<i64, Option<Arc<Vec<u8>>>>>,
    pub task_sync_tx: broadcast::Sender<rip_tasks::TaskSyncEvent>,
    pub active_tasks: Arc<parking_lot::RwLock<HashMap<String, rip_tasks::ServerTaskMeta>>>,
    pub admin_id: i64,
    pub sync_hub: playback_sync::PlaybackSyncHub,
    pub http_client: reqwest::Client,
    pub rip_task_runner: RipTaskRunner,
    pub app_key: String,
    pub started_at: std::time::Instant,
}

impl ServerState {
    pub fn new(
        stream_engine: Arc<stream::StreamEngine>,
        session_mgr: Arc<db::SessionManager>,
        tracks_repo: Arc<db::TracksRepository>,
        settings_store: Arc<db::SettingsStore>,
        rip_orchestrator: Arc<engine::orchestrator::RipOrchestrator>,
        app_key: String,
    ) -> Self {
        let (task_sync_tx, _) = broadcast::channel(256);
        let active_tasks = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let admin_id = std::env::var("ADMIN_ID")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let token_cache = Arc::new(
            Cache::builder()
                .max_capacity(5000)
                .time_to_live(Duration::from_secs(60))
                .build(),
        );
        let avatar_cache = Arc::new(
            Cache::builder()
                .max_capacity(500)
                .time_to_live(Duration::from_secs(15 * 60))
                .build(),
        );
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let rip_task_runner: RipTaskRunner = Arc::new(
            |_state, _task_id, _provider, _track_id, _codec, _user_id, _controller| {
                tokio::spawn(async move {})
            },
        );
        let db = tracks_repo.pool().clone();

        Self {
            db,
            stream_engine,
            session_mgr,
            tracks_repo,
            settings_store,
            rip_orchestrator,
            token_cache,
            telegram_client: None,
            avatar_cache,
            task_sync_tx,
            active_tasks,
            admin_id,
            sync_hub: playback_sync::PlaybackSyncHub::default(),
            http_client,
            rip_task_runner,
            app_key,
            started_at: std::time::Instant::now(),
        }
    }

    pub fn with_admin_id(mut self, admin_id: i64) -> Self {
        self.admin_id = admin_id;
        self
    }

    pub fn with_telegram_client(mut self, telegram_client: ferogram::Client) -> Self {
        self.telegram_client = Some(telegram_client);
        self
    }

    pub fn with_rip_task_runner(mut self, rip_task_runner: RipTaskRunner) -> Self {
        self.rip_task_runner = rip_task_runner;
        self
    }

    fn remove_active_task(&self, task_id: &str) -> Option<rip_tasks::ServerTaskMeta> {
        let task = self.active_tasks.write().remove(task_id)?;
        let _ = self.task_sync_tx.send(rip_tasks::TaskSyncEvent::Dismissed {
            task_id: task_id.to_string(),
        });
        Some(task)
    }

    pub fn complete_task(&self, task_id: &str) {
        let _ = self.remove_active_task(task_id);
    }

    pub fn fail_task(&self, task_id: &str, _error: &str) {
        let _ = self.remove_active_task(task_id);
    }

    pub fn cancel_task(&self, task_id: &str, _reason: &str) {
        let Some(task) = self.remove_active_task(task_id) else {
            return;
        };
        task.controller.cancel();
        if !task.rip_task_id.is_empty() {
            self.rip_orchestrator
                .cancel_task(&task.rip_task_id, Some("user"));
        }
    }

    pub fn update_task_progress(&self, task_id: &str, progress: rip_tasks::RipTaskProgress) {
        self.update_task_progress_extended(task_id, progress);
    }

    pub fn update_task_progress_extended(
        &self,
        task_id: &str,
        mut progress: rip_tasks::RipTaskProgress,
    ) {
        let mut tasks = self.active_tasks.write();
        let Some(task) = tasks.get_mut(task_id) else {
            return;
        };
        let previous = &task.latest_progress;
        let is_archive_stage = progress.upload.as_ref().is_some_and(|lane| {
            matches!(
                lane.stage,
                rip_tasks::RipTaskUploadStage::BuildingArchive
                    | rip_tasks::RipTaskUploadStage::UploadingArchive
            )
        });
        progress.total_tracks = progress.total_tracks.or(previous.total_tracks);
        if is_archive_stage {
            if progress.current_track_title.is_none() {
                progress.current_track_title = Some("Album ZIP archive".to_owned());
            }
            progress.current_track_artist = None;
            progress.current_track_index = None;
            progress.completed_tracks = progress.total_tracks;
        } else {
            progress.current_track_title = progress
                .current_track_title
                .or_else(|| previous.current_track_title.clone());
            progress.current_track_artist = progress
                .current_track_artist
                .or_else(|| previous.current_track_artist.clone());
            progress.current_track_index = progress
                .current_track_index
                .or(previous.current_track_index);
            progress.completed_tracks = progress.completed_tracks.or(previous.completed_tracks);
        }
        progress.current_track_artwork_url = progress
            .current_track_artwork_url
            .or_else(|| previous.current_track_artwork_url.clone());
        task.artwork_url = progress
            .current_track_artwork_url
            .clone()
            .or_else(|| task.artwork_url.clone());
        task.latest_progress = progress;
        drop(tasks);
        let _ = self.task_sync_tx.send(rip_tasks::TaskSyncEvent::Updated {
            task_id: task_id.to_string(),
        });
    }
}

pub fn create_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/api/v1/auth/exchange", post(auth::exchange))
        .route("/api/v1/auth/refresh", post(auth::refresh))
        .route("/api/v1/auth/logout", post(auth::logout))
        .route("/api/v1/auth/me", get(auth::me))
        .route("/api/v1/auth/me/avatar", get(auth::me_avatar))
        .route(
            "/api/v1/tracks/{id}/playback",
            get(streaming::issue_playback_ticket),
        )
        .route("/api/v1/tracks/{id}/stream", get(streaming::stream_handler))
        .route("/api/v1/ws/sync", get(playback_sync::ws_handler))
        .route("/api/v1/lookup", post(lookup::lookup))
        .nest("/api/v1/integrations/lastfm", integrations::lastfm_router())
        .nest(
            "/api/v1/integrations/listenbrainz",
            integrations::listenbrainz_router(),
        )
        .route("/api/v1/docs", get(docs::scalar_html))
        .route("/api/v1/docs.json", get(docs::openapi_json))
        .route("/api/v1/docs.yaml", get(docs::openapi_yaml))
        .route("/api/v1/docs-ws.json", get(docs::asyncapi_json))
        .route("/open", get(gateway::open_gateway))
        .route("/api/v1/health", get(health::health_check))
        .route("/api/v1/status", get(health::status_report))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn run_server(
    config: ServerConfig,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) -> Result<(), ServerError> {
    if config.app_key.is_empty() {
        return Err(ServerError::Internal(
            "ServerConfig.app_key is required; set the APP_KEY environment variable".to_string(),
        ));
    }
    let addr = (config.host, config.port);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to bind to {addr:?}: {e}")))?;

    tracing::info!(host = %config.host, port = config.port, "Axum HTTP streaming server listening");

    let router = create_router(state);

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            tracing::info!("Axum server shutting down gracefully");
        })
        .await
        .map_err(|e| ServerError::Internal(format!("Server error: {e}")))?;

    Ok(())
}
