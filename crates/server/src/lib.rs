//! Axum HTTP streaming server and REST API.
//!
//! Provides a deep module interface (`run_server`, `create_router`, `ServerState`, `ServerConfig`)
//! encapsulating all routing, middleware, range streaming, and authentication.

pub mod assets;
pub mod auth;
pub mod catalog;
pub mod docs;
pub mod error;
pub mod gateway;
pub mod health;
pub mod integrations;
pub mod library;
pub mod playback_sync;
pub mod streaming;
pub mod tasks;

use std::{collections::HashMap, net::IpAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    routing::{delete, get, post},
};
pub use error::ServerError;
use moka::future::Cache;
use tokio::{net::TcpListener, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: IpAddr,
    pub port: u16,
    pub app_key: String,
    pub cors_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: [0, 0, 0, 0].into(),
            port: 4444,
            app_key: "default_dev_key_change_in_production".to_string(),
            cors_origins: vec!["*".to_string()],
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
    pub library_mgr: Arc<db::LibraryManager>,
    pub tracks_repo: Arc<db::TracksRepository>,
    pub settings_store: Arc<db::SettingsStore>,
    pub rip_orchestrator: Arc<engine::orchestrator::RipOrchestrator>,
    pub token_cache: Arc<Cache<String, auth::AuthedUser>>,
    pub telegram_client: Option<ferogram::Client>,
    pub avatar_cache: Arc<Cache<i64, Option<Arc<Vec<u8>>>>>,
    pub task_sync_tx: broadcast::Sender<tasks::TaskSyncEvent>,
    pub active_tasks: Arc<parking_lot::RwLock<HashMap<String, tasks::ServerTaskMeta>>>,
    pub admin_id: i64,
    pub sync_hub: playback_sync::PlaybackSyncHub,
    pub http_client: reqwest::Client,
    pub catalog_service: Option<apple::SharedCatalog>,
    pub rip_task_runner: RipTaskRunner,
    pub app_key: String,
    pub cors_origins: Vec<String>,
    pub started_at: std::time::Instant,
}

impl ServerState {
    pub fn new(
        stream_engine: Arc<stream::StreamEngine>,
        session_mgr: Arc<db::SessionManager>,
        library_mgr: Arc<db::LibraryManager>,
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
            library_mgr,
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
            catalog_service: None,
            rip_task_runner,
            app_key,
            cors_origins: vec!["*".to_string()],
            started_at: std::time::Instant::now(),
        }
    }

    pub fn with_admin_id(mut self, admin_id: i64) -> Self {
        self.admin_id = admin_id;
        self
    }

    pub fn with_catalog_service(mut self, catalog_service: apple::SharedCatalog) -> Self {
        self.catalog_service = Some(catalog_service);
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

    pub fn with_cors_origins(mut self, cors_origins: Vec<String>) -> Self {
        self.cors_origins = cors_origins;
        self
    }

    fn remove_active_task(&self, task_id: &str) -> Option<tasks::ServerTaskMeta> {
        let task = self.active_tasks.write().remove(task_id)?;
        let _ = self.task_sync_tx.send(tasks::TaskSyncEvent::Dismissed {
            task_id: task_id.to_string(),
        });
        Some(task)
    }

    /// Marks a task as successfully completed.
    pub fn complete_task(&self, task_id: &str) {
        let _ = self.remove_active_task(task_id);
    }

    /// Marks a task as failed with an error description.
    pub fn fail_task(&self, task_id: &str, _error: &str) {
        let _ = self.remove_active_task(task_id);
    }

    /// Cancels an in-flight task and notifies all listeners.
    pub fn cancel_task(&self, task_id: &str, _reason: &str) {
        let Some(task) = self.remove_active_task(task_id) else {
            return;
        };
        task.controller.cancel();
        if let Some(job_id) = task.job_id {
            self.rip_orchestrator.cancel_job(&job_id, Some("user"));
        }
    }

    /// Updates current progress for an active task and broadcasts it over the playback WebSocket.
    pub fn update_task_progress(
        &self,
        task_id: &str,
        stage: &str,
        percent: Option<f32>,
        speed: Option<String>,
    ) {
        self.update_task_progress_extended(
            task_id, stage, percent, speed, None, None, None, None, None,
        );
    }

    /// Extended task progress update including album / multi-track context.
    #[allow(clippy::too_many_arguments)]
    pub fn update_task_progress_extended(
        &self,
        task_id: &str,
        stage: &str,
        percent: Option<f32>,
        speed: Option<String>,
        current_track_title: Option<String>,
        current_track_artist: Option<String>,
        current_track_index: Option<u32>,
        total_tracks: Option<u32>,
        completed_tracks: Option<u32>,
    ) {
        let mut tasks = self.active_tasks.write();
        let Some(task) = tasks.get_mut(task_id) else {
            return;
        };
        let is_archive_stage = stage == "packaging_zip" || stage == "uploading_zip";
        let progress = tasks::RipTaskProgress {
            stage: stage.to_string(),
            percent,
            speed: speed.or_else(|| task.latest_progress.speed.clone()),
            current_track_title: if current_track_title.is_some() {
                current_track_title
            } else if is_archive_stage {
                Some("Album ZIP archive".to_string())
            } else {
                task.latest_progress.current_track_title.clone()
            },
            current_track_artist: if current_track_artist.is_some() {
                current_track_artist
            } else if is_archive_stage {
                None
            } else {
                task.latest_progress.current_track_artist.clone()
            },
            current_track_index: if is_archive_stage {
                None
            } else {
                current_track_index.or(task.latest_progress.current_track_index)
            },
            total_tracks: total_tracks.or(task.latest_progress.total_tracks),
            completed_tracks: if is_archive_stage {
                total_tracks.or(task.latest_progress.total_tracks)
            } else {
                completed_tracks.or(task.latest_progress.completed_tracks)
            },
        };
        task.latest_progress = progress;
        drop(tasks);
        let _ = self.task_sync_tx.send(tasks::TaskSyncEvent::Updated {
            task_id: task_id.to_string(),
        });
    }
}

pub fn create_router(state: Arc<ServerState>) -> Router {
    let cors = if state.cors_origins.iter().any(|o| o == "*") {
        CorsLayer::permissive()
    } else {
        use tower_http::cors::Any;
        let origins: Vec<axum::http::HeaderValue> = state
            .cors_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    Router::new()
        // Auth
        .route("/api/v1/auth/exchange", post(auth::exchange))
        .route("/api/v1/auth/refresh", post(auth::refresh))
        .route("/api/v1/auth/logout", post(auth::logout))
        .route("/api/v1/auth/me", get(auth::me))
        .route("/api/v1/auth/me/avatar", get(auth::me_avatar))
        // Streaming & Playback
        .route(
            "/api/v1/tracks/{id}/playback",
            get(streaming::get_playback_info).post(streaming::get_playback_info),
        )
        .route(
            "/api/v1/stream",
            get(streaming::stream_handler).head(streaming::stream_handler),
        )
        .route("/api/v1/ws/playback", get(playback_sync::ws_handler))
        // Catalog & Search
        .route("/api/v1/search", get(catalog::search_catalog))
        .route("/api/v1/tracks/{id}", get(catalog::get_track))
        .route("/api/v1/albums", get(catalog::list_albums))
        .route("/api/v1/albums/{id}", get(catalog::get_album_tracks))
        .route(
            "/api/v1/artists/{name}/tracks",
            get(catalog::get_artist_tracks),
        )
        // Rip tasks
        .route("/api/v1/tasks", get(tasks::list_rip_tasks))
        .route("/api/v1/tasks/rip", post(tasks::create_rip_task))
        .route("/api/v1/tasks/{id}", delete(tasks::cancel_rip_task))
        // Assets
        .route(
            "/api/v1/assets/tracks/{id}/artwork",
            get(assets::get_artwork),
        )
        .route(
            "/api/v1/assets/providers/{provider}/tracks/{track_id}/artwork",
            get(assets::get_provider_artwork),
        )
        .route("/api/v1/assets/tracks/{id}/lyrics", get(assets::get_lyrics))
        // Library
        .route("/api/v1/me/favorites", get(library::list_favorites))
        .route(
            "/api/v1/me/favorites/{track_id}",
            post(library::add_favorite).delete(library::remove_favorite),
        )
        .route(
            "/api/v1/me/playlists",
            get(library::list_playlists).post(library::create_playlist),
        )
        .route(
            "/api/v1/me/playlists/{id}",
            get(library::get_playlist)
                .put(library::update_playlist)
                .delete(library::delete_playlist),
        )
        // Integrations
        .nest("/api/v1/integrations/lastfm", integrations::router())
        // Scalar UI & OpenAPI Docs
        .route("/api/v1/docs", get(docs::scalar_html))
        .route("/api/v1/docs.json", get(docs::openapi_json))
        .route("/api/v1/docs.yaml", get(docs::openapi_yaml))
        .route("/api/v1/docs-ws.json", get(docs::asyncapi_json))
        // Intent Gateway
        .route("/open", get(gateway::open_gateway))
        // Health & Telemetry
        .route("/api/v1/health", get(health::health_check))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn run_server(
    config: ServerConfig,
    state: Arc<ServerState>,
    shutdown: CancellationToken,
) -> Result<(), ServerError> {
    let state = if state.cors_origins == vec!["*".to_string()]
        && config.cors_origins != vec!["*".to_string()]
    {
        let mut s = (*state).clone();
        s.cors_origins = config.cors_origins;
        Arc::new(s)
    } else {
        state
    };
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
