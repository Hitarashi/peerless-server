//! Axum HTTP streaming server and REST/SSE API.
//!
//! Provides a deep module interface (`run_server`, `create_router`, `ServerState`, `ServerConfig`)
//! encapsulating all routing, middleware, range streaming, authentication, and SSE adapters.

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

use std::{net::IpAddr, sync::Arc, time::Duration};

use axum::{
    routing::{get, post},
    Router,
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
            String,
            music::Provider,
            String,
            Option<music::Codec>,
            i64,
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
    pub tasks_tx: broadcast::Sender<tasks::TaskProgressEvent>,
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
        let (tasks_tx, _) = broadcast::channel(256);
        let token_cache = Arc::new(
            Cache::builder()
                .max_capacity(5000)
                .time_to_live(Duration::from_secs(60))
                .build(),
        );
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        let rip_task_runner: RipTaskRunner =
            Arc::new(|_task_id, _provider, _track_id, _codec, _user_id| {
                tokio::spawn(async move {})
            });
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
            tasks_tx,
            sync_hub: playback_sync::PlaybackSyncHub::default(),
            http_client,
            catalog_service: None,
            rip_task_runner,
            app_key,
            cors_origins: vec!["*".to_string()],
            started_at: std::time::Instant::now(),
        }
    }

    pub fn with_catalog_service(mut self, catalog_service: apple::SharedCatalog) -> Self {
        self.catalog_service = Some(catalog_service);
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
        // Tasks & SSE
        .route("/api/v1/tasks/rip", post(tasks::create_rip_task))
        .route("/api/v1/tasks/{id}/events", get(tasks::task_events))
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
