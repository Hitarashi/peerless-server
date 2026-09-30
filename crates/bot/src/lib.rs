//! Telegram handlers and small formatting helpers.

pub mod command_catalog;
pub mod dashboard;
pub mod dashboard_map;
pub mod event_bridge;
pub mod handlers;
pub mod html;
pub mod interaction;
pub mod mirror_health;
pub mod musicbrainz;
pub mod presentation;
pub mod providers;
pub mod rip_deps;
pub mod spectrogram;
pub mod telegram_retry;
pub mod telegram_sink;

use std::{
    sync::{Arc, OnceLock},
    time::Instant,
};

#[derive(Clone)]
pub struct BotState {
    pub client: ferogram::Client,
    pub auth: db::Auth,
    pub rip_deps: Arc<rip_deps::RipDeps>,
    pub rip_orchestrator: Arc<engine::orchestrator::RipOrchestrator>,
    /// Env-driven ids/peers the ops commands need . `dump_peer` is the TL-level peer used for
    /// sends/queries; `dump_channel_id` is the raw id for link building.
    pub admin_id: i64,
    pub bot_id: i64,
    pub bot_username: Option<String>,
    pub dump_channel_id: i64,
    pub dump_peer: ferogram::PeerRef,
    /// Stats aggregation for `/stats` (lazy-initialized in main).
    pub stats: Option<db::StatsRepository>,
    /// Raw DB client for the dump service (`/export`, `/import`) — cheap
    /// Arc-pool clone shared with the repositories.
    pub db_client: db::DbPool,
    /// Process start, for `/ping` uptime ).
    pub started_at: Instant,
    /// Streaming engine coordinating MTProto worker pool and chunk cache.
    pub stream_engine: Option<Arc<stream::StreamEngine>>,
    /// Session manager handling OTP codes, refresh tokens, and sliding sessions.
    pub session_manager: Arc<db::SessionManager>,
    /// App HMAC secret key for generating signed playback tickets.
    pub app_key: String,
    /// Tracks repository for audio catalog lookups.
    pub tracks_repo: Arc<db::TracksRepository>,
}

pub type SharedState = Arc<BotState>;

static DASHBOARD: OnceLock<Arc<dashboard::DashboardManager>> = OnceLock::new();
pub fn dashboard_manager() -> Arc<dashboard::DashboardManager> {
    DASHBOARD
        .get_or_init(|| Arc::new(dashboard::DashboardManager::new()))
        .clone()
}
