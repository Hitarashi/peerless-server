pub mod command_catalog;
pub mod dashboard;
pub mod dashboard_map;
pub mod event_bridge;
pub mod handlers;
pub mod html;
pub mod interaction;
pub mod mirror_health;
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

    pub admin_id: i64,
    pub bot_id: i64,
    pub bot_username: Option<String>,
    pub dump_channel_id: i64,
    pub dump_peer: ferogram::PeerRef,

    pub stats: Option<db::StatsRepository>,

    pub db_client: db::DbPool,

    pub started_at: Instant,

    pub stream_engine: Option<Arc<stream::StreamEngine>>,

    pub session_manager: Arc<db::SessionManager>,

    pub app_key: String,

    pub tracks_repo: Arc<db::TracksRepository>,
}

pub type SharedState = Arc<BotState>;

static DASHBOARD: OnceLock<Arc<dashboard::DashboardManager>> = OnceLock::new();
pub fn dashboard_manager() -> Arc<dashboard::DashboardManager> {
    DASHBOARD
        .get_or_init(|| Arc::new(dashboard::DashboardManager::new()))
        .clone()
}
