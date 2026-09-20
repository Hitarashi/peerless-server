use std::sync::Arc;

use axum::{Json, extract::State};
use serde::Serialize;
use utoipa::ToSchema;

use crate::ServerState;

/// Telemetry and health status of the streaming server.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthResponse {
    /// Server health status.
    #[schema(example = "healthy")]
    pub status: &'static str,
    /// Total number of auxiliary MTProto stream workers configured.
    #[schema(example = 4)]
    pub workers_total: usize,
    /// Number of healthy MTProto workers available to service requests.
    #[schema(example = 4)]
    pub workers_available: usize,
    /// Total number of cached 512KB media chunks in memory.
    #[schema(example = 128)]
    pub cache_entries: u64,
    /// Total memory size in bytes consumed by cached chunks.
    #[schema(example = 67108864)]
    pub cache_bytes: u64,
    /// Number of seconds the server process has been running.
    #[schema(example = 3600)]
    pub uptime_seconds: u64,
}

#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "system",
    summary = "Server Health & Telemetry Check",
    description = "Returns current streaming server status, MTProto worker pool availability, in-memory chunk cache metrics, and uptime.",
    responses(
        (status = 200, description = "Server is healthy and functioning properly", body = HealthResponse)
    )
)]
pub async fn health_check(State(state): State<Arc<ServerState>>) -> Json<HealthResponse> {
    let pool = state.stream_engine.worker_pool();
    let cache = state.stream_engine.cache();

    Json(HealthResponse {
        status: "healthy",
        workers_total: pool.worker_count(),
        workers_available: pool.available_worker_count(),
        cache_entries: cache.entry_count(),
        cache_bytes: cache.weighted_size(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}
