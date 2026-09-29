use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;
use utoipa::ToSchema;

use crate::{ServerState, error::ServerError, probe};

/// Telemetry and liveness status of the streaming server.
///
/// This is the **liveness** view: it answers only "should this process be
/// restarted?", and the accompanying HTTP status code is derived from that
/// question alone -- 200 when the process, the database, and at least one
/// stream worker are serving, 503 otherwise.
///
/// The counters below are pure telemetry. They are useful while debugging a
/// 503, but they never influence the status code themselves.
///
/// For the wide, per-subsystem breakdown -- including the Apple wrapper and the
/// Qobuz backend, neither of which can trigger a restart -- see
/// [`status_report`] / `GET /api/v1/status`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthResponse {
    /// `healthy` when the process is live, `unhealthy` when it is not.
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

/// Liveness probe, polled by the Docker `HEALTHCHECK` (`healthcheck.sh`).
///
/// Returns 200 while the process can serve requests, and **503** once it
/// cannot. Restarting only helps for a fault the process cannot recover from,
/// so this deliberately ignores the Apple wrapper and the Qobuz backend: a dead
/// wrapper stops new rips but leaves streaming, catalog, lyrics, and
/// already-ripped playback working, and a restart would not fix it anyway.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "system",
    summary = "Server Liveness Check",
    description = "Liveness probe. Returns 200 while the process, the PostgreSQL pool, and the MTProto stream worker pool are all serving, and 503 when any of them is not. Also reports in-memory chunk cache metrics and uptime as informational telemetry. Does NOT consider the Apple wrapper or the Qobuz backend: a dead wrapper must not restart a container that is otherwise serving. For the full per-subsystem breakdown, use GET /api/v1/status.",
    responses(
        (status = 200, description = "Process is live and able to serve requests", body = HealthResponse),
        (status = 503, description = "A liveness-critical subsystem (database or stream workers) is down; the process should be restarted", body = HealthResponse)
    )
)]
pub async fn health_check(
    State(state): State<Arc<ServerState>>,
) -> Result<(StatusCode, Json<HealthResponse>), ServerError> {
    let liveness = probe::collect_liveness(&state).await;

    let pool = state.stream_engine.worker_pool();
    let cache = state.stream_engine.cache();

    // The one and only thing that decides the status code.
    let status_code = if liveness.live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((
        status_code,
        Json(HealthResponse {
            status: if liveness.live {
                "healthy"
            } else {
                "unhealthy"
            },
            workers_total: pool.worker_count(),
            workers_available: pool.available_worker_count(),
            cache_entries: cache.entry_count(),
            cache_bytes: cache.weighted_size(),
            uptime_seconds: state.started_at.elapsed().as_secs(),
        }),
    ))
}

/// Wide diagnostics: every subsystem with an explicit state.
///
/// **Always returns HTTP 200**, whatever it finds. This is the endpoint an
/// operator reads precisely *because* something is wrong, so it must always
/// answer -- a 500 from the diagnostics endpoint would be the one failure mode
/// that hides the actual fault.
#[utoipa::path(
    get,
    path = "/api/v1/status",
    tag = "system",
    summary = "Server Diagnostics",
    description = "Wide per-subsystem diagnostics: PostgreSQL reachability, MTProto stream worker pool, in-memory chunk cache, the Apple ALAC wrapper, and the hosted Qobuz backend (the latter two only when configured). Always returns HTTP 200 so it remains readable during an outage. Unlike GET /api/v1/health, this endpoint's findings do not drive any restart decision.",
    responses(
        (status = 200, description = "Diagnostics collected. Inspect `status` and each subsystem entry: `healthy` means every probed subsystem is `ok`, `degraded` means at least one is not.", body = probe::StatusReport)
    )
)]
pub async fn status_report(State(state): State<Arc<ServerState>>) -> Json<probe::StatusReport> {
    Json(probe::collect_status(&state).await)
}
