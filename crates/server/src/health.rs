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
    /// Cumulative stream performance counters since process start.
    pub stream_metrics: StreamingMetrics,
}

/// Cumulative measurements for tuning streaming and worker capacity.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StreamingMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub streams_started: u64,
    pub streams_with_first_chunk: u64,
    pub first_chunk_latency_avg_ms: Option<u64>,
    pub first_chunk_latency_max_ms: Option<u64>,
    pub chunk_fetches: u64,
    pub chunk_fetch_failures: u64,
    pub chunk_fetch_latency_avg_ms: Option<u64>,
    pub chunk_fetch_latency_max_ms: Option<u64>,
    pub rpc_attempts: u64,
    pub retries: u64,
    pub network_retries: u64,
    pub timeout_retries: u64,
    pub flood_wait_retries: u64,
    pub connection_not_inited_retries: u64,
    pub dc_migration_retries: u64,
    pub primary_fallback_retries: u64,
    pub telegram_bytes_received: u64,
    pub bytes_enqueued: u64,
    pub file_reference_refreshes: u64,
    pub worker_lock_waits: u64,
    pub worker_lock_wait_avg_ms: Option<u64>,
    pub worker_lock_wait_max_ms: Option<u64>,
    pub rpc_slot_waits: u64,
    pub rpc_slot_wait_avg_ms: Option<u64>,
    pub rpc_slot_wait_max_ms: Option<u64>,
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
    description = "Liveness probe. Returns 200 while the process, the PostgreSQL pool, and the MTProto stream worker pool are all serving, and 503 when any of them is not. Also reports process-lifetime streaming counters, latency summaries, cache statistics, and uptime as informational telemetry. Does NOT consider the Apple wrapper or the Qobuz backend: a dead wrapper must not restart a container that is otherwise serving. For the full per-subsystem breakdown, use GET /api/v1/status.",
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
    let cache_metrics = cache.metrics_snapshot();
    let stream_metrics = pool.metrics().snapshot();

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
            stream_metrics: StreamingMetrics {
                cache_hits: cache_metrics.hits,
                cache_misses: cache_metrics.misses,
                streams_started: stream_metrics.streams_started,
                streams_with_first_chunk: stream_metrics.streams_with_first_chunk,
                first_chunk_latency_avg_ms: stream_metrics.first_chunk_latency_avg_ms,
                first_chunk_latency_max_ms: stream_metrics.first_chunk_latency_max_ms,
                chunk_fetches: stream_metrics.chunk_fetches,
                chunk_fetch_failures: stream_metrics.chunk_fetch_failures,
                chunk_fetch_latency_avg_ms: stream_metrics.chunk_fetch_latency_avg_ms,
                chunk_fetch_latency_max_ms: stream_metrics.chunk_fetch_latency_max_ms,
                rpc_attempts: stream_metrics.rpc_attempts,
                retries: stream_metrics.retries,
                network_retries: stream_metrics.network_retries,
                timeout_retries: stream_metrics.timeout_retries,
                flood_wait_retries: stream_metrics.flood_wait_retries,
                connection_not_inited_retries: stream_metrics.connection_not_inited_retries,
                dc_migration_retries: stream_metrics.dc_migration_retries,
                primary_fallback_retries: stream_metrics.primary_fallback_retries,
                telegram_bytes_received: stream_metrics.telegram_bytes_received,
                bytes_enqueued: stream_metrics.bytes_enqueued,
                file_reference_refreshes: stream_metrics.file_reference_refreshes,
                worker_lock_waits: stream_metrics.worker_lock_waits,
                worker_lock_wait_avg_ms: stream_metrics.worker_lock_wait_avg_ms,
                worker_lock_wait_max_ms: stream_metrics.worker_lock_wait_max_ms,
                rpc_slot_waits: stream_metrics.rpc_slot_waits,
                rpc_slot_wait_avg_ms: stream_metrics.rpc_slot_wait_avg_ms,
                rpc_slot_wait_max_ms: stream_metrics.rpc_slot_wait_max_ms,
            },
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
