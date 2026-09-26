use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, auth::AuthedUser, error::ServerError};

/// Request payload to trigger an on-demand provider ripping job.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RipTaskRequest {
    /// Music provider name (`apple` or `qobuz`).
    #[schema(example = "apple")]
    pub provider: String,
    /// Provider-native track identifier.
    #[schema(example = "1440857781")]
    pub track_id: String,
    /// Desired codec (`alac`, `flac`, or `aac`); unsupported values fall back to `alac`.
    #[schema(example = "alac")]
    pub codec: Option<String>,
    /// Display metadata retained by the server for task recovery.
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
}

/// Response returned when a rip task is requested; cached tracks may complete immediately.
#[derive(Debug, Serialize, ToSchema)]
pub struct RipTaskResponse {
    /// Unique task identifier used to correlate WebSocket progress updates.
    #[schema(example = "task_01h7xyz...")]
    pub task_id: String,
    /// `completed` for a cached track, otherwise `queued` (including an existing in-flight task).
    #[schema(example = "queued")]
    pub status: String,
}

/// Thread-safe active task metadata stored in `ServerState::active_tasks`.
#[derive(Debug, Clone)]
pub struct ServerTaskMeta {
    pub task_id: String,
    pub job_id: Option<String>,
    pub owner_id: i64,
    pub provider: music::Provider,
    pub track_id: String,
    pub codec: Option<String>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
    pub controller: tokio_util::sync::CancellationToken,
    pub created_at: std::time::Instant,
    pub latest_progress: RipTaskProgress,
    pub is_album: bool,
}

impl ServerTaskMeta {
    pub(crate) fn snapshot(&self, is_owner: bool) -> RipTaskSnapshot {
        let progress = &self.latest_progress;
        RipTaskSnapshot {
            task_id: self.task_id.clone(),
            provider: self.provider.as_str().to_owned(),
            source_track_id: self.track_id.clone(),
            codec: self.codec.clone(),
            title: self.title.clone(),
            artist: self.artist.clone(),
            album: self.album.clone(),
            duration: self.duration,
            stage: progress.stage.clone(),
            percent: progress.percent,
            speed: progress.speed.clone(),
            result_track_id: None,
            is_cached: None,
            completed: false,
            error: None,
            is_owner,
            is_album: self.is_album,
            current_track_title: progress.current_track_title.clone(),
            current_track_artist: progress.current_track_artist.clone(),
            current_track_index: progress.current_track_index,
            total_tracks: progress.total_tracks,
            completed_tracks: progress.completed_tracks,
        }
    }
}

/// Server-owned task state returned to clients on reconnect and app startup.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct RipTaskSnapshot {
    pub task_id: String,
    pub provider: String,
    pub source_track_id: String,
    pub codec: Option<String>,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<i32>,
    pub stage: String,
    pub percent: Option<f32>,
    pub speed: Option<String>,
    pub result_track_id: Option<i32>,
    pub is_cached: Option<bool>,
    pub completed: bool,
    pub error: Option<String>,
    pub is_owner: bool,
    #[serde(default)]
    pub is_album: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_artist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_track_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tracks: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_tracks: Option<u32>,
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks",
    tag = "tasks",
    summary = "List Server Rip Tasks",
    description = "Returns all currently active server-owned rip tasks. Completed, failed, and cancelled tasks are omitted. Live updates are sent over the authenticated playback WebSocket.",
    responses(
        (status = 200, description = "Server-owned rip task snapshots", body = [RipTaskSnapshot]),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_rip_tasks(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Json<Vec<RipTaskSnapshot>> {
    let tasks = state.active_tasks.read();
    let mut snapshots = tasks
        .values()
        .map(|task| {
            (
                task.created_at,
                task.snapshot(task.owner_id == user.telegram_id),
            )
        })
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|(created_at, _)| std::cmp::Reverse(*created_at));
    let snapshots = snapshots
        .into_iter()
        .map(|(_, snapshot)| snapshot)
        .collect();
    Json(snapshots)
}

#[derive(Debug, Clone)]
pub struct RipTaskProgress {
    pub stage: String,
    pub percent: Option<f32>,
    pub speed: Option<String>,
    pub current_track_title: Option<String>,
    pub current_track_artist: Option<String>,
    pub current_track_index: Option<u32>,
    pub total_tracks: Option<u32>,
    pub completed_tracks: Option<u32>,
}

/// Internal notification forwarded to authenticated playback WebSocket clients.
#[derive(Debug, Clone)]
pub enum TaskSyncEvent {
    Updated { task_id: String },
    Dismissed { task_id: String },
}

#[utoipa::path(
    post,
    path = "/api/v1/tasks/rip",
    tag = "tasks",
    summary = "Create On-Demand Rip Task",
    description = "For an uncached track, dispatches an asynchronous background rip and returns a queued `task_id`; if an equivalent task is already active, returns its queued ID. If the track is already cached, returns a new ID with status `completed` without queuing a rip. Task snapshots and progress updates are sent over the authenticated playback WebSocket.",
    request_body = RipTaskRequest,
    responses(
        (status = 200, description = "Rip request accepted; status is `completed` for a cached track or `queued` for a new or reused active task", body = RipTaskResponse),
        (status = 400, description = "Unsupported provider; provider must be `apple` or `qobuz`"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn create_rip_task(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Json(payload): Json<RipTaskRequest>,
) -> Result<Json<RipTaskResponse>, ServerError> {
    let provider = match payload.provider.to_lowercase().as_str() {
        "apple" => music::Provider::Apple,
        "qobuz" => music::Provider::Qobuz,
        _ => {
            return Err(ServerError::BadRequest(format!(
                "Unsupported provider: {}",
                payload.provider
            )));
        }
    };

    let codec = payload
        .codec
        .as_deref()
        .map(|c| match c.to_lowercase().as_str() {
            "alac" => music::Codec::Alac,
            "flac" => music::Codec::Flac,
            "aac" => music::Codec::Aac,
            _ => music::Codec::Alac,
        });

    // 1. Fast-path: Check if track is ALREADY cached in database
    match state
        .tracks_repo
        .get_track_by_provider(provider, &payload.track_id)
        .await
    {
        Ok(Some(track)) => {
            let task_id = format!("task_{}", cuid2::create_id());
            tracing::info!(
                task_id = %task_id,
                user_id = user.telegram_id,
                track_id = track.id,
                "Rip task completed via fast-path database cache hit"
            );
            return Ok(Json(RipTaskResponse {
                task_id,
                status: "completed".to_string(),
            }));
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!(error = %e, "Failed to query tracks repo during rip task creation");
        }
    }

    // 2. In-flight deduplication: If a task for (provider, track_id) is already active, reuse it
    {
        let tasks = state.active_tasks.read();
        if let Some(existing) = tasks
            .values()
            .find(|t| t.provider == provider && t.track_id == payload.track_id)
        {
            tracing::info!(
                existing_task_id = %existing.task_id,
                user_id = user.telegram_id,
                provider = ?provider,
                track_id = %payload.track_id,
                "Reusing existing in-flight rip task"
            );
            return Ok(Json(RipTaskResponse {
                task_id: existing.task_id.clone(),
                status: "queued".to_string(),
            }));
        }
    }

    // 3. Otherwise, create new background rip task
    let task_id = format!("task_{}", cuid2::create_id());
    let controller = tokio_util::sync::CancellationToken::new();

    let initial_progress = RipTaskProgress {
        stage: "queued".to_string(),
        percent: Some(0.0),
        speed: None,
        current_track_title: None,
        current_track_artist: None,
        current_track_index: None,
        total_tracks: None,
        completed_tracks: None,
    };

    let meta = ServerTaskMeta {
        task_id: task_id.clone(),
        job_id: None,
        owner_id: user.telegram_id,
        provider,
        track_id: payload.track_id.clone(),
        codec: payload.codec.clone(),
        title: payload.title.clone(),
        artist: payload.artist.clone(),
        album: payload.album.clone(),
        duration: payload.duration,
        controller: controller.clone(),
        created_at: std::time::Instant::now(),
        latest_progress: initial_progress,
        is_album: false,
    };
    state.active_tasks.write().insert(task_id.clone(), meta);

    let _ = state.task_sync_tx.send(TaskSyncEvent::Updated {
        task_id: task_id.clone(),
    });

    tracing::info!(
        task_id = %task_id,
        user_id = user.telegram_id,
        provider = ?provider,
        track_id = %payload.track_id,
        codec = ?codec,
        "Rip task submitted and dispatched"
    );

    // Dispatch background ripping via RipOrchestrator runner
    (state.rip_task_runner)(
        state.clone(),
        task_id.clone(),
        provider,
        payload.track_id.clone(),
        codec,
        user.telegram_id,
        controller,
    );

    Ok(Json(RipTaskResponse {
        task_id,
        status: "queued".to_string(),
    }))
}

#[utoipa::path(
    delete,
    path = "/api/v1/tasks/{id}",
    tag = "tasks",
    summary = "Cancel Rip Task",
    description = "Cancels an active background rip.",
    params(
        ("id" = String, Path, description = "Task ID returned by /api/v1/tasks/rip", example = "task_01h7xyz...")
    ),
    responses(
        (status = 200, description = "Task cancelled successfully"),
        (status = 403, description = "Forbidden - Not task owner or administrator"),
        (status = 404, description = "Task not found"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn cancel_rip_task(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), ServerError> {
    let task = {
        let tasks = state.active_tasks.read();
        tasks.get(&id).cloned()
    };
    let Some(task) = task else {
        return Err(ServerError::NotFound(format!("Active task {id} not found")));
    };
    if user.telegram_id != task.owner_id && user.telegram_id != state.admin_id {
        return Err(ServerError::Forbidden(
            "Only the task owner or administrator can cancel this rip".to_string(),
        ));
    }

    state.cancel_task(&id, "Cancelled by user");
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "cancelled",
            "task_id": id,
        })),
    ))
}
