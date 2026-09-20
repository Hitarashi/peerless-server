use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json,
    extract::{Path, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::stream::{self, Stream};
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
    /// Desired lossless or compressed codec (`alac`, `flac`, `aac`).
    #[schema(example = "alac")]
    pub codec: Option<String>,
}

/// Initial response returned when an on-demand rip task is queued.
#[derive(Debug, Serialize, ToSchema)]
pub struct RipTaskResponse {
    /// Unique task identifier for subscribing to SSE progress events.
    #[schema(example = "task_01h7xyz...")]
    pub task_id: String,
    /// Initial task status (`queued`).
    #[schema(example = "queued")]
    pub status: String,
}

/// Real-time progress event emitted over Server-Sent Events (SSE).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TaskProgressEvent {
    /// Task identifier.
    #[schema(example = "task_01h7xyz...")]
    pub task_id: String,
    /// Current pipeline stage (`queued`, `downloading`, `decrypting`, `tagging`, `uploading_telegram`, `completed`, `failed`).
    #[schema(example = "downloading")]
    pub stage: String,
    /// Download or upload progress percentage (0.0 to 100.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 45.2)]
    pub percent: Option<f32>,
    /// Download/upload throughput speed string (e.g. `8.5 MB/s`).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "8.5 MB/s")]
    pub speed: Option<String>,
    /// Database track ID once ripping and caching completes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 142)]
    pub track_id: Option<i32>,
    /// True when track is cached in Telegram dump channel.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = true)]
    pub is_cached: Option<bool>,
    /// True if the task has concluded.
    #[schema(example = false)]
    pub completed: bool,
    /// Error message if the ripping job failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/v1/tasks/rip",
    tag = "tasks",
    summary = "Create On-Demand Rip Task",
    description = "Dispatches an asynchronous background ripping job via RipOrchestrator for uncached provider tracks. Returns a `task_id` for monitoring real-time SSE progress at `/api/v1/tasks/{id}/events`.",
    request_body = RipTaskRequest,
    responses(
        (status = 200, description = "Rip task queued successfully", body = RipTaskResponse),
        (status = 400, description = "Invalid request payload or unsupported provider"),
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

    let task_id = format!("task_{}", cuid2::create_id());

    // Emit initial event
    let initial_event = TaskProgressEvent {
        task_id: task_id.clone(),
        stage: "queued".to_string(),
        percent: Some(0.0),
        speed: None,
        track_id: None,
        is_cached: None,
        completed: false,
        error: None,
    };
    let _ = state.tasks_tx.send(initial_event);

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
        task_id.clone(),
        provider,
        payload.track_id.clone(),
        codec,
        user.telegram_id,
    );

    Ok(Json(RipTaskResponse {
        task_id,
        status: "queued".to_string(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/tasks/{id}/events",
    tag = "tasks",
    summary = "Stream Task Progress Events (SSE)",
    description = "Streams real-time Server-Sent Events (SSE) broadcasting downloading, decrypting, tagging, and Telegram upload progress until completion.",
    params(
        ("id" = String, Path, description = "Task ID returned by /api/v1/tasks/rip", example = "task_01h7xyz...")
    ),
    responses(
        (status = 200, description = "Real-time Server-Sent Events stream", content_type = "text/event-stream")
    )
)]
pub async fn task_events(
    State(state): State<Arc<ServerState>>,
    Path(task_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tasks_tx.subscribe();
    let target_id = task_id.clone();

    let stream = stream::unfold(
        (rx, target_id, false),
        |(mut rx, target_id, finished)| async move {
            if finished {
                return None;
            }
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if event.task_id == target_id {
                            let is_done = event.completed || event.error.is_some();
                            let json = serde_json::to_string(&event).unwrap_or_default();
                            return Some((
                                Ok(Event::default().event("progress").data(json)),
                                (rx, target_id, is_done),
                            ));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(task_id = %target_id, skipped, "SSE receiver lagged, continuing");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return None;
                    }
                }
            }
        },
    );

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}
