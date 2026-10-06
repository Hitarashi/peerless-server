use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    ServerError, ServerState,
    rip_tasks::{RipTaskProgress, RipTaskRequest, ServerTaskMeta},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticatedIdentity {
    pub telegram_id: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskRpcStatus {
    Queued,
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskRpcErrorCode {
    Validation,
    NotFound,
    NotAuthorized,
    Unavailable,
    Internal,
}

impl RipTaskRpcErrorCode {
    fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

#[derive(Debug, Clone)]
pub enum RipTaskRpcRequest {
    Create {
        request_id: String,
        request: RipTaskRequest,
    },
    Cancel {
        request_id: String,
        task_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RipTaskRpcSuccess {
    Created {
        request_id: String,
        task_id: String,
        status: RipTaskRpcStatus,
        result_track_id: Option<i32>,
    },
    Cancelled {
        request_id: String,
        task_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RipTaskRpcError {
    pub request_id: Option<String>,
    pub code: RipTaskRpcErrorCode,
    pub message: String,
    pub retryable: bool,
}

impl RipTaskRpcError {
    fn new(request_id: Option<String>, code: RipTaskRpcErrorCode, message: &'static str) -> Self {
        Self {
            request_id,
            code,
            message: message.to_owned(),
            retryable: code.retryable(),
        }
    }
}

fn map_server_error(request_id: Option<String>, error: ServerError) -> RipTaskRpcError {
    let (code, message) = match error {
        ServerError::BadRequest(_) => (RipTaskRpcErrorCode::Validation, "invalid rip-task request"),
        ServerError::NotFound(_) => (RipTaskRpcErrorCode::NotFound, "rip task not found"),
        ServerError::Unauthorized(_) | ServerError::Forbidden(_) => (
            RipTaskRpcErrorCode::NotAuthorized,
            "not authorized to perform this rip-task operation",
        ),
        ServerError::Internal(_) => (RipTaskRpcErrorCode::Internal, "rip-task request failed"),
    };
    RipTaskRpcError::new(request_id, code, message)
}

fn map_cache_lookup_error(request_id: Option<String>, _error: db::DbError) -> RipTaskRpcError {
    RipTaskRpcError::new(
        request_id,
        RipTaskRpcErrorCode::Unavailable,
        "rip-task cache lookup is temporarily unavailable",
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelFailure {
    NotFound,
    NotAuthorized,
}

pub async fn handle_request(
    identity: AuthenticatedIdentity,
    request: RipTaskRpcRequest,
    state: Arc<ServerState>,
) -> Result<RipTaskRpcSuccess, RipTaskRpcError> {
    match request {
        RipTaskRpcRequest::Create {
            request_id,
            request,
        } => create_task(identity, request_id, request, state).await,
        RipTaskRpcRequest::Cancel {
            request_id,
            task_id,
        } => cancel_task(identity, request_id, task_id, state),
    }
}

async fn create_task(
    identity: AuthenticatedIdentity,
    request_id: String,
    request: RipTaskRequest,
    state: Arc<ServerState>,
) -> Result<RipTaskRpcSuccess, RipTaskRpcError> {
    if request_id.trim().is_empty() {
        return Err(RipTaskRpcError::new(
            Some(request_id),
            RipTaskRpcErrorCode::Validation,
            "request_id must not be empty",
        ));
    }

    let provider = match request.provider.to_lowercase().as_str() {
        "apple" => music::Provider::Apple,
        _ => {
            return Err(map_server_error(
                Some(request_id),
                ServerError::BadRequest("unsupported provider".to_owned()),
            ));
        }
    };

    if request.track_id.trim().is_empty() {
        return Err(RipTaskRpcError::new(
            Some(request_id),
            RipTaskRpcErrorCode::Validation,
            "track_id must not be empty",
        ));
    }

    let codec = request
        .codec
        .as_deref()
        .map(|value| match value.to_lowercase().as_str() {
            "alac" => music::Codec::Alac,
            "aac" | "mp4a.40.2" | "mp4a.40.5" => music::Codec::Aac,
            "ec-3" | "ec3" | "atmos" | "dolby" => music::Codec::Ec3,
            _ => music::Codec::Alac,
        });

    let cached_tracks = match state
        .tracks_repo
        .find_all_by_track_id(&request.track_id)
        .await
    {
        Ok(tracks) => tracks,
        Err(error) => {
            return Err(map_cache_lookup_error(Some(request_id), error));
        }
    };
    if let Some(track) = cached_tracks
        .into_iter()
        .find(|track| codec.is_none_or(|requested| requested == track.codec))
    {
        return Ok(RipTaskRpcSuccess::Created {
            request_id,

            task_id: String::new(),
            status: RipTaskRpcStatus::Completed,
            result_track_id: Some(track.id),
        });
    }

    let task_id = format!("task_{}", cuid2::create_id());
    let controller = tokio_util::sync::CancellationToken::new();
    let meta = ServerTaskMeta {
        task_id: task_id.clone(),

        rip_task_id: String::new(),
        owner_id: identity.telegram_id,
        provider: provider.clone(),
        track_id: request.track_id.clone(),
        codec: codec.map(|codec| codec.as_str().to_owned()),
        title: request.title,
        artist: request.artist,
        album: request.album,
        duration: request.duration,
        artwork_url: request.artwork_url,
        controller: controller.clone(),
        created_at: std::time::Instant::now(),
        latest_progress: RipTaskProgress {
            job_stage: Some(crate::rip_tasks::RipTaskJobStage::Queued),
            download: None,
            upload: None,
            percent: Some(0.0),
            current_track_title: None,
            current_track_artist: None,
            current_track_artwork_url: None,
            current_track_index: None,
            total_tracks: None,
            completed_tracks: None,
            failed_tracks: None,
        },
        is_album: false,
    };

    let reserved_task_id = match reserve_task(&state.active_tasks, meta) {
        Ok(task_id) => task_id,
        Err(existing_task_id) => {
            return Ok(RipTaskRpcSuccess::Created {
                request_id,
                task_id: existing_task_id,
                status: RipTaskRpcStatus::Queued,
                result_track_id: None,
            });
        }
    };

    let _ = state
        .task_sync_tx
        .send(crate::rip_tasks::TaskSyncEvent::Updated {
            task_id: reserved_task_id.clone(),
        });

    tracing::info!(
        task_id = %reserved_task_id,
        user_id = identity.telegram_id,
        provider = ?provider,
        track_id = %request.track_id,
        codec = ?codec,
        "Rip task submitted and dispatched"
    );

    (state.rip_task_runner)(
        state.clone(),
        reserved_task_id.clone(),
        provider,
        request.track_id,
        codec,
        identity.telegram_id,
        controller,
    );

    Ok(RipTaskRpcSuccess::Created {
        request_id,
        task_id: reserved_task_id,
        status: RipTaskRpcStatus::Queued,
        result_track_id: None,
    })
}

fn cancel_task(
    identity: AuthenticatedIdentity,
    request_id: String,
    task_id: String,
    state: Arc<ServerState>,
) -> Result<RipTaskRpcSuccess, RipTaskRpcError> {
    if request_id.trim().is_empty() || task_id.trim().is_empty() {
        return Err(RipTaskRpcError::new(
            Some(request_id),
            RipTaskRpcErrorCode::Validation,
            "request_id and task_id must not be empty",
        ));
    }

    let task = remove_task_if_authorized(
        &state.active_tasks,
        &task_id,
        identity.telegram_id,
        state.admin_id,
    )
    .map_err(|failure| match failure {
        CancelFailure::NotFound => map_server_error(
            Some(request_id.clone()),
            ServerError::NotFound("active task missing".to_owned()),
        ),
        CancelFailure::NotAuthorized => map_server_error(
            Some(request_id.clone()),
            ServerError::Forbidden("task owner/admin required".to_owned()),
        ),
    })?;

    let _ = state
        .task_sync_tx
        .send(crate::rip_tasks::TaskSyncEvent::Dismissed {
            task_id: task_id.clone(),
        });
    task.controller.cancel();
    if !task.rip_task_id.is_empty() {
        state
            .rip_orchestrator
            .cancel_task(&task.rip_task_id, Some("user"));
    }

    Ok(RipTaskRpcSuccess::Cancelled {
        request_id,
        task_id,
    })
}

fn reserve_task(
    active_tasks: &parking_lot::RwLock<HashMap<String, ServerTaskMeta>>,
    meta: ServerTaskMeta,
) -> Result<String, String> {
    let mut tasks = active_tasks.write();
    if let Some(existing) = tasks.values().find(|task| {
        !task.is_album
            && task.provider == meta.provider
            && task.track_id == meta.track_id
            && task.codec == meta.codec
    }) {
        return Err(existing.task_id.clone());
    }

    let task_id = meta.task_id.clone();
    tasks.insert(task_id.clone(), meta);
    Ok(task_id)
}

fn remove_task_if_authorized(
    active_tasks: &parking_lot::RwLock<HashMap<String, ServerTaskMeta>>,
    task_id: &str,
    user_id: i64,
    admin_id: i64,
) -> Result<ServerTaskMeta, CancelFailure> {
    let mut tasks = active_tasks.write();
    let Some(task) = tasks.get(task_id) else {
        return Err(CancelFailure::NotFound);
    };
    if user_id != task.owner_id && user_id != admin_id {
        return Err(CancelFailure::NotAuthorized);
    }

    Ok(tasks
        .remove(task_id)
        .expect("task was checked while holding write lock"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Barrier},
    };

    use super::*;

    fn test_task(task_id: &str, owner_id: i64) -> ServerTaskMeta {
        ServerTaskMeta {
            task_id: task_id.to_owned(),
            rip_task_id: String::new(),
            owner_id,
            provider: music::Provider::Apple,
            track_id: "track-1".to_owned(),
            codec: Some("flac".to_owned()),
            title: None,
            artist: None,
            album: None,
            duration: None,
            artwork_url: None,
            controller: tokio_util::sync::CancellationToken::new(),
            created_at: std::time::Instant::now(),
            latest_progress: RipTaskProgress {
                job_stage: Some(crate::rip_tasks::RipTaskJobStage::Queued),
                download: None,
                upload: None,
                percent: Some(0.0),
                current_track_title: None,
                current_track_artist: None,
                current_track_artwork_url: None,
                current_track_index: None,
                total_tracks: None,
                completed_tracks: None,
                failed_tracks: None,
            },
            is_album: false,
        }
    }

    #[test]
    fn concurrent_identical_creates_reserve_exactly_one_task() {
        const REQUESTS: usize = 16;
        let tasks = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let barrier = Arc::new(Barrier::new(REQUESTS));

        let handles = (0..REQUESTS)
            .map(|n| {
                let tasks = Arc::clone(&tasks);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    reserve_task(&tasks, test_task(&format!("task-{n}"), 100))
                })
            })
            .collect::<Vec<_>>();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().expect("reservation thread succeeds"))
            .collect::<Vec<_>>();

        assert_eq!(tasks.read().len(), 1);
        let winner = tasks.read().keys().next().unwrap().clone();
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert!(outcomes.iter().all(|outcome| match outcome {
            Ok(task_id) => task_id == &winner,
            Err(task_id) => task_id == &winner,
        }));
    }

    #[test]
    fn cancellation_keeps_owner_or_admin_rule_and_checks_atomically() {
        let tasks = parking_lot::RwLock::new(HashMap::from([(
            "task-1".to_owned(),
            test_task("task-1", 100),
        )]));

        assert_eq!(
            remove_task_if_authorized(&tasks, "task-1", 200, 300).unwrap_err(),
            CancelFailure::NotAuthorized
        );
        assert!(tasks.read().contains_key("task-1"));
        assert_eq!(
            remove_task_if_authorized(&tasks, "task-1", 300, 300)
                .unwrap()
                .task_id,
            "task-1"
        );
        assert!(!tasks.read().contains_key("task-1"));

        tasks
            .write()
            .insert("task-2".to_owned(), test_task("task-2", 100));
        assert_eq!(
            remove_task_if_authorized(&tasks, "task-2", 100, 300)
                .unwrap()
                .task_id,
            "task-2"
        );
    }

    #[test]
    fn codec_is_part_of_the_active_task_reservation_key() {
        let tasks = parking_lot::RwLock::new(HashMap::new());
        assert_eq!(
            reserve_task(&tasks, test_task("flac", 100)),
            Ok("flac".to_owned())
        );
        let mut alac_task = test_task("alac", 101);
        alac_task.codec = Some("alac".to_owned());
        assert_eq!(reserve_task(&tasks, alac_task), Ok("alac".to_owned()));
        assert_eq!(tasks.read().len(), 2);
    }

    #[test]
    fn maps_internal_errors_to_the_rpc_taxonomy_without_exposing_details() {
        let cases = [
            (
                ServerError::BadRequest("sensitive bad-request detail".to_owned()),
                RipTaskRpcErrorCode::Validation,
                false,
            ),
            (
                ServerError::NotFound("internal task identifier".to_owned()),
                RipTaskRpcErrorCode::NotFound,
                false,
            ),
            (
                ServerError::Forbidden("owner details".to_owned()),
                RipTaskRpcErrorCode::NotAuthorized,
                false,
            ),
            (
                ServerError::Unauthorized("session details".to_owned()),
                RipTaskRpcErrorCode::NotAuthorized,
                false,
            ),
            (
                ServerError::Internal("database credentials".to_owned()),
                RipTaskRpcErrorCode::Internal,
                false,
            ),
        ];

        for (source, expected_code, expected_retryable) in cases {
            let error = map_server_error(Some("request-1".to_owned()), source);
            assert_eq!(error.code, expected_code);
            assert_eq!(error.retryable, expected_retryable);
            assert!(!error.message.contains("sensitive"));
            assert!(!error.message.contains("credentials"));
        }

        let unavailable = map_cache_lookup_error(
            Some("request-2".to_owned()),
            db::DbError::Pool("database credentials".to_owned()),
        );
        assert_eq!(unavailable.code, RipTaskRpcErrorCode::Unavailable);
        assert!(unavailable.retryable);
        assert!(!unavailable.message.contains("credentials"));
    }
}
