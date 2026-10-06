use std::sync::Arc;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    ServerError, ServerState,
    rip_tasks::{RipTaskProgress, RipTaskRequest, ServerTaskMeta},
    task_registry::{TaskRegistry, TaskRemovalRejected},
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

    let Some((target_id, is_album)) = request.target() else {
        return Err(RipTaskRpcError::new(
            Some(request_id),
            RipTaskRpcErrorCode::Validation,
            "track_id or album_id must not be empty",
        ));
    };

    let (is_cached, result_track_id) = if is_album {
        let album_repo = db::AlbumsRepository::new(state.db.clone());
        match album_repo
            .find_album_parts_by_ids(std::slice::from_ref(&target_id))
            .await
        {
            Ok(parts) => {
                let alac_parts: Vec<_> = parts.iter().filter(|p| p.codec == music::Codec::Alac).collect();
                let aac_parts: Vec<_> = parts.iter().filter(|p| p.codec == music::Codec::Aac).collect();
                let any_complete = crate::lookup::has_complete_archive_parts(&alac_parts)
                    || crate::lookup::has_complete_archive_parts(&aac_parts);
                (any_complete, None)
            }
            Err(error) => {
                return Err(map_cache_lookup_error(Some(request_id), error));
            }
        }
    } else {
        let cached_tracks = match state
            .tracks_repo
            .find_all_by_track_id(&target_id)
            .await
        {
            Ok(tracks) => tracks,
            Err(error) => {
                return Err(map_cache_lookup_error(Some(request_id), error));
            }
        };
        let matching = cached_tracks
            .iter()
            .find(|t| t.codec == music::Codec::Alac)
            .or_else(|| cached_tracks.iter().find(|t| t.codec == music::Codec::Aac));
        (matching.is_some(), matching.map(|t| t.id))
    };

    if is_cached {
        return Ok(RipTaskRpcSuccess::Created {
            request_id,
            task_id: String::new(),
            status: RipTaskRpcStatus::Completed,
            result_track_id,
        });
    }

    let task_id = format!("task_{}", cuid2::create_id());
    let controller = tokio_util::sync::CancellationToken::new();
    let meta = ServerTaskMeta {
        task_id: task_id.clone(),

        rip_task_id: String::new(),
        owner_id: identity.telegram_id,
        track_id: target_id.clone(),
        codec: None,
        title: None,
        artist: None,
        album: None,
        duration: None,
        artwork_url: None,
        controller: controller.clone(),
        created_at: std::time::Instant::now(),
        latest_progress: RipTaskProgress {
            job_stage: Some(
                engine::orchestrator::types::TaskActivity::Queued { position: 1 }.into(),
            ),
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
        is_album,
        completed: false,
        result_track_id: None,
        error: None,
    };

    let reserved_task_id = match reserve_task(state.tasks(), meta) {
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
        track_id = %target_id,
        "Rip task submitted and dispatched"
    );

    (state.rip_task_runner)(
        state.clone(),
        reserved_task_id.clone(),
        target_id,
        is_album,
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
        state.tasks(),
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

fn reserve_task(tasks: &TaskRegistry, meta: ServerTaskMeta) -> Result<String, String> {
    tasks.insert_unique(meta, |existing, candidate| {
        existing.is_album == candidate.is_album
            && existing.track_id == candidate.track_id
    })
}

fn remove_task_if_authorized(
    tasks: &TaskRegistry,
    task_id: &str,
    user_id: i64,
    admin_id: i64,
) -> Result<ServerTaskMeta, CancelFailure> {
    match tasks.remove_if(task_id, |task| {
        user_id == task.owner_id || user_id == admin_id
    }) {
        Ok(Some(task)) => Ok(task),
        Ok(None) => Err(CancelFailure::NotFound),
        Err(TaskRemovalRejected) => Err(CancelFailure::NotAuthorized),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    fn test_task(task_id: &str, owner_id: i64) -> ServerTaskMeta {
        ServerTaskMeta {
            task_id: task_id.to_owned(),
            rip_task_id: String::new(),
            owner_id,
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
                job_stage: Some(
                    engine::orchestrator::types::TaskActivity::Queued { position: 1 }.into(),
                ),
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
            completed: false,
            result_track_id: None,
            error: None,
        }
    }

    #[test]
    fn concurrent_identical_creates_reserve_exactly_one_task() {
        const REQUESTS: usize = 16;
        let tasks = TaskRegistry::default();
        let barrier = Arc::new(Barrier::new(REQUESTS));

        let handles = (0..REQUESTS)
            .map(|n| {
                let tasks = tasks.clone();
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

        assert_eq!(tasks.len(), 1);
        let winner = tasks
            .all()
            .into_iter()
            .next()
            .expect("exactly one reserved task")
            .task_id;
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert!(outcomes.iter().all(|outcome| match outcome {
            Ok(task_id) => task_id == &winner,
            Err(task_id) => task_id == &winner,
        }));
    }

    #[test]
    fn cancellation_keeps_owner_or_admin_rule_and_checks_atomically() {
        let tasks = TaskRegistry::default();
        tasks.insert("task-1".to_owned(), test_task("task-1", 100));

        assert_eq!(
            remove_task_if_authorized(&tasks, "task-1", 200, 300).unwrap_err(),
            CancelFailure::NotAuthorized
        );
        assert!(tasks.contains_key("task-1"));
        assert_eq!(
            remove_task_if_authorized(&tasks, "task-1", 300, 300)
                .unwrap()
                .task_id,
            "task-1"
        );
        assert!(!tasks.contains_key("task-1"));

        tasks.insert("task-2".to_owned(), test_task("task-2", 100));
        assert_eq!(
            remove_task_if_authorized(&tasks, "task-2", 100, 300)
                .unwrap()
                .task_id,
            "task-2"
        );
    }

    #[test]
    fn reservation_key_distinguishes_album_and_track_for_same_id() {
        let tasks = TaskRegistry::default();
        assert_eq!(
            reserve_task(&tasks, test_task("track", 100)),
            Ok("track".to_owned())
        );
        let duplicate = test_task("duplicate", 101);
        assert_eq!(reserve_task(&tasks, duplicate), Err("track".to_owned()));

        let mut album_task = test_task("album", 102);
        album_task.is_album = true;
        assert_eq!(reserve_task(&tasks, album_task), Ok("album".to_owned()));
        assert_eq!(tasks.len(), 2);
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
