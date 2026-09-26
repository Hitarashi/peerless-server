use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use utoipa::ToSchema;

use crate::{
    ServerState,
    error::ServerError,
    rip_task_rpc::{
        AuthenticatedIdentity, RipTaskRpcError, RipTaskRpcErrorCode, RipTaskRpcRequest,
        RipTaskRpcStatus, RipTaskRpcSuccess,
    },
};

/// Session revocation is detected within this interval while a socket is idle.
const AUTH_RECHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Identifies a device currently connected to the authenticated user's playback room.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ConnectedDeviceInfo {
    /// Client-chosen identifier for this device.
    pub device_id: String,
    /// Human-readable device name shown to other clients in the room.
    pub device_name: String,
    /// Client-reported platform, such as `ios`, `android`, `web`, or `desktop`.
    pub platform: String,
}

/// Wire status for a create-rip-task reply.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RipTaskStatus {
    Queued,
    Completed,
}

/// Wire taxonomy for protocol errors.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RpcErrorCode {
    Validation,
    NotFound,
    NotAuthorized,
    Unavailable,
    Internal,
}

impl From<RipTaskRpcStatus> for RipTaskStatus {
    fn from(status: RipTaskRpcStatus) -> Self {
        match status {
            RipTaskRpcStatus::Queued => Self::Queued,
            RipTaskRpcStatus::Completed => Self::Completed,
        }
    }
}

impl From<RipTaskRpcErrorCode> for RpcErrorCode {
    fn from(code: RipTaskRpcErrorCode) -> Self {
        match code {
            RipTaskRpcErrorCode::Validation => Self::Validation,
            RipTaskRpcErrorCode::NotFound => Self::NotFound,
            RipTaskRpcErrorCode::NotAuthorized => Self::NotAuthorized,
            RipTaskRpcErrorCode::Unavailable => Self::Unavailable,
            RipTaskRpcErrorCode::Internal => Self::Internal,
        }
    }
}

/// Messages sent by a client to the playback synchronization server.
///
/// The wire representation is adjacent-tagged JSON: `{"type":"...","payload":{...}}`.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "payload")]
pub enum ClientMessage {
    /// Register this connection as a device in the user's room. Required before receiving rip-task events.
    #[serde(rename = "hello")]
    Hello {
        /// Client-chosen identifier for this device.
        device_id: String,
        /// Human-readable device name.
        device_name: String,
        /// Client-reported platform.
        platform: String,
    },
    /// Publish playback state. The server treats `snapshot` as opaque JSON and stores/rebroadcasts it unchanged.
    #[serde(rename = "report_state")]
    ReportState {
        /// Opaque client playback state; the server does not parse or validate its contents.
        snapshot: serde_json::Value,
    },
    /// Ask every device in the room (including the sender) to perform an action.
    #[serde(rename = "command")]
    Command {
        /// Action name, commonly `play`, `pause`, `seek`, `next`, `prev`, or `select_track`.
        action: String,
        /// Optional action-specific data, whose structure is defined by the client application.
        #[serde(default)]
        data: Option<serde_json::Value>,
    },
    /// Make the named device the active playback device and broadcast the resulting room state.
    #[serde(rename = "transfer_playback")]
    TransferPlayback {
        /// Identifier of the device to make active.
        target_device_id: String,
    },
    /// Create a rip task and correlate its direct reply using `request_id`.
    #[serde(rename = "create_rip_task")]
    CreateRipTask {
        /// Client-generated identifier echoed in the RPC reply.
        request_id: String,
        /// Task request data.
        request: crate::tasks::RipTaskRequest,
    },
    /// Cancel an active rip task and correlate its direct reply using `request_id`.
    #[serde(rename = "cancel_rip_task")]
    CancelRipTask {
        /// Client-generated identifier echoed in the RPC reply.
        request_id: String,
        /// Active task identifier returned by `create_rip_task`.
        task_id: String,
    },
}

/// Messages sent by the server to clients in a playback room.
///
/// The wire representation is adjacent-tagged JSON: `{"type":"...","payload":{...}}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(tag = "type", content = "payload")]
pub enum ServerMessage {
    /// Current room membership, active device, and most recently reported playback snapshot.
    #[serde(rename = "room_state")]
    RoomState {
        /// Device currently controlling playback, if one is active.
        active_device_id: Option<String>,
        /// Devices currently connected to the room.
        devices: Vec<ConnectedDeviceInfo>,
        /// Latest opaque playback snapshot, if any has been reported by the active device.
        snapshot: Option<serde_json::Value>,
    },
    /// A new playback snapshot reported by the active device.
    #[serde(rename = "state_updated")]
    StateUpdated {
        /// Device currently controlling playback.
        active_device_id: Option<String>,
        /// Opaque playback state, forwarded without server-side interpretation.
        snapshot: serde_json::Value,
    },
    /// A room-wide command to execute locally; this is also delivered back to its sender.
    #[serde(rename = "execute_command")]
    ExecuteCommand {
        /// Action name supplied by the client that issued the command.
        action: String,
        /// Optional action-specific data supplied by the sender.
        data: Option<serde_json::Value>,
    },
    /// Full active rip-task state, sent after `hello` and when a task subscriber falls behind.
    #[serde(rename = "rip_tasks_snapshot")]
    RipTasksSnapshot {
        /// Current active server-owned task snapshots; each task's `is_owner` identifies ownership.
        tasks: Vec<crate::tasks::RipTaskSnapshot>,
    },
    /// Updated state for one server-owned rip task.
    #[serde(rename = "rip_task_updated")]
    RipTaskUpdated {
        /// Updated task snapshot.
        task: Box<crate::tasks::RipTaskSnapshot>,
    },
    /// Notification that a task was dismissed or is no longer active.
    #[serde(rename = "rip_task_dismissed")]
    RipTaskDismissed {
        /// Identifier of the dismissed task.
        task_id: String,
    },
    /// Direct reply to a create request; sent only to the initiating connection.
    #[serde(rename = "rip_task_created")]
    RipTaskCreated {
        /// Request correlation identifier supplied by the client.
        request_id: String,
        /// Empty for a cache hit, which has no cancellable task; see `result_track_id`.
        task_id: String,
        /// Whether a task was queued or an existing cached result completed the request.
        status: RipTaskStatus,
        /// Database track id when the request was satisfied from cache.
        result_track_id: Option<i32>,
        /// Initial active-task snapshot for queued requests; null when there is no active task.
        task: Option<Box<crate::tasks::RipTaskSnapshot>>,
    },
    /// Direct reply to a cancel request; sent only to the initiating connection.
    #[serde(rename = "rip_task_cancelled")]
    RipTaskCancelled {
        /// Request correlation identifier supplied by the client.
        request_id: String,
        /// Identifier of the task that was cancelled.
        task_id: String,
    },
    /// Direct RPC error; sent only to the initiating connection.
    #[serde(rename = "error")]
    Error {
        /// Request id when it could be recovered from the incoming frame.
        request_id: Option<String>,
        /// Stable protocol error category.
        code: RpcErrorCode,
        /// Sanitized client-facing description.
        message: String,
        /// Whether retrying the same operation may succeed.
        retryable: bool,
    },
}

pub struct SyncRoom {
    pub devices: HashMap<String, ConnectedDeviceInfo>,
    device_connections: HashMap<String, u64>,
    pub active_device_id: Option<String>,
    pub latest_snapshot: Option<serde_json::Value>,
    pub tx: broadcast::Sender<ServerMessage>,
}

#[derive(Clone, Default)]
pub struct PlaybackSyncHub {
    pub rooms: Arc<Mutex<HashMap<i64, Arc<Mutex<SyncRoom>>>>>,
}

impl PlaybackSyncHub {
    pub fn new() -> Self {
        Self {
            rooms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_or_create_room(&self, telegram_id: i64) -> Arc<Mutex<SyncRoom>> {
        let mut rooms = self.rooms.lock().await;
        rooms
            .entry(telegram_id)
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(128);
                Arc::new(Mutex::new(SyncRoom {
                    devices: HashMap::new(),
                    device_connections: HashMap::new(),
                    active_device_id: None,
                    latest_snapshot: None,
                    tx,
                }))
            })
            .clone()
    }
}

#[derive(Debug, Deserialize)]
pub struct WsAuthQuery {
    pub token: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/ws/playback",
    tag = "ws",
    summary = "Upgrade to the playback synchronization WebSocket",
    description = "This operation documents only the HTTP WebSocket upgrade handshake. The bidirectional WebSocket message contract is documented separately in the [Playback WebSocket AsyncAPI document](/api/v1/docs-ws.json), which is also available from the Scalar API reference.",
    params(
        ("token" = Option<String>, Query, description = "Optional session token query parameter. Prefer the Authorization: Bearer header because query-string tokens can appear in URLs, proxy/access logs, and browser history. A blank query value falls back to the header.")
    ),
    responses(
        (status = 101, description = "WebSocket protocol switch; the connection is upgraded."),
        (status = 401, description = "Unauthorized: token is missing, empty, or invalid.")
    ),
    security(("bearer_auth" = []))
)]
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    Query(query): Query<WsAuthQuery>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let token = if let Some(t) = query.token.filter(|t| !t.trim().is_empty()) {
        t
    } else if let Some(auth_val) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        let stripped = auth_val.strip_prefix("Bearer ").unwrap_or(auth_val).trim();
        if stripped.is_empty() {
            return Err(ServerError::Unauthorized(
                "Empty authorization token".into(),
            ));
        }
        stripped.to_string()
    } else {
        return Err(ServerError::Unauthorized(
            "Missing token query parameter or Authorization header".into(),
        ));
    };

    let telegram_id = if let Some(user) = state.token_cache.get(&token).await {
        user.telegram_id
    } else {
        let identity = state
            .session_mgr
            .verify_and_slide(&token)
            .await
            .map_err(|e| ServerError::Unauthorized(e.to_string()))?;
        let user = crate::auth::AuthedUser {
            telegram_id: identity.telegram_id,
            session_id: identity.session_id,
        };
        state.token_cache.insert(token.clone(), user).await;
        identity.telegram_id
    };

    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state, telegram_id, token)))
}

async fn handle_socket(
    socket: WebSocket,
    state: Arc<ServerState>,
    telegram_id: i64,
    token: String,
) {
    let connection_id = rand::random::<u64>();
    let room_arc = state.sync_hub.get_or_create_room(telegram_id).await;
    let mut rx = {
        let room = room_arc.lock().await;
        room.tx.subscribe()
    };
    let mut task_rx = state.task_sync_tx.subscribe();

    let (mut sender, mut receiver) = socket.split();
    let mut my_device_id: Option<String> = None;
    let mut auth_recheck = tokio::time::interval_at(
        tokio::time::Instant::now() + AUTH_RECHECK_INTERVAL,
        AUTH_RECHECK_INTERVAL,
    );

    loop {
        tokio::select! {
            _ = auth_recheck.tick() => {
                match state.session_mgr.verify_session(&token).await {
                    Ok(identity) if identity.telegram_id == telegram_id => {}
                    _ => {
                        tracing::info!(telegram_id, "Closing playback WebSocket after session verification failed");
                        break;
                    }
                }
            }
            msg_res = rx.recv() => {
                match msg_res {
                    Ok(server_msg) => {
                        if let Ok(json_str) = serde_json::to_string(&server_msg)
                            && sender.send(Message::Text(json_str.into())).await.is_err() {
                                break;
                            }
                    }
                    Err(broadcast::error::RecvError::Lagged(lag)) => {
                        tracing::warn!(telegram_id, lag, "WebSocket subscriber lagged behind");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            task_event = task_rx.recv() => {
                let outgoing = match task_event {
                    Ok(crate::tasks::TaskSyncEvent::Updated { task_id }) if my_device_id.is_some() => {
                        let tasks = state.active_tasks.read();
                        tasks.get(&task_id).map(|task| {
                            let is_owner = task.owner_id == telegram_id;
                            ServerMessage::RipTaskUpdated {
                                task: Box::new(task.snapshot(is_owner)),
                            }
                        })
                    }
                    Ok(crate::tasks::TaskSyncEvent::Dismissed { task_id })
                        if my_device_id.is_some() =>
                    {
                        Some(ServerMessage::RipTaskDismissed { task_id })
                    }
                    Ok(_) => None,
                    Err(broadcast::error::RecvError::Lagged(lag)) => {
                        tracing::warn!(telegram_id, lag, "WebSocket task subscriber lagged behind");
                        my_device_id.as_ref().map(|_| ServerMessage::RipTasksSnapshot {
                            tasks: task_snapshots_for_user(&state, telegram_id),
                        })
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                if let Some(server_msg) = outgoing
                    && let Ok(json_str) = serde_json::to_string(&server_msg)
                    && sender.send(Message::Text(json_str.into())).await.is_err() {
                    break;
                }
            }
            item = receiver.next() => {
                match item {
                    Some(Ok(frame)) => {
                        match frame {
                            Message::Text(text) => {
                                let client_msg: ClientMessage = match serde_json::from_str(&text) {
                                    Ok(m) => m,
                                    Err(_) => {
                                        let error = ServerMessage::Error {
                                            request_id: recover_request_id(&text),
                                            code: RpcErrorCode::Validation,
                                            message: "malformed client message".to_owned(),
                                            retryable: false,
                                        };
                                        if !send_server_message(&mut sender, error).await {
                                            break;
                                        }
                                        continue;
                                    }
                                };

                                match client_msg {
                                    ClientMessage::Hello { device_id, device_name, platform } => {
                                        let mut room = room_arc.lock().await;
                                        if let Some(prev) = my_device_id.replace(device_id.clone())
                                            && prev != device_id {
                                                remove_device_connection(&mut room, &prev, connection_id);
                                            }
                                        room.devices.insert(
                                            device_id.clone(),
                                            ConnectedDeviceInfo {
                                                device_id: device_id.clone(),
                                                device_name,
                                                platform,
                                            },
                                        );
                                        room.device_connections.insert(device_id.clone(), connection_id);
                                        if room.active_device_id.is_none() {
                                            room.active_device_id = Some(device_id);
                                        }
                                        let room_state = ServerMessage::RoomState {
                                            active_device_id: room.active_device_id.clone(),
                                            devices: room.devices.values().cloned().collect(),
                                            snapshot: room.latest_snapshot.clone(),
                                        };
                                        let _ = room.tx.send(room_state);
                                        drop(room);
                                        let task_snapshot = ServerMessage::RipTasksSnapshot {
                                            tasks: task_snapshots_for_user(&state, telegram_id),
                                        };
                                        if let Ok(json_str) = serde_json::to_string(&task_snapshot)
                                            && sender.send(Message::Text(json_str.into())).await.is_err() {
                                                break;
                                            }
                                    }
                                    ClientMessage::ReportState { snapshot } => {
                                        let mut room = room_arc.lock().await;
                                        let is_active = match (&my_device_id, &room.active_device_id) {
                                            (Some(curr), Some(active)) => curr == active,
                                            _ => false,
                                        };
                                        if is_active {
                                            room.latest_snapshot = Some(snapshot.clone());
                                            let update_msg = ServerMessage::StateUpdated {
                                                active_device_id: room.active_device_id.clone(),
                                                snapshot,
                                            };
                                            let _ = room.tx.send(update_msg);
                                        }
                                    }
                                    ClientMessage::Command { action, data } => {
                                        let room = room_arc.lock().await;
                                        let cmd_msg = ServerMessage::ExecuteCommand {
                                            action,
                                            data,
                                        };
                                        let _ = room.tx.send(cmd_msg);
                                    }
                                    ClientMessage::TransferPlayback { target_device_id } => {
                                        let mut room = room_arc.lock().await;
                                        room.active_device_id = Some(target_device_id);
                                        let room_state = ServerMessage::RoomState {
                                            active_device_id: room.active_device_id.clone(),
                                            devices: room.devices.values().cloned().collect(),
                                            snapshot: room.latest_snapshot.clone(),
                                        };
                                        let _ = room.tx.send(room_state);
                                    }
                                    ClientMessage::CreateRipTask { request_id, request } => {
                                        let rpc_request = RipTaskRpcRequest::Create {
                                            request_id,
                                            request,
                                        };
                                        if !dispatch_rip_task_rpc(
                                            &mut sender,
                                            &state,
                                            &token,
                                            telegram_id,
                                            rpc_request,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                    }
                                    ClientMessage::CancelRipTask { request_id, task_id } => {
                                        let rpc_request = RipTaskRpcRequest::Cancel {
                                            request_id,
                                            task_id,
                                        };
                                        if !dispatch_rip_task_rpc(
                                            &mut sender,
                                            &state,
                                            &token,
                                            telegram_id,
                                            rpc_request,
                                        )
                                        .await
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                            Message::Ping(bytes) => {
                                if sender.send(Message::Pong(bytes)).await.is_err() {
                                    break;
                                }
                            }
                            Message::Close(_) => {
                                break;
                            }
                            _ => {}
                        }
                    }
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "WebSocket error received from client");
                        break;
                    }
                    None => {
                        // Client disconnected
                        break;
                    }
                }
            }
        }
    }

    // When socket drops or errors:
    if let Some(dev_id) = my_device_id {
        let mut room = room_arc.lock().await;
        let removed = remove_device_connection(&mut room, &dev_id, connection_id);
        if removed && room.active_device_id.as_deref() == Some(&dev_id) {
            room.active_device_id = room.devices.keys().next().cloned();
        }
        let updated_state = ServerMessage::RoomState {
            active_device_id: room.active_device_id.clone(),
            devices: room.devices.values().cloned().collect(),
            snapshot: room.latest_snapshot.clone(),
        };
        let _ = room.tx.send(updated_state);
    }
}

async fn dispatch_rip_task_rpc(
    sender: &mut SplitSink<WebSocket, Message>,
    state: &Arc<ServerState>,
    token: &str,
    connection_telegram_id: i64,
    request: RipTaskRpcRequest,
) -> bool {
    let request_id = match &request {
        RipTaskRpcRequest::Create { request_id, .. }
        | RipTaskRpcRequest::Cancel { request_id, .. } => request_id.clone(),
    };

    // Never rely on token_cache for operation freshness. Verification is deliberately
    // immediately before entering the service, before it can reserve or cancel work.
    let identity = match state.session_mgr.verify_session(token).await {
        Ok(identity) if identity.telegram_id == connection_telegram_id => AuthenticatedIdentity {
            telegram_id: identity.telegram_id,
        },
        _ => {
            return send_server_message(
                sender,
                ServerMessage::Error {
                    request_id: Some(request_id),
                    code: RpcErrorCode::NotAuthorized,
                    message: "session is no longer authorized".to_owned(),
                    retryable: false,
                },
            )
            .await;
        }
    };

    let reply = match crate::rip_task_rpc::handle_request(identity, request, state.clone()).await {
        Ok(RipTaskRpcSuccess::Created {
            request_id,
            task_id,
            status,
            result_track_id,
        }) => {
            let task = if task_id.is_empty() {
                None
            } else {
                state
                    .active_tasks
                    .read()
                    .get(&task_id)
                    .map(|task| Box::new(task.snapshot(task.owner_id == identity.telegram_id)))
            };
            ServerMessage::RipTaskCreated {
                request_id,
                task_id,
                status: status.into(),
                result_track_id,
                task,
            }
        }
        Ok(RipTaskRpcSuccess::Cancelled {
            request_id,
            task_id,
        }) => ServerMessage::RipTaskCancelled {
            request_id,
            task_id,
        },
        Err(RipTaskRpcError {
            request_id,
            code,
            message,
            retryable,
        }) => ServerMessage::Error {
            request_id,
            code: code.into(),
            message,
            retryable,
        },
    };
    send_server_message(sender, reply).await
}

async fn send_server_message(
    sender: &mut SplitSink<WebSocket, Message>,
    message: ServerMessage,
) -> bool {
    match serde_json::to_string(&message) {
        Ok(json) => sender.send(Message::Text(json.into())).await.is_ok(),
        Err(_) => false,
    }
}

fn recover_request_id(text: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    value
        .get("payload")?
        .get("request_id")?
        .as_str()
        .map(str::to_owned)
}

fn task_snapshots_for_user(
    state: &ServerState,
    telegram_id: i64,
) -> Vec<crate::tasks::RipTaskSnapshot> {
    let tasks = state.active_tasks.read();
    let mut snapshots = tasks
        .values()
        .map(|task| {
            let is_owner = task.owner_id == telegram_id;
            (task.created_at, task.snapshot(is_owner))
        })
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|(created_at, _)| std::cmp::Reverse(*created_at));
    snapshots
        .into_iter()
        .map(|(_, snapshot)| snapshot)
        .collect()
}

fn remove_device_connection(room: &mut SyncRoom, device_id: &str, connection_id: u64) -> bool {
    if room.device_connections.get(device_id) != Some(&connection_id) {
        return false;
    }
    room.device_connections.remove(device_id);
    room.devices.remove(device_id).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_message_serde() {
        let hello_json = r#"{"type":"hello","payload":{"device_id":"d1","device_name":"Pixel","platform":"android"}}"#;
        let msg: ClientMessage = serde_json::from_str(hello_json).unwrap();
        match msg {
            ClientMessage::Hello {
                device_id,
                device_name,
                platform,
            } => {
                assert_eq!(device_id, "d1");
                assert_eq!(device_name, "Pixel");
                assert_eq!(platform, "android");
            }
            _ => panic!("Expected Hello variant"),
        }

        let report_json = r#"{"type":"report_state","payload":{"snapshot":{"playing":true}}}"#;
        let msg: ClientMessage = serde_json::from_str(report_json).unwrap();
        match msg {
            ClientMessage::ReportState { snapshot } => {
                assert_eq!(snapshot["playing"], true);
            }
            _ => panic!("Expected ReportState variant"),
        }

        let cmd_json = r#"{"type":"command","payload":{"action":"play","data":null}}"#;
        let msg: ClientMessage = serde_json::from_str(cmd_json).unwrap();
        match msg {
            ClientMessage::Command { action, data } => {
                assert_eq!(action, "play");
                assert!(data.is_none());
            }
            _ => panic!("Expected Command variant"),
        }

        let transfer_json = r#"{"type":"transfer_playback","payload":{"target_device_id":"d2"}}"#;
        let msg: ClientMessage = serde_json::from_str(transfer_json).unwrap();
        match msg {
            ClientMessage::TransferPlayback { target_device_id } => {
                assert_eq!(target_device_id, "d2");
            }
            _ => panic!("Expected TransferPlayback variant"),
        }

        let create = ClientMessage::CreateRipTask {
            request_id: "create-1".to_owned(),
            request: crate::tasks::RipTaskRequest {
                provider: "apple".to_owned(),
                track_id: "123".to_owned(),
                codec: Some("flac".to_owned()),
                title: None,
                artist: None,
                album: None,
                duration: None,
            },
        };
        let round_trip: ClientMessage =
            serde_json::from_str(&serde_json::to_string(&create).unwrap()).unwrap();
        match round_trip {
            ClientMessage::CreateRipTask {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "create-1");
                assert_eq!(request.provider, "apple");
                assert_eq!(request.track_id, "123");
                assert_eq!(request.codec.as_deref(), Some("flac"));
            }
            _ => panic!("Expected CreateRipTask variant"),
        }

        let cancel = ClientMessage::CancelRipTask {
            request_id: "cancel-1".to_owned(),
            task_id: "task-1".to_owned(),
        };
        let round_trip: ClientMessage =
            serde_json::from_str(&serde_json::to_string(&cancel).unwrap()).unwrap();
        match round_trip {
            ClientMessage::CancelRipTask {
                request_id,
                task_id,
            } => {
                assert_eq!(request_id, "cancel-1");
                assert_eq!(task_id, "task-1");
            }
            _ => panic!("Expected CancelRipTask variant"),
        }
    }

    #[test]
    fn test_server_message_serde() {
        let server_msg = ServerMessage::RoomState {
            active_device_id: Some("d1".to_string()),
            devices: vec![ConnectedDeviceInfo {
                device_id: "d1".to_string(),
                device_name: "Pixel".to_string(),
                platform: "android".to_string(),
            }],
            snapshot: Some(serde_json::json!({"track_id": 42})),
        };
        let serialized = serde_json::to_string(&server_msg).unwrap();
        let parsed: ServerMessage = serde_json::from_str(&serialized).unwrap();
        assert_eq!(server_msg, parsed);

        let new_variants = [
            ServerMessage::RipTaskCreated {
                request_id: "create-1".to_owned(),
                task_id: "task-1".to_owned(),
                status: RipTaskStatus::Queued,
                result_track_id: None,
                task: None,
            },
            ServerMessage::RipTaskCreated {
                request_id: "create-2".to_owned(),
                task_id: String::new(),
                status: RipTaskStatus::Completed,
                result_track_id: Some(42),
                task: None,
            },
            ServerMessage::RipTaskCancelled {
                request_id: "cancel-1".to_owned(),
                task_id: "task-1".to_owned(),
            },
            ServerMessage::Error {
                request_id: Some("error-1".to_owned()),
                code: RpcErrorCode::Unavailable,
                message: "temporarily unavailable".to_owned(),
                retryable: true,
            },
            ServerMessage::Error {
                request_id: None,
                code: RpcErrorCode::Validation,
                message: "malformed client message".to_owned(),
                retryable: false,
            },
        ];
        for message in new_variants {
            let serialized = serde_json::to_string(&message).unwrap();
            let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
            let expected_type = match &message {
                ServerMessage::RipTaskCreated { .. } => "rip_task_created",
                ServerMessage::RipTaskCancelled { .. } => "rip_task_cancelled",
                ServerMessage::Error { .. } => "error",
                _ => unreachable!(),
            };
            assert_eq!(value["type"], expected_type);
            let parsed: ServerMessage = serde_json::from_str(&serialized).unwrap();
            assert_eq!(message, parsed);
        }
    }

    #[test]
    fn stale_connection_cannot_remove_its_replacement() {
        let (tx, _) = broadcast::channel(4);
        let mut room = SyncRoom {
            devices: HashMap::from([(
                "desktop".to_string(),
                ConnectedDeviceInfo {
                    device_id: "desktop".to_string(),
                    device_name: "Workstation".to_string(),
                    platform: "Linux".to_string(),
                },
            )]),
            device_connections: HashMap::from([("desktop".to_string(), 2)]),
            active_device_id: Some("desktop".to_string()),
            latest_snapshot: None,
            tx,
        };

        assert!(!remove_device_connection(&mut room, "desktop", 1));
        assert!(room.devices.contains_key("desktop"));
    }
}
