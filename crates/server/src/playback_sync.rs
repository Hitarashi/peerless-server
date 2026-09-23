use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};

use crate::{ServerState, error::ServerError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectedDeviceInfo {
    pub device_id: String,
    pub device_name: String,
    pub platform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ClientMessage {
    #[serde(rename = "hello")]
    Hello {
        device_id: String,
        device_name: String,
        platform: String,
    },
    #[serde(rename = "report_state")]
    ReportState { snapshot: serde_json::Value },
    #[serde(rename = "command")]
    Command {
        action: String, // "play", "pause", "seek", "next", "prev", "select_track"
        #[serde(default)]
        data: Option<serde_json::Value>,
    },
    #[serde(rename = "transfer_playback")]
    TransferPlayback { target_device_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload")]
pub enum ServerMessage {
    #[serde(rename = "room_state")]
    RoomState {
        active_device_id: Option<String>,
        devices: Vec<ConnectedDeviceInfo>,
        snapshot: Option<serde_json::Value>,
    },
    #[serde(rename = "state_updated")]
    StateUpdated {
        active_device_id: Option<String>,
        snapshot: serde_json::Value,
    },
    #[serde(rename = "execute_command")]
    ExecuteCommand {
        action: String,
        data: Option<serde_json::Value>,
    },
    #[serde(rename = "rip_tasks_snapshot")]
    RipTasksSnapshot {
        tasks: Vec<crate::tasks::RipTaskSnapshot>,
    },
    #[serde(rename = "rip_task_updated")]
    RipTaskUpdated { task: crate::tasks::RipTaskSnapshot },
    #[serde(rename = "rip_task_dismissed")]
    RipTaskDismissed { task_id: String },
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
        state.token_cache.insert(token, user).await;
        identity.telegram_id
    };

    Ok(ws.on_upgrade(move |socket| handle_socket(socket, state, telegram_id)))
}

async fn handle_socket(socket: WebSocket, state: Arc<ServerState>, telegram_id: i64) {
    let connection_id = rand::random::<u64>();
    let room_arc = state.sync_hub.get_or_create_room(telegram_id).await;
    let mut rx = {
        let room = room_arc.lock().await;
        room.tx.subscribe()
    };
    let mut task_rx = state.task_sync_tx.subscribe();

    let (mut sender, mut receiver) = socket.split();
    let mut my_device_id: Option<String> = None;

    loop {
        tokio::select! {
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
                                task: task.snapshot(is_owner),
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
                                    Err(e) => {
                                        tracing::warn!(error = %e, "Invalid ClientMessage JSON received");
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
