use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, auth::AuthedUser, error::ServerError};

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListenbrainzLoginRequest {
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListenbrainzStatusResponse {
    pub connected: bool,
    pub username: Option<String>,
    pub token: Option<String>,
}

#[utoipa::path(
    post,
    path = "/api/v1/integrations/listenbrainz/login",
    tag = "integrations",
    summary = "Connect ListenBrainz Account",
    description = "Validates the user token against the ListenBrainz API and stores it encrypted at rest using AES-256-GCM.",
    request_body = ListenbrainzLoginRequest,
    responses(
        (status = 200, description = "Connected account and decrypted user token", body = ListenbrainzStatusResponse),
        (status = 400, description = "Invalid token or validation error"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error validating token or saving integration")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn listenbrainz_login(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Json(req): Json<ListenbrainzLoginRequest>,
) -> Result<Json<ListenbrainzStatusResponse>, ServerError> {
    let clean_token = req.token.trim();
    if clean_token.is_empty() {
        return Err(ServerError::BadRequest("Token cannot be empty".into()));
    }

    let response = state
        .http_client
        .get("https://api.listenbrainz.org/1/validate-token")
        .header("Authorization", format!("Token {clean_token}"))
        .send()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to connect to ListenBrainz: {e}")))?;

    let json_val: serde_json::Value = response.json().await.map_err(|e| {
        ServerError::Internal(format!("Failed to parse ListenBrainz response: {e}"))
    })?;

    let is_valid = json_val
        .get("valid")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !is_valid {
        let msg = json_val
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("Invalid ListenBrainz token");
        return Err(ServerError::BadRequest(msg.into()));
    }

    let user_name = json_val
        .get("user_name")
        .and_then(|u| u.as_str())
        .ok_or_else(|| {
            ServerError::Internal("Missing user_name in ListenBrainz response".into())
        })?;

    let cipher = db::crypto::CryptoCipher::new(&state.app_key)
        .map_err(|e| ServerError::Internal(format!("Cipher init error: {e}")))?;

    let encrypted_token = cipher
        .encrypt(clean_token)
        .map_err(|e| ServerError::Internal(format!("Encryption error: {e}")))?;

    db::integrations::save_integration(
        &state.db,
        user.telegram_id,
        "listenbrainz",
        user_name,
        &encrypted_token,
    )
    .await?;

    Ok(Json(ListenbrainzStatusResponse {
        connected: true,
        username: Some(user_name.to_string()),
        token: Some(clean_token.to_string()),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/integrations/listenbrainz/status",
    tag = "integrations",
    summary = "Get ListenBrainz Connection Status",
    description = "Checks if the authenticated user has connected their ListenBrainz account, returning decrypted token if available.",
    responses(
        (status = 200, description = "Integration status and decrypted token when connected", body = ListenbrainzStatusResponse),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while retrieving the integration or decrypting its token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn listenbrainz_status(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Json<ListenbrainzStatusResponse>, ServerError> {
    let integration =
        db::integrations::get_integration(&state.db, user.telegram_id, "listenbrainz").await?;

    match integration {
        Some(int) => {
            let cipher = db::crypto::CryptoCipher::new(&state.app_key)
                .map_err(|e| ServerError::Internal(format!("Cipher init error: {e}")))?;
            let decrypted_token = cipher
                .decrypt(&int.encrypted_session_key)
                .map_err(|e| ServerError::Internal(format!("Decryption error: {e}")))?;

            Ok(Json(ListenbrainzStatusResponse {
                connected: true,
                username: Some(int.username),
                token: decrypted_token,
            }))
        }
        None => Ok(Json(ListenbrainzStatusResponse {
            connected: false,
            username: None,
            token: None,
        })),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/integrations/listenbrainz",
    tag = "integrations",
    summary = "Disconnect ListenBrainz Account",
    description = "Removes the stored ListenBrainz integration for the authenticated user, if one is present.",
    responses(
        (status = 204, description = "ListenBrainz integration removed if present"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while deleting the integration")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn listenbrainz_disconnect(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<StatusCode, ServerError> {
    db::integrations::delete_integration(&state.db, user.telegram_id, "listenbrainz").await?;
    Ok(StatusCode::NO_CONTENT)
}

pub fn listenbrainz_router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/login", post(listenbrainz_login))
        .route("/status", get(listenbrainz_status))
        .route("/", delete(listenbrainz_disconnect))
}
