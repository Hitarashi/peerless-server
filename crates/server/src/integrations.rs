use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{delete, get, post},
};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, auth::AuthedUser, error::ServerError};

#[derive(Debug, Deserialize, ToSchema)]
pub struct LastfmLoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LastfmStatusResponse {
    pub connected: bool,
    pub username: Option<String>,
    /// Raw Last.fm session key returned to the client in this response when connected.
    pub session_key: Option<String>,
    /// Last.fm API key from `LASTFM_API_KEY`, included in this response when configured.
    pub api_key: Option<String>,
    /// Last.fm API shared secret from `LASTFM_SHARED_SECRET`, included in this response when configured.
    pub api_secret: Option<String>,
}

pub fn lastfm_credentials() -> Result<(String, String), ServerError> {
    let api_key = std::env::var("LASTFM_API_KEY").map_err(|_| {
        ServerError::Internal("LASTFM_API_KEY is not configured in environment".into())
    })?;
    let api_secret = std::env::var("LASTFM_SHARED_SECRET").map_err(|_| {
        ServerError::Internal("LASTFM_SHARED_SECRET is not configured in environment".into())
    })?;
    Ok((api_key, api_secret))
}

pub fn compute_api_sig(
    api_key: &str,
    method: &str,
    password: &str,
    username: &str,
    api_secret: &str,
) -> String {
    let raw =
        format!("api_key{api_key}method{method}password{password}username{username}{api_secret}");
    let mut hasher = Md5::new();
    hasher.update(raw.as_bytes());
    let result = hasher.finalize();
    result.iter().map(|b| format!("{b:02x}")).collect()
}

#[utoipa::path(
    post,
    path = "/api/v1/integrations/lastfm/login",
    tag = "integrations",
    summary = "Connect Last.fm Account",
    description = "Authenticates with Last.fm using auth.getMobileSession and stores the session key encrypted at rest. The response body includes the raw Last.fm session key, API key, and API shared secret.",
    request_body = LastfmLoginRequest,
    responses(
        (status = 200, description = "Connected account and the raw Last.fm session key and configured API credentials", body = LastfmStatusResponse),
        (status = 400, description = "Invalid credentials or Last.fm authentication error"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while contacting Last.fm, loading credentials, encrypting the session key, or saving the integration")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn login(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Json(req): Json<LastfmLoginRequest>,
) -> Result<Json<LastfmStatusResponse>, ServerError> {
    let (api_key, api_secret) = lastfm_credentials()?;
    let method = "auth.getMobileSession";
    let api_sig = compute_api_sig(&api_key, method, &req.password, &req.username, &api_secret);

    let form_data = [
        ("api_key", api_key.as_str()),
        ("method", method),
        ("password", req.password.as_str()),
        ("username", req.username.as_str()),
        ("api_sig", api_sig.as_str()),
    ];

    let response = state
        .http_client
        .post("https://ws.audioscrobbler.com/2.0/?format=json")
        .form(&form_data)
        .send()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to connect to Last.fm: {e}")))?;

    let json_val: serde_json::Value = response
        .json()
        .await
        .map_err(|e| ServerError::Internal(format!("Failed to parse Last.fm response: {e}")))?;

    if let Some(err_msg) = json_val.get("message").and_then(|m| m.as_str())
        && json_val.get("error").is_some()
    {
        return Err(ServerError::BadRequest(format!("Last.fm error: {err_msg}")));
    }

    let session = json_val.get("session").ok_or_else(|| {
        ServerError::BadRequest("Missing 'session' object in Last.fm response".into())
    })?;

    let session_name = session
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or(&req.username);

    let session_key = session.get("key").and_then(|k| k.as_str()).ok_or_else(|| {
        ServerError::BadRequest("Missing 'key' in Last.fm session response".into())
    })?;

    let cipher = db::crypto::CryptoCipher::new(&state.app_key)
        .map_err(|e| ServerError::Internal(format!("Cipher init error: {e}")))?;

    let encrypted_session_key = cipher
        .encrypt(session_key)
        .map_err(|e| ServerError::Internal(format!("Encryption error: {e}")))?;

    db::integrations::save_integration(
        &state.db,
        user.telegram_id,
        "lastfm",
        session_name,
        &encrypted_session_key,
    )
    .await?;

    Ok(Json(LastfmStatusResponse {
        connected: true,
        username: Some(session_name.to_string()),
        session_key: Some(session_key.to_string()),
        api_key: Some(api_key),
        api_secret: Some(api_secret),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/integrations/lastfm/status",
    tag = "integrations",
    summary = "Get Last.fm Connection Status",
    description = "Checks if the authenticated user has connected their Last.fm account, returning decrypted credentials if available.",
    responses(
        (status = 200, description = "Integration status, decrypted session key when connected, and configured API credentials", body = LastfmStatusResponse),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while retrieving the integration or decrypting its session key")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn status(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Json<LastfmStatusResponse>, ServerError> {
    let (api_key, api_secret) = match lastfm_credentials().ok() {
        Some((k, s)) => (Some(k), Some(s)),
        None => (None, None),
    };
    let integration =
        db::integrations::get_integration(&state.db, user.telegram_id, "lastfm").await?;

    match integration {
        Some(int) => {
            let cipher = db::crypto::CryptoCipher::new(&state.app_key)
                .map_err(|e| ServerError::Internal(format!("Cipher init error: {e}")))?;
            let decrypted_key = cipher
                .decrypt(&int.encrypted_session_key)
                .map_err(|e| ServerError::Internal(format!("Decryption error: {e}")))?;

            Ok(Json(LastfmStatusResponse {
                connected: true,
                username: Some(int.username),
                session_key: decrypted_key,
                api_key,
                api_secret,
            }))
        }
        None => Ok(Json(LastfmStatusResponse {
            connected: false,
            username: None,
            session_key: None,
            api_key,
            api_secret,
        })),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v1/integrations/lastfm",
    tag = "integrations",
    summary = "Disconnect Last.fm Account",
    description = "Removes the stored Last.fm integration for the authenticated user, if one is present.",
    responses(
        (status = 204, description = "Last.fm integration removed if present"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while deleting the integration")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn disconnect(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<StatusCode, ServerError> {
    db::integrations::delete_integration(&state.db, user.telegram_id, "lastfm").await?;
    Ok(StatusCode::NO_CONTENT)
}

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/login", post(login))
        .route("/status", get(status))
        .route("/", delete(disconnect))
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_api_sig() {
        let sig = compute_api_sig(
            "test_api_key",
            "auth.getMobileSession",
            "test_password",
            "test_user",
            "test_secret",
        );
        // MD5 of "api_keytest_api_keymethodauth.getMobileSessionpasswordtest_passwordusernametest_usertest_secret"
        assert_eq!(sig.len(), 32);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));

        // Check determinism
        let sig2 = compute_api_sig(
            "test_api_key",
            "auth.getMobileSession",
            "test_password",
            "test_user",
            "test_secret",
        );
        assert_eq!(sig, sig2);
    }
}
