use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{FromRef, FromRequestParts, State},
    http::{HeaderValue, header, request::Parts},
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, error::ServerError};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AuthedUser {
    pub telegram_id: i64,
    pub session_id: String,
}

impl<S> FromRequestParts<S> for AuthedUser
where
    S: Send + Sync,
    Arc<ServerState>: FromRef<S>,
{
    type Rejection = ServerError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let server_state = Arc::<ServerState>::from_ref(state);

        let auth_header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ServerError::Unauthorized("Missing Authorization header".into()))?;

        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| {
                ServerError::Unauthorized(
                    "Invalid Authorization format, expected 'Bearer <token>'".into(),
                )
            })?
            .trim();

        if let Some(user) = server_state.token_cache.get(token).await {
            return Ok(user);
        }

        let identity = server_state
            .session_mgr
            .verify_and_slide(token)
            .await
            .map_err(|e| ServerError::Unauthorized(e.to_string()))?;

        let user = AuthedUser {
            telegram_id: identity.telegram_id,
            session_id: identity.session_id,
        };

        server_state
            .token_cache
            .insert(token.to_string(), user.clone())
            .await;

        Ok(user)
    }
}

pub struct MaybeAuthedUser(pub Option<AuthedUser>);

impl<S> FromRequestParts<S> for MaybeAuthedUser
where
    Arc<ServerState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match AuthedUser::from_request_parts(parts, state).await {
            Ok(user) => Ok(MaybeAuthedUser(Some(user))),
            Err(_) => Ok(MaybeAuthedUser(None)),
        }
    }
}

/// Request payload to exchange a single-use Telegram OTP for session tokens.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ExchangeRequest {
    /// The single-use OTP code generated via Telegram bot `/stream`.
    #[schema(example = "ABC-123")]
    pub code: String,
    /// Client device hardware or model name.
    #[schema(example = "Pixel 8 Pro")]
    pub device_name: Option<String>,
    /// Client operating platform (e.g. android, ios, desktop, linux).
    #[schema(example = "android")]
    pub platform: Option<String>,
}

/// Authenticated user profile.
#[derive(Debug, Serialize, ToSchema)]
pub struct UserDto {
    /// Telegram user ID.
    #[schema(example = 123456789)]
    pub telegram_id: i64,
    /// Telegram first name or display name.
    #[schema(example = "Sayeed")]
    pub name: Option<String>,
    /// Telegram username without the leading `@`, when available.
    #[schema(example = "sayeed")]
    pub username: Option<String>,
    /// Telegram first name, when available.
    pub first_name: Option<String>,
    /// Telegram last name, when available.
    pub last_name: Option<String>,
}

fn profile_display_name(
    first_name: Option<&str>,
    last_name: Option<&str>,
    username: Option<&str>,
    stored_name: Option<String>,
) -> Option<String> {
    let first_name = first_name.map(str::trim).filter(|name| !name.is_empty());
    let last_name = last_name.map(str::trim).filter(|name| !name.is_empty());
    let full_name = [first_name, last_name]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");

    if !full_name.is_empty() {
        return Some(full_name);
    }

    if let Some(username) = username.map(str::trim).filter(|name| !name.is_empty()) {
        return Some(format!("@{username}"));
    }

    stored_name
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

async fn load_user_profile(state: &ServerState, telegram_id: i64) -> UserDto {
    let stored_name = state
        .session_mgr
        .get_user_name(telegram_id)
        .await
        .ok()
        .flatten();

    let telegram_user = if let Some(client) = &state.telegram_client {
        match client.get_users_by_id(&[telegram_id]).await {
            Ok(mut users) => users.pop().flatten(),
            Err(error) => {
                tracing::warn!(telegram_id, error = %error, "failed to refresh Telegram profile");
                None
            }
        }
    } else {
        None
    };

    let first_name = telegram_user
        .as_ref()
        .and_then(|user| user.first_name())
        .map(str::to_owned);
    let last_name = telegram_user
        .as_ref()
        .and_then(|user| user.last_name())
        .map(str::to_owned);
    let username = telegram_user
        .as_ref()
        .and_then(|user| user.username())
        .map(str::to_owned);
    let telegram_name = profile_display_name(
        first_name.as_deref(),
        last_name.as_deref(),
        username.as_deref(),
        None,
    );
    let name = if let Some(telegram_name) = telegram_name {
        if stored_name.as_deref() != Some(telegram_name.as_str())
            && let Err(error) = state
                .session_mgr
                .update_user_name_if_changed(telegram_id, &telegram_name)
                .await
        {
            tracing::warn!(telegram_id, error = %error, "failed to sync Telegram display name");
        }
        Some(telegram_name)
    } else {
        stored_name
    };

    UserDto {
        telegram_id,
        name,
        username,
        first_name,
        last_name,
    }
}

/// Token exchange response containing sliding access tokens and user profile.
#[derive(Debug, Serialize, ToSchema)]
pub struct ExchangeResponse {
    /// Standard Bearer token type.
    #[schema(example = "Bearer")]
    pub token_type: &'static str,
    /// Raw token string (AdonisJS opaque token).
    pub token: String,
    /// Access token for Bearer Authorization headers.
    pub access_token: String,
    /// Refresh token used to slide sessions forward.
    pub refresh_token: String,
    /// Number of seconds until session expiry (default 259,200s / 3 days).
    #[schema(example = 259200)]
    pub expires_in: i64,
    /// ISO-8601 UTC expiration timestamp.
    pub expires_at: DateTime<Utc>,
    /// Unix epoch timestamp (seconds) when the token expires.
    #[schema(example = 1742468000)]
    pub expires_at_unix: i64,
    /// Authenticated user summary.
    pub user: UserDto,
}

/// Request payload to refresh an existing sliding session.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RefreshRequest {
    /// Raw token to slide forward.
    pub token: Option<String>,
    /// Refresh token to slide forward.
    pub refresh_token: Option<String>,
}

/// Response returned when a sliding session is refreshed.
#[derive(Debug, Serialize, ToSchema)]
pub struct RefreshResponse {
    /// Standard Bearer token type.
    #[schema(example = "Bearer")]
    pub token_type: &'static str,
    /// Raw token string.
    pub token: String,
    /// Refreshed access token.
    pub access_token: String,
    /// Refreshed refresh token.
    pub refresh_token: String,
    /// Number of seconds until session expiry.
    #[schema(example = 259200)]
    pub expires_in: i64,
    /// ISO-8601 UTC expiration timestamp.
    pub expires_at: DateTime<Utc>,
    /// Unix epoch timestamp (seconds) when the token expires.
    #[schema(example = 1742468000)]
    pub expires_at_unix: i64,
}

/// Request payload to revoke an active session.
#[derive(Debug, Deserialize, ToSchema)]
pub struct LogoutRequest {
    /// Token to revoke.
    pub token: Option<String>,
    /// Refresh token to revoke.
    pub refresh_token: Option<String>,
}

/// Details of an active user session device.
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionDto {
    /// Unique session identifier.
    #[schema(example = "session_01h7x...")]
    pub id: String,
    /// Registered device name.
    #[schema(example = "MacBook Pro")]
    pub device_name: Option<String>,
    /// Registered operating platform.
    #[schema(example = "macos")]
    pub platform: Option<String>,
    /// Last recorded active request timestamp.
    pub last_active_at: DateTime<Utc>,
    /// Session expiration timestamp.
    pub expires_at: DateTime<Utc>,
}

/// Response containing current user profile and active device sessions.
#[derive(Debug, Serialize, ToSchema)]
pub struct MeResponse {
    /// User profile.
    pub user: UserDto,
    /// List of all active device sessions for this user.
    pub sessions: Vec<SessionDto>,
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/exchange",
    tag = "auth",
    summary = "Exchange OTP for Session Tokens",
    description = "Exchanges a single-use Telegram OTP code generated by `/stream` for sliding session tokens (`access_token`, `refresh_token`, `expires_at_unix`).",
    request_body = ExchangeRequest,
    responses(
        (status = 200, description = "Code successfully exchanged for session tokens", body = ExchangeResponse),
        (status = 401, description = "Invalid or expired OTP code")
    )
)]
pub async fn exchange(
    State(state): State<Arc<ServerState>>,
    Json(payload): Json<ExchangeRequest>,
) -> Result<Json<ExchangeResponse>, ServerError> {
    let metadata = db::ClientMetadata {
        device_name: payload.device_name.as_deref(),
        platform: payload.platform.as_deref(),
    };

    let session = state
        .session_mgr
        .exchange_code(&payload.code, metadata)
        .await
        .map_err(|e| ServerError::Unauthorized(e.to_string()))?;

    let authed_user = AuthedUser {
        telegram_id: session.telegram_id,
        session_id: session.session_id,
    };

    state
        .token_cache
        .insert(session.refresh_token.clone(), authed_user)
        .await;

    let user = load_user_profile(&state, session.telegram_id).await;

    let expires_at_unix = session.expires_at.timestamp();

    Ok(Json(ExchangeResponse {
        token_type: "Bearer",
        token: session.refresh_token.clone(),
        access_token: session.refresh_token.clone(),
        refresh_token: session.refresh_token,
        expires_in: 259200,
        expires_at: session.expires_at,
        expires_at_unix,
        user,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/refresh",
    tag = "auth",
    summary = "Refresh Session Tokens",
    description = "Extends the user session forward by 3 days and issues refreshed sliding session tokens.",
    request_body = RefreshRequest,
    responses(
        (status = 200, description = "Token renewed and slid forward by 3 days", body = RefreshResponse),
        (status = 401, description = "Invalid, expired, or revoked token")
    )
)]
pub async fn refresh(
    State(state): State<Arc<ServerState>>,
    Json(payload): Json<RefreshRequest>,
) -> Result<Json<RefreshResponse>, ServerError> {
    let token = payload
        .refresh_token
        .or(payload.token)
        .ok_or_else(|| ServerError::BadRequest("Missing token or refresh_token".into()))?;

    let identity = state
        .session_mgr
        .verify_and_slide(&token)
        .await
        .map_err(|e| ServerError::Unauthorized(e.to_string()))?;

    let authed_user = AuthedUser {
        telegram_id: identity.telegram_id,
        session_id: identity.session_id,
    };

    state.token_cache.insert(token.clone(), authed_user).await;

    let expires_at_unix = identity.expires_at.timestamp();

    Ok(Json(RefreshResponse {
        token_type: "Bearer",
        token: token.clone(),
        access_token: token.clone(),
        refresh_token: token,
        expires_in: 259200,
        expires_at: identity.expires_at,
        expires_at_unix,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/auth/logout",
    tag = "auth",
    summary = "Revoke Session (Logout)",
    description = "Revokes an active session from PostgreSQL and evicts it from the in-memory token cache. Can be called either with an `Authorization: Bearer <token>` header or with `{ \"refresh_token\": \"...\" }` in the JSON request body.",
    request_body = LogoutRequest,
    responses(
        (status = 200, description = "Session successfully revoked"),
        (status = 401, description = "Missing token or authorization")
    ),
    security(
        (),
        ("bearer_auth" = [])
    )
)]
pub async fn logout(
    State(state): State<Arc<ServerState>>,
    MaybeAuthedUser(maybe_user): MaybeAuthedUser,
    Json(payload): Json<LogoutRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let token_to_revoke = payload.refresh_token.or(payload.token);
    if let Some(token) = token_to_revoke {
        state.token_cache.remove(&token).await;
        let _ = state.session_mgr.revoke(&token).await;
        Ok(Json(serde_json::json!({ "revoked": true })))
    } else if let Some(user) = maybe_user {
        // Revoke all sessions for current user if no specific token given
        state.token_cache.invalidate_all();
        let _ = state
            .session_mgr
            .revoke_all_for_user(user.telegram_id)
            .await;
        Ok(Json(serde_json::json!({ "revoked": true })))
    } else {
        Err(ServerError::Unauthorized(
            "Missing refresh_token or Bearer authorization header".into(),
        ))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/auth/me",
    tag = "auth",
    summary = "Get Authenticated User Profile & Active Sessions",
    description = "Returns current authenticated user profile and all active device sessions registered in the database.",
    responses(
        (status = 200, description = "Current authenticated user profile and sessions", body = MeResponse),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn me(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Json<MeResponse>, ServerError> {
    let db_sessions = state
        .session_mgr
        .list_active_sessions(user.telegram_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    let sessions = db_sessions
        .into_iter()
        .map(|s| SessionDto {
            id: s.id,
            device_name: s.device_name,
            platform: s.platform,
            last_active_at: s.last_active_at,
            expires_at: s.expires_at,
        })
        .collect();

    Ok(Json(MeResponse {
        user: load_user_profile(&state, user.telegram_id).await,
        sessions,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/auth/me/avatar",
    tag = "auth",
    summary = "Get the authenticated Telegram user's profile photo",
    responses(
        (status = 200, description = "Telegram profile photo", content_type = "image/jpeg"),
        (status = 404, description = "No Telegram profile photo is available"),
        (status = 401, description = "Unauthorized")
    ),
    security(("bearer_auth" = []))
)]
pub async fn me_avatar(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Response, ServerError> {
    let Some(client) = &state.telegram_client else {
        return Err(ServerError::NotFound(
            "Telegram profile photo is unavailable".into(),
        ));
    };

    let avatar = if let Some(cached) = state.avatar_cache.get(&user.telegram_id).await {
        cached
    } else {
        let photos = client
            .get_profile_photos(ferogram::PeerRef::from(user.telegram_id), 1)
            .await
            .map_err(|error| {
                tracing::warn!(telegram_id = user.telegram_id, error = %error, "failed to fetch Telegram profile photo");
                ServerError::Internal("Failed to retrieve Telegram profile photo".into())
            })?;

        let raw_photo = photos.into_iter().next().and_then(|photo| match photo {
            ferogram::tl::enums::Photo::Photo(photo) => Some(photo),
            ferogram::tl::enums::Photo::Empty(_) => None,
        });

        let photo_bytes = if let Some(raw_photo) = raw_photo {
            let photo = ferogram::media::Photo::from_raw(raw_photo);
            let thumbnail = photo
                .thumb("s")
                .or_else(|| photo.thumb(photo.largest_thumb_type()));
            if let Some(thumbnail) = thumbnail {
                let mut bytes = Vec::new();
                client
                    .download(&thumbnail, &mut bytes, None)
                    .await
                    .map_err(|error| {
                        tracing::warn!(telegram_id = user.telegram_id, error = %error, "failed to download Telegram profile photo");
                        ServerError::Internal("Failed to download Telegram profile photo".into())
                    })?;
                (!bytes.is_empty()).then(|| Arc::new(bytes))
            } else {
                None
            }
        } else {
            None
        };
        state
            .avatar_cache
            .insert(user.telegram_id, photo_bytes.clone())
            .await;
        photo_bytes
    };

    let Some(avatar) = avatar else {
        return Err(ServerError::NotFound(
            "Telegram profile photo is unavailable".into(),
        ));
    };

    let mut response = Response::new(Body::from(avatar.as_ref().clone()));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("image/jpeg"));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=900"),
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::profile_display_name;

    #[test]
    fn profile_display_name_prefers_current_telegram_name_over_stored_name() {
        assert_eq!(
            profile_display_name(
                Some("Sayeed"),
                Some("Hitarashi"),
                Some("sayeeddev"),
                Some("Old database name".to_string())
            ),
            Some("Sayeed Hitarashi".to_string())
        );
    }

    #[test]
    fn profile_display_name_falls_back_to_username_then_stored_name() {
        assert_eq!(
            profile_display_name(None, None, Some("sayeeddev"), Some("Stored".to_string())),
            Some("@sayeeddev".to_string())
        );
        assert_eq!(
            profile_display_name(None, None, None, Some(" Stored ".to_string())),
            Some("Stored".to_string())
        );
    }
}
