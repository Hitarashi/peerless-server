use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Query, State},
    http::{Method, StatusCode, header},
    response::Response,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as BASE64URL};
use chrono::Utc;
use hmac::{Hmac, Mac, digest::KeyInit};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use utoipa::{IntoParams, ToSchema};

use crate::{ServerState, auth::AuthedUser, error::ServerError};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamTicket {
    pub track_id: i32,
    pub user_id: i64,
    pub expires_at: i64,
}

impl StreamTicket {
    pub fn new(track_id: i32, user_id: i64, ttl_secs: i64) -> Self {
        Self {
            track_id,
            user_id,
            expires_at: Utc::now().timestamp() + ttl_secs,
        }
    }

    fn payload(&self) -> String {
        format!("{}:{}:{}", self.track_id, self.user_id, self.expires_at)
    }

    pub fn encode(&self, secret: &str) -> String {
        let payload = self.payload();
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(payload.as_bytes());
        let sig = mac.finalize().into_bytes();
        let full = format!("{payload}:{}", BASE64URL.encode(sig));
        BASE64URL.encode(full)
    }

    pub fn decode(secret: &str, ticket_str: &str) -> Result<Self, ServerError> {
        let decoded = BASE64URL
            .decode(ticket_str.trim())
            .map_err(|_| ServerError::Unauthorized("Invalid ticket encoding".into()))?;
        let ticket_payload = String::from_utf8(decoded)
            .map_err(|_| ServerError::Unauthorized("Invalid ticket format".into()))?;

        let parts: Vec<&str> = ticket_payload.split(':').collect();
        if parts.len() != 4 {
            return Err(ServerError::Unauthorized("Malformed stream ticket".into()));
        }

        let track_id: i32 = parts[0]
            .parse()
            .map_err(|_| ServerError::Unauthorized("Invalid track ID in ticket".into()))?;
        let user_id: i64 = parts[1]
            .parse()
            .map_err(|_| ServerError::Unauthorized("Invalid user ID in ticket".into()))?;
        let expires_at: i64 = parts[2]
            .parse()
            .map_err(|_| ServerError::Unauthorized("Invalid expiry in ticket".into()))?;
        let provided_sig = parts[3];

        if Utc::now().timestamp() > expires_at {
            return Err(ServerError::Unauthorized(
                "Stream ticket has expired".into(),
            ));
        }

        let ticket = Self {
            track_id,
            user_id,
            expires_at,
        };
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(ticket.payload().as_bytes());

        let decoded_sig = BASE64URL
            .decode(provided_sig)
            .map_err(|_| ServerError::Unauthorized("Invalid ticket signature format".into()))?;

        mac.verify_slice(&decoded_sig)
            .map_err(|_| ServerError::Unauthorized("Invalid stream ticket signature".into()))?;

        Ok(ticket)
    }
}

pub fn create_stream_ticket(secret: &str, track_id: i32, user_id: i64, ttl_secs: i64) -> String {
    StreamTicket::new(track_id, user_id, ttl_secs).encode(secret)
}

pub fn verify_stream_ticket(secret: &str, ticket: &str) -> Result<(i32, i64), ServerError> {
    let t = StreamTicket::decode(secret, ticket)?;
    Ok((t.track_id, t.user_id))
}

pub use create_stream_ticket as create_playback_ticket;
pub use verify_stream_ticket as verify_playback_ticket;

/// Playback metadata and signed stream URL returned to audio player clients.
#[derive(Debug, Serialize, ToSchema)]
pub struct PlaybackInfo {
    /// Signed playback URL pointing to `/api/v1/stream?ticket=...`.
    #[schema(example = "/api/v1/stream?ticket=eyJhbGciOi...")]
    pub stream_url: String,
    /// Number of seconds the stream ticket remains valid (7,200s / 2 hours).
    #[schema(example = 7200)]
    pub expires_in: i64,
    /// MIME type of the lossless audio container (e.g. `audio/mp4` for ALAC, `audio/flac` for FLAC).
    #[schema(example = "audio/mp4")]
    pub mime_type: &'static str,
    /// Audio codec identifier (`alac`, `flac`, `aac`, `ec3`).
    #[schema(example = "alac")]
    pub codec: String,
    /// Duration of the track in seconds.
    #[schema(example = 245)]
    pub duration: i32,
    /// Audio bit depth (e.g. 16 or 24-bit lossless).
    #[schema(example = 24)]
    pub bit_depth: Option<i32>,
    /// Audio sampling rate in Hz (e.g. 44100, 48000, 96000, 192000).
    #[schema(example = 96000)]
    pub sample_rate: Option<i32>,
    /// Audio file size in bytes; estimated from duration when media metadata cannot be resolved.
    #[schema(example = 48920110)]
    pub file_size: i64,
}

#[utoipa::path(
    method(get, post),
    path = "/api/v1/tracks/{id}/playback",
    tag = "stream",
    summary = "Generate Stream Ticket & Playback Info",
    description = "Generates an HMAC-SHA256 signed playback ticket and metadata for serving original ripped audio bytes over HTTP byte ranges without server-side transcoding or re-encoding. GET and POST run the same operation: both require the track ID and Bearer authentication, read no request body, and return the same PlaybackInfo response.",
    params(
        ("id" = i32, Path, description = "Unique database track ID")
    ),
    responses(
        (status = 200, description = "Signed stream ticket and audio format metadata", body = PlaybackInfo),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 403, description = "Forbidden - Last.fm account connection required"),
        (status = 404, description = "Track not found in database cache"),
        (status = 500, description = "Internal error while checking the Last.fm connection or resolving the track")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn get_playback_info(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    axum::extract::Path(track_id): axum::extract::Path<i32>,
) -> Result<Json<PlaybackInfo>, ServerError> {
    if !db::integrations::has_integration(&state.db, user.telegram_id, "lastfm").await? {
        return Err(ServerError::Forbidden(
            "Last.fm account connection is required to stream audio".into(),
        ));
    }

    let track = state
        .tracks_repo
        .find_track_by_id(track_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .ok_or_else(|| ServerError::NotFound(format!("Track {track_id} not found")))?;

    let expires_in = 7200; // 2 hours
    let ticket = create_playback_ticket(&state.app_key, track_id, user.telegram_id, expires_in);
    let stream_url = format!("/api/v1/stream?ticket={ticket}");

    let file_size = match state
        .stream_engine
        .resolve_track_media(track_id, false)
        .await
    {
        Ok(meta) => meta.file_size as i64,
        Err(_) => i64::from(track.duration) * 50_000,
    };

    Ok(Json(PlaybackInfo {
        stream_url,
        expires_in,
        mime_type: track.codec.mime_type(),
        codec: track.codec.as_str().to_string(),
        duration: track.duration,
        bit_depth: Some(track.bit_depth),
        sample_rate: Some(track.sample_rate),
        file_size,
    }))
}

/// Query parameters for streaming audio chunks.
#[derive(Debug, Deserialize, IntoParams)]
pub struct StreamQuery {
    /// Signed stream ticket generated via `/api/v1/tracks/{id}/playback`.
    pub ticket: Option<String>,
}

#[utoipa::path(
    method(get, head),
    path = "/api/v1/stream",
    tag = "stream",
    summary = "Direct Lossless Audio Stream",
    description = "GET streams audio bytes using a valid signed HMAC ticket. Without a `Range` header it returns the full stream with 200; a single byte range returns 206. Range values may use `bytes=start-end`, `bytes=start-`, or `bytes=-suffix-length`; the response `Content-Range` uses inclusive offsets in the form `bytes start-end/total`, and an end beyond the file size is clamped. HEAD performs the same ticket and media checks and returns the corresponding status and headers without an audio response body.",
    params(
        StreamQuery
    ),
    responses(
        (status = 200, description = "GET response: full audio byte stream (`Content-Type`, `Content-Length`, and `Accept-Ranges: bytes` headers). HEAD returns the same headers with no body.", body = Vec<u8>, content_type = "audio/*"),
        (status = 206, description = "GET response: partial audio byte stream for the requested range, with `Content-Range: bytes start-end/total` and inclusive offsets. HEAD returns the same range status and headers with no body.", body = Vec<u8>, content_type = "audio/*"),
        (status = 400, description = "Missing or empty stream ticket, or malformed or out-of-bounds Range header"),
        (status = 401, description = "Invalid, expired, or tampered stream ticket"),
        (status = 404, description = "Track or its media document not found"),
        (status = 500, description = "Internal error while resolving media or constructing the response")
    )
)]
pub async fn stream_handler(
    State(state): State<Arc<ServerState>>,
    method: Method,
    headers: axum::http::HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Response, ServerError> {
    let ticket = query
        .ticket
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| ServerError::BadRequest("Missing 'ticket' query parameter".into()))?;

    let (track_id, _user_id) = verify_playback_ticket(&state.app_key, ticket)?;

    let range_header = headers.get(header::RANGE).and_then(|h| h.to_str().ok());

    let response = state
        .stream_engine
        .open_stream(track_id, range_header)
        .await?;

    let mut builder = Response::builder()
        .status(StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK))
        .header(header::CONTENT_TYPE, response.content_type)
        .header(header::ACCEPT_RANGES, response.accept_ranges)
        .header(header::CONTENT_LENGTH, response.content_length.to_string());

    if let Some(content_range) = response.content_range {
        builder = builder.header(header::CONTENT_RANGE, content_range);
    }

    if method == Method::HEAD {
        return builder
            .body(Body::empty())
            .map_err(|e| ServerError::Internal(e.to_string()));
    }

    builder
        .body(Body::from_stream(response.stream))
        .map_err(|e| ServerError::Internal(e.to_string()))
}
