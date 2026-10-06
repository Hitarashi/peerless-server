use axum::{
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("Token")
                        .description(Some(
                            "Enter your access token generated via /api/v1/auth/exchange",
                        ))
                        .build(),
                ),
            );
        }

        if let Some(stream_path) = openapi.paths.paths.get_mut("/api/v1/tracks/{id}/stream")
            && let Some(head) = stream_path.head.as_mut()
        {
            for response in head.responses.responses.values_mut() {
                if let utoipa::openapi::RefOr::T(response) = response {
                    response.content.clear();
                }
            }
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "ALAC Lossless Media Streaming Server API",
        version = "1.0.0",
        description = "# Lossless Audio & Media Streaming Engine\n\nHigh-performance lossless audio streaming server powered by Telegram MTProto backend, serving ripped source bytes via HTTP byte ranges without server-side transcoding, with sliding session auth, Apple cache lookup, and on-demand ripping.\n\nFor the authenticated playback WebSocket message contract, see the [Playback WebSocket AsyncAPI document](/api/v1/docs-ws.json).\n\n### Core Workflows\n1. **Authentication**: Users authenticate via the Telegram bot command `/stream` to obtain a single-use OTP code, exchanged at `/api/v1/auth/exchange` for sliding session tokens.\n2. **Source-byte Streaming**: Lossless streams are requested via `/api/v1/tracks/{id}/playback` and served at `/api/v1/tracks/{id}/stream` with HTTP 206 Partial Content Range support. The server returns the cached source bytes without server-side transcoding.\n3. **Apple Cache Lookup**: Check cached track formats with file sizes and complete album ZIP availability with total sizes by Apple Music IDs at `/api/v1/lookup`.\n4. **On-Demand Ripping**: Create and cancel rip tasks over the authenticated playback WebSocket RPC; `rip_tasks_snapshot` remains the authoritative active-task list, with live progress delivered on the same WebSocket.",
        license(name = "MIT")
    ),
    servers(
        (url = "/", description = "Current Server Gateway"),
        (url = "http://127.0.0.1:4444", description = "Local Development Server")
    ),
    modifiers(&SecurityAddon),
    paths(
        crate::auth::exchange,
        crate::auth::refresh,
        crate::auth::logout,
        crate::auth::me,
        crate::auth::me_avatar,
        crate::streaming::issue_playback_ticket,
        crate::streaming::stream_handler,
        crate::lookup::lookup,
        crate::integrations::lastfm_login,
        crate::integrations::lastfm_status,
        crate::integrations::lastfm_disconnect,
        crate::integrations::listenbrainz_login,
        crate::integrations::listenbrainz_status,
        crate::integrations::listenbrainz_disconnect,
        crate::health::health_check,
        crate::health::status_report,
        crate::playback_sync::ws_handler,
    ),
    components(
        schemas(
            crate::auth::ExchangeRequest,
            crate::auth::ExchangeResponse,
            crate::auth::RefreshRequest,
            crate::auth::RefreshResponse,
            crate::auth::LogoutRequest,
            crate::auth::UserDto,
            crate::auth::AuthedUser,
            crate::auth::SessionDto,
            crate::auth::MeResponse,
            crate::streaming::PlaybackInfo,
            crate::lookup::LookupRequest,
            crate::lookup::TrackFormatResult,
            crate::lookup::TrackLookupResult,
            crate::lookup::AlbumZipPartResult,
            crate::lookup::AlbumLookupResult,
            crate::lookup::LookupResponse,
            crate::rip_tasks::RipTaskRequest,
            crate::rip_tasks::RipTaskSnapshot,
            crate::rip_tasks::RipTaskDownloadLane,
            crate::rip_tasks::RipTaskUploadLane,
            crate::integrations::LastfmLoginRequest,
            crate::integrations::LastfmStatusResponse,
            crate::integrations::ListenbrainzLoginRequest,
            crate::integrations::ListenbrainzStatusResponse,
            crate::health::HealthResponse,
            crate::health::StreamingMetrics,
            crate::probe::StatusReport,
            crate::probe::SubsystemReport,
            crate::probe::SubsystemState,
            crate::playback_sync::ConnectedDeviceInfo,
            crate::playback_sync::ClientMessage,
            crate::playback_sync::ServerMessage,
        )
    ),
    tags(
        (name = "auth", description = "Telegram OTP exchange, sliding session refresh, and user profile management"),
        (name = "stream", description = "Direct ripped ALAC/FLAC source-byte streaming via HTTP byte ranges without server-side transcoding, and HMAC-SHA256 playback ticket generation"),
        (name = "lookup", description = "Batch Apple Music cache lookup for track formats, file sizes, and complete album ZIP availability and total sizes"),
        (name = "integrations", description = "Third-party integrations (Last.fm, ListenBrainz) and scrobbling/playlist authentication"),
        (name = "system", description = "Server liveness check, per-subsystem diagnostics, and telemetry. GET /api/v1/health is the narrow liveness probe driving the Docker HEALTHCHECK (200 or 503); GET /api/v1/status is the wide diagnostics report and always returns 200."),
        (name = "ws", description = "Authenticated, bidirectional playback synchronization WebSocket handshake; see /api/v1/docs-ws.json for the AsyncAPI message protocol"),
    )
)]
pub struct ApiDoc;

const SCALAR_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>ALAC Lossless Streaming API Reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" type="image/svg+xml" href="https://scalar.com/favicon.svg" />
    <style>
      body {
        margin: 0;
      }
    </style>
  </head>
  <body>
    <script
      id="api-reference"
      data-configuration='{
        "sources": [
          { "title": "REST API", "url": "/api/v1/docs.json" },
          { "title": "Playback WebSocket", "url": "/api/v1/docs-ws.json" }
        ],
        "theme": "purple",
        "layout": "modern",
        "showSidebar": true,
        "searchHotKey": "k",
        "hideModels": false,
        "defaultHttpClient": {
          "targetKey": "shell",
          "clientKey": "curl"
        }
      }'
    ></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>
"#;

pub async fn scalar_html() -> Html<&'static str> {
    Html(SCALAR_HTML)
}

pub async fn openapi_json() -> Response {
    let doc = ApiDoc::openapi();
    match doc.to_json() {
        Ok(json) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            json,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to serialize OpenAPI json: {e}"),
        )
            .into_response(),
    }
}

pub async fn openapi_yaml() -> Response {
    let doc = ApiDoc::openapi();
    match doc.to_yaml() {
        Ok(yaml) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/yaml")],
            yaml,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to serialize OpenAPI yaml: {e}"),
        )
            .into_response(),
    }
}

pub async fn asyncapi_json() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        include_str!("playback_asyncapi.json"),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use utoipa::OpenApi;

    use super::ApiDoc;

    #[test]
    fn registers_auth_user_and_omits_stream_body_from_head() {
        let document = ApiDoc::openapi();
        let document: serde_json::Value =
            serde_json::from_str(&document.to_json().expect("OpenAPI serialization succeeds"))
                .expect("OpenAPI JSON is valid");

        assert!(document["components"]["schemas"]["AuthedUser"].is_object());
        assert!(
            document["paths"]["/api/v1/tracks/{id}/stream"]["get"]["responses"]["200"]["content"]["audio/*"]
                .is_object()
        );
        for status in ["200", "206"] {
            assert!(
                document["paths"]["/api/v1/tracks/{id}/stream"]["head"]["responses"][status]
                    .get("content")
                    .is_none()
            );
        }
    }
}
