use axum::{
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
};
use utoipa::{
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
    Modify, OpenApi,
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
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "ALAC Lossless Media Streaming Server API",
        version = "1.0.0",
        description = "# Lossless Audio & Media Streaming Engine\n\nHigh-performance lossless audio streaming server powered by Telegram MTProto backend, providing direct bit-perfect ALAC/FLAC streaming, sliding session auth, live catalog discovery, on-demand ripping, and synchronized lyrics.\n\n### Core Workflows\n1. **Authentication**: Users authenticate via the Telegram bot command `/stream` to obtain a single-use OTP code, exchanged at `/api/v1/auth/exchange` for sliding session tokens.\n2. **Bit-Perfect Streaming**: Lossless streams are requested via `/api/v1/tracks/{id}/playback` and served at `/api/v1/stream` with full HTTP 206 Partial Content Range support.\n3. **Catalog & Discovery**: Query cached tracks and live Apple Music catalog items simultaneously via `/api/v1/search`.\n4. **On-Demand Ripping**: Trigger background rip jobs for uncached tracks via `/api/v1/tasks/rip` and monitor real-time progress via Server-Sent Events (SSE).",
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
        crate::streaming::get_playback_info,
        crate::streaming::stream_handler,
        crate::catalog::search_catalog,
        crate::catalog::get_track,
        crate::catalog::list_albums,
        crate::catalog::get_album_tracks,
        crate::catalog::get_artist_tracks,
        crate::tasks::create_rip_task,
        crate::tasks::task_events,
        crate::assets::get_artwork,
        crate::assets::get_provider_artwork,
        crate::assets::get_lyrics,
        crate::library::list_favorites,
        crate::library::add_favorite,
        crate::library::remove_favorite,
        crate::library::list_playlists,
        crate::library::create_playlist,
        crate::library::get_playlist,
        crate::library::update_playlist,
        crate::library::delete_playlist,
        crate::integrations::login,
        crate::integrations::status,
        crate::integrations::disconnect,
        crate::health::health_check,
    ),
    components(
        schemas(
            crate::auth::ExchangeRequest,
            crate::auth::ExchangeResponse,
            crate::auth::RefreshRequest,
            crate::auth::RefreshResponse,
            crate::auth::LogoutRequest,
            crate::auth::UserDto,
            crate::auth::SessionDto,
            crate::auth::MeResponse,
            crate::streaming::PlaybackInfo,
            crate::catalog::TrackSummaryDto,
            crate::catalog::TrackDetailDto,
            crate::catalog::UncachedTrackDto,
            crate::catalog::TrackSourceDto,
            crate::catalog::CanonicalTrackDto,
            crate::catalog::SearchResponse,
            crate::catalog::AlbumSummaryDto,
            crate::catalog::AlbumDetailsDto,
            crate::tasks::RipTaskRequest,
            crate::tasks::RipTaskResponse,
            crate::tasks::TaskProgressEvent,
            crate::assets::LyricsResponse,
            crate::assets::LyricsLineDto,
            crate::assets::LyricsWordDto,
            crate::library::PlaylistSummaryDto,
            crate::library::CreatePlaylistRequest,
            crate::library::PlaylistWithTracksDto,
            crate::library::UpdatePlaylistRequest,
            crate::integrations::LastfmLoginRequest,
            crate::integrations::LastfmStatusResponse,
            crate::health::HealthResponse,
        )
    ),
    tags(
        (name = "auth", description = "Telegram OTP exchange, sliding session refresh, and user profile management"),
        (name = "stream", description = "Direct bit-perfect lossless audio streaming and HMAC-SHA256 playback ticket generation"),
        (name = "catalog", description = "Music catalog search, track metadata, album tracklists, and artist discographies"),
        (name = "tasks", description = "On-demand provider ripping and real-time Server-Sent Events (SSE) progress tracking"),
        (name = "assets", description = "High-resolution album artwork redirection and synchronized TTML/LRC lyrics resolution"),
        (name = "library", description = "User favorited tracks and custom playlist management"),
        (name = "integrations", description = "Third-party integrations and Last.fm scrobbling authentication"),
        (name = "system", description = "Server health check, telemetry, and metrics"),
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
      data-url="/api/v1/docs.json"
      data-configuration='{
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
