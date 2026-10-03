use std::sync::{Arc, LazyLock};

use axum::{
    Json,
    extract::{Path, Query, State},
};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{ServerState, auth::AuthedUser, error::ServerError};

static ARTWORK_CACHE: LazyLock<Cache<(String, String, u16), String>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(std::time::Duration::from_secs(86400 * 7))
        .build()
});

static ARTIST_ARTWORK_CACHE: LazyLock<Cache<(String, String, u16), String>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(std::time::Duration::from_secs(86400 * 7))
        .build()
});

/// Query parameters for fetching artwork images.
#[derive(Debug, Deserialize, IntoParams)]
pub struct ArtworkQuery {
    /// Desired square image dimension in pixels (e.g. 300, 600, 1200). Default is 600.
    #[param(example = 600)]
    pub size: Option<u16>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ArtistArtworkQuery {
    /// Artist name used to resolve portrait artwork.
    pub name: String,
    /// Desired square image dimension in pixels (e.g. 300, 600, 1200). Default is 600.
    #[param(example = 600)]
    pub size: Option<u16>,
}

/// Apple Music artwork URL resolved for a track.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtworkUrlResponse {
    /// Direct Apple Music CDN URL of the artwork image. The client is
    /// responsible for fetching it; it is not an endpoint on this server.
    #[schema(example = "https://is1-ssl.mzstatic.com/image/thumb/.../600x600bb.jpg")]
    pub url: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/assets/tracks/{id}/artwork",
    tag = "assets",
    summary = "Get Track Artwork URL",
    description = "Resolves artwork for Apple Music tracks through Lyricsporn's Apple-ID track endpoint, scales it to the requested pixel dimensions, and returns the Apple CDN URL as JSON. If the selected track has no artwork, the handler tries Apple Music tracks with the same non-empty ISRC. The client fetches the image itself.",
    params(
        ("id" = i32, Path, description = "Unique database track ID", example = 42),
        ArtworkQuery
    ),
    responses(
        (status = 200, description = "Apple Music artwork URL for the selected track or an Apple Music ISRC sibling", body = ArtworkUrlResponse),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Track not found, provider is not Apple Music, or no Apple Music artwork was found for the track or any ISRC sibling"),
        (status = 500, description = "Internal error while retrieving the track or searching its ISRC siblings")
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_artwork(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
    Path(db_track_id): Path<i32>,
    Query(query): Query<ArtworkQuery>,
) -> Result<Json<ArtworkUrlResponse>, ServerError> {
    let track = state
        .tracks_repo
        .find_track_by_id(db_track_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .ok_or_else(|| ServerError::NotFound(format!("Track {db_track_id} not found")))?;

    if track.provider != music::Provider::Apple {
        return Err(ServerError::NotFound(
            "Artwork lookup only supports Apple Music tracks".to_string(),
        ));
    }

    let size = query.size.unwrap_or(600).clamp(100, 3000);
    let api_url = state
        .settings_store
        .get_settings()
        .lyricsporn_api_url
        .ok_or_else(|| ServerError::NotFound("Artwork service is not configured".to_owned()))?;
    match resolve_artwork(&state, &api_url, track.provider, &track.track_id, size).await {
        Ok(url) => Ok(Json(ArtworkUrlResponse { url })),
        Err(ServerError::NotFound(_)) => {
            let Some(isrc) = track.isrc.as_deref().filter(|isrc| !isrc.trim().is_empty()) else {
                return Err(ServerError::NotFound(format!(
                    "Artwork not found for track {db_track_id}"
                )));
            };
            let sibling_tracks = state
                .tracks_repo
                .find_tracks_by_isrc(isrc)
                .await
                .map_err(|error| ServerError::Internal(error.to_string()))?;

            for sibling in sibling_tracks {
                if sibling.id == track.id {
                    continue;
                }
                match resolve_artwork(&state, &api_url, sibling.provider, &sibling.track_id, size)
                    .await
                {
                    Ok(url) => return Ok(Json(ArtworkUrlResponse { url })),
                    Err(ServerError::NotFound(_)) => continue,
                    Err(error) => return Err(error),
                }
            }

            Err(ServerError::NotFound(format!(
                "Artwork not found for track {db_track_id} or its provider siblings"
            )))
        }
        Err(error) => Err(error),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/assets/artists/artwork",
    tag = "assets",
    summary = "Get Artist Artwork URL",
    description = "Resolves authentic portrait artwork for an artist by name and returns its image URL as JSON. The client fetches the image itself.",
    params(
        ArtistArtworkQuery
    ),
    responses(
        (status = 200, description = "Resolved artist artwork URL", body = ArtworkUrlResponse),
        (status = 400, description = "Artist name query parameter is missing or empty"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "No artist artwork could be resolved"),
        (status = 500, description = "Internal error while resolving the artist artwork URL")
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_artist_artwork(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
    Query(query): Query<ArtistArtworkQuery>,
) -> Result<Json<ArtworkUrlResponse>, ServerError> {
    let artist_name = query.name.trim();
    if artist_name.is_empty() {
        return Err(ServerError::BadRequest(
            "Query parameter 'name' must not be empty".to_string(),
        ));
    }
    let size = query.size.unwrap_or(600).clamp(100, 3000);
    let api_url = state
        .settings_store
        .get_settings()
        .lyricsporn_api_url
        .ok_or_else(|| ServerError::NotFound("Artwork service is not configured".to_owned()))?;
    let url = resolve_artist_artwork(&state, &api_url, artist_name, size).await?;
    Ok(Json(ArtworkUrlResponse { url }))
}

async fn resolve_artist_artwork(
    state: &ServerState,
    api_url: &str,
    artist_name: &str,
    size: u16,
) -> Result<String, ServerError> {
    let cache_key = (
        api_url.to_owned(),
        artist_name.to_lowercase().trim().to_string(),
        size,
    );
    if let Some(cached_url) = ARTIST_ARTWORK_CACHE.get(&cache_key).await {
        return Ok(cached_url);
    }

    let term = music::url::urlencode(artist_name.trim());
    let response = state
        .http_client
        .get(format!(
            "{api_url}/catalog/search?term={term}&types=artists&limit=5&artworkSize={size}"
        ))
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .map_err(|error| ServerError::Internal(error.to_string()))?;
    if response.status().is_success() {
        let json = response
            .json::<serde_json::Value>()
            .await
            .map_err(|error| ServerError::Internal(error.to_string()))?;
        let target_norm = normalized_name(artist_name);
        if let Some(artist) = json
            .pointer("/results/artists/items")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .find(|item| {
                item.get("name")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|name| normalized_name(name) == target_norm)
            })
            && let Some(url) = artist
                .pointer("/artwork/url")
                .and_then(serde_json::Value::as_str)
        {
            let url = format_artwork_url(url, size);
            ARTIST_ARTWORK_CACHE.insert(cache_key, url.clone()).await;
            return Ok(url);
        }
    }

    Err(ServerError::NotFound(format!(
        "Artwork not found for artist {artist_name}"
    )))
}

async fn resolve_artwork(
    state: &ServerState,
    api_url: &str,
    provider: music::Provider,
    provider_track_id: &str,
    size: u16,
) -> Result<String, ServerError> {
    if provider != music::Provider::Apple {
        return Err(ServerError::NotFound(format!(
            "Artwork lookup only supports Apple Music tracks, not {}",
            provider.as_str()
        )));
    }

    let cache_key = (
        api_url.to_owned(),
        format!("{}:{provider_track_id}", provider.as_str()),
        size,
    );
    if let Some(cached_url) = ARTWORK_CACHE.get(&cache_key).await {
        return Ok(cached_url);
    }

    let artwork_url = fetch_lyricsporn_artwork(state, api_url, provider_track_id, size).await;

    if let Some(url) = artwork_url {
        ARTWORK_CACHE.insert(cache_key, url.clone()).await;
        Ok(url)
    } else {
        Err(ServerError::NotFound(format!(
            "Artwork not found for {} track {provider_track_id}",
            provider.as_str()
        )))
    }
}

async fn fetch_lyricsporn_artwork(
    state: &ServerState,
    api_url: &str,
    apple_track_id: &str,
    size: u16,
) -> Option<String> {
    if apple_track_id.is_empty()
        || apple_track_id.len() > 20
        || !apple_track_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let response = state
        .http_client
        .get(format!(
            "{api_url}/tracks/{apple_track_id}?include=artwork&artworkSize={size}"
        ))
        .timeout(std::time::Duration::from_secs(6))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }

    let body = response.json::<serde_json::Value>().await.ok()?;
    let artwork_url = body.pointer("/track/artwork/url")?.as_str()?;
    Some(format_artwork_url(artwork_url, size))
}

fn normalized_name(value: &str) -> String {
    value
        .to_lowercase()
        .chars()
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn format_artwork_url(url: &str, size: u16) -> String {
    let size = size.to_string();
    url.replace("{w}", &size)
        .replace("{h}", &size)
        .replace("{f}", "jpg")
        .replace("100x100bb", &format!("{size}x{size}bb"))
}
