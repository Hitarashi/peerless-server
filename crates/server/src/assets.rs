use std::sync::{Arc, LazyLock};

use axum::{
    Json,
    extract::{Path, Query, State},
};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{ServerState, auth::AuthedUser, error::ServerError};

static ARTWORK_CACHE: LazyLock<Cache<(String, u16), String>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(std::time::Duration::from_secs(86400 * 7))
        .build()
});

static ARTIST_ARTWORK_CACHE: LazyLock<Cache<(String, u16), String>> = LazyLock::new(|| {
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

#[derive(Debug, Deserialize, IntoParams)]
pub struct ProviderArtworkQuery {
    /// Desired square image dimension in pixels (e.g. 300, 600, 1200). Default is 600.
    #[param(example = 600)]
    pub size: Option<u16>,
    /// Track title used when resolving artwork for a provider result not in the database.
    pub title: Option<String>,
    /// Track artist used when resolving artwork for a provider result not in the database.
    pub artist: Option<String>,
}

/// Provider artwork URL resolved for a track.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtworkUrlResponse {
    /// Direct provider CDN URL of the artwork image. The client is responsible
    /// for fetching it; it is not an endpoint on this server.
    #[schema(example = "https://is1-ssl.mzstatic.com/image/thumb/.../600x600bb.jpg")]
    pub url: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/assets/tracks/{id}/artwork",
    tag = "assets",
    summary = "Get Track Artwork URL",
    description = "Resolves provider artwork scaled to the requested pixel dimensions and returns it as a JSON provider CDN URL. If the selected track has no artwork, the handler tries tracks with the same non-empty ISRC and returns the first available sibling artwork URL. The client fetches the image itself.",
    params(
        ("id" = i32, Path, description = "Unique database track ID", example = 42),
        ArtworkQuery
    ),
    responses(
        (status = 200, description = "Provider artwork URL for the selected track or an ISRC sibling track", body = ArtworkUrlResponse),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Track not found, or no artwork was found for the track or any ISRC sibling"),
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

    let size = query.size.unwrap_or(600).clamp(100, 3000);
    match resolve_artwork(
        &state,
        track.provider,
        &track.track_id,
        &track.title,
        &track.artist,
        size,
    )
    .await
    {
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
                match resolve_artwork(
                    &state,
                    sibling.provider,
                    &sibling.track_id,
                    &sibling.title,
                    &sibling.artist,
                    size,
                )
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
    path = "/api/v1/assets/providers/{provider}/tracks/{track_id}/artwork",
    tag = "assets",
    summary = "Get Provider Track Artwork URL",
    description = "Resolves artwork for a provider catalog track, including uncached tracks, and returns its image URL as JSON. The client fetches the image itself.",
    params(
        ("provider" = String, Path, description = "Provider name: apple or qobuz", example = "qobuz"),
        ("track_id" = String, Path, description = "Provider catalog track ID", example = "123456"),
        ProviderArtworkQuery
    ),
    responses(
        (status = 200, description = "Resolved provider artwork URL", body = ArtworkUrlResponse),
        (status = 400, description = "Unsupported provider"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "No artwork could be resolved for the provider track"),
        (status = 500, description = "Internal error while resolving the provider artwork URL")
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_provider_artwork(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
    Path((provider_name, provider_track_id)): Path<(String, String)>,
    Query(query): Query<ProviderArtworkQuery>,
) -> Result<Json<ArtworkUrlResponse>, ServerError> {
    let provider = provider_name
        .parse::<music::Provider>()
        .map_err(ServerError::BadRequest)?;
    let size = query.size.unwrap_or(600).clamp(100, 3000);
    let url = resolve_artwork(
        &state,
        provider,
        &provider_track_id,
        query.title.as_deref().unwrap_or_default(),
        query.artist.as_deref().unwrap_or_default(),
        size,
    )
    .await?;
    Ok(Json(ArtworkUrlResponse { url }))
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
    let url = resolve_artist_artwork(&state, artist_name, size).await?;
    Ok(Json(ArtworkUrlResponse { url }))
}

async fn resolve_artist_artwork(
    state: &ServerState,
    artist_name: &str,
    size: u16,
) -> Result<String, ServerError> {
    let cache_key = (artist_name.to_lowercase().trim().to_string(), size);
    if let Some(cached_url) = ARTIST_ARTWORK_CACHE.get(&cache_key).await {
        return Ok(cached_url);
    }

    let encoded_name = music::url::urlencode(artist_name);

    let search_url =
        format!("https://itunes.apple.com/search?term={encoded_name}&entity=musicArtist&limit=5");

    let mut artist_id = None;
    let mut artist_link_url = None;

    if let Ok(resp) = state.http_client.get(&search_url).send().await
        && resp.status().is_success()
        && let Ok(json) = resp.json::<serde_json::Value>().await
        && let Some(arr) = json.get("results").and_then(|r| r.as_array())
    {
        let target_norm: String = artist_name
            .to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric())
            .collect();
        for item in arr {
            let res_artist = item
                .get("artistName")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_lowercase();
            let res_norm: String = res_artist.chars().filter(|c| c.is_alphanumeric()).collect();
            if res_norm == target_norm || res_artist == artist_name.to_lowercase() {
                artist_id = item.get("artistId").and_then(|id| id.as_i64());
                artist_link_url = item
                    .get("artistLinkUrl")
                    .and_then(|u| u.as_str())
                    .map(String::from);
                break;
            }
        }
    }

    if let Some(id) = artist_id
        && let Some(catalog) = &state.catalog_service
        && let Some(token_prov) = catalog.token_provider()
    {
        let amp_url = format!("https://amp-api.music.apple.com/v1/catalog/us/artists/{id}");
        if let Ok(body) = token_prov
            .fetch_amp(&amp_url, std::time::Duration::from_secs(6))
            .await
            && let Ok(json) = serde_json::from_str::<serde_json::Value>(&body)
            && let Some(url_template) = json
                .get("data")
                .and_then(|d| d.as_array())
                .and_then(|arr| arr.first())
                .and_then(|item| item.get("attributes"))
                .and_then(|attr| attr.get("artwork"))
                .and_then(|art| art.get("url"))
                .and_then(|u| u.as_str())
        {
            let formatted = url_template
                .replace("{w}", &size.to_string())
                .replace("{h}", &size.to_string())
                .replace("{f}", "jpg");
            ARTIST_ARTWORK_CACHE
                .insert(cache_key, formatted.clone())
                .await;
            return Ok(formatted);
        }
    }

    if let Some(link) = artist_link_url {
        let clean_link = if let Some((base, _)) = link.split_once('?') {
            base
        } else {
            &link
        };
        if let Ok(resp) = state
            .http_client
            .get(clean_link)
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .send()
            .await
            && resp.status().is_success()
            && let Ok(html) = resp.text().await
            && let Some(pos) = html.find("property=\"og:image\" content=\"")
        {
            let rest = &html[pos + 29..];
            if let Some(end) = rest.find('\"') {
                let og_image = &rest[..end];
                let formatted = if let Some(last_slash) = og_image.rfind('/') {
                    format!("{}/{size}x{size}bb.jpg", &og_image[..last_slash])
                } else {
                    og_image.to_string()
                };
                ARTIST_ARTWORK_CACHE.insert(cache_key, formatted.clone()).await;
                return Ok(formatted);
            }
        }
    }

    Err(ServerError::NotFound(format!(
        "Artwork not found for artist {artist_name}"
    )))
}

async fn resolve_artwork(
    state: &ServerState,
    provider: music::Provider,
    provider_track_id: &str,
    title: &str,
    artist: &str,
    size: u16,
) -> Result<String, ServerError> {
    let cache_key = (format!("{}:{provider_track_id}", provider.as_str()), size);
    if let Some(cached_url) = ARTWORK_CACHE.get(&cache_key).await {
        return Ok(cached_url);
    }

    // If track is from Apple or Qobuz, try resolving CDN artwork
    // Apple Music artwork URLs follow standard format or catalog lookup
    let artwork_url = match provider {
        music::Provider::Apple => {
            let mut resolved = None;

            // 1. Try iTunes lookup: default storefront first, then regional storefronts (in, gb, us)
            let countries = ["", "in", "gb", "us"];
            for country in countries {
                let url = if country.is_empty() {
                    format!(
                        "https://itunes.apple.com/lookup?id={}&entity=song",
                        provider_track_id
                    )
                } else {
                    format!(
                        "https://itunes.apple.com/lookup?id={}&entity=song&country={country}",
                        provider_track_id
                    )
                };

                if let Ok(resp) = state.http_client.get(&url).send().await
                    && resp.status().is_success()
                    && let Ok(json) = resp.json::<serde_json::Value>().await
                    && let Some(url_str) = json
                        .get("results")
                        .and_then(|r| r.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|item| item.get("artworkUrl100"))
                        .and_then(|u| u.as_str())
                {
                    resolved = Some(url_str.replace("100x100bb", &format!("{size}x{size}bb")));
                    break;
                }
            }

            // 2. Fallback: search iTunes catalog by title and artist
            if resolved.is_none() && !title.trim().is_empty() && !artist.trim().is_empty() {
                let term = format!("{title} {artist}");
                let encoded_term = music::url::urlencode(&term);
                let search_countries = ["in", "us", "gb"];
                for country in search_countries {
                    let search_url = format!(
                        "https://itunes.apple.com/search?term={encoded_term}&entity=song&limit=1&country={country}"
                    );
                    if let Ok(resp) = state.http_client.get(&search_url).send().await
                        && resp.status().is_success()
                        && let Ok(json) = resp.json::<serde_json::Value>().await
                        && let Some(url_str) = json
                            .get("results")
                            .and_then(|r| r.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|item| item.get("artworkUrl100"))
                            .and_then(|u| u.as_str())
                    {
                        resolved = Some(url_str.replace("100x100bb", &format!("{size}x{size}bb")));
                        break;
                    }
                }
            }

            resolved
        }
        music::Provider::Qobuz => {
            let backend_url = std::env::var("QOBUZ_BACKEND_URL")
                .unwrap_or_else(|_| "https://qobuz.kanjijewels.com".to_string());
            let clean_backend_url = backend_url.trim().trim_end_matches('/');
            let backend_key = std::env::var("QOBUZ_BACKEND_KEY")
                .ok()
                .filter(|k| !k.trim().is_empty());

            let mut req = state
                .http_client
                .get(format!("{clean_backend_url}/api/track/{provider_track_id}"));
            if let Some(ref key) = backend_key {
                req = req.header("X-API-Key", key);
            }

            let mut resolved = None;
            if let Ok(resp) = req.send().await
                && resp.status().is_success()
                && let Ok(json) = resp.json::<serde_json::Value>().await
            {
                let track_obj = json.get("track").unwrap_or(&json);
                let img_url = track_obj
                    .get("album")
                    .and_then(|a| a.get("image"))
                    .and_then(|img| {
                        img.get("large")
                            .or_else(|| img.get("small"))
                            .or_else(|| img.get("thumbnail"))
                    })
                    .and_then(|u| u.as_str())
                    .or_else(|| {
                        track_obj
                            .get("tags")
                            .and_then(|t| t.get("coverUrl600").or_else(|| t.get("coverUrl")))
                            .and_then(|u| u.as_str())
                    })
                    .or_else(|| track_obj.get("originalCoverUrl").and_then(|u| u.as_str()));

                if let Some(base_url) = img_url {
                    let mapped_url = if size > 600 {
                        base_url
                            .replace("_600.jpg", "_org.jpg")
                            .replace("_230.jpg", "_org.jpg")
                    } else if size <= 230 {
                        base_url
                            .replace("_600.jpg", "_230.jpg")
                            .replace("_org.jpg", "_230.jpg")
                    } else {
                        base_url
                            .replace("_230.jpg", "_600.jpg")
                            .replace("_org.jpg", "_600.jpg")
                    };
                    resolved = Some(mapped_url);
                }
            }

            resolved
        }
    };

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
