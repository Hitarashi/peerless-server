use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{ServerState, error::ServerError};

/// Summary information for a track in catalog listings.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TrackSummaryDto {
    /// Unique database track ID (0 for uncached live tracks).
    #[schema(example = 42)]
    pub id: i32,
    /// Music provider name (`apple`, `qobuz`).
    #[schema(example = "apple")]
    pub provider: String,
    /// Provider-native track identifier.
    #[schema(example = "1440857781")]
    pub track_id: String,
    /// Track title.
    #[schema(example = "Blank Space")]
    pub title: String,
    /// Primary artist name.
    #[schema(example = "Taylor Swift")]
    pub artist: String,
    /// Album name.
    #[schema(example = "1989")]
    pub album: String,
    /// Duration of the audio track in seconds.
    #[schema(example = 231)]
    pub duration: i32,
    /// Lossless or compressed audio codec.
    #[schema(example = "alac")]
    pub codec: String,
    /// Audio bit depth (e.g. 16 or 24).
    #[schema(example = 24)]
    pub bit_depth: Option<i32>,
    /// Audio sample rate in Hz (e.g. 44100, 96000).
    #[schema(example = 44100)]
    pub sample_rate: Option<i32>,
    /// Whether this track is cached in Telegram and playable instantly.
    #[schema(example = true)]
    pub is_cached: bool,
    /// International Standard Recording Code, when known.
    #[schema(example = "USUM71703861")]
    pub isrc: Option<String>,
}

impl From<db::Track> for TrackSummaryDto {
    fn from(t: db::Track) -> Self {
        Self {
            id: t.id,
            provider: t.provider.as_str().to_string(),
            track_id: t.track_id,
            title: t.title,
            artist: t.artist,
            album: t.album,
            duration: t.duration,
            codec: t.codec.as_str().to_string(),
            bit_depth: Some(t.bit_depth),
            sample_rate: Some(t.sample_rate),
            is_cached: true,
            isrc: t.isrc,
        }
    }
}

/// Comprehensive track metadata and technical specifications.
#[derive(Debug, Serialize, ToSchema)]
pub struct TrackDetailDto {
    /// Unique database track ID.
    #[schema(example = 42)]
    pub id: i32,
    /// Music provider (`apple`, `qobuz`).
    #[schema(example = "apple")]
    pub provider: String,
    /// Provider-native track ID.
    #[schema(example = "1440857781")]
    pub track_id: String,
    /// Track title.
    #[schema(example = "Blank Space")]
    pub title: String,
    /// Primary artist name.
    #[schema(example = "Taylor Swift")]
    pub artist: String,
    /// Album name.
    #[schema(example = "1989")]
    pub album: String,
    /// Duration in seconds.
    #[schema(example = 231)]
    pub duration: i32,
    /// Codec identifier (`alac`, `flac`, `aac`).
    #[schema(example = "alac")]
    pub codec: String,
    /// Bit depth.
    #[schema(example = 24)]
    pub bit_depth: i32,
    /// Sample rate in Hz.
    #[schema(example = 44100)]
    pub sample_rate: i32,
    /// Musical genre.
    #[schema(example = "Pop")]
    pub genre: String,
    /// Release date string (YYYY-MM-DD).
    #[schema(example = "2014-10-27")]
    pub release_date: String,
    /// Track sequence number on disc.
    #[schema(example = 2)]
    pub track_number: i32,
    /// Total tracks on disc.
    #[schema(example = 13)]
    pub track_count: i32,
    /// True if already cached in Telegram dump channel.
    #[schema(example = true)]
    pub is_cached: bool,
    /// International Standard Recording Code.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "USCJY1431245")]
    pub isrc: Option<String>,
    /// Track composer.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "Taylor Swift, Max Martin, Shellback")]
    pub composer: Option<String>,
    /// Disc number for multi-disc releases.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 1)]
    pub disc_number: Option<i32>,
}

impl From<db::Track> for TrackDetailDto {
    fn from(t: db::Track) -> Self {
        Self {
            id: t.id,
            provider: t.provider.as_str().to_string(),
            track_id: t.track_id,
            title: t.title,
            artist: t.artist,
            album: t.album,
            duration: t.duration,
            codec: t.codec.as_str().to_string(),
            bit_depth: t.bit_depth,
            sample_rate: t.sample_rate,
            genre: t.genre,
            release_date: t.release_date,
            track_number: t.track_number,
            track_count: t.track_count,
            is_cached: true,
            isrc: t.isrc,
            composer: None,
            disc_number: None,
        }
    }
}

/// A specific audio source/rendition available for a canonical track.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TrackSourceDto {
    pub id: i32,
    pub provider: String,
    pub track_id: String,
    pub codec: String,
    pub bit_depth: Option<i32>,
    pub sample_rate: Option<i32>,
    pub is_cached: bool,
}

/// Canonical representation of a unique music recording, aggregating sources
/// across providers and cache tiers.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CanonicalTrackDto {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i32,
    pub artwork_url: Option<String>,
    pub isrc: Option<String>,
    pub sources: Vec<TrackSourceDto>,
}

/// Representation of an uncached live catalog track discovered via provider search.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UncachedTrackDto {
    /// Music provider (`apple`, `qobuz`).
    #[schema(example = "apple")]
    pub provider: String,
    /// Provider item or store identifier.
    #[schema(example = "1440857781")]
    pub item_id: String,
    /// Provider track identifier.
    #[schema(example = "1440857781")]
    pub track_id: String,
    /// Track title.
    #[schema(example = "Blank Space")]
    pub title: String,
    /// Artist name.
    #[schema(example = "Taylor Swift")]
    pub artist: String,
    /// Album name.
    #[schema(example = "1989")]
    pub album: String,
    /// Duration in seconds.
    #[schema(example = 231)]
    pub duration: i32,
    /// False for uncached live search results.
    #[schema(example = false)]
    pub is_cached: bool,
    /// High-resolution artwork URL from provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "https://is1-ssl.mzstatic.com/image/thumb/.../600x600bb.jpg")]
    pub artwork_url: Option<String>,
}

/// Search query parameters.
#[derive(Debug, Deserialize, IntoParams)]
pub struct SearchQuery {
    /// Search query string (song title, artist, or album).
    #[param(example = "Taylor Swift Blank Space")]
    pub q: String,
    /// Target provider filter (`apple` or `qobuz`).
    #[param(example = "apple")]
    pub provider: Option<String>,
    /// Page number (1-based pagination).
    #[param(example = 1)]
    pub page: Option<i64>,
    /// Results per page (default: 20, max: 100).
    #[param(example = 20)]
    pub limit: Option<i64>,
}

/// Unified search response containing both cached, live, and canonical catalog results.
#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResponse {
    /// Cached tracks playable instantly without ripping.
    pub cached: Vec<TrackSummaryDto>,
    /// Live provider catalog tracks that can be ripped on-demand.
    pub live: Vec<UncachedTrackDto>,
    /// Unified canonical tracks grouping sources across providers and cache states.
    pub canonical: Vec<CanonicalTrackDto>,
}

#[utoipa::path(
    get,
    path = "/api/v1/search",
    tag = "catalog",
    summary = "Search Cached & Live Music Catalog",
    description = "Searches tracks with pagination (`page`, `limit`). Returns both instantly playable cached tracks and live provider catalog results.",
    params(
        SearchQuery
    ),
    responses(
        (status = 200, description = "Deduplicated cached and live search results", body = SearchResponse)
    )
)]
pub async fn search_catalog(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, ServerError> {
    let limit = query.limit.unwrap_or(20).clamp(1, 100) as usize;
    let page = query.page.unwrap_or(1).max(1);
    let offset = ((page - 1) * (limit as i64)) as usize;

    let cached_tracks = state
        .tracks_repo
        .search_cached_tracks(&query.q, offset + limit)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    let cached_slice: Vec<db::Track> = cached_tracks.into_iter().skip(offset).take(limit).collect();

    let cached: Vec<TrackSummaryDto> = cached_slice.iter().cloned().map(Into::into).collect();

    // Query live catalog from catalog_service if available
    let mut live = Vec::new();
    let mut live_results: Vec<music::TrackMeta> = Vec::new();
    if let Some(ref catalog) = state.catalog_service {
        let provider = query.provider.as_deref().unwrap_or("apple");
        if provider.eq_ignore_ascii_case("apple")
            && !query.q.trim().is_empty()
            && let Ok(results) = catalog.search_catalog(&query.q, 10, "us").await
        {
            let cached_track_ids: std::collections::HashSet<&str> =
                cached.iter().map(|c| c.track_id.as_str()).collect();
            for item in &results {
                if !cached_track_ids.contains(item.id.as_str()) {
                    live.push(UncachedTrackDto {
                        provider: "apple".to_string(),
                        item_id: item.id.clone(),
                        track_id: item.id.clone(),
                        title: item.title.clone(),
                        artist: item.artist.clone(),
                        album: item.album.clone(),
                        duration: item.duration_secs as i32,
                        is_cached: false,
                        artwork_url: if item.artwork_url.is_empty() {
                            None
                        } else {
                            Some(item.artwork_url.clone())
                        },
                    });
                }
            }
            live_results = results;
        }
    }

    let canonical = build_canonical_tracks(&cached_slice, &live_results);

    Ok(Json(SearchResponse {
        cached,
        live,
        canonical,
    }))
}

fn normalize_for_canonical(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn matches_canonical(
    canonical: &CanonicalTrackDto,
    candidate_isrc: Option<&str>,
    candidate_provider: &str,
    candidate_track_id: &str,
    candidate_title: &str,
    candidate_artist: &str,
    candidate_duration: i32,
) -> bool {
    let same_provider_track = canonical.sources.iter().any(|s| {
        s.provider.eq_ignore_ascii_case(candidate_provider) && s.track_id == candidate_track_id
    });
    if same_provider_track {
        return true;
    }

    if let (Some(c_isrc), Some(cand_isrc)) = (canonical.isrc.as_deref(), candidate_isrc) {
        let c_trim = c_isrc.trim();
        let cand_trim = cand_isrc.trim();
        if !c_trim.is_empty() && !cand_trim.is_empty() {
            return c_trim.eq_ignore_ascii_case(cand_trim);
        }
    }

    let norm_can_title = normalize_for_canonical(&canonical.title);
    let norm_cand_title = normalize_for_canonical(candidate_title);
    let norm_can_artist = normalize_for_canonical(&canonical.artist);
    let norm_cand_artist = normalize_for_canonical(candidate_artist);

    if !norm_can_title.is_empty()
        && norm_can_title == norm_cand_title
        && !norm_can_artist.is_empty()
        && norm_can_artist == norm_cand_artist
        && (canonical.duration - candidate_duration).abs() <= 3
    {
        return true;
    }

    false
}

pub fn build_canonical_tracks(
    cached: &[db::Track],
    live_meta: &[music::TrackMeta],
) -> Vec<CanonicalTrackDto> {
    let mut canonical: Vec<CanonicalTrackDto> = Vec::new();

    // 1. Group cached tracks
    for t in cached {
        let provider = t.provider.as_str();
        let track_id = &t.track_id;
        let isrc = t.isrc.as_deref().filter(|s| !s.trim().is_empty());

        if let Some(existing) = canonical.iter_mut().find(|c| {
            matches_canonical(c, isrc, provider, track_id, &t.title, &t.artist, t.duration)
        }) {
            if existing.isrc.is_none() && isrc.is_some() {
                existing.isrc = isrc.map(ToOwned::to_owned);
            }
            if !existing.sources.iter().any(|s| {
                s.provider.eq_ignore_ascii_case(provider)
                    && s.track_id == *track_id
                    && s.codec.eq_ignore_ascii_case(t.codec.as_str())
            }) {
                existing.sources.push(TrackSourceDto {
                    id: t.id,
                    provider: provider.to_string(),
                    track_id: track_id.clone(),
                    codec: t.codec.as_str().to_string(),
                    bit_depth: Some(t.bit_depth),
                    sample_rate: Some(t.sample_rate),
                    is_cached: true,
                });
            }
        } else {
            let canon_id = isrc.unwrap_or(track_id).to_owned();
            canonical.push(CanonicalTrackDto {
                id: canon_id,
                title: t.title.clone(),
                artist: t.artist.clone(),
                album: t.album.clone(),
                duration: t.duration,
                artwork_url: None,
                isrc: isrc.map(ToOwned::to_owned),
                sources: vec![TrackSourceDto {
                    id: t.id,
                    provider: provider.to_string(),
                    track_id: track_id.clone(),
                    codec: t.codec.as_str().to_string(),
                    bit_depth: Some(t.bit_depth),
                    sample_rate: Some(t.sample_rate),
                    is_cached: true,
                }],
            });
        }
    }

    // 2. Group live catalog results
    for item in live_meta {
        let provider = "apple";
        let track_id = &item.id;
        let isrc = item.isrc.as_deref().filter(|s| !s.trim().is_empty());
        let duration = item.duration_secs as i32;

        if let Some(existing) = canonical.iter_mut().find(|c| {
            matches_canonical(
                c,
                isrc,
                provider,
                track_id,
                &item.title,
                &item.artist,
                duration,
            )
        }) {
            if existing.artwork_url.is_none() && !item.artwork_url.is_empty() {
                existing.artwork_url = Some(item.artwork_url.clone());
            }
            if existing.isrc.is_none() && isrc.is_some() {
                existing.isrc = isrc.map(ToOwned::to_owned);
            }
            if !existing
                .sources
                .iter()
                .any(|s| s.provider.eq_ignore_ascii_case(provider) && s.track_id == *track_id)
            {
                existing.sources.push(TrackSourceDto {
                    id: 0,
                    provider: provider.to_string(),
                    track_id: track_id.clone(),
                    codec: "alac".to_string(),
                    bit_depth: None,
                    sample_rate: None,
                    is_cached: false,
                });
            }
        } else {
            let canon_id = isrc.unwrap_or(track_id).to_owned();
            canonical.push(CanonicalTrackDto {
                id: canon_id,
                title: item.title.clone(),
                artist: item.artist.clone(),
                album: item.album.clone(),
                duration,
                artwork_url: if item.artwork_url.is_empty() {
                    None
                } else {
                    Some(item.artwork_url.clone())
                },
                isrc: isrc.map(ToOwned::to_owned),
                sources: vec![TrackSourceDto {
                    id: 0,
                    provider: provider.to_string(),
                    track_id: track_id.clone(),
                    codec: "alac".to_string(),
                    bit_depth: None,
                    sample_rate: None,
                    is_cached: false,
                }],
            });
        }
    }

    canonical
}

#[utoipa::path(
    get,
    path = "/api/v1/tracks/{id}",
    tag = "catalog",
    summary = "Get Track Metadata & Audio Specs",
    description = "Retrieves full track details including codec, bit depth, sample rate, ISRC, composer, and cached status.",
    params(
        ("id" = i32, Path, description = "Unique database track ID", example = 42)
    ),
    responses(
        (status = 200, description = "Comprehensive track metadata", body = TrackDetailDto),
        (status = 404, description = "Track not found in database cache")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn get_track(
    State(state): State<Arc<ServerState>>,
    Path(track_id): Path<i32>,
) -> Result<Json<TrackDetailDto>, ServerError> {
    let track = state
        .tracks_repo
        .find_track_by_id(track_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .ok_or_else(|| ServerError::NotFound(format!("Track {track_id} not found")))?;

    Ok(Json(track.into()))
}

/// Summary of a distinct cached album.
#[derive(Debug, Serialize, ToSchema)]
pub struct AlbumSummaryDto {
    /// Album title.
    #[schema(example = "1989")]
    pub album: String,
    /// Primary album artist.
    #[schema(example = "Taylor Swift")]
    pub artist: String,
}

/// Standard pagination query parameters.
#[derive(Debug, Deserialize, IntoParams)]
pub struct PaginationQuery {
    /// Page number (1-based index).
    #[param(example = 1)]
    pub page: Option<i64>,
    /// Number of items per page (default: 30, max: 100).
    #[param(example = 30)]
    pub limit: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/v1/albums",
    tag = "catalog",
    summary = "List Cached Albums",
    description = "Returns paginated list of distinct albums and artists cached in the database.",
    params(
        PaginationQuery
    ),
    responses(
        (status = 200, description = "Paginated list of distinct cached albums", body = Vec<AlbumSummaryDto>)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn list_albums(
    State(state): State<Arc<ServerState>>,
    Query(query): Query<PaginationQuery>,
) -> Result<Json<Vec<AlbumSummaryDto>>, ServerError> {
    let limit = query.limit.unwrap_or(30).clamp(1, 100);
    let page = query.page.unwrap_or(1).max(1);
    let offset = (page - 1) * limit;

    let rows = state
        .tracks_repo
        .list_distinct_albums(limit, offset)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    let albums = rows
        .into_iter()
        .map(|row| AlbumSummaryDto {
            album: row.album,
            artist: row.artist,
        })
        .collect();

    Ok(Json(albums))
}

/// Album details with complete tracklist.
#[derive(Debug, Serialize, ToSchema)]
pub struct AlbumDetailsDto {
    /// Album title.
    #[schema(example = "1989")]
    pub album: String,
    /// Album artist.
    #[schema(example = "Taylor Swift")]
    pub artist: String,
    /// Total count of tracks in this album.
    #[schema(example = 13)]
    pub track_count: usize,
    /// Ordered list of tracks in the album.
    pub tracks: Vec<TrackSummaryDto>,
}

#[utoipa::path(
    get,
    path = "/api/v1/albums/{id}",
    tag = "catalog",
    summary = "Get Album Tracklist",
    description = "Returns the complete ordered tracklist for an album, checking the database cache first and falling back to live Apple Music catalog if uncached.",
    params(
        ("id" = String, Path, description = "Album title, collection ID, or track ID", example = "1989")
    ),
    responses(
        (status = 200, description = "Album metadata and ordered tracklist", body = AlbumDetailsDto),
        (status = 404, description = "Album not found in cache or live catalog")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn get_album_tracks(
    State(state): State<Arc<ServerState>>,
    Path(id_or_name): Path<String>,
) -> Result<Json<AlbumDetailsDto>, ServerError> {
    let mut tracks = state
        .tracks_repo
        .find_tracks_by_album(&id_or_name)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    if tracks.is_empty()
        && let Ok(track_id) = id_or_name.parse::<i32>()
        && let Ok(Some(track)) = state.tracks_repo.find_track_by_id(track_id).await
    {
        tracks = state
            .tracks_repo
            .find_tracks_by_album(&track.album)
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;
    }

    if tracks.is_empty() {
        if let Some(ref catalog) = state.catalog_service
            && let Ok(album_res) = catalog.fetch_album_tracks(&id_or_name, "us").await
        {
            let album = album_res.album.album.clone();
            let artist = album_res.album.artist.clone();
            let track_count = album_res.tracks.len();
            let dtos = album_res
                .tracks
                .into_iter()
                .map(|t| TrackSummaryDto {
                    id: 0,
                    provider: "apple".to_string(),
                    track_id: t.id,
                    title: t.title,
                    artist: t.artist,
                    album: album.clone(),
                    duration: t.duration_secs as i32,
                    codec: "alac".to_string(),
                    bit_depth: None,
                    sample_rate: None,
                    is_cached: false,
                    isrc: None,
                })
                .collect();
            return Ok(Json(AlbumDetailsDto {
                album,
                artist,
                track_count,
                tracks: dtos,
            }));
        }
        return Err(ServerError::NotFound(format!(
            "Album '{id_or_name}' not found"
        )));
    }

    let album = tracks[0].album.clone();
    let artist = tracks[0].artist.clone();
    let track_count = tracks.len();
    let track_dtos = tracks.into_iter().map(Into::into).collect();

    Ok(Json(AlbumDetailsDto {
        album,
        artist,
        track_count,
        tracks: track_dtos,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/artists/{name}/tracks",
    tag = "catalog",
    summary = "Get Artist Tracks",
    description = "Returns all cached tracks for a specified artist.",
    params(
        ("name" = String, Path, description = "Artist name", example = "Taylor Swift")
    ),
    responses(
        (status = 200, description = "List of cached tracks by artist", body = Vec<TrackSummaryDto>)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn get_artist_tracks(
    State(state): State<Arc<ServerState>>,
    Path(artist_name): Path<String>,
) -> Result<Json<Vec<TrackSummaryDto>>, ServerError> {
    let tracks = state
        .tracks_repo
        .find_tracks_by_artist(&artist_name, 50)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(tracks.into_iter().map(Into::into).collect()))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use music::{Codec, Provider, TrackMeta};

    use super::*;

    struct FakeDbTrackSpec<'a> {
        id: i32,
        provider: Provider,
        track_id: &'a str,
        codec: Codec,
        title: &'a str,
        artist: &'a str,
        album: &'a str,
        duration: i32,
        isrc: Option<&'a str>,
    }

    fn fake_db_track(spec: FakeDbTrackSpec<'_>) -> db::Track {
        let FakeDbTrackSpec {
            id,
            provider,
            track_id,
            codec,
            title,
            artist,
            album,
            duration,
            isrc,
        } = spec;
        db::Track {
            id,
            provider,
            track_id: track_id.to_string(),
            codec,
            message_id: 100 + id,
            file_id: format!("file_{id}"),
            file_unique_id: format!("uniq_{id}"),
            title: title.to_string(),
            artist: artist.to_string(),
            album: album.to_string(),
            duration,
            bit_depth: 24,
            sample_rate: 48000,
            genre: "Pop".to_string(),
            release_date: "2024-01-01".to_string(),
            track_number: 1,
            track_count: 1,
            isrc: isrc.map(ToOwned::to_owned),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn fake_track_meta(
        id: &str,
        title: &str,
        artist: &str,
        album: &str,
        duration_secs: i64,
        artwork_url: &str,
        isrc: Option<&str>,
    ) -> TrackMeta {
        TrackMeta {
            id: id.to_string(),
            title: title.to_string(),
            artist: artist.to_string(),
            album: album.to_string(),
            album_artist: artist.to_string(),
            genre: Some("Pop".to_string()),
            release_date: "2024-01-01".to_string(),
            composer: None,
            track_number: Some(1),
            track_count: Some(1),
            disc_number: Some(1),
            disc_count: Some(1),
            duration_secs,
            explicit: false,
            content_advisory: None,
            artwork_url: artwork_url.to_string(),
            album_id: None,
            artist_id: None,
            isrc: isrc.map(ToOwned::to_owned),
            record_label: None,
            copyright: None,
            upc: None,
            is_streamable: Some(true),
        }
    }

    #[test]
    fn groups_tracks_by_isrc_across_providers() {
        let cached = vec![
            fake_db_track(FakeDbTrackSpec {
                id: 1,
                provider: Provider::Apple,
                track_id: "1440857781",
                codec: Codec::Alac,
                title: "Blank Space",
                artist: "Taylor Swift",
                album: "1989",
                duration: 231,
                isrc: Some("USCJY1431245"),
            }),
            fake_db_track(FakeDbTrackSpec {
                id: 2,
                provider: Provider::Qobuz,
                track_id: "8888888",
                codec: Codec::Flac,
                title: "Blank Space (Qobuz Master)",
                artist: "Taylor Swift",
                album: "1989 Deluxe",
                duration: 231,
                isrc: Some("USCJY1431245"),
            }),
        ];

        let live = vec![fake_track_meta(
            "1440857781",
            "Blank Space",
            "Taylor Swift",
            "1989",
            231,
            "https://artwork.url/image.jpg",
            Some("USCJY1431245"),
        )];

        let canonical = build_canonical_tracks(&cached, &live);
        assert_eq!(canonical.len(), 1);
        let track = &canonical[0];
        assert_eq!(track.id, "USCJY1431245");
        assert_eq!(track.isrc.as_deref(), Some("USCJY1431245"));
        assert_eq!(
            track.artwork_url.as_deref(),
            Some("https://artwork.url/image.jpg")
        );
        assert_eq!(track.sources.len(), 2);
        assert!(
            track
                .sources
                .iter()
                .any(|s| s.provider == "apple" && s.id == 1)
        );
        assert!(
            track
                .sources
                .iter()
                .any(|s| s.provider == "qobuz" && s.id == 2)
        );
    }

    #[test]
    fn groups_tracks_by_provider_track_id() {
        let cached = vec![
            fake_db_track(FakeDbTrackSpec {
                id: 1,
                provider: Provider::Apple,
                track_id: "1440857781",
                codec: Codec::Alac,
                title: "Blank Space",
                artist: "Taylor Swift",
                album: "1989",
                duration: 231,
                isrc: None,
            }),
            fake_db_track(FakeDbTrackSpec {
                id: 2,
                provider: Provider::Apple,
                track_id: "1440857781",
                codec: Codec::Ec3,
                title: "Blank Space",
                artist: "Taylor Swift",
                album: "1989",
                duration: 231,
                isrc: None,
            }),
        ];

        let canonical = build_canonical_tracks(&cached, &[]);
        assert_eq!(canonical.len(), 1);
        let track = &canonical[0];
        assert_eq!(track.sources.len(), 2);
        assert!(track.sources.iter().any(|s| s.codec == "alac"));
        assert!(track.sources.iter().any(|s| s.codec == "ec-3"));
    }

    #[test]
    fn groups_tracks_by_normalized_title_artist_and_duration_within_3s() {
        let cached = vec![fake_db_track(FakeDbTrackSpec {
            id: 1,
            provider: Provider::Apple,
            track_id: "111",
            codec: Codec::Alac,
            title: "Blank Space!",
            artist: "Taylor Swift",
            album: "1989",
            duration: 231,
            isrc: None,
        })];

        let live = vec![fake_track_meta(
            "222",
            "blank space",
            "taylor   swift",
            "1989 (Live)",
            233, // diff is 2 seconds (<= 3s)
            "https://artwork.url/thumb.jpg",
            None,
        )];

        let canonical = build_canonical_tracks(&cached, &live);
        assert_eq!(canonical.len(), 1);
        let track = &canonical[0];
        assert_eq!(track.sources.len(), 2);
        assert_eq!(
            track.artwork_url.as_deref(),
            Some("https://artwork.url/thumb.jpg")
        );
    }

    #[test]
    fn does_not_group_tracks_with_different_isrcs() {
        let cached = vec![
            fake_db_track(FakeDbTrackSpec {
                id: 1,
                provider: Provider::Apple,
                track_id: "111",
                codec: Codec::Alac,
                title: "Song A",
                artist: "Artist",
                album: "Album",
                duration: 200,
                isrc: Some("ISRC11111111"),
            }),
            fake_db_track(FakeDbTrackSpec {
                id: 2,
                provider: Provider::Apple,
                track_id: "222",
                codec: Codec::Alac,
                title: "Song A",
                artist: "Artist",
                album: "Album",
                duration: 200,
                isrc: Some("ISRC22222222"),
            }),
        ];

        let canonical = build_canonical_tracks(&cached, &[]);
        assert_eq!(canonical.len(), 2);
    }

    #[test]
    fn does_not_group_tracks_with_duration_diff_greater_than_3s() {
        let cached = vec![fake_db_track(FakeDbTrackSpec {
            id: 1,
            provider: Provider::Apple,
            track_id: "111",
            codec: Codec::Alac,
            title: "Song",
            artist: "Artist",
            album: "Album",
            duration: 200,
            isrc: None,
        })];

        let live = vec![fake_track_meta(
            "222", "Song", "Artist", "Album", 205, // diff is 5 seconds (> 3s)
            "", None,
        )];

        let canonical = build_canonical_tracks(&cached, &live);
        assert_eq!(canonical.len(), 2);
    }
}
