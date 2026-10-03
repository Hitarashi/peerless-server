use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{ServerState, auth::AuthedUser, error::ServerError};

/// Summary information for a track in catalog listings.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TrackSummaryDto {
    /// Unique database track ID. `None` when the track is not yet cached
    /// locally, so there is no local row to reference.
    #[schema(example = 42)]
    pub id: Option<i32>,
    /// Music provider name. Catalog responses contain Apple Music only.
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
    /// Lossless or compressed audio codec. `None` when the codec that will
    /// actually be delivered is not known (e.g. not yet ripped).
    #[schema(example = "alac")]
    pub codec: Option<String>,
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
    /// MusicBrainz recording identifier, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "2c2f8a0d-7f34-4d9e-a944-877112fc2285")]
    pub recording_mbid: Option<String>,
}

impl From<db::Track> for TrackSummaryDto {
    fn from(t: db::Track) -> Self {
        Self {
            id: Some(t.id),
            provider: t.provider.as_str().to_string(),
            track_id: t.track_id,
            title: t.title,
            artist: t.artist,
            album: t.album,
            duration: t.duration,
            codec: Some(t.codec.as_str().to_string()),
            bit_depth: Some(t.bit_depth),
            sample_rate: Some(t.sample_rate),
            is_cached: true,
            isrc: t.isrc,
            recording_mbid: t.recording_mbid,
        }
    }
}

/// Comprehensive track metadata and technical specifications.
#[derive(Debug, Serialize, ToSchema)]
pub struct TrackDetailDto {
    /// Unique database track ID.
    #[schema(example = 42)]
    pub id: i32,
    /// Music provider. Catalog responses contain Apple Music only.
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
    /// MusicBrainz recording identifier, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "2c2f8a0d-7f34-4d9e-a944-877112fc2285")]
    pub recording_mbid: Option<String>,
    /// Track composer; currently unavailable in database-backed track responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "Taylor Swift, Max Martin, Shellback")]
    pub composer: Option<String>,
    /// Disc number for multi-disc releases; currently unavailable in database-backed track responses.
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
            recording_mbid: t.recording_mbid,
            composer: None,
            disc_number: None,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/tracks/{id}",
    tag = "catalog",
    summary = "Get Track Metadata & Audio Specs",
    description = "Retrieves database track metadata and technical specifications, including codec, bit depth, sample rate, ISRC, and cached status. Composer and disc number are currently unavailable in database-backed track responses and are omitted.",
    params(
        ("id" = i32, Path, description = "Unique database track ID", example = 42)
    ),
    responses(
        (status = 200, description = "Comprehensive track metadata", body = TrackDetailDto),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Track not found in database cache"),
        (status = 500, description = "Internal error while retrieving track metadata")
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_track(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
    Path(db_track_id): Path<i32>,
) -> Result<Json<TrackDetailDto>, ServerError> {
    let track = state
        .tracks_repo
        .find_track_by_id(db_track_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
        .ok_or_else(|| ServerError::NotFound(format!("Track {db_track_id} not found")))?;

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
    /// Number of items per page (default: 30, clamped to 1-100).
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
        (status = 200, description = "Paginated list of distinct cached albums", body = Vec<AlbumSummaryDto>),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while retrieving cached albums")
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_albums(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
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
    path = "/api/v1/albums/{album_ref}",
    tag = "catalog",
    summary = "Get Album Tracklist",
    description = "Returns the complete ordered tracklist for an album, checking the database cache first and falling back to live Apple Music catalog if uncached.",
    params(
        ("album_ref" = String, Path, description = "Album reference: an album title, a provider collection ID, or a cached track ID whose album is used", example = "1989")
    ),
    responses(
        (status = 200, description = "Album metadata and ordered tracklist", body = AlbumDetailsDto),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Album not found in cache or live catalog"),
        (status = 500, description = "Internal error while retrieving cached album tracks")
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_album_tracks(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
    Path(album_ref): Path<String>,
) -> Result<Json<AlbumDetailsDto>, ServerError> {
    let mut tracks = state
        .tracks_repo
        .find_tracks_by_album(&album_ref)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    if tracks.is_empty()
        && let Ok(db_track_id) = album_ref.parse::<i32>()
        && let Ok(Some(track)) = state.tracks_repo.find_track_by_id(db_track_id).await
    {
        tracks = state
            .tracks_repo
            .find_tracks_by_album(&track.album)
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;
    }

    if tracks.is_empty() {
        let settings = state.settings_store.get_settings();
        let default_storefront = engine::settings::resolve_default_storefront(&settings);
        if let Some(ref catalog) = state.catalog_service
            && let Ok(album_res) = catalog
                .fetch_album_tracks(&album_ref, default_storefront)
                .await
        {
            let album = album_res.album.album.clone();
            let artist = album_res.album.artist.clone();
            let track_count = album_res.tracks.len();
            let dtos = album_res
                .tracks
                .into_iter()
                .map(|t| TrackSummaryDto {
                    // Live catalog tracks are not in the local database yet.
                    id: None,
                    provider: "apple".to_string(),
                    track_id: t.id,
                    title: t.title,
                    artist: t.artist,
                    album: album.clone(),
                    duration: t.duration_secs as i32,
                    // The codec is only known once the track is ripped.
                    codec: None,
                    bit_depth: None,
                    sample_rate: None,
                    is_cached: false,
                    isrc: None,
                    recording_mbid: None,
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
            "Album '{album_ref}' not found"
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
    description = "Returns up to 50 cached tracks whose artist field contains the supplied text, case-insensitively.",
    params(
        ("name" = String, Path, description = "Artist name", example = "Taylor Swift")
    ),
    responses(
        (status = 200, description = "List of up to 50 cached tracks by artist", body = Vec<TrackSummaryDto>),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while retrieving the artist's cached tracks")
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_artist_tracks(
    State(state): State<Arc<ServerState>>,
    _user: AuthedUser,
    Path(artist_name): Path<String>,
) -> Result<Json<Vec<TrackSummaryDto>>, ServerError> {
    let tracks = state
        .tracks_repo
        .find_tracks_by_artist(&artist_name, 50)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(tracks.into_iter().map(Into::into).collect()))
}
