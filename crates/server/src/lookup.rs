use std::collections::{HashMap, HashSet};

use axum::{Json, extract::State};
use music::Codec;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, auth::AuthedUser, error::ServerError};

#[derive(Debug, Deserialize, ToSchema)]
pub struct LookupRequest {
    #[serde(default)]
    pub track_ids: Option<Vec<String>>,

    #[serde(default)]
    pub album_ids: Option<Vec<String>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TrackFormatResult {
    #[schema(example = 123)]
    pub id: i32,

    #[schema(example = "alac")]
    pub format: String,

    #[schema(example = 123456789_u64)]
    pub file_size_bytes: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TrackLookupResult {
    #[schema(example = "1440832410")]
    pub apple_track_id: String,

    pub formats: Vec<TrackFormatResult>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AlbumZipPartResult {
    #[schema(example = 223)]
    pub id: i32,

    #[schema(example = "alac")]
    pub format: String,

    #[schema(example = 34343_u64)]
    pub file_size_bytes: u64,

    #[schema(example = 1)]
    pub part: i32,

    #[schema(example = 1)]
    pub total_parts: i32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AlbumLookupResult {
    #[schema(example = "1451234567")]
    pub apple_album_id: String,

    pub zip_available: bool,

    pub zip_formats: Vec<AlbumZipPartResult>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LookupResponse {
    pub tracks: Vec<TrackLookupResult>,

    pub albums: Vec<AlbumLookupResult>,
}

#[utoipa::path(
    post,
    path = "/api/v1/lookup",
    tag = "lookup",
    summary = "Look Up Cached Apple Tracks and Album ZIPs",
    description = "Checks local Apple Music cache entries by provider-native IDs. Each track format includes its database track ID for requesting playback, and its exact file size when media metadata can be resolved. Album results include whether a complete ZIP rendition is available and list each part of every complete rendition with its database row ID, size, part number, and total part count. Unavailable tracks are omitted; every requested album is returned. At least one of `track_ids` or `album_ids` must contain an ID.",
    request_body = LookupRequest,
    responses(
        (status = 200, description = "Cached track formats and file sizes, plus album ZIP availability and per-part IDs, sizes, and counts", body = LookupResponse),
        (status = 400, description = "Both ID lists are empty or an ID is blank"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 500, description = "Internal error while checking the cache")
    ),
    security(("bearer_auth" = []))
)]
pub async fn lookup(
    State(state): State<std::sync::Arc<ServerState>>,
    _user: AuthedUser,
    Json(request): Json<LookupRequest>,
) -> Result<Json<LookupResponse>, ServerError> {
    let track_ids = normalize_ids(request.track_ids, "track_ids")?;
    let album_ids = normalize_ids(request.album_ids, "album_ids")?;
    if track_ids.is_empty() && album_ids.is_empty() {
        return Err(ServerError::BadRequest(
            "At least one ID is required in 'track_ids' or 'album_ids'".to_owned(),
        ));
    }

    let track_rows = state.tracks_repo.find_track_formats(&track_ids).await?;
    let album_rows = db::AlbumsRepository::new(state.db.clone())
        .find_album_parts_by_ids(&album_ids)
        .await?;

    let mut track_formats = HashMap::<String, HashMap<Codec, (i32, Option<u64>)>>::new();
    for (db_track_id, track_id, codec) in track_rows {
        if apple_format(codec).is_some() {
            let file_size_bytes = state
                .stream_engine
                .resolve_track_media(db_track_id, false)
                .await
                .ok()
                .map(|metadata| metadata.file_size);
            track_formats
                .entry(track_id)
                .or_default()
                .insert(codec, (db_track_id, file_size_bytes));
        }
    }
    let tracks = track_ids
        .into_iter()
        .filter_map(|apple_track_id| {
            let formats = track_formats
                .remove(&apple_track_id)
                .map(ordered_track_formats)
                .unwrap_or_default();
            (!formats.is_empty()).then_some(TrackLookupResult {
                apple_track_id,
                formats,
            })
        })
        .collect();

    let mut album_parts = HashMap::<String, Vec<db::Album>>::new();
    for part in album_rows {
        album_parts
            .entry(part.album_id.clone())
            .or_default()
            .push(part);
    }
    let albums = album_ids
        .into_iter()
        .map(|apple_album_id| {
            let zip_formats = album_parts
                .get(&apple_album_id)
                .map(|parts| complete_zip_parts(parts))
                .unwrap_or_default();
            AlbumLookupResult {
                apple_album_id,
                zip_available: !zip_formats.is_empty(),
                zip_formats,
            }
        })
        .collect();

    Ok(Json(LookupResponse { tracks, albums }))
}

fn normalize_ids(ids: Option<Vec<String>>, field: &str) -> Result<Vec<String>, ServerError> {
    let mut unique_ids = Vec::new();
    let mut seen = HashSet::new();
    for id in ids.unwrap_or_default() {
        let id = id.trim();
        if id.is_empty() {
            return Err(ServerError::BadRequest(format!(
                "IDs in '{field}' must not be empty"
            )));
        }
        if seen.insert(id.to_owned()) {
            unique_ids.push(id.to_owned());
        }
    }
    Ok(unique_ids)
}

fn apple_format(codec: Codec) -> Option<&'static str> {
    match codec {
        Codec::Alac => Some("alac"),
        Codec::Aac => Some("aac"),
        Codec::Ec3 => Some("ec-3"),
    }
}

fn ordered_track_formats(formats: HashMap<Codec, (i32, Option<u64>)>) -> Vec<TrackFormatResult> {
    [Codec::Alac, Codec::Aac, Codec::Ec3]
        .into_iter()
        .filter_map(|codec| {
            let (id, file_size_bytes) = formats.get(&codec)?;
            Some(TrackFormatResult {
                id: *id,
                format: apple_format(codec)?.to_owned(),
                file_size_bytes: *file_size_bytes,
            })
        })
        .collect()
}

fn complete_zip_parts(parts: &[db::Album]) -> Vec<AlbumZipPartResult> {
    [Codec::Alac, Codec::Aac, Codec::Ec3]
        .into_iter()
        .filter_map(|codec| {
            let mut codec_parts = parts
                .iter()
                .filter(|part| part.codec == codec)
                .collect::<Vec<_>>();
            if !has_complete_archive_parts(&codec_parts) {
                return None;
            }
            codec_parts.sort_by_key(|part| part.part_index);
            let format = apple_format(codec)?.to_owned();
            codec_parts
                .into_iter()
                .map(|part| {
                    Some(AlbumZipPartResult {
                        id: part.id,
                        format: format.clone(),
                        file_size_bytes: u64::try_from(part.file_size).ok()?,
                        part: part.part_index,
                        total_parts: part.total_parts,
                    })
                })
                .collect::<Option<Vec<_>>>()
        })
        .flatten()
        .collect()
}

pub(crate) fn has_complete_archive_parts(parts: &[&db::Album]) -> bool {
    let Some(first) = parts.first() else {
        return false;
    };
    let Ok(expected_parts) = usize::try_from(first.total_parts) else {
        return false;
    };
    if expected_parts == 0 || parts.len() != expected_parts {
        return false;
    }

    let mut seen_parts = vec![false; expected_parts];
    for part in parts {
        if part.total_parts != first.total_parts || part.generation_hash != first.generation_hash {
            return false;
        }
        let Some(index) = part
            .part_index
            .checked_sub(1)
            .and_then(|index| usize::try_from(index).ok())
        else {
            return false;
        };
        let Some(seen) = seen_parts.get_mut(index) else {
            return false;
        };
        if *seen {
            return false;
        }
        *seen = true;
    }

    seen_parts.into_iter().all(|seen| seen)
}
