use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, auth::AuthedUser, catalog::TrackSummaryDto, error::ServerError};

#[utoipa::path(
    get,
    path = "/api/v1/me/favorites",
    tag = "library",
    summary = "List User Favorite Tracks",
    description = "Returns all tracks bookmarked as favorites by the authenticated user with complete metadata.",
    responses(
        (status = 200, description = "List of user favorited tracks", body = Vec<TrackSummaryDto>),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn list_favorites(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Json<Vec<TrackSummaryDto>>, ServerError> {
    let tracks = state
        .library_mgr
        .list_favorites(user.telegram_id, 0, 1000)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(tracks.into_iter().map(Into::into).collect()))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/favorites/{track_id}",
    tag = "library",
    summary = "Add Track to Favorites",
    description = "Bookmarks a track in the authenticated user's favorites library.",
    params(
        ("track_id" = i32, Path, description = "Unique database track ID", example = 42)
    ),
    responses(
        (status = 200, description = "Track successfully added to favorites"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn add_favorite(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(track_id): Path<i32>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let added = state
        .library_mgr
        .add_favorite(user.telegram_id, track_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(serde_json::json!({ "added": added })))
}

#[utoipa::path(
    delete,
    path = "/api/v1/me/favorites/{track_id}",
    tag = "library",
    summary = "Remove Track from Favorites",
    description = "Removes a track from the authenticated user's favorites library.",
    params(
        ("track_id" = i32, Path, description = "Unique database track ID", example = 42)
    ),
    responses(
        (status = 200, description = "Track successfully removed from favorites"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn remove_favorite(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(track_id): Path<i32>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let removed = state
        .library_mgr
        .remove_favorite(user.telegram_id, track_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(serde_json::json!({ "removed": removed })))
}

/// Summary information for a user playlist.
#[derive(Debug, Serialize, ToSchema)]
pub struct PlaylistSummaryDto {
    /// Playlist database identifier.
    #[schema(example = 1)]
    pub id: i32,
    /// Custom playlist name.
    #[schema(example = "Late Night Lossless")]
    pub name: String,
    /// Total number of tracks in the playlist.
    #[schema(example = 24)]
    pub track_count: i64,
    /// Playlist creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp.
    pub updated_at: DateTime<Utc>,
}

impl From<db::UserPlaylistSummary> for PlaylistSummaryDto {
    fn from(p: db::UserPlaylistSummary) -> Self {
        Self {
            id: p.id,
            name: p.name,
            track_count: p.track_count,
            created_at: p.created_at,
            updated_at: p.updated_at,
        }
    }
}

/// Request payload to create a new user playlist.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePlaylistRequest {
    /// Custom playlist name.
    #[schema(example = "Late Night Lossless")]
    pub name: String,
}

/// Playlist details including full ordered list of tracks.
#[derive(Debug, Serialize, ToSchema)]
pub struct PlaylistWithTracksDto {
    /// Playlist database identifier.
    #[schema(example = 1)]
    pub id: i32,
    /// Playlist title.
    #[schema(example = "Late Night Lossless")]
    pub name: String,
    /// Ordered list of tracks in the playlist.
    pub tracks: Vec<TrackSummaryDto>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Modification timestamp.
    pub updated_at: DateTime<Utc>,
}

/// Request payload to reorder or set tracks in a playlist.
#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdatePlaylistRequest {
    /// Complete ordered array of database track IDs.
    #[schema(example = json!([42, 108, 256]))]
    pub track_ids: Vec<i32>,
}

#[utoipa::path(
    get,
    path = "/api/v1/me/playlists",
    tag = "library",
    summary = "List User Playlists",
    description = "Returns summaries of all custom playlists created by the authenticated user.",
    responses(
        (status = 200, description = "List of user playlists", body = Vec<PlaylistSummaryDto>),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn list_playlists(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Json<Vec<PlaylistSummaryDto>>, ServerError> {
    let playlists = state
        .library_mgr
        .list_playlists(user.telegram_id)
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?;

    Ok(Json(playlists.into_iter().map(Into::into).collect()))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/playlists",
    tag = "library",
    summary = "Create Custom Playlist",
    description = "Creates a new custom user playlist.",
    request_body = CreatePlaylistRequest,
    responses(
        (status = 200, description = "Playlist created successfully", body = PlaylistSummaryDto),
        (status = 400, description = "Invalid playlist name"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn create_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Json(payload): Json<CreatePlaylistRequest>,
) -> Result<Json<PlaylistSummaryDto>, ServerError> {
    let p = state
        .library_mgr
        .create_playlist(user.telegram_id, &payload.name)
        .await?;

    Ok(Json(PlaylistSummaryDto {
        id: p.id,
        name: p.name,
        track_count: 0,
        created_at: p.created_at,
        updated_at: p.updated_at,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/me/playlists/{id}",
    tag = "library",
    summary = "Get Playlist with Tracks",
    description = "Retrieves a custom playlist and its ordered list of tracks.",
    params(
        ("id" = i32, Path, description = "Unique playlist database ID", example = 1)
    ),
    responses(
        (status = 200, description = "Playlist details with ordered tracklist", body = PlaylistWithTracksDto),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Playlist not found")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn get_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<i32>,
) -> Result<Json<PlaylistWithTracksDto>, ServerError> {
    let details = state
        .library_mgr
        .get_playlist(user.telegram_id, playlist_id)
        .await?;

    let tracks = details.tracks.into_iter().map(Into::into).collect();

    Ok(Json(PlaylistWithTracksDto {
        id: details.playlist.id,
        name: details.playlist.name,
        tracks,
        created_at: details.playlist.created_at,
        updated_at: details.playlist.updated_at,
    }))
}

#[utoipa::path(
    put,
    path = "/api/v1/me/playlists/{id}",
    tag = "library",
    summary = "Update Playlist Tracks (Reorder / Set)",
    description = "Replaces or reorders the track IDs within a custom playlist.",
    params(
        ("id" = i32, Path, description = "Unique playlist database ID", example = 1)
    ),
    request_body = UpdatePlaylistRequest,
    responses(
        (status = 200, description = "Playlist tracks successfully updated"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Playlist not found")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn update_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<i32>,
    Json(payload): Json<UpdatePlaylistRequest>,
) -> Result<Json<serde_json::Value>, ServerError> {
    state
        .library_mgr
        .reorder_playlist(user.telegram_id, playlist_id, &payload.track_ids)
        .await?;

    Ok(Json(serde_json::json!({ "updated": true })))
}

#[utoipa::path(
    delete,
    path = "/api/v1/me/playlists/{id}",
    tag = "library",
    summary = "Delete Custom Playlist",
    description = "Deletes a custom user playlist from the database.",
    params(
        ("id" = i32, Path, description = "Unique playlist database ID", example = 1)
    ),
    responses(
        (status = 200, description = "Playlist successfully deleted"),
        (status = 401, description = "Unauthorized - Missing or invalid Bearer token"),
        (status = 404, description = "Playlist not found")
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn delete_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<i32>,
) -> Result<Json<serde_json::Value>, ServerError> {
    let deleted = state
        .library_mgr
        .delete_playlist(user.telegram_id, playlist_id)
        .await?;

    Ok(Json(serde_json::json!({ "deleted": deleted })))
}
