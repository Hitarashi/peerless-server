use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{ServerState, auth::AuthedUser, error::ServerError};

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreatePlaylistRequest {
    #[schema(example = "Road trip")]
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RenamePlaylistRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ImportPlaylistRequest {
    pub name: String,
    pub source_url: String,
    pub track_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PlaylistResponse {
    pub id: String,
    pub name: String,
    pub is_favorites: bool,
    pub source_url: Option<String>,
    pub track_count: i64,
}

impl From<db::Playlist> for PlaylistResponse {
    fn from(playlist: db::Playlist) -> Self {
        Self {
            id: playlist.id,
            name: playlist.name,
            is_favorites: playlist.is_favorites,
            source_url: playlist.source_url,
            track_count: playlist.track_count,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PlaylistDetailResponse {
    pub playlist: PlaylistResponse,
    pub track_ids: Vec<String>,
}

impl From<db::PlaylistDetail> for PlaylistDetailResponse {
    fn from(detail: db::PlaylistDetail) -> Self {
        Self {
            playlist: detail.playlist.into(),
            track_ids: detail.track_ids,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ReplacePlaylistTracksRequest {
    pub track_ids: Vec<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/playlists",
    tag = "playlists",
    summary = "List Playlists",
    responses((status = 200, body = [PlaylistResponse])),
    security(("bearer_auth" = []))
)]
pub async fn list_playlists(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
) -> Result<Json<Vec<PlaylistResponse>>, ServerError> {
    let playlists = db::list_playlists(&state.db, user.telegram_id).await?;
    Ok(Json(
        playlists.into_iter().map(PlaylistResponse::from).collect(),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/playlists",
    tag = "playlists",
    summary = "Create Playlist",
    request_body = CreatePlaylistRequest,
    responses((status = 201, body = PlaylistResponse), (status = 400, description = "Invalid name or playlist limit reached")),
    security(("bearer_auth" = []))
)]
pub async fn create_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Json(request): Json<CreatePlaylistRequest>,
) -> Result<(StatusCode, Json<PlaylistResponse>), ServerError> {
    let playlist = db::create_playlist(&state.db, user.telegram_id, &request.name).await?;
    Ok((StatusCode::CREATED, Json(playlist.into())))
}

#[utoipa::path(
    post,
    path = "/api/v1/playlists/import",
    tag = "playlists",
    summary = "Save Imported Playlist",
    description = "Saves tracks already matched to Apple catalog entries. Importing the same source URL replaces its saved tracks.",
    request_body = ImportPlaylistRequest,
    responses((status = 200, body = PlaylistResponse), (status = 400, description = "Invalid imported playlist"), (status = 409, description = "Playlist limit reached")),
    security(("bearer_auth" = []))
)]
pub async fn import_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Json(request): Json<ImportPlaylistRequest>,
) -> Result<Json<PlaylistResponse>, ServerError> {
    if !request
        .source_url
        .starts_with("https://www.youtube.com/playlist")
        && !request
            .source_url
            .starts_with("https://music.youtube.com/playlist")
    {
        return Err(ServerError::BadRequest(
            "Only public YouTube playlists can be imported".into(),
        ));
    }
    let playlist = db::import_playlist(
        &state.db,
        user.telegram_id,
        &request.name,
        &request.source_url,
        &request.track_ids,
    )
    .await?;
    Ok(Json(playlist.into()))
}

#[utoipa::path(
    get,
    path = "/api/v1/playlists/{playlist_id}",
    tag = "playlists",
    summary = "Get Playlist",
    params(("playlist_id" = String, Path, description = "Playlist ID")),
    responses((status = 200, body = PlaylistDetailResponse), (status = 404, description = "Playlist not found")),
    security(("bearer_auth" = []))
)]
pub async fn get_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<String>,
) -> Result<Json<PlaylistDetailResponse>, ServerError> {
    let detail = db::get_playlist(&state.db, user.telegram_id, &playlist_id).await?;
    Ok(Json(detail.into()))
}

#[utoipa::path(
    patch,
    path = "/api/v1/playlists/{playlist_id}",
    tag = "playlists",
    summary = "Rename Playlist",
    params(("playlist_id" = String, Path, description = "Playlist ID")),
    request_body = RenamePlaylistRequest,
    responses((status = 200, body = PlaylistResponse), (status = 404, description = "Playlist not found")),
    security(("bearer_auth" = []))
)]
pub async fn rename_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<String>,
    Json(request): Json<RenamePlaylistRequest>,
) -> Result<Json<PlaylistResponse>, ServerError> {
    let playlist =
        db::rename_playlist(&state.db, user.telegram_id, &playlist_id, &request.name).await?;
    Ok(Json(playlist.into()))
}

#[utoipa::path(
    delete,
    path = "/api/v1/playlists/{playlist_id}",
    tag = "playlists",
    summary = "Delete Playlist",
    params(("playlist_id" = String, Path, description = "Playlist ID")),
    responses((status = 204, description = "Playlist deleted"), (status = 404, description = "Playlist not found")),
    security(("bearer_auth" = []))
)]
pub async fn delete_playlist(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<String>,
) -> Result<StatusCode, ServerError> {
    db::delete_playlist(&state.db, user.telegram_id, &playlist_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    put,
    path = "/api/v1/playlists/{playlist_id}/tracks",
    tag = "playlists",
    summary = "Replace Playlist Tracks",
    params(("playlist_id" = String, Path, description = "Playlist ID")),
    request_body = ReplacePlaylistTracksRequest,
    responses((status = 200, body = PlaylistDetailResponse), (status = 404, description = "Playlist not found")),
    security(("bearer_auth" = []))
)]
pub async fn replace_playlist_tracks(
    State(state): State<Arc<ServerState>>,
    user: AuthedUser,
    Path(playlist_id): Path<String>,
    Json(request): Json<ReplacePlaylistTracksRequest>,
) -> Result<Json<PlaylistDetailResponse>, ServerError> {
    let detail = db::replace_playlist_tracks(
        &state.db,
        user.telegram_id,
        &playlist_id,
        &request.track_ids,
    )
    .await?;
    Ok(Json(detail.into()))
}

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/", get(list_playlists).post(create_playlist))
        .route("/import", post(import_playlist))
        .route(
            "/{playlist_id}",
            get(get_playlist)
                .patch(rename_playlist)
                .delete(delete_playlist),
        )
        .route("/{playlist_id}/tracks", put(replace_playlist_tracks))
}
