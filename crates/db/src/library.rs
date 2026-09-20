//! Deep user library and playlist management module.
//!
//! Encapsulates track bookmarking (favorites), playlist CRUD, ordered track associations,
//! position shifting, and cross-user authorization barriers.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::{
    DbError, DbPool,
    models::{
        NewUserFavorite, NewUserPlaylist, NewUserPlaylistTrack, Track, UserFavorite, UserPlaylist,
    },
    schema::{tracks, user_favorites, user_playlist_tracks, user_playlists},
};

#[derive(Debug, Clone)]
pub struct UserPlaylistSummary {
    pub id: i32,
    pub telegram_id: i64,
    pub name: String,
    pub track_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PlaylistDetails {
    pub playlist: UserPlaylist,
    pub tracks: Vec<Track>,
}

#[derive(Clone)]
pub struct LibraryManager {
    pool: DbPool,
}

impl LibraryManager {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    // --- Favorites ---

    /// Toggles a track in the user's favorites.
    /// Returns `Ok(true)` if added, `Ok(false)` if removed.
    pub async fn toggle_favorite(&self, telegram_id: i64, track_id: i32) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;

        let existing = user_favorites::table
            .filter(user_favorites::telegram_id.eq(telegram_id))
            .filter(user_favorites::track_id.eq(track_id))
            .first::<UserFavorite>(&mut *conn)
            .await
            .optional()?;

        if existing.is_some() {
            diesel::delete(
                user_favorites::table
                    .filter(user_favorites::telegram_id.eq(telegram_id))
                    .filter(user_favorites::track_id.eq(track_id)),
            )
            .execute(&mut *conn)
            .await?;
            Ok(false)
        } else {
            diesel::insert_into(user_favorites::table)
                .values(NewUserFavorite {
                    telegram_id,
                    track_id,
                })
                .execute(&mut *conn)
                .await?;
            Ok(true)
        }
    }

    /// Checks if a track is favorited by the user.
    pub async fn is_favorite(&self, telegram_id: i64, track_id: i32) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        let count = user_favorites::table
            .filter(user_favorites::telegram_id.eq(telegram_id))
            .filter(user_favorites::track_id.eq(track_id))
            .count()
            .get_result::<i64>(&mut *conn)
            .await?;
        Ok(count > 0)
    }

    /// Lists tracks favorited by the user, ordered by most recently favorited.
    pub async fn list_favorites(
        &self,
        telegram_id: i64,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Track>, DbError> {
        let mut conn = self.pool.connection().await?;
        let rows = user_favorites::table
            .inner_join(tracks::table.on(tracks::id.eq(user_favorites::track_id)))
            .filter(user_favorites::telegram_id.eq(telegram_id))
            .order(user_favorites::created_at.desc())
            .offset(offset)
            .limit(limit)
            .select(Track::as_select())
            .load::<Track>(&mut *conn)
            .await?;
        Ok(rows)
    }

    /// Explicitly adds a track to the user's favorites.
    /// Returns `Ok(true)` if newly added, `Ok(false)` if already favorited.
    pub async fn add_favorite(&self, telegram_id: i64, track_id: i32) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        let affected = diesel::insert_into(user_favorites::table)
            .values(NewUserFavorite {
                telegram_id,
                track_id,
            })
            .on_conflict_do_nothing()
            .execute(&mut *conn)
            .await?;

        Ok(affected > 0)
    }

    /// Explicitly removes a track from the user's favorites.
    /// Returns `Ok(true)` if deleted, `Ok(false)` if wasn't present.
    pub async fn remove_favorite(&self, telegram_id: i64, track_id: i32) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        let affected = diesel::delete(
            user_favorites::table
                .filter(user_favorites::telegram_id.eq(telegram_id))
                .filter(user_favorites::track_id.eq(track_id)),
        )
        .execute(&mut *conn)
        .await?;

        Ok(affected > 0)
    }

    /// Lists track IDs favorited by the user, ordered by most recently favorited.
    pub async fn list_favorite_ids(&self, telegram_id: i64) -> Result<Vec<i32>, DbError> {
        let mut conn = self.pool.connection().await?;
        let ids = user_favorites::table
            .filter(user_favorites::telegram_id.eq(telegram_id))
            .order(user_favorites::created_at.desc())
            .select(user_favorites::track_id)
            .load::<i32>(&mut *conn)
            .await?;

        Ok(ids)
    }

    // --- Playlists ---

    /// Creates a new custom playlist for the user.
    pub async fn create_playlist(
        &self,
        telegram_id: i64,
        name: &str,
    ) -> Result<UserPlaylist, DbError> {
        let mut conn = self.pool.connection().await?;
        let clean_name = name.trim();
        if clean_name.is_empty() {
            return Err(DbError::Validation("Playlist name cannot be empty".into()));
        }

        let playlist = diesel::insert_into(user_playlists::table)
            .values(NewUserPlaylist {
                telegram_id,
                name: clean_name,
            })
            .get_result::<UserPlaylist>(&mut *conn)
            .await?;

        Ok(playlist)
    }

    /// Lists all playlists belonging to a user with their track count.
    pub async fn list_playlists(
        &self,
        telegram_id: i64,
    ) -> Result<Vec<UserPlaylistSummary>, DbError> {
        let mut conn = self.pool.connection().await?;
        let playlists = user_playlists::table
            .filter(user_playlists::telegram_id.eq(telegram_id))
            .order(user_playlists::updated_at.desc())
            .load::<UserPlaylist>(&mut *conn)
            .await?;

        let mut summaries = Vec::with_capacity(playlists.len());
        for p in playlists {
            let count = user_playlist_tracks::table
                .filter(user_playlist_tracks::playlist_id.eq(p.id))
                .count()
                .get_result::<i64>(&mut *conn)
                .await?;
            summaries.push(UserPlaylistSummary {
                id: p.id,
                telegram_id: p.telegram_id,
                name: p.name,
                track_count: count,
                created_at: p.created_at,
                updated_at: p.updated_at,
            });
        }

        Ok(summaries)
    }

    /// Gets a playlist and its ordered tracks. Enforces user ownership.
    pub async fn get_playlist(
        &self,
        telegram_id: i64,
        playlist_id: i32,
    ) -> Result<PlaylistDetails, DbError> {
        let mut conn = self.pool.connection().await?;
        let playlist = user_playlists::table
            .filter(user_playlists::id.eq(playlist_id))
            .filter(user_playlists::telegram_id.eq(telegram_id))
            .first::<UserPlaylist>(&mut *conn)
            .await
            .optional()?
            .ok_or_else(|| DbError::NotFound(format!("Playlist {playlist_id} not found")))?;

        let tracks = user_playlist_tracks::table
            .inner_join(tracks::table.on(tracks::id.eq(user_playlist_tracks::track_id)))
            .filter(user_playlist_tracks::playlist_id.eq(playlist_id))
            .order(user_playlist_tracks::position.asc())
            .select(Track::as_select())
            .load::<Track>(&mut *conn)
            .await?;

        Ok(PlaylistDetails { playlist, tracks })
    }

    /// Adds one or more tracks to the end of a playlist. Enforces user ownership.
    pub async fn add_tracks_to_playlist(
        &self,
        telegram_id: i64,
        playlist_id: i32,
        track_ids: &[i32],
    ) -> Result<usize, DbError> {
        if track_ids.is_empty() {
            return Ok(0);
        }

        let mut conn = self.pool.connection().await?;
        ensure_playlist_owner(&mut conn, telegram_id, playlist_id).await?;

        let max_pos: Option<i32> = user_playlist_tracks::table
            .filter(user_playlist_tracks::playlist_id.eq(playlist_id))
            .select(diesel::dsl::max(user_playlist_tracks::position))
            .first::<Option<i32>>(&mut *conn)
            .await?;

        let mut next_pos = max_pos.map_or(0, |pos| pos + 1);
        let mut inserted = 0;

        for &track_id in track_ids {
            let res = diesel::insert_into(user_playlist_tracks::table)
                .values(NewUserPlaylistTrack {
                    playlist_id,
                    track_id,
                    position: next_pos,
                })
                .on_conflict_do_nothing()
                .execute(&mut *conn)
                .await?;

            if res > 0 {
                next_pos += 1;
                inserted += res;
            }
        }

        if inserted > 0 {
            diesel::update(user_playlists::table.filter(user_playlists::id.eq(playlist_id)))
                .set(user_playlists::updated_at.eq(Utc::now()))
                .execute(&mut *conn)
                .await?;
        }

        Ok(inserted)
    }

    /// Removes a track from a playlist.
    pub async fn remove_track_from_playlist(
        &self,
        telegram_id: i64,
        playlist_id: i32,
        track_id: i32,
    ) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        ensure_playlist_owner(&mut conn, telegram_id, playlist_id).await?;

        let deleted = diesel::delete(
            user_playlist_tracks::table
                .filter(user_playlist_tracks::playlist_id.eq(playlist_id))
                .filter(user_playlist_tracks::track_id.eq(track_id)),
        )
        .execute(&mut *conn)
        .await?;

        if deleted > 0 {
            diesel::update(user_playlists::table.filter(user_playlists::id.eq(playlist_id)))
                .set(user_playlists::updated_at.eq(Utc::now()))
                .execute(&mut *conn)
                .await?;
        }

        Ok(deleted > 0)
    }

    /// Reorders tracks in a playlist to match the provided list of track IDs.
    pub async fn reorder_playlist(
        &self,
        telegram_id: i64,
        playlist_id: i32,
        ordered_track_ids: &[i32],
    ) -> Result<(), DbError> {
        let mut conn = self.pool.connection().await?;
        ensure_playlist_owner(&mut conn, telegram_id, playlist_id).await?;

        conn.build_transaction()
            .run(async |transaction| -> Result<(), diesel::result::Error> {
                diesel::delete(
                    user_playlist_tracks::table
                        .filter(user_playlist_tracks::playlist_id.eq(playlist_id)),
                )
                .execute(&mut *transaction)
                .await?;

                for (pos, &track_id) in ordered_track_ids.iter().enumerate() {
                    diesel::insert_into(user_playlist_tracks::table)
                        .values(NewUserPlaylistTrack {
                            playlist_id,
                            track_id,
                            position: pos as i32,
                        })
                        .on_conflict_do_nothing()
                        .execute(&mut *transaction)
                        .await?;
                }

                diesel::update(user_playlists::table.filter(user_playlists::id.eq(playlist_id)))
                    .set(user_playlists::updated_at.eq(Utc::now()))
                    .execute(&mut *transaction)
                    .await?;

                Ok(())
            })
            .await?;

        Ok(())
    }

    /// Deletes a playlist and its associated track mappings (via ON DELETE CASCADE).
    pub async fn delete_playlist(
        &self,
        telegram_id: i64,
        playlist_id: i32,
    ) -> Result<bool, DbError> {
        let mut conn = self.pool.connection().await?;
        let deleted = diesel::delete(
            user_playlists::table
                .filter(user_playlists::id.eq(playlist_id))
                .filter(user_playlists::telegram_id.eq(telegram_id)),
        )
        .execute(&mut *conn)
        .await?;

        Ok(deleted > 0)
    }
}

async fn ensure_playlist_owner(
    conn: &mut AsyncPgConnection,
    telegram_id: i64,
    playlist_id: i32,
) -> Result<(), DbError> {
    let exists = user_playlists::table
        .filter(user_playlists::id.eq(playlist_id))
        .filter(user_playlists::telegram_id.eq(telegram_id))
        .count()
        .get_result::<i64>(conn)
        .await?;

    if exists == 0 {
        return Err(DbError::NotFound(format!(
            "Playlist {playlist_id} not found"
        )));
    }

    Ok(())
}
