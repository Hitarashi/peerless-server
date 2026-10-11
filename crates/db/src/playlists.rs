use chrono::{DateTime, Utc};
use diesel::{OptionalExtension, QueryableByName, sql_query, sql_types::*};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::Serialize;

use crate::{DbError, DbPool};

pub const MAX_CUSTOM_PLAYLISTS: i64 = 7;
const MAX_PLAYLIST_TRACKS: usize = 1_000;

#[derive(Debug, Clone, Serialize)]
pub struct Playlist {
    pub id: String,
    pub name: String,
    pub is_favorites: bool,
    pub source_url: Option<String>,
    pub track_count: i64,
    pub preview_track_ids: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlaylistDetail {
    pub playlist: Playlist,
    pub track_ids: Vec<String>,
}

#[derive(QueryableByName)]
struct PlaylistRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Bool)]
    is_favorites: bool,
    #[diesel(sql_type = Nullable<Text>)]
    source_url: Option<String>,
    #[diesel(sql_type = BigInt)]
    track_count: i64,
    #[diesel(sql_type = Array<Text>)]
    preview_track_ids: Vec<String>,
    #[diesel(sql_type = Timestamptz)]
    created_at: DateTime<Utc>,
    #[diesel(sql_type = Timestamptz)]
    updated_at: DateTime<Utc>,
}

#[derive(QueryableByName)]
struct PlaylistTrackRow {
    #[diesel(sql_type = Text)]
    track_id: String,
}

impl From<PlaylistRow> for Playlist {
    fn from(row: PlaylistRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            is_favorites: row.is_favorites,
            source_url: row.source_url,
            track_count: row.track_count,
            preview_track_ids: row.preview_track_ids,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

pub async fn list_playlists(pool: &DbPool, telegram_id: i64) -> Result<Vec<Playlist>, DbError> {
    let mut connection = pool.connection().await?;
    ensure_favorites(&mut connection, telegram_id).await?;
    let rows = sql_query(playlist_select(
        "WHERE p.telegram_id = $1 AND p.is_favorites = FALSE ORDER BY p.updated_at DESC",
    ))
    .bind::<BigInt, _>(telegram_id)
    .load::<PlaylistRow>(&mut *connection)
    .await?;
    Ok(rows.into_iter().map(Playlist::from).collect())
}

pub async fn create_playlist(
    pool: &DbPool,
    telegram_id: i64,
    name: &str,
) -> Result<Playlist, DbError> {
    let name = normalize_name(name)?;
    let mut connection = pool.connection().await?;
    sql_query("BEGIN").execute(&mut *connection).await?;
    let result = async {
        lock_user_playlists(&mut connection, telegram_id).await?;
        let count = custom_playlist_count(&mut connection, telegram_id).await?;
        if count >= MAX_CUSTOM_PLAYLISTS {
            return Err(DbError::Validation(format!(
                "You can create up to {MAX_CUSTOM_PLAYLISTS} playlists"
            )));
        }
        let id = cuid2::create_id();
        sql_query("INSERT INTO user_playlists (id, telegram_id, name) VALUES ($1, $2, $3)")
            .bind::<Text, _>(&id)
            .bind::<BigInt, _>(telegram_id)
            .bind::<Text, _>(&name)
            .execute(&mut *connection)
            .await?;
        Ok(find_playlist(&mut connection, telegram_id, &id).await?)
    }
    .await;
    finish_transaction(&mut connection, result).await
}

pub async fn import_playlist(
    pool: &DbPool,
    telegram_id: i64,
    name: &str,
    source_url: &str,
    track_ids: &[String],
) -> Result<Playlist, DbError> {
    let name = normalize_name(name)?;
    validate_track_ids(track_ids)?;
    let source_url = source_url.trim();
    if source_url.is_empty() || source_url.len() > 2_048 {
        return Err(DbError::Validation("Invalid playlist source URL".into()));
    }

    let mut connection = pool.connection().await?;
    sql_query("BEGIN").execute(&mut *connection).await?;
    let result = async {
        lock_user_playlists(&mut connection, telegram_id).await?;
        let existing = sql_query("SELECT id FROM user_playlists WHERE telegram_id = $1 AND source_url = $2")
            .bind::<BigInt, _>(telegram_id)
            .bind::<Text, _>(source_url)
            .get_result::<PlaylistIdRow>(&mut *connection)
            .await
            .optional()?;
        let id = if let Some(existing) = existing {
            sql_query("UPDATE user_playlists SET name = $1, updated_at = now() WHERE id = $2")
                .bind::<Text, _>(&name)
                .bind::<Text, _>(&existing.id)
                .execute(&mut *connection)
                .await?;
            existing.id
        } else {
            let count = custom_playlist_count(&mut connection, telegram_id).await?;
            if count >= MAX_CUSTOM_PLAYLISTS {
                return Err(DbError::Validation(format!(
                    "You can create up to {MAX_CUSTOM_PLAYLISTS} playlists"
                )));
            }
            let id = cuid2::create_id();
            sql_query("INSERT INTO user_playlists (id, telegram_id, name, source_url) VALUES ($1, $2, $3, $4)")
                .bind::<Text, _>(&id)
                .bind::<BigInt, _>(telegram_id)
                .bind::<Text, _>(&name)
                .bind::<Text, _>(source_url)
                .execute(&mut *connection)
                .await?;
            id
        };
        replace_tracks_for_playlist(&mut connection, &id, track_ids).await?;
        find_playlist(&mut connection, telegram_id, &id).await
    }
    .await;
    finish_transaction(&mut connection, result).await
}

pub async fn get_playlist(
    pool: &DbPool,
    telegram_id: i64,
    id: &str,
) -> Result<PlaylistDetail, DbError> {
    let mut connection = pool.connection().await?;
    let playlist = find_playlist(&mut connection, telegram_id, id).await?;
    let track_ids = sql_query(
        "SELECT track_id FROM user_playlist_tracks WHERE playlist_id = $1 ORDER BY position",
    )
    .bind::<Text, _>(id)
    .load::<PlaylistTrackRow>(&mut *connection)
    .await?
    .into_iter()
    .map(|row| row.track_id)
    .collect();
    Ok(PlaylistDetail {
        playlist,
        track_ids,
    })
}

pub async fn rename_playlist(
    pool: &DbPool,
    telegram_id: i64,
    id: &str,
    name: &str,
) -> Result<Playlist, DbError> {
    let name = normalize_name(name)?;
    let mut connection = pool.connection().await?;
    let updated = sql_query("UPDATE user_playlists SET name = $1, updated_at = now() WHERE telegram_id = $2 AND id = $3 AND is_favorites = FALSE")
        .bind::<Text, _>(&name)
        .bind::<BigInt, _>(telegram_id)
        .bind::<Text, _>(id)
        .execute(&mut *connection)
        .await?;
    if updated == 0 {
        return Err(playlist_not_found());
    }
    find_playlist(&mut connection, telegram_id, id).await
}

pub async fn delete_playlist(pool: &DbPool, telegram_id: i64, id: &str) -> Result<(), DbError> {
    let mut connection = pool.connection().await?;
    let deleted = sql_query(
        "DELETE FROM user_playlists WHERE telegram_id = $1 AND id = $2 AND is_favorites = FALSE",
    )
    .bind::<BigInt, _>(telegram_id)
    .bind::<Text, _>(id)
    .execute(&mut *connection)
    .await?;
    if deleted == 0 {
        return Err(playlist_not_found());
    }
    Ok(())
}

pub async fn replace_playlist_tracks(
    pool: &DbPool,
    telegram_id: i64,
    id: &str,
    track_ids: &[String],
) -> Result<PlaylistDetail, DbError> {
    validate_track_ids(track_ids)?;
    let mut connection = pool.connection().await?;
    sql_query("BEGIN").execute(&mut *connection).await?;
    let result = async {
        find_playlist(&mut connection, telegram_id, id).await?;
        replace_tracks_for_playlist(&mut connection, id, track_ids).await?;
        get_playlist_on(&mut connection, telegram_id, id).await
    }
    .await;
    finish_transaction(&mut connection, result).await
}

pub async fn remove_playlist_track(
    pool: &DbPool,
    telegram_id: i64,
    id: &str,
    track_id: &str,
) -> Result<(), DbError> {
    let mut connection = pool.connection().await?;
    let removed = sql_query("DELETE FROM user_playlist_tracks t USING user_playlists p WHERE t.playlist_id = p.id AND p.telegram_id = $1 AND p.id = $2 AND t.track_id = $3")
        .bind::<BigInt, _>(telegram_id)
        .bind::<Text, _>(id)
        .bind::<Text, _>(track_id)
        .execute(&mut *connection)
        .await?;
    if removed == 0 {
        return Err(DbError::NotFound("Playlist track not found".into()));
    }
    sql_query("UPDATE user_playlists SET updated_at = now() WHERE telegram_id = $1 AND id = $2")
        .bind::<BigInt, _>(telegram_id)
        .bind::<Text, _>(id)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn ensure_favorites(
    connection: &mut AsyncPgConnection,
    telegram_id: i64,
) -> Result<(), DbError> {
    let id = format!("favorites-{telegram_id}");
    sql_query("INSERT INTO user_playlists (id, telegram_id, name, is_favorites) VALUES ($1, $2, 'Favorites', TRUE) ON CONFLICT (telegram_id) WHERE is_favorites DO NOTHING")
        .bind::<Text, _>(&id)
        .bind::<BigInt, _>(telegram_id)
        .execute(connection)
        .await?;
    Ok(())
}

async fn lock_user_playlists(
    connection: &mut AsyncPgConnection,
    telegram_id: i64,
) -> Result<(), DbError> {
    sql_query("SELECT 1::BIGINT AS count FROM (SELECT pg_advisory_xact_lock($1)) AS user_lock")
        .bind::<BigInt, _>(telegram_id)
        .get_result::<CountRow>(connection)
        .await?;
    Ok(())
}

async fn custom_playlist_count(
    connection: &mut AsyncPgConnection,
    telegram_id: i64,
) -> Result<i64, DbError> {
    Ok(sql_query("SELECT COUNT(*) AS count FROM user_playlists WHERE telegram_id = $1 AND is_favorites = FALSE")
        .bind::<BigInt, _>(telegram_id)
        .get_result::<CountRow>(connection)
        .await?
        .count)
}

async fn replace_tracks_for_playlist(
    connection: &mut AsyncPgConnection,
    playlist_id: &str,
    track_ids: &[String],
) -> Result<(), DbError> {
    sql_query("DELETE FROM user_playlist_tracks WHERE playlist_id = $1")
        .bind::<Text, _>(playlist_id)
        .execute(&mut *connection)
        .await?;
    if !track_ids.is_empty() {
        let track_ids = track_ids
            .iter()
            .map(|track_id| track_id.trim().to_owned())
            .collect::<Vec<_>>();
        sql_query("INSERT INTO user_playlist_tracks (playlist_id, position, track_id) SELECT $1, (tracks.ordinality - 1)::INTEGER, tracks.track_id FROM UNNEST($2::TEXT[]) WITH ORDINALITY AS tracks(track_id, ordinality)")
            .bind::<Text, _>(playlist_id)
            .bind::<Array<Text>, _>(&track_ids)
            .execute(&mut *connection)
            .await?;
    }
    sql_query("UPDATE user_playlists SET updated_at = now() WHERE id = $1")
        .bind::<Text, _>(playlist_id)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn find_playlist(
    connection: &mut AsyncPgConnection,
    telegram_id: i64,
    id: &str,
) -> Result<Playlist, DbError> {
    sql_query(playlist_select("WHERE p.telegram_id = $1 AND p.id = $2"))
        .bind::<BigInt, _>(telegram_id)
        .bind::<Text, _>(id)
        .get_result::<PlaylistRow>(connection)
        .await
        .map(Playlist::from)
        .map_err(|error| match error {
            diesel::result::Error::NotFound => playlist_not_found(),
            other => DbError::Database(other),
        })
}

async fn get_playlist_on(
    connection: &mut AsyncPgConnection,
    telegram_id: i64,
    id: &str,
) -> Result<PlaylistDetail, DbError> {
    let playlist = find_playlist(connection, telegram_id, id).await?;
    let track_ids = sql_query(
        "SELECT track_id FROM user_playlist_tracks WHERE playlist_id = $1 ORDER BY position",
    )
    .bind::<Text, _>(id)
    .load::<PlaylistTrackRow>(connection)
    .await?
    .into_iter()
    .map(|row| row.track_id)
    .collect();
    Ok(PlaylistDetail {
        playlist,
        track_ids,
    })
}

async fn finish_transaction<T>(
    connection: &mut AsyncPgConnection,
    result: Result<T, DbError>,
) -> Result<T, DbError> {
    match result {
        Ok(value) => {
            sql_query("COMMIT").execute(connection).await?;
            Ok(value)
        }
        Err(error) => {
            let _ = sql_query("ROLLBACK").execute(connection).await;
            Err(error)
        }
    }
}

fn playlist_select(where_clause: &str) -> String {
    format!(
        "SELECT p.id, p.name, p.is_favorites, p.source_url, (SELECT COUNT(*) FROM user_playlist_tracks t WHERE t.playlist_id = p.id) AS track_count, ARRAY(SELECT t.track_id FROM user_playlist_tracks t WHERE t.playlist_id = p.id ORDER BY t.position LIMIT 4) AS preview_track_ids, p.created_at, p.updated_at FROM user_playlists p {where_clause}"
    )
}

fn normalize_name(name: &str) -> Result<String, DbError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(DbError::Validation(
            "Playlist name must be 1–80 characters".into(),
        ));
    }
    Ok(name.to_owned())
}

fn validate_track_ids(track_ids: &[String]) -> Result<(), DbError> {
    if track_ids.len() > MAX_PLAYLIST_TRACKS {
        return Err(DbError::Validation(format!(
            "Playlist cannot contain more than {MAX_PLAYLIST_TRACKS} tracks"
        )));
    }
    if track_ids.iter().any(|track_id| {
        let track_id = track_id.trim();
        track_id.is_empty()
            || track_id.len() > 128
            || !track_id.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return Err(DbError::Validation(
            "Every playlist track needs a numeric Apple catalog ID".into(),
        ));
    }
    Ok(())
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

fn playlist_not_found() -> DbError {
    DbError::NotFound("Playlist not found".into())
}

#[cfg(test)]
mod tests {
    use super::validate_track_ids;

    #[test]
    fn accepts_apple_catalog_id_lists() {
        assert!(validate_track_ids(&["1559885421".into(), "1440643709".into()]).is_ok());
    }

    #[test]
    fn rejects_non_apple_track_identifiers() {
        assert!(validate_track_ids(&["mbid-123".into()]).is_err());
        assert!(validate_track_ids(&[String::new()]).is_err());
    }
}

#[derive(QueryableByName)]
struct PlaylistIdRow {
    #[diesel(sql_type = Text)]
    id: String,
}
