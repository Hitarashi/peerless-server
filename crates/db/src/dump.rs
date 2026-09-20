//! Versioned, typed database archive export/import.
//!
//! The wire format is JSON compressed with gzip.  JSON's explicit null and
//! string escaping rules make this safe for names, error messages, and other
//! values containing newlines or SQL punctuation; no SQL is generated or
//! parsed during restore.

use std::{io::Write, time::Instant};

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use engine::limits::MAX_DOCUMENT_BYTES;
use flate2::{Compression, write::GzEncoder};
use music::Provider;
use serde::{Deserialize, Serialize};

use crate::{
    Album, DbError, DbPool, Request, SettingsRow, Track, User,
    schema::{albums, requests, settings, tracks, users},
};

const ARCHIVE_VERSION: u32 = 2;
const MAX_ARCHIVE_ROWS: usize = 1_000_000;
const MAX_ARCHIVE_STRING_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpStats {
    pub users_count: i64,
    pub tracks_count: i64,
    pub requests_count: i64,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreStats {
    pub users_merged: u64,
    pub tracks_merged: u64,
    pub requests_merged: u64,
    pub duration_ms: u128,
}

#[derive(Debug, Serialize, Deserialize)]
struct Archive {
    format_version: u32,
    generated_at: String,
    /// The Telegram dump channel this archive belongs to.  Unbound archives
    /// are retained for the low-level API, but the bot always exports and
    /// imports channel-bound archives.
    #[serde(default)]
    dump_channel_id: Option<i64>,
    users: Vec<UserArchive>,
    tracks: Vec<TrackArchive>,
    requests: Vec<RequestArchive>,
    #[serde(default, alias = "album_zips")]
    albums: Vec<AlbumArchive>,
    settings: SettingsArchive,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = users)]
struct UserArchive {
    telegram_id: i64,
    name: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = tracks)]
struct TrackArchive {
    provider: Provider,
    track_id: String,
    message_id: i32,
    file_id: String,
    file_unique_id: String,
    title: String,
    artist: String,
    album: String,
    duration: i32,
    bit_depth: i32,
    sample_rate: i32,
    genre: String,
    release_date: String,
    track_number: i32,
    track_count: i32,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = requests)]
struct RequestArchive {
    telegram_id: i64,
    chat_id: i64,
    provider: Provider,
    track_id: String,
    is_cache_hit: bool,
    duration_ms: Option<i32>,
    status: String,
    error_reason: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = albums)]
struct AlbumArchive {
    provider: Provider,
    album_id: String,
    part_index: i32,
    total_parts: i32,
    message_id: i32,
    file_id: String,
    file_unique_id: String,
    file_size: i64,
    file_name: String,
    /// Empty string = unknown generation; never reused, forces one rebuild.
    #[serde(default)]
    generation_hash: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum SettingsArchive {
    Modern {
        data: serde_json::Value,
        updated_at: chrono::DateTime<chrono::Utc>,
    },
    Legacy {
        ripping_mode: String,
        album_rip_enabled: bool,
        playlist_rip_enabled: bool,
        artist_rip_enabled: bool,
        txt_rip_enabled: bool,
        multi_link_rip_enabled: bool,
        max_collection_tracks: i32,
        auto_dump_enabled: bool,
        auto_dump_storefronts: Vec<String>,
        updated_at: chrono::DateTime<chrono::Utc>,
    },
}

impl From<User> for UserArchive {
    fn from(row: User) -> Self {
        Self {
            telegram_id: row.telegram_id,
            name: row.name,
            created_at: row.created_at,
        }
    }
}

impl From<Track> for TrackArchive {
    fn from(row: Track) -> Self {
        Self {
            provider: row.provider,
            track_id: row.track_id,
            message_id: row.message_id,
            file_id: row.file_id,
            file_unique_id: row.file_unique_id,
            title: row.title,
            artist: row.artist,
            album: row.album,
            duration: row.duration,
            bit_depth: row.bit_depth,
            sample_rate: row.sample_rate,
            genre: row.genre,
            release_date: row.release_date,
            track_number: row.track_number,
            track_count: row.track_count,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

impl From<Request> for RequestArchive {
    fn from(row: Request) -> Self {
        Self {
            telegram_id: row.telegram_id,
            chat_id: row.chat_id,
            provider: row.provider,
            track_id: row.track_id,
            is_cache_hit: row.is_cache_hit,
            duration_ms: row.duration_ms,
            status: row.status,
            error_reason: row.error_reason,
            created_at: row.created_at,
        }
    }
}

impl From<Album> for AlbumArchive {
    fn from(row: Album) -> Self {
        Self {
            provider: row.provider,
            album_id: row.album_id,
            part_index: row.part_index,
            total_parts: row.total_parts,
            message_id: row.message_id,
            file_id: row.file_id,
            file_unique_id: row.file_unique_id,
            file_size: row.file_size,
            file_name: row.file_name,
            generation_hash: row.generation_hash,
            created_at: row.created_at,
        }
    }
}

impl From<SettingsRow> for SettingsArchive {
    fn from(row: SettingsRow) -> Self {
        Self::Modern {
            data: row.data,
            updated_at: row.updated_at,
        }
    }
}

/// Backup and restore operations.
pub struct DbDumpService {
    pool: DbPool,
}

impl DbDumpService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Export the entire database as a gzip-compressed JSON archive.
    pub async fn export_dump(&self) -> Result<(Vec<u8>, DumpStats, String), DbError> {
        self.export_dump_with_channel(None).await
    }

    pub async fn export_dump_for_channel(
        &self,
        dump_channel_id: i64,
    ) -> Result<(Vec<u8>, DumpStats, String), DbError> {
        self.export_dump_with_channel(Some(dump_channel_id)).await
    }

    async fn export_dump_with_channel(
        &self,
        dump_channel_id: Option<i64>,
    ) -> Result<(Vec<u8>, DumpStats, String), DbError> {
        let started = Instant::now();
        let mut connection = self.pool.connection().await?;
        let users = users::table
            .select(User::as_select())
            .load::<User>(&mut *connection)
            .await?;
        let tracks = tracks::table
            .select(Track::as_select())
            .load::<Track>(&mut *connection)
            .await?;
        let requests = requests::table
            .select(Request::as_select())
            .load::<Request>(&mut *connection)
            .await?;
        let albums = albums::table
            .select(Album::as_select())
            .load::<Album>(&mut *connection)
            .await?;
        let settings = settings::table
            .select(SettingsRow::as_select())
            .first::<SettingsRow>(&mut *connection)
            .await?;
        let archive = Archive {
            format_version: ARCHIVE_VERSION,
            generated_at: chrono::Utc::now().to_rfc3339(),
            dump_channel_id,
            users: users.into_iter().map(UserArchive::from).collect(),
            tracks: tracks.into_iter().map(TrackArchive::from).collect(),
            requests: requests.into_iter().map(RequestArchive::from).collect(),
            albums: albums.into_iter().map(AlbumArchive::from).collect(),
            settings: SettingsArchive::from(settings),
        };
        let json = serde_json::to_vec(&archive)
            .map_err(|error| DbError::Row(format!("archive encode failed: {error}")))?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&json)
            .map_err(|error| DbError::Row(format!("gzip encode failed: {error}")))?;
        let compressed = encoder
            .finish()
            .map_err(|error| DbError::Row(format!("gzip finish failed: {error}")))?;
        let filename = format!(
            "alac_dump_{}.json.gz",
            chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S")
        );
        let stats = DumpStats {
            users_count: archive.users.len() as i64,
            tracks_count: archive.tracks.len() as i64,
            requests_count: archive.requests.len() as i64,
            bytes: compressed.len(),
        };
        tracing::info!(
            users = stats.users_count,
            tracks = stats.tracks_count,
            requests = stats.requests_count,
            elapsed_ms = started.elapsed().as_millis(),
            "database archive exported"
        );
        Ok((compressed, stats, filename))
    }

    /// Restore a typed archive inside one transaction. Every row is decoded
    /// before the transaction starts, so malformed input cannot partially
    /// modify the database.
    pub async fn import_dump(&self, gzip_bytes: &[u8]) -> Result<RestoreStats, DbError> {
        self.import_dump_with_channel(gzip_bytes, None).await
    }

    pub async fn import_dump_for_channel(
        &self,
        gzip_bytes: &[u8],
        dump_channel_id: i64,
    ) -> Result<RestoreStats, DbError> {
        self.import_dump_with_channel(gzip_bytes, Some(dump_channel_id))
            .await
    }

    async fn import_dump_with_channel(
        &self,
        gzip_bytes: &[u8],
        expected_channel_id: Option<i64>,
    ) -> Result<RestoreStats, DbError> {
        let started = Instant::now();
        if gzip_bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            return Err(DbError::Row(
                "database archive exceeds size limit".to_owned(),
            ));
        }
        let archive: Archive = serde_json::from_slice(&gunzip(gzip_bytes)?)
            .map_err(|error| DbError::Row(format!("invalid archive: {error}")))?;
        if archive.format_version != ARCHIVE_VERSION {
            return Err(DbError::Row(format!(
                "unsupported archive version {}",
                archive.format_version
            )));
        }
        if let Some(expected) = expected_channel_id
            && archive.dump_channel_id != Some(expected)
        {
            return Err(DbError::Row(
                "database archive belongs to a different dump channel".to_owned(),
            ));
        }
        validate_archive_limits(&archive)?;
        let users_merged = archive.users.len() as u64;
        let tracks_merged = archive.tracks.len() as u64;
        let requests_merged = archive.requests.len() as u64;
        let mut connection = self.pool.connection().await?;
        connection
            .build_transaction()
            .run(async |transaction| {
                diesel::delete(requests::table)
                    .execute(&mut *transaction)
                    .await?;
                diesel::delete(tracks::table)
                    .execute(&mut *transaction)
                    .await?;
                diesel::delete(albums::table)
                    .execute(&mut *transaction)
                    .await?;
                diesel::delete(users::table)
                    .execute(&mut *transaction)
                    .await?;

                for row in archive.users {
                    diesel::insert_into(users::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                for row in archive.tracks {
                    diesel::insert_into(tracks::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                for row in archive.albums {
                    diesel::insert_into(albums::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                for row in archive.requests {
                    diesel::insert_into(requests::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                let (data, updated_at) = match archive.settings {
                    SettingsArchive::Modern { data, updated_at } => (data, updated_at),
                    SettingsArchive::Legacy {
                        ripping_mode,
                        album_rip_enabled,
                        playlist_rip_enabled,
                        artist_rip_enabled,
                        txt_rip_enabled,
                        multi_link_rip_enabled,
                        max_collection_tracks,
                        auto_dump_enabled,
                        auto_dump_storefronts,
                        updated_at,
                    } => {
                        let json_val = serde_json::json!({
                            "ripping_mode": ripping_mode,
                            "album_rip_enabled": album_rip_enabled,
                            "playlist_rip_enabled": playlist_rip_enabled,
                            "artist_rip_enabled": artist_rip_enabled,
                            "txt_rip_enabled": txt_rip_enabled,
                            "multi_link_rip_enabled": multi_link_rip_enabled,
                            "max_collection_tracks": max_collection_tracks,
                            "auto_dump_enabled": auto_dump_enabled,
                            "auto_dump_storefronts": auto_dump_storefronts,
                            "apple_rip_enabled": true,
                            "qobuz_rip_enabled": true,
                        });
                        (json_val, updated_at)
                    }
                };
                diesel::update(settings::table.filter(settings::id.eq(1_i16)))
                    .set((settings::data.eq(data), settings::updated_at.eq(updated_at)))
                    .execute(&mut *transaction)
                    .await?;
                Ok::<(), DbError>(())
            })
            .await?;

        tracing::info!(
            users = users_merged,
            tracks = tracks_merged,
            requests = requests_merged,
            elapsed_ms = started.elapsed().as_millis(),
            "database archive restored"
        );
        Ok(RestoreStats {
            users_merged,
            tracks_merged,
            requests_merged,
            duration_ms: started.elapsed().as_millis(),
        })
    }
}

fn gunzip(bytes: &[u8]) -> Result<Vec<u8>, DbError> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|error| DbError::Row(format!("gzip decode failed: {error}")))?;
    Ok(out)
}

fn validate_archive_limits(archive: &Archive) -> Result<(), DbError> {
    let row_count = archive.users.len()
        + archive.tracks.len()
        + archive.requests.len()
        + archive.albums.len()
        + 1;
    if row_count > MAX_ARCHIVE_ROWS {
        return Err(DbError::Row("archive exceeds maximum row limit".to_owned()));
    }
    for track in &archive.tracks {
        if track.title.len() > MAX_ARCHIVE_STRING_BYTES
            || track.artist.len() > MAX_ARCHIVE_STRING_BYTES
            || track.album.len() > MAX_ARCHIVE_STRING_BYTES
            || track.genre.len() > MAX_ARCHIVE_STRING_BYTES
        {
            return Err(DbError::Row(
                "archive contains string field exceeding limit".to_owned(),
            ));
        }
    }
    Ok(())
}
